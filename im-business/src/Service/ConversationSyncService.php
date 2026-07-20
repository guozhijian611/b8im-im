<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 会话读状态同步
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;

/**
 * 会话读状态同步
 *
 * 会话级已读（区别于 ack 的单条消息回执）：
 * 一次把某会话的已读位置推进到某条消息，清零未读数，并做两件事——
 *   1. 推给自己其他设备：多端已读位置同步
 *   2. 推给会话其他成员：对方感知"已读到哪"（已读回执）
 */
final class ConversationSyncService
{
    private readonly CrossOrganizationConversationAccess $conversationAccess;

    public function __construct(
        private readonly ImRepository $repository,
        private readonly OutboxService $outbox,
        ?CrossOrganizationConversationAccess $conversationAccess = null,
    ) {
        $this->conversationAccess = $conversationAccess ?? new CrossOrganizationConversationAccess(
            $repository,
            new CrossOrganizationSocialPolicy($repository),
        );
    }

    /**
     * 标记会话已读并同步。
     *
     * @param AuthContext $context           当前鉴权上下文
     * @param string      $clientId          当前客户端连接 ID
     * @param string      $conversationId    会话 ID
     * @param string      $lastReadMessageId 已读到的最后一条 message_id
     *
     * @return array{conversation_id: string, last_read_message_id: string, last_read_seq: int, user_id: string, time: string}
     */
    public function markRead(AuthContext $context, string $clientId, string $conversationId, string $lastReadMessageId): array
    {
        $conversationId = trim($conversationId);
        if ($conversationId === '') {
            throw new ImException('缺少 conversation_id', 'CONVERSATION_READ_CONVERSATION_ID_EMPTY');
        }
        $lastReadMessageId = trim($lastReadMessageId);
        if ($lastReadMessageId === '') {
            throw new ImException('缺少 last_read_message_id', 'CONVERSATION_READ_MESSAGE_ID_EMPTY');
        }
        $accessPreview = $this->conversationAccess->assertAccessible(
            $context->organization,
            $conversationId,
        );
        $this->assertActiveMember($context, $conversationId);
        $lastReadSeq = $this->visibleMessageSeq($context, $conversationId, $lastReadMessageId);

        $now = date('Y-m-d H:i:s');
        $result = $this->repository->transaction(function () use (
            $context,
            $conversationId,
            $lastReadMessageId,
            $lastReadSeq,
            $now,
            $accessPreview,
        ): array {
            if (count($accessPreview['home_organizations']) === 2) {
                $this->conversationAccess->lockCrossOrganizationWriteBoundary(
                    $accessPreview['home_organizations'],
                );
                $this->conversationAccess->lockHomeTenantPolicies(
                    $accessPreview['home_organizations'],
                );
            }
            $access = $this->conversationAccess->assertAccessible(
                $context->organization,
                $conversationId,
                true,
                $accessPreview['home_organizations'],
                $accessPreview['participant_identities'],
            );
            $homeOrganizations = $access['home_organizations'];
            $homeReadStates = [];
            foreach ($homeOrganizations as $homeOrganization) {
                $homeReadStates[$homeOrganization] = $this->advanceReadState(
                    $homeOrganization,
                    $context->organization,
                    $context->userId,
                    $conversationId,
                    $lastReadMessageId,
                    $lastReadSeq,
                    $now,
                );
            }
            $readState = $homeReadStates[$context->organization]
                ?? throw new ImException('当前home读状态投影缺失', 'CONVERSATION_READ_HOME_MISSING');
            $result = [
                'conversation_id' => $conversationId,
                'last_read_message_id' => $readState['last_read_message_id'],
                'last_read_seq' => $readState['last_read_seq'],
                'unread_count' => $readState['unread_count'],
                'user_organization' => $context->organization,
                'user_id' => $context->userId,
                'time' => $now,
                'cross_org_access_snapshot_id' => $access['access_snapshot_id'],
            ];
            $conversation = $this->repository->fetchOne(
                'SELECT conversation_type FROM im_conversation
                  WHERE organization = ? AND conversation_id = ? AND status = 1
                  LIMIT 1',
                [$context->organization, $conversationId],
            );
            if ($conversation === null) {
                throw new ImException('会话不存在或无权访问', 'CONVERSATION_READ_MEMBER_NOT_FOUND');
            }
            foreach ($homeOrganizations as $homeOrganization) {
                $allRecipientIdentities = $this->conversationMemberIdentities($homeOrganization, $conversationId);
                if ((int) $conversation['conversation_type'] === 2) {
                    foreach ($allRecipientIdentities as $identity) {
                        if ((int) $identity['organization'] !== $homeOrganization) {
                            throw new ImException('群聊不允许包含外机构成员', 'GROUP_MEMBER_ORGANIZATION_FORBIDDEN');
                        }
                    }
                }
                $recipientIdentities = array_values(array_filter(
                    $allRecipientIdentities,
                    static fn (array $identity): bool => (int) $identity['organization'] === $homeOrganization,
                ));
                $this->outbox->createConversationRead(
                    context: $context,
                    homeOrganization: $homeOrganization,
                    readState: [
                        ...$result,
                        'last_read_message_id' => $homeReadStates[$homeOrganization]['last_read_message_id'],
                        'last_read_seq' => $homeReadStates[$homeOrganization]['last_read_seq'],
                        'unread_count' => $homeReadStates[$homeOrganization]['unread_count'],
                    ],
                    conversationType: (int) $conversation['conversation_type'],
                    recipientIdentities: $recipientIdentities,
                    crossOrgAccessSnapshotId: $access['is_cross_organization']
                        ? $access['access_snapshot_id']
                        : null,
                );
            }

            return $result;
        });

        return $result;
    }

    /**
     * 查询会话中的活跃成员 user_id 列表。
     *
     * @return list<array{organization:int,user_id:string}>
     */
    private function conversationMemberIdentities(int $organization, string $conversationId): array
    {
        $rows = $this->repository->fetchAll(
            'SELECT member_organization, user_id FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ? AND status = 1 AND delete_time IS NULL',
            [$organization, $conversationId],
        );

        return array_values(array_map(
            static fn (array $row): array => [
                'organization' => (int) $row['member_organization'],
                'user_id' => (string) $row['user_id'],
            ],
            $rows,
        ));
    }

    private function visibleMessageSeq(AuthContext $context, string $conversationId, string $messageId): int
    {
        $index = $this->repository->fetchOne(
            'SELECT shard_table, message_seq FROM im_message_index
              WHERE organization = ? AND conversation_id = ? AND message_id = ?
              LIMIT 1',
            [$context->organization, $conversationId, $messageId],
        );

        if ($index === null) {
            throw new ImException('已读消息不存在或不属于该会话', 'CONVERSATION_READ_MESSAGE_NOT_FOUND');
        }

        $table = (string) ($index['shard_table'] ?? '');
        if (preg_match('/^im_message_\d{4}_\d{6}$/', $table) !== 1) {
            throw new ImException('消息分片表无效', 'CONVERSATION_READ_MESSAGE_SHARD_INVALID');
        }

        $message = $this->repository->fetchOne(
            'SELECT m.message_id FROM `' . $table . '` m
              WHERE m.organization = ?
                AND m.conversation_id = ?
                AND m.message_id = ?
                AND m.delete_time IS NULL
                AND EXISTS (
                    SELECT 1
                      FROM im_conversation_membership_period mp
                     WHERE mp.organization = m.organization
                       AND mp.conversation_id = m.conversation_id
                       AND mp.member_organization = ?
                       AND mp.user_id = ?
                       AND mp.status = 1
                       AND m.message_seq >= mp.visible_from_message_seq
                       AND (mp.visible_until_message_seq IS NULL OR m.message_seq <= mp.visible_until_message_seq)
                )
                AND NOT EXISTS (
                    SELECT 1 FROM im_message_user_delete ud
                     WHERE ud.organization = m.organization
                       AND ud.conversation_id = m.conversation_id
                       AND ud.message_id = m.message_id
                       AND ud.user_organization = ?
                       AND ud.user_id = ?
                )
              LIMIT 1',
            [
                $context->organization,
                $conversationId,
                $messageId,
                $context->organization,
                $context->userId,
                $context->organization,
                $context->userId,
            ],
        );
        if ($message === null) {
            throw new ImException('已读消息不存在或当前用户不可见', 'CONVERSATION_READ_MESSAGE_NOT_VISIBLE');
        }

        return (int) $index['message_seq'];
    }

    private function assertActiveMember(AuthContext $context, string $conversationId): void
    {
        $member = $this->repository->fetchOne(
            'SELECT cm.id FROM im_conversation_member cm
              INNER JOIN im_conversation c
                 ON c.organization = cm.organization
                AND c.conversation_id = cm.conversation_id
                AND c.status = 1
                AND c.delete_time IS NULL
              WHERE cm.organization = ?
                AND cm.conversation_id = ?
                AND cm.member_organization = ?
                AND cm.user_id = ?
                AND cm.status = 1
                AND cm.delete_time IS NULL
              LIMIT 1',
            [$context->organization, $conversationId, $context->organization, $context->userId],
        );
        if ($member === null) {
            throw new ImException('会话不存在或无权访问', 'CONVERSATION_READ_MEMBER_NOT_FOUND');
        }
    }

    /** @return array{last_read_message_id: string, last_read_seq: int, unread_count: int} */
    private function advanceReadState(
        int $homeOrganization,
        int $userOrganization,
        string $userId,
        string $conversationId,
        string $requestedMessageId,
        int $requestedSeq,
        string $now,
    ): array {
        $member = $this->repository->fetchOne(
            'SELECT cm.last_read_message_id, cm.last_read_seq
               FROM im_conversation_member cm
               INNER JOIN im_conversation c
                  ON c.organization = cm.organization
                 AND c.conversation_id = cm.conversation_id
                 AND c.status = 1
                 AND c.delete_time IS NULL
              WHERE cm.organization = ?
                AND cm.conversation_id = ?
                AND cm.member_organization = ?
                AND cm.user_id = ?
                AND cm.status = 1
                AND cm.delete_time IS NULL
              LIMIT 1 FOR UPDATE',
            [$homeOrganization, $conversationId, $userOrganization, $userId],
        );
        if ($member === null) {
            throw new ImException('会话不存在或无权访问', 'CONVERSATION_READ_MEMBER_NOT_FOUND');
        }

        $currentSeq = max(0, (int) ($member['last_read_seq'] ?? 0));
        $effectiveSeq = max($currentSeq, $requestedSeq);
        $effectiveMessageId = $requestedSeq > $currentSeq
            ? $requestedMessageId
            : (string) ($member['last_read_message_id'] ?? '');
        if ($effectiveMessageId === '' && $effectiveSeq === $requestedSeq) {
            $effectiveMessageId = $requestedMessageId;
        }
        $unreadCount = $this->countUnreadAfter(
            $homeOrganization,
            $conversationId,
            $userOrganization,
            $userId,
            $effectiveSeq,
        );
        $this->repository->execute(
            'UPDATE im_conversation_member
                SET last_read_message_id = ?, last_read_seq = ?, unread_count = ?, update_time = ?
              WHERE organization = ?
                AND conversation_id = ?
                AND member_organization = ?
                AND user_id = ?
                AND status = 1
                AND delete_time IS NULL',
            [
                $effectiveMessageId,
                $effectiveSeq,
                $unreadCount,
                $now,
                $homeOrganization,
                $conversationId,
                $userOrganization,
                $userId,
            ],
        );

        return [
            'last_read_message_id' => $effectiveMessageId,
            'last_read_seq' => $effectiveSeq,
            'unread_count' => $unreadCount,
        ];
    }

    private function countUnreadAfter(
        int $homeOrganization,
        string $conversationId,
        int $userOrganization,
        string $userId,
        int $afterSeq,
    ): int {
        $row = $this->repository->fetchOne(
            'SELECT COUNT(*) AS aggregate
               FROM im_message_index i
              WHERE i.organization = ?
                AND i.conversation_id = ?
                AND i.message_seq > ?
                AND NOT (i.sender_organization = ? AND i.sender_id = ?)
                AND EXISTS (
                    SELECT 1 FROM im_conversation_membership_period mp
                     WHERE mp.organization = i.organization
                       AND mp.conversation_id = i.conversation_id
                       AND mp.member_organization = ?
                       AND mp.user_id = ?
                       AND mp.status = 1
                       AND i.message_seq >= mp.visible_from_message_seq
                       AND (mp.visible_until_message_seq IS NULL OR i.message_seq <= mp.visible_until_message_seq)
                )
                AND NOT EXISTS (
                    SELECT 1 FROM im_message_user_delete ud
                     WHERE ud.organization = i.organization
                       AND ud.message_id = i.message_id
                       AND ud.user_organization = ?
                       AND ud.user_id = ?
                )
                AND NOT EXISTS (
                    SELECT 1 FROM im_message_change mc
                     WHERE mc.organization = i.organization
                       AND mc.message_id = i.message_id
                       AND mc.change_type = \'delete_both\'
                )',
            [
                $homeOrganization,
                $conversationId,
                $afterSeq,
                $userOrganization,
                $userId,
                $userOrganization,
                $userId,
                $userOrganization,
                $userId,
            ],
        );

        return max(0, (int) ($row['aggregate'] ?? 0));
    }

}
