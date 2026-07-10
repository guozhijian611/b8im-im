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
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Protocol\Packet;
use GatewayWorker\Lib\Gateway;

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
    public function __construct(
        private readonly ImRepository $repository,
    ) {
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
        $this->assertActiveMember($context, $conversationId);
        $lastReadSeq = $this->visibleMessageSeq($context, $conversationId, $lastReadMessageId);

        $now = date('Y-m-d H:i:s');
        $readState = $this->repository->transaction(fn (): array => $this->advanceReadState(
            $context,
            $conversationId,
            $lastReadMessageId,
            $lastReadSeq,
            $now,
        ));

        $result = [
            'conversation_id' => $conversationId,
            'last_read_message_id' => $readState['last_read_message_id'],
            'last_read_seq' => $readState['last_read_seq'],
            'unread_count' => $readState['unread_count'],
            'user_id' => $context->userId,
            'time' => $now,
        ];

        $payload = Packet::make(Command::CONVERSATION_READ, $result, $context->organization)->encode();

        // 1. 自己其他设备：多端已读位置同步
        $ownClientIds = Gateway::getClientIdByUid($context->uid());
        foreach ($ownClientIds as $cid) {
            if ($cid !== $clientId) {
                Gateway::sendToClient($cid, $payload);
            }
        }

        // 2. 会话其他成员：已读回执（对方感知"已读到哪"）
        foreach ($this->conversationMembers($context->organization, $conversationId) as $userId) {
            if ($userId === $context->userId) {
                continue;
            }
            Gateway::sendToUid(AuthContext::uidFor($context->organization, $userId), $payload);
        }

        return $result;
    }

    /**
     * 查询会话中的活跃成员 user_id 列表。
     *
     * @return list<string>
     */
    private function conversationMembers(int $organization, string $conversationId): array
    {
        $rows = $this->repository->fetchAll(
            'SELECT user_id FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ? AND status = 1 AND delete_time IS NULL',
            [$organization, $conversationId],
        );

        return array_values(array_map(static fn (array $row): string => (string) $row['user_id'], $rows));
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
                       AND ud.user_id = ?
                )
              LIMIT 1',
            [$context->organization, $conversationId, $messageId, $context->userId, $context->userId],
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
                AND cm.user_id = ?
                AND cm.status = 1
                AND cm.delete_time IS NULL
              LIMIT 1',
            [$context->organization, $conversationId, $context->userId],
        );
        if ($member === null) {
            throw new ImException('会话不存在或无权访问', 'CONVERSATION_READ_MEMBER_NOT_FOUND');
        }
    }

    /** @return array{last_read_message_id: string, last_read_seq: int, unread_count: int} */
    private function advanceReadState(
        AuthContext $context,
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
                AND cm.user_id = ?
                AND cm.status = 1
                AND cm.delete_time IS NULL
              LIMIT 1 FOR UPDATE',
            [$context->organization, $conversationId, $context->userId],
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
            $context->organization,
            $conversationId,
            $context->userId,
            $effectiveSeq,
        );
        $this->repository->execute(
            'UPDATE im_conversation_member
                SET last_read_message_id = ?, last_read_seq = ?, unread_count = ?, update_time = ?
              WHERE organization = ?
                AND conversation_id = ?
                AND user_id = ?
                AND status = 1
                AND delete_time IS NULL',
            [
                $effectiveMessageId,
                $effectiveSeq,
                $unreadCount,
                $now,
                $context->organization,
                $conversationId,
                $context->userId,
            ],
        );

        return [
            'last_read_message_id' => $effectiveMessageId,
            'last_read_seq' => $effectiveSeq,
            'unread_count' => $unreadCount,
        ];
    }

    private function countUnreadAfter(
        int $organization,
        string $conversationId,
        string $userId,
        int $afterSeq,
    ): int {
        $row = $this->repository->fetchOne(
            'SELECT COUNT(*) AS aggregate
               FROM im_message_index i
              WHERE i.organization = ?
                AND i.conversation_id = ?
                AND i.message_seq > ?
                AND i.sender_id <> ?
                AND EXISTS (
                    SELECT 1 FROM im_conversation_membership_period mp
                     WHERE mp.organization = i.organization
                       AND mp.conversation_id = i.conversation_id
                       AND mp.user_id = ?
                       AND mp.status = 1
                       AND i.message_seq >= mp.visible_from_message_seq
                       AND (mp.visible_until_message_seq IS NULL OR i.message_seq <= mp.visible_until_message_seq)
                )
                AND NOT EXISTS (
                    SELECT 1 FROM im_message_user_delete ud
                     WHERE ud.organization = i.organization
                       AND ud.message_id = i.message_id
                       AND ud.user_id = ?
                )
                AND NOT EXISTS (
                    SELECT 1 FROM im_message_change mc
                     WHERE mc.organization = i.organization
                       AND mc.message_id = i.message_id
                       AND mc.change_type = \'delete_both\'
                )',
            [$organization, $conversationId, $afterSeq, $userId, $userId, $userId],
        );

        return max(0, (int) ($row['aggregate'] ?? 0));
    }
}
