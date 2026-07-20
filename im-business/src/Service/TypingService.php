<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 正在输入状态中继
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
 * 正在输入状态中继
 *
 * 纯实时中继，无写库，无 ack。
 * 收到客户端 typing 帧 → 查出会话成员 → 推给会话中其他在线用户及自己的其他设备。
 */
final class TypingService
{
    private readonly CrossOrganizationConversationAccess $conversationAccess;

    public function __construct(
        private readonly ImRepository $repository,
        ?CrossOrganizationConversationAccess $conversationAccess = null,
    ) {
        $this->conversationAccess = $conversationAccess ?? new CrossOrganizationConversationAccess(
            $repository,
            new CrossOrganizationSocialPolicy($repository),
        );
    }

    /**
     * 将 typing 状态中继给会话其他成员及自身其他设备。
     *
     * @param AuthContext $context   当前鉴权上下文
     * @param string      $clientId  当前客户端连接 ID
     * @param array       $data      请求 data，需含 conversation_id
     */
    public function relay(AuthContext $context, string $clientId, array $data): void
    {
        $conversationId = trim((string) ($data['conversation_id'] ?? ''));
        if ($conversationId === '') {
            throw new ImException('缺少 conversation_id', 'TYPING_CONVERSATION_ID_EMPTY');
        }
        $accessPreview = $this->conversationAccess->assertAccessible(
            $context->organization,
            $conversationId,
        );

        $this->repository->transaction(function () use (
            $context,
            $clientId,
            $conversationId,
            $accessPreview,
        ): void {
            // Keep the shared access boundary locked until every relay has been
            // handed to Gateway, so a revoke cannot commit in the middle.
            if (count($accessPreview['home_organizations']) === 2) {
                $this->conversationAccess->lockCrossOrganizationWriteBoundary(
                    $accessPreview['home_organizations'],
                );
                $this->conversationAccess->lockHomeTenantPolicies(
                    $accessPreview['home_organizations'],
                );
            }
            $this->conversationAccess->assertAccessible(
                $context->organization,
                $conversationId,
                true,
                $accessPreview['home_organizations'],
                $accessPreview['participant_identities'],
            );

            $memberIdentities = $this->conversationMemberIdentities(
                $context->organization,
                $conversationId,
            );
            if (empty($memberIdentities)) {
                return;
            }
            $isMember = array_filter(
                $memberIdentities,
                static fn (array $identity): bool =>
                    (int) $identity['organization'] === $context->organization
                    && (string) $identity['user_id'] === $context->userId,
            );
            if ($isMember === []) {
                throw new ImException('会话不存在或无权访问', 'TYPING_MEMBER_NOT_FOUND');
            }

            foreach ($memberIdentities as $identity) {
                $memberOrganization = (int) $identity['organization'];
                $userId = (string) $identity['user_id'];
                $payload = Packet::make(Command::TYPING, [
                    'conversation_id' => $conversationId,
                    'actor_organization' => $context->organization,
                    'actor_user_id' => $context->userId,
                    'username' => $context->username,
                ], $memberOrganization)->encode();
                if ($memberOrganization === $context->organization && $userId === $context->userId) {
                    foreach (Gateway::getClientIdByUid($context->uid()) as $cid) {
                        if ($cid !== $clientId) {
                            Gateway::sendToClient($cid, $payload);
                        }
                    }
                    continue;
                }
                Gateway::sendToUid(AuthContext::uidFor($memberOrganization, $userId), $payload);
            }
        });
    }

    /**
     * 查询会话中的活跃成员 user_id 列表。
     *
     * @return list<string>
     */
    private function conversationMemberIdentities(int $organization, string $conversationId): array
    {
        $rows = $this->repository->fetchAll(
            'SELECT cm.member_organization, cm.user_id FROM im_conversation_member cm
              INNER JOIN im_conversation c
                 ON c.organization = cm.organization
                AND c.conversation_id = cm.conversation_id
                AND c.status = 1
                AND c.delete_time IS NULL
              WHERE cm.organization = ?
                AND cm.conversation_id = ?
                AND cm.status = 1
                AND cm.delete_time IS NULL',
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
}
