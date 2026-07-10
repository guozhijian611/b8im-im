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
    public function __construct(
        private readonly ImRepository $repository,
    ) {
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

        $memberUserIds = $this->conversationMembers($context->organization, $conversationId);
        if (empty($memberUserIds)) {
            return;
        }
        if (!in_array($context->userId, $memberUserIds, true)) {
            throw new ImException('会话不存在或无权访问', 'TYPING_MEMBER_NOT_FOUND');
        }

        $payload = Packet::make(Command::TYPING, [
            'conversation_id' => $conversationId,
            'user_id' => $context->userId,
            'username' => $context->username,
        ], $context->organization)->encode();

        // 推给会话中其他成员
        foreach ($memberUserIds as $userId) {
            if ($userId === $context->userId) {
                continue;
            }
            Gateway::sendToUid(AuthContext::uidFor($context->organization, $userId), $payload);
        }

        // 推给自己的其他设备（多端同步"对方在输入"的状态）
        $clientIds = Gateway::getClientIdByUid($context->uid());
        foreach ($clientIds as $cid) {
            if ($cid !== $clientId) {
                Gateway::sendToClient($cid, $payload);
            }
        }
    }

    /**
     * 查询会话中的活跃成员 user_id 列表。
     *
     * @return list<string>
     */
    private function conversationMembers(int $organization, string $conversationId): array
    {
        $rows = $this->repository->fetchAll(
            'SELECT cm.user_id FROM im_conversation_member cm
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

        return array_values(array_map(static fn (array $row): string => (string) $row['user_id'], $rows));
    }
}
