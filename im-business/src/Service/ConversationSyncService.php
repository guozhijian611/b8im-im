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
     * @return array{conversation_id: string, last_read_message_id: string, user_id: string, time: string}
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
        $this->ensureMessageVisibleToUser($context, $conversationId, $lastReadMessageId);

        $now = date('Y-m-d H:i:s');
        $affected = $this->repository->execute(
            'UPDATE im_conversation_member
                SET unread_count = 0, last_read_message_id = ?, update_time = ?
              WHERE organization = ? AND conversation_id = ? AND user_id = ? AND delete_time IS NULL',
            [$lastReadMessageId, $now, $context->organization, $conversationId, $context->userId],
        );
        if ($affected <= 0) {
            throw new ImException('会话不存在或无权访问', 'CONVERSATION_READ_MEMBER_NOT_FOUND');
        }

        $result = [
            'conversation_id' => $conversationId,
            'last_read_message_id' => $lastReadMessageId,
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

    private function ensureMessageVisibleToUser(AuthContext $context, string $conversationId, string $messageId): void
    {
        $index = $this->repository->fetchOne(
            'SELECT shard_table FROM im_message_index
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
                AND NOT EXISTS (
                    SELECT 1 FROM im_message_user_delete ud
                     WHERE ud.organization = m.organization
                       AND ud.conversation_id = m.conversation_id
                       AND ud.message_id = m.message_id
                       AND ud.user_id = ?
                )
              LIMIT 1',
            [$context->organization, $conversationId, $messageId, $context->userId],
        );
        if ($message === null) {
            throw new ImException('已读消息不存在或当前用户不可见', 'CONVERSATION_READ_MESSAGE_NOT_VISIBLE');
        }
    }
}
