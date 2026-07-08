<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 消息写库、同步、回执和撤回
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Protocol\MessageType;
use B8im\ImShared\Protocol\Packet;
use PDOException;

final class MessageService
{
    private const MESSAGE_CONFIG_CACHE_TTL = 30;
    private const SYSTEM_NOTIFICATION_USER_ID = 'system_notification';
    private const NOTICE_RECALL = 'recall';
    private const NOTICE_SCREENSHOT = 'screenshot';

    private const CONVERSATION_SINGLE = 1;
    private const CONVERSATION_GROUP = 2;

    private const MESSAGE_NORMAL = 1;
    private const MESSAGE_RECALLED = 2;

    private const RECEIPT_SENT = 1;
    private const RECEIPT_DELIVERED = 2;
    private const RECEIPT_READ = 3;

    private array $messageConfigCache = [];
    private MessageShardRouter $messageShardRouter;

    public function __construct(
        private readonly ImRepository $repository,
        private readonly Config $config,
        private readonly OutboxService $outbox,
    ) {
        $this->messageShardRouter = new MessageShardRouter($repository, $config->messageShardBuckets);
    }

    public function send(AuthContext $context, Packet $packet): array
    {
        $data = $packet->data;
        $clientMsgId = trim((string) ($packet->clientMsgId ?? $data['client_msg_id'] ?? ''));
        if ($clientMsgId === '') {
            throw new ImException('缺少 client_msg_id', 'SEND_CLIENT_MSG_ID_EMPTY');
        }

        $messageType = (int) ($data['message_type'] ?? 0);
        if (!MessageType::isFirstStage($messageType)) {
            throw new ImException('暂不支持该消息类型', 'SEND_MESSAGE_TYPE_INVALID');
        }

        $content = $this->normalizeContent($messageType, $data['content'] ?? []);
        $conversationType = $this->normalizeConversationType($data);

        $duplicated = $this->findMessageByClientMsg($context->organization, $context->userId, $clientMsgId);
        if ($duplicated !== null) {
            return [
                'duplicated' => true,
                'message' => $this->formatMessage($duplicated),
                'recipient_user_ids' => [],
            ];
        }

        $now = $this->now();
        $data = $this->ensureWriteConversationId($context, $conversationType, $data);
        $messageTable = $this->messageShardRouter->writeTable($context->organization, (string) $data['conversation_id'], $now);
        $this->messageShardRouter->ensureIndexTable();
        try {
            return $this->repository->transaction(function () use ($context, $clientMsgId, $messageType, $content, $conversationType, $data, $messageTable, $now): array {
                $conversation = $conversationType === self::CONVERSATION_SINGLE
                    ? $this->ensureSingleConversation($context, $data)
                    : $this->ensureGroupConversation($context, $data);

                $messageSeq = $this->allocateMessageSeq($context->organization, $conversation['conversation_id']);
                $messageId = $this->newMessageId();
                $contentJson = json_encode($content, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
                $summary = $this->summary($messageType, $content);

                $this->repository->execute(
                    'INSERT INTO ' . $this->messageShardRouter->quote($messageTable) . '
                        (organization, conversation_id, conversation_type, message_id, message_seq, client_msg_id, sender_id, message_type, content, status, create_time, update_time)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [
                        $context->organization,
                        $conversation['conversation_id'],
                        $conversationType,
                        $messageId,
                        $messageSeq,
                        $clientMsgId,
                        $context->userId,
                        $messageType,
                        $contentJson,
                        self::MESSAGE_NORMAL,
                        $now,
                        $now,
                    ],
                );

                $messageRow = $this->repository->fetchOne(
                    'SELECT * FROM ' . $this->messageShardRouter->quote($messageTable) . ' WHERE organization = ? AND message_id = ? LIMIT 1',
                    [$context->organization, $messageId],
                );
                if ($messageRow === null) {
                    throw new ImException('消息写入失败', 'SEND_MESSAGE_CREATE_FAILED');
                }
                $messageRow['_message_table'] = $messageTable;

                $this->createMessageIndex($context->organization, $conversation['conversation_id'], $messageId, $messageSeq, $messageTable, $now);

                $recipientUserIds = $conversation['recipient_user_ids'];
                $this->createReceipts($context, $conversation['conversation_id'], $messageId, $recipientUserIds);
                $this->increaseUnread($context->organization, $conversation['conversation_id'], $recipientUserIds);
                $this->repository->execute(
                    'UPDATE im_conversation
                        SET last_message_id = ?, last_message_seq = ?, last_message_time = ?, last_message_summary = ?, update_time = ?
                      WHERE organization = ? AND conversation_id = ?',
                    [$messageId, $messageSeq, $now, $summary, $now, $context->organization, $conversation['conversation_id']],
                );

                $this->outbox->createMessageCreated(
                    $context,
                    $this->formatMessage($messageRow),
                    $recipientUserIds,
                );

                return [
                    'duplicated' => false,
                    'message' => $this->formatMessage($messageRow),
                    'recipient_user_ids' => $recipientUserIds,
                ];
            });
        } catch (PDOException $exception) {
            if (($exception->errorInfo[0] ?? '') === '23000') {
                $message = $this->findMessageByClientMsg($context->organization, $context->userId, $clientMsgId);
                if ($message !== null) {
                    return [
                        'duplicated' => true,
                        'message' => $this->formatMessage($message),
                        'recipient_user_ids' => [],
                    ];
                }
            }

            throw $exception;
        }
    }

    public function ack(AuthContext $context, array $data): array
    {
        $messageId = trim((string) ($data['message_id'] ?? ''));
        if ($messageId === '') {
            throw new ImException('缺少 message_id', 'ACK_MESSAGE_ID_EMPTY');
        }

        $receiptStatus = $this->normalizeReceiptStatus($data['status'] ?? self::RECEIPT_DELIVERED);
        $this->messageShardRouter->ensureIndexTable();
        $message = $this->findVisibleMessage($context, $messageId);
        $now = $this->now();

        $deliveredTime = $receiptStatus >= self::RECEIPT_DELIVERED ? $now : null;
        $readTime = $receiptStatus >= self::RECEIPT_READ ? $now : null;
        $this->repository->execute(
            'INSERT INTO im_message_receipt
                (organization, conversation_id, message_id, user_id, status, delivered_time, read_time, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                status = GREATEST(status, VALUES(status)),
                delivered_time = COALESCE(delivered_time, VALUES(delivered_time)),
                read_time = COALESCE(read_time, VALUES(read_time)),
                update_time = VALUES(update_time)',
            [
                $context->organization,
                $message['conversation_id'],
                $messageId,
                $context->userId,
                $receiptStatus,
                $deliveredTime,
                $readTime,
                $now,
                $now,
            ],
        );

        if ($receiptStatus >= self::RECEIPT_READ) {
            $this->repository->execute(
                'UPDATE im_conversation_member
                    SET unread_count = 0, last_read_message_id = ?, update_time = ?
                  WHERE organization = ? AND conversation_id = ? AND user_id = ? AND delete_time IS NULL',
                [$messageId, $now, $context->organization, $message['conversation_id'], $context->userId],
            );
        }

        return [
            'message_id' => $messageId,
            'conversation_id' => $message['conversation_id'],
            'sender_id' => $message['sender_id'],
            'user_id' => $context->userId,
            'status' => $receiptStatus,
            'time' => $now,
        ];
    }

    public function recall(AuthContext $context, array $data): array
    {
        $messageId = trim((string) ($data['message_id'] ?? ''));
        if ($messageId === '') {
            throw new ImException('缺少 message_id', 'RECALL_MESSAGE_ID_EMPTY');
        }

        $this->messageShardRouter->ensureIndexTable();
        $previewMessage = $this->findVisibleMessage($context, $messageId);
        $this->messageShardRouter->writeTable($context->organization, (string) $previewMessage['conversation_id'], $this->now());
        return $this->repository->transaction(function () use ($context, $messageId): array {
            $message = $this->findVisibleMessage($context, $messageId);
            if ((string) $message['sender_id'] !== $context->userId) {
                throw new ImException('只能撤回自己发送的消息', 'RECALL_FORBIDDEN');
            }
            if ((int) $message['status'] === self::MESSAGE_RECALLED) {
                return [
                    'message_id' => $messageId,
                    'conversation_id' => $message['conversation_id'],
                    'recipient_user_ids' => $this->conversationMembers($context->organization, $message['conversation_id']),
                    'recalled' => true,
                ];
            }
            $recallWindowSeconds = $this->messageOperationWindowSeconds(
                $context->organization,
                'message_recall_window_seconds',
                $this->config->recallWindowSeconds,
            );
            if ($recallWindowSeconds > 0 && strtotime((string) $message['create_time']) + $recallWindowSeconds < time()) {
                throw new ImException('消息已超过可撤回时间', 'RECALL_EXPIRED');
            }

            $now = $this->now();
            $this->repository->execute(
                'UPDATE ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . ' SET status = ?, update_time = ?, delete_time = ? WHERE organization = ? AND message_id = ?',
                [self::MESSAGE_RECALLED, $now, $now, $context->organization, $messageId],
            );
            $noticeState = [];
            if ($this->messageNoticeEnabled($context->organization, self::NOTICE_RECALL, (int) $message['conversation_type'])) {
                $noticeState = $this->createSystemNotice(
                    $context,
                    (string) $message['conversation_id'],
                    (int) $message['conversation_type'],
                    self::NOTICE_RECALL,
                    ['target_message_id' => $messageId],
                );
            }
            $conversationState = $noticeState ?: $this->refreshConversationLastMessage(
                $context->organization,
                (string) $message['conversation_id'],
                $now,
            );

            return [
                'message_id' => $messageId,
                'conversation_id' => $message['conversation_id'],
                'recipient_user_ids' => $this->conversationMembers($context->organization, $message['conversation_id']),
                'recalled' => true,
                'notice_message' => $noticeState['message'] ?? null,
                'time' => $now,
                ...$conversationState,
            ];
        });
    }

    public function screenshot(AuthContext $context, array $data): array
    {
        $conversationId = trim((string) ($data['conversation_id'] ?? ''));
        if ($conversationId === '') {
            throw new ImException('缺少 conversation_id', 'SCREENSHOT_CONVERSATION_ID_EMPTY');
        }

        $this->messageShardRouter->writeTable($context->organization, $conversationId, $this->now());
        $this->messageShardRouter->ensureIndexTable();
        return $this->repository->transaction(function () use ($context, $conversationId): array {
            $conversation = $this->findVisibleConversation($context, $conversationId);
            $conversationType = (int) $conversation['conversation_type'];
            if (!$this->messageNoticeEnabled($context->organization, self::NOTICE_SCREENSHOT, $conversationType)) {
                return [
                    'conversation_id' => $conversationId,
                    'recipient_user_ids' => [],
                    'enabled' => false,
                ];
            }

            $noticeState = $this->createSystemNotice(
                $context,
                $conversationId,
                $conversationType,
                self::NOTICE_SCREENSHOT,
            );

            return [
                'conversation_id' => $conversationId,
                'recipient_user_ids' => $this->conversationMembers($context->organization, $conversationId),
                'enabled' => true,
                'notice_message' => $noticeState['message'] ?? null,
                'time' => $noticeState['last_message_time'] ?? $this->now(),
                ...$noticeState,
            ];
        });
    }

    public function delete(AuthContext $context, array $data): array
    {
        $messageId = trim((string) ($data['message_id'] ?? ''));
        if ($messageId === '') {
            throw new ImException('缺少 message_id', 'DELETE_MESSAGE_ID_EMPTY');
        }

        $scope = strtolower(trim((string) ($data['scope'] ?? 'self')));
        if (!in_array($scope, ['self', 'both'], true)) {
            throw new ImException('删除范围无效', 'DELETE_SCOPE_INVALID');
        }

        $this->messageShardRouter->ensureIndexTable();

        return $this->repository->transaction(function () use ($context, $messageId, $scope): array {
            $message = $this->findVisibleMessage($context, $messageId);
            $now = $this->now();

            if ($scope === 'self') {
                if (!$this->messageDeleteEnabled($context->organization, 'single')) {
                    throw new ImException('当前租户未开启单向删除消息', 'DELETE_SINGLE_DISABLED');
                }

                $this->repository->execute(
                    'INSERT INTO im_message_user_delete
                        (organization, conversation_id, message_id, user_id, delete_time, create_time)
                     VALUES (?, ?, ?, ?, ?, ?)
                     ON DUPLICATE KEY UPDATE delete_time = VALUES(delete_time)',
                    [
                        $context->organization,
                        (string) $message['conversation_id'],
                        $messageId,
                        $context->userId,
                        $now,
                        $now,
                    ],
                );

                return [
                    'message_id' => $messageId,
                    'conversation_id' => (string) $message['conversation_id'],
                    'scope' => 'self',
                    'recipient_user_ids' => [],
                    'time' => $now,
                ];
            }

            if (!$this->messageDeleteEnabled($context->organization, 'both')) {
                throw new ImException('当前租户未开启双向删除消息', 'DELETE_BOTH_DISABLED');
            }
            if ((string) $message['sender_id'] !== $context->userId) {
                throw new ImException('只能双向删除自己发送的消息', 'DELETE_BOTH_FORBIDDEN');
            }

            $this->repository->execute(
                'UPDATE ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . '
                    SET update_time = ?, delete_time = ?
                  WHERE organization = ? AND message_id = ?',
                [$now, $now, $context->organization, $messageId],
            );

            $conversationState = $this->refreshConversationLastMessage(
                $context->organization,
                (string) $message['conversation_id'],
                $now,
            );

            return [
                'message_id' => $messageId,
                'conversation_id' => (string) $message['conversation_id'],
                'scope' => 'both',
                'recipient_user_ids' => $this->conversationMembers($context->organization, (string) $message['conversation_id']),
                'time' => $now,
                ...$conversationState,
            ];
        });
    }

    public function edit(AuthContext $context, array $data): array
    {
        $messageId = trim((string) ($data['message_id'] ?? ''));
        if ($messageId === '') {
            throw new ImException('缺少 message_id', 'EDIT_MESSAGE_ID_EMPTY');
        }

        $content = $data['content'] ?? [];
        if (is_string($content)) {
            $content = ['text' => $content];
        }
        if (!is_array($content)) {
            throw new ImException('消息内容格式无效', 'EDIT_CONTENT_INVALID');
        }

        $text = trim((string) ($content['text'] ?? ''));
        if ($text === '') {
            throw new ImException('文本消息不能为空', 'EDIT_TEXT_EMPTY');
        }

        $this->messageShardRouter->ensureIndexTable();
        return $this->repository->transaction(function () use ($context, $messageId, $text): array {
            $message = $this->findVisibleMessage($context, $messageId);
            if ((string) $message['sender_id'] !== $context->userId) {
                throw new ImException('只能编辑自己发送的消息', 'EDIT_FORBIDDEN');
            }
            if ((int) $message['message_type'] !== MessageType::TEXT) {
                throw new ImException('只能编辑文本消息', 'EDIT_MESSAGE_TYPE_INVALID');
            }
            if ((int) $message['status'] !== self::MESSAGE_NORMAL) {
                throw new ImException('消息状态不允许编辑', 'EDIT_STATUS_INVALID');
            }

            $editWindowSeconds = $this->messageOperationWindowSeconds(
                $context->organization,
                'message_edit_window_seconds',
                $this->config->editWindowSeconds,
            );
            if ($editWindowSeconds > 0 && strtotime((string) $message['create_time']) + $editWindowSeconds < time()) {
                throw new ImException('消息已超过可编辑时间', 'EDIT_EXPIRED');
            }

            $now = $this->now();
            $messageContent = json_decode((string) ($message['content'] ?? '{}'), true);
            $messageContent = is_array($messageContent) ? $messageContent : [];
            $messageContent['text'] = $text;
            $contentJson = json_encode($messageContent, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);

            $this->repository->execute(
                'UPDATE ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . '
                    SET content = ?, edit_time = ?, edit_count = edit_count + 1, update_time = ?
                  WHERE organization = ? AND message_id = ?',
                [$contentJson, $now, $now, $context->organization, $messageId],
            );

            $updatedMessage = $this->repository->fetchOne(
                'SELECT * FROM ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . ' WHERE organization = ? AND message_id = ? LIMIT 1',
                [$context->organization, $messageId],
            );
            if ($updatedMessage === null) {
                throw new ImException('消息编辑失败', 'EDIT_MESSAGE_NOT_FOUND');
            }
            $updatedMessage['_message_table'] = $this->messageTableOf($message);

            $conversationState = [];
            $conversation = $this->repository->fetchOne(
                'SELECT last_message_id FROM im_conversation WHERE organization = ? AND conversation_id = ? LIMIT 1',
                [$context->organization, (string) $message['conversation_id']],
            );
            if ($conversation !== null && (string) ($conversation['last_message_id'] ?? '') === $messageId) {
                $conversationState = $this->refreshConversationLastMessage(
                    $context->organization,
                    (string) $message['conversation_id'],
                    $now,
                );
            }

            return [
                'message_id' => $messageId,
                'conversation_id' => $message['conversation_id'],
                'recipient_user_ids' => $this->conversationMembers($context->organization, $message['conversation_id']),
                'message' => $this->formatMessage($updatedMessage),
                'time' => $now,
                ...$conversationState,
            ];
        });
    }

    public function sync(AuthContext $context, array $data): array
    {
        $limit = min(max((int) ($data['limit'] ?? 50), 1), $this->config->syncMaxLimit);
        $afterId = max((int) ($data['after_id'] ?? 0), 0);
        $afterSeq = max((int) ($data['after_seq'] ?? 0), 0);
        $conversationId = trim((string) ($data['conversation_id'] ?? ''));
        $seqCursor = 0;

        if ($conversationId !== '') {
            $this->assertVisibleMember($context->organization, $conversationId, $context->userId);
            $seqCursor = $afterSeq > 0 ? $afterSeq : $afterId;
            $messages = $this->fetchConversationMessages($context->organization, $conversationId, $context->userId, $seqCursor, $limit);
        } else {
            $messages = $this->fetchJoinedMessages($context->organization, $context->userId, $afterId, $limit);
        }

        return [
            'messages' => array_map(fn (array $message): array => $this->formatMessage($message), $messages),
            'next_after_id' => empty($messages) ? $afterId : (int) end($messages)['id'],
            'next_after_seq' => $conversationId === '' ? 0 : (empty($messages) ? $seqCursor : (int) end($messages)['message_seq']),
        ];
    }

    private function ensureWriteConversationId(AuthContext $context, int $conversationType, array $data): array
    {
        if ($conversationType === self::CONVERSATION_SINGLE) {
            $toUserId = trim((string) ($data['to_user_id'] ?? $data['receiver_id'] ?? ''));
            if ($toUserId === '' || $toUserId === $context->userId) {
                throw new ImException('单聊接收人无效', 'SEND_SINGLE_RECEIVER_INVALID');
            }
            $data['conversation_id'] = self::singleConversationId($context->organization, $context->userId, $toUserId);

            return $data;
        }

        if (trim((string) ($data['conversation_id'] ?? '')) === '') {
            $data['conversation_id'] = 'group_' . bin2hex(random_bytes(8));
        }

        return $data;
    }

    private function createMessageIndex(
        int $organization,
        string $conversationId,
        string $messageId,
        int $messageSeq,
        string $messageTable,
        string $now,
    ): void {
        $this->repository->execute(
            'INSERT INTO `im_message_index`
                (organization, conversation_id, message_id, message_seq, shard_table, create_time)
             VALUES (?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                conversation_id = VALUES(conversation_id),
                message_seq = VALUES(message_seq),
                shard_table = VALUES(shard_table)',
            [$organization, $conversationId, $messageId, $messageSeq, $messageTable, $now],
        );
    }

    private function findMessageByClientMsg(int $organization, string $senderId, string $clientMsgId): ?array
    {
        foreach ($this->messageShardRouter->tablesNewestFirst() as $table) {
            $message = $this->repository->fetchOne(
                'SELECT * FROM ' . $this->messageShardRouter->quote($table) . '
                  WHERE organization = ? AND sender_id = ? AND client_msg_id = ? AND delete_time IS NULL
                  LIMIT 1',
                [$organization, $senderId, $clientMsgId],
            );
            if ($message !== null) {
                $message['_message_table'] = $table;
                return $message;
            }
        }

        return null;
    }

    private function fetchConversationMessages(int $organization, string $conversationId, string $userId, int $afterSeq, int $limit): array
    {
        $rows = [];
        foreach ($this->messageShardRouter->tablesForConversationNewestFirst($organization, $conversationId) as $table) {
            $rows = array_merge($rows, $this->repository->fetchAll(
                'SELECT m.* FROM ' . $this->messageShardRouter->quote($table) . ' m
                  WHERE m.organization = ?
                    AND m.conversation_id = ?
                    AND m.message_seq > ?
                    AND m.delete_time IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM im_message_user_delete ud
                         WHERE ud.organization = m.organization
                           AND ud.message_id = m.message_id
                           AND ud.user_id = ?
                    )
                  ORDER BY message_seq ASC LIMIT ' . $limit,
                [$organization, $conversationId, $afterSeq, $userId],
            ));
        }

        usort($rows, static fn (array $left, array $right): int => (int) $left['message_seq'] <=> (int) $right['message_seq']);

        return array_slice($rows, 0, $limit);
    }

    private function fetchJoinedMessages(int $organization, string $userId, int $afterId, int $limit): array
    {
        $rows = [];
        foreach ($this->messageShardRouter->tablesNewestFirst() as $table) {
            $rows = array_merge($rows, $this->repository->fetchAll(
                'SELECT m.* FROM ' . $this->messageShardRouter->quote($table) . ' m
                  INNER JOIN im_conversation_member cm
                    ON cm.organization = m.organization
                   AND cm.conversation_id = m.conversation_id
                   AND cm.user_id = ?
                   AND cm.status IN (1, 2)
                   AND cm.delete_time IS NULL
                  WHERE m.organization = ? AND m.id > ? AND m.delete_time IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM im_message_user_delete ud
                         WHERE ud.organization = m.organization
                           AND ud.message_id = m.message_id
                           AND ud.user_id = ?
                    )
                  ORDER BY m.id ASC LIMIT ' . $limit,
                [$userId, $organization, $afterId, $userId],
            ));
        }

        usort($rows, static fn (array $left, array $right): int => strcmp((string) $left['create_time'], (string) $right['create_time']) ?: ((int) $left['id'] <=> (int) $right['id']));

        return array_slice($rows, 0, $limit);
    }

    private function ensureSingleConversation(AuthContext $context, array $data): array
    {
        $toUserId = trim((string) ($data['to_user_id'] ?? $data['receiver_id'] ?? ''));
        if ($toUserId === '' || $toUserId === $context->userId) {
            throw new ImException('单聊接收人无效', 'SEND_SINGLE_RECEIVER_INVALID');
        }

        $conversationId = self::singleConversationId($context->organization, $context->userId, $toUserId);
        $now = $this->now();
        $this->repository->execute(
            'INSERT INTO im_conversation
                (organization, conversation_id, conversation_type, title, owner_user_id, status, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, 1, ?, ?)
             ON DUPLICATE KEY UPDATE update_time = VALUES(update_time)',
            [$context->organization, $conversationId, self::CONVERSATION_SINGLE, '', $context->userId, $now, $now],
        );
        $this->ensureMember($context->organization, $conversationId, $context->userId, 2);
        $this->ensureMember($context->organization, $conversationId, $toUserId, 1);

        return [
            'conversation_id' => $conversationId,
            'recipient_user_ids' => [$toUserId],
        ];
    }

    private function ensureGroupConversation(AuthContext $context, array $data): array
    {
        $conversationId = trim((string) ($data['conversation_id'] ?? ''));
        $memberIds = $this->normalizeUserIds($data['member_ids'] ?? []);
        if (!in_array($context->userId, $memberIds, true)) {
            $memberIds[] = $context->userId;
        }
        $this->assertGroupMemberIdsAllowed($context->organization, $memberIds);

        if ($conversationId === '') {
            if (count($memberIds) < 2) {
                throw new ImException('创建群聊至少需要 2 个成员', 'SEND_GROUP_MEMBERS_INVALID');
            }
            $conversationId = 'group_' . bin2hex(random_bytes(8));
        }

        $conversation = $this->repository->fetchOne(
            'SELECT * FROM im_conversation WHERE organization = ? AND conversation_id = ? AND delete_time IS NULL LIMIT 1',
            [$context->organization, $conversationId],
        );
        $now = $this->now();
        if ($conversation === null) {
            $this->repository->execute(
                'INSERT INTO im_conversation
                    (organization, conversation_id, conversation_type, title, owner_user_id, status, create_time, update_time)
                 VALUES (?, ?, ?, ?, ?, 1, ?, ?)',
                [
                    $context->organization,
                    $conversationId,
                    self::CONVERSATION_GROUP,
                    (string) ($data['title'] ?? ''),
                    $context->userId,
                    $now,
                    $now,
                ],
            );
            foreach ($memberIds as $memberId) {
                $this->ensureMember($context->organization, $conversationId, $memberId, $memberId === $context->userId ? 2 : 1);
            }
        } else {
            if ((int) $conversation['conversation_type'] !== self::CONVERSATION_GROUP) {
                throw new ImException('会话类型不是群聊', 'SEND_CONVERSATION_TYPE_MISMATCH');
            }
            $this->assertMember($context->organization, $conversationId, $context->userId);
            $this->assertConversationWithoutSystemMember($context->organization, $conversationId);
        }

        $recipientUserIds = array_values(array_filter(
            $this->conversationMembers($context->organization, $conversationId),
            fn (string $userId): bool => $userId !== $context->userId,
        ));
        if (empty($recipientUserIds)) {
            throw new ImException('群聊没有可接收成员', 'SEND_GROUP_RECIPIENT_EMPTY');
        }

        return [
            'conversation_id' => $conversationId,
            'recipient_user_ids' => $recipientUserIds,
        ];
    }

    private function createReceipts(AuthContext $context, string $conversationId, string $messageId, array $recipientUserIds): void
    {
        $now = $this->now();
        $this->upsertReceipt($context->organization, $conversationId, $messageId, $context->userId, self::RECEIPT_READ, $now, $now);
        foreach ($recipientUserIds as $userId) {
            $this->upsertReceipt($context->organization, $conversationId, $messageId, $userId, self::RECEIPT_SENT, null, null);
        }
    }

    private function allocateMessageSeq(int $organization, string $conversationId): int
    {
        $row = $this->repository->fetchOne(
            'SELECT next_message_seq FROM im_conversation WHERE organization = ? AND conversation_id = ? FOR UPDATE',
            [$organization, $conversationId],
        );
        if ($row === null) {
            throw new ImException('会话不存在，无法分配消息序号', 'CONVERSATION_NOT_FOUND');
        }

        $messageSeq = max((int) $row['next_message_seq'], 1);
        $this->repository->execute(
            'UPDATE im_conversation SET next_message_seq = ? WHERE organization = ? AND conversation_id = ?',
            [$messageSeq + 1, $organization, $conversationId],
        );

        return $messageSeq;
    }

    private function upsertReceipt(int $organization, string $conversationId, string $messageId, string $userId, int $status, ?string $deliveredTime, ?string $readTime): void
    {
        $now = $this->now();
        $this->repository->execute(
            'INSERT INTO im_message_receipt
                (organization, conversation_id, message_id, user_id, status, delivered_time, read_time, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                status = GREATEST(status, VALUES(status)),
                delivered_time = COALESCE(delivered_time, VALUES(delivered_time)),
                read_time = COALESCE(read_time, VALUES(read_time)),
                update_time = VALUES(update_time)',
            [$organization, $conversationId, $messageId, $userId, $status, $deliveredTime, $readTime, $now, $now],
        );
    }

    private function increaseUnread(int $organization, string $conversationId, array $recipientUserIds): void
    {
        if (empty($recipientUserIds)) {
            return;
        }

        $placeholders = implode(',', array_fill(0, count($recipientUserIds), '?'));
        $this->repository->execute(
            'UPDATE im_conversation_member
                SET unread_count = unread_count + 1, update_time = ?
              WHERE organization = ? AND conversation_id = ? AND user_id IN (' . $placeholders . ') AND status = 1 AND delete_time IS NULL',
            array_merge([$this->now(), $organization, $conversationId], $recipientUserIds),
        );
    }

    private function ensureMember(int $organization, string $conversationId, string $userId, int $role): void
    {
        $now = $this->now();
        $this->repository->execute(
            'INSERT INTO im_conversation_member
                (organization, conversation_id, user_id, role, status, mute_until, join_time, create_time, update_time)
             VALUES (?, ?, ?, ?, 1, NULL, ?, ?, ?)
             ON DUPLICATE KEY UPDATE status = 1, mute_until = NULL, delete_time = NULL, update_time = VALUES(update_time)',
            [$organization, $conversationId, $userId, $role, $now, $now, $now],
        );
    }

    private function assertMember(int $organization, string $conversationId, string $userId): void
    {
        $member = $this->repository->fetchOne(
            'SELECT id, status, mute_until FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ? AND user_id = ? AND status IN (1, 2) AND delete_time IS NULL LIMIT 1',
            [$organization, $conversationId, $userId],
        );
        if ($member === null) {
            throw new ImException('没有该会话的发送权限', 'CONVERSATION_MEMBER_FORBIDDEN');
        }
        if ((int) ($member['status'] ?? 1) === 1) {
            return;
        }

        $muteUntil = trim((string) ($member['mute_until'] ?? ''));
        if ($muteUntil !== '' && strtotime($muteUntil) !== false && strtotime($muteUntil) <= time()) {
            $this->repository->execute(
                'UPDATE im_conversation_member
                    SET status = 1, mute_until = NULL, update_time = ?
                  WHERE organization = ? AND conversation_id = ? AND user_id = ? AND delete_time IS NULL',
                [$this->now(), $organization, $conversationId, $userId],
            );
            return;
        }

        throw new ImException($muteUntil !== '' ? '你已被禁言至 ' . $muteUntil : '你已被禁言', 'CONVERSATION_MEMBER_MUTED');
    }

    private function assertVisibleMember(int $organization, string $conversationId, string $userId): void
    {
        $member = $this->repository->fetchOne(
            'SELECT id FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ? AND user_id = ? AND status IN (1, 2) AND delete_time IS NULL LIMIT 1',
            [$organization, $conversationId, $userId],
        );
        if ($member === null) {
            throw new ImException('没有该会话的访问权限', 'CONVERSATION_MEMBER_FORBIDDEN');
        }
    }

    private function findVisibleConversation(AuthContext $context, string $conversationId): array
    {
        $conversation = $this->repository->fetchOne(
            'SELECT c.* FROM im_conversation c
              INNER JOIN im_conversation_member cm
                ON cm.organization = c.organization
               AND cm.conversation_id = c.conversation_id
               AND cm.user_id = ?
               AND cm.status IN (1, 2)
               AND cm.delete_time IS NULL
              WHERE c.organization = ? AND c.conversation_id = ? AND c.delete_time IS NULL
              LIMIT 1',
            [$context->userId, $context->organization, $conversationId],
        );
        if ($conversation === null) {
            throw new ImException('会话不存在或无权访问', 'CONVERSATION_NOT_FOUND');
        }

        return $conversation;
    }

    private function assertGroupMemberIdsAllowed(int $organization, array $memberIds): void
    {
        $systemUserIds = $this->systemUserIds($organization, $memberIds);
        if (!empty($systemUserIds)) {
            throw new ImException('系统联系人不允许拉入群聊', 'GROUP_SYSTEM_MEMBER_FORBIDDEN');
        }
    }

    private function assertConversationWithoutSystemMember(int $organization, string $conversationId): void
    {
        $row = $this->repository->fetchOne(
            'SELECT u.user_id
               FROM im_conversation_member cm
               INNER JOIN im_user u
                  ON u.user_id = cm.user_id
                 AND u.organization IN (0, cm.organization)
                 AND u.is_system = 1
                 AND u.delete_time IS NULL
              WHERE cm.organization = ?
                AND cm.conversation_id = ?
                AND cm.status IN (1, 2)
                AND cm.delete_time IS NULL
              LIMIT 1',
            [$organization, $conversationId],
        );
        if ($row !== null) {
            throw new ImException('系统联系人不允许拉入群聊', 'GROUP_SYSTEM_MEMBER_FORBIDDEN');
        }
    }

    private function systemUserIds(int $organization, array $userIds): array
    {
        $userIds = array_values(array_unique(array_filter($userIds, static fn (string $userId): bool => $userId !== '')));
        if (empty($userIds)) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($userIds), '?'));
        $rows = $this->repository->fetchAll(
            'SELECT user_id FROM im_user
              WHERE organization IN (0, ?) AND user_id IN (' . $placeholders . ') AND is_system = 1 AND delete_time IS NULL',
            array_merge([$organization], $userIds),
        );

        return array_values(array_map(static fn (array $row): string => (string) $row['user_id'], $rows));
    }

    private function conversationMembers(int $organization, string $conversationId): array
    {
        $rows = $this->repository->fetchAll(
            'SELECT user_id FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ? AND status = 1 AND delete_time IS NULL',
            [$organization, $conversationId],
        );

        return array_values(array_map(static fn (array $row): string => (string) $row['user_id'], $rows));
    }

    private function findVisibleMessage(AuthContext $context, string $messageId): array
    {
        $index = $this->repository->fetchOne(
            'SELECT shard_table FROM `im_message_index` WHERE organization = ? AND message_id = ? LIMIT 1',
            [$context->organization, $messageId],
        );
        $tables = $index !== null
            ? [(string) $index['shard_table']]
            : $this->messageShardRouter->tablesNewestFirst();

        foreach ($tables as $table) {
            $message = $this->repository->fetchOne(
                'SELECT m.* FROM ' . $this->messageShardRouter->quote($table) . ' m
                  INNER JOIN im_conversation_member cm
                    ON cm.organization = m.organization
                   AND cm.conversation_id = m.conversation_id
                   AND cm.user_id = ?
                   AND cm.status IN (1, 2)
                   AND cm.delete_time IS NULL
                  WHERE m.organization = ? AND m.message_id = ? AND m.delete_time IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM im_message_user_delete ud
                         WHERE ud.organization = m.organization
                           AND ud.message_id = m.message_id
                           AND ud.user_id = ?
                    )
                  LIMIT 1',
                [$context->userId, $context->organization, $messageId, $context->userId],
            );
            if ($message !== null) {
                $message['_message_table'] = $table;
                return $message;
            }
        }

        throw new ImException('消息不存在或无权访问', 'MESSAGE_NOT_FOUND');
    }

    private function normalizeConversationType(array $data): int
    {
        $value = $data['conversation_type'] ?? null;
        if ($value === null) {
            return (isset($data['to_user_id']) || isset($data['receiver_id'])) ? self::CONVERSATION_SINGLE : self::CONVERSATION_GROUP;
        }
        if (is_string($value)) {
            return match (strtolower($value)) {
                'single', 'private', '1' => self::CONVERSATION_SINGLE,
                'group', '2' => self::CONVERSATION_GROUP,
                default => throw new ImException('会话类型无效', 'CONVERSATION_TYPE_INVALID'),
            };
        }

        return match ((int) $value) {
            self::CONVERSATION_SINGLE => self::CONVERSATION_SINGLE,
            self::CONVERSATION_GROUP => self::CONVERSATION_GROUP,
            default => throw new ImException('会话类型无效', 'CONVERSATION_TYPE_INVALID'),
        };
    }

    private function normalizeReceiptStatus(mixed $status): int
    {
        if (is_string($status)) {
            return match (strtolower($status)) {
                'delivered' => self::RECEIPT_DELIVERED,
                'read' => self::RECEIPT_READ,
                default => throw new ImException('回执状态无效', 'ACK_STATUS_INVALID'),
            };
        }

        return match ((int) $status) {
            self::RECEIPT_DELIVERED => self::RECEIPT_DELIVERED,
            self::RECEIPT_READ => self::RECEIPT_READ,
            default => throw new ImException('回执状态无效', 'ACK_STATUS_INVALID'),
        };
    }

    private function normalizeContent(int $messageType, mixed $content): array
    {
        if (is_string($content)) {
            $content = ['text' => $content];
        }
        if (!is_array($content)) {
            throw new ImException('消息内容格式无效', 'SEND_CONTENT_INVALID');
        }
        if ($messageType === MessageType::TEXT && trim((string) ($content['text'] ?? '')) === '') {
            throw new ImException('文本消息不能为空', 'SEND_TEXT_EMPTY');
        }

        return $content;
    }

    private function normalizeUserIds(mixed $value): array
    {
        if (is_string($value)) {
            $value = array_filter(array_map('trim', explode(',', $value)));
        }
        if (!is_array($value)) {
            return [];
        }

        $ids = [];
        foreach ($value as $item) {
            $id = trim((string) $item);
            if ($id !== '') {
                $ids[$id] = $id;
            }
        }

        return array_values($ids);
    }

    private function summary(int $messageType, array $content): string
    {
        return match ($messageType) {
            MessageType::TEXT => mb_substr((string) ($content['text'] ?? ''), 0, 120),
            MessageType::IMAGE => '[图片]',
            MessageType::FILE => '[文件]',
            MessageType::VOICE => '[语音]',
            MessageType::VIDEO => '[视频]',
            MessageType::SYSTEM => mb_substr((string) ($content['text'] ?? '[系统通知]'), 0, 120),
            default => '[消息]',
        };
    }

    private function createSystemNotice(
        AuthContext $context,
        string $conversationId,
        int $conversationType,
        string $event,
        array $extra = [],
    ): array {
        $messageSeq = $this->allocateMessageSeq($context->organization, $conversationId);
        $messageId = $this->newMessageId();
        $now = $this->now();
        $messageTable = $this->messageShardRouter->writeTable($context->organization, $conversationId, $now);
        $actor = $this->formatMessageSender($context->organization, $context->userId);
        $actorName = trim((string) ($actor['nickname'] ?? $actor['account'] ?? ''));
        $content = [
            'event' => $event,
            'text' => $event === self::NOTICE_RECALL ? '消息已撤回' : '截屏提示',
            'actor_user_id' => $context->userId,
            'actor_name' => $actorName !== '' ? $actorName : $context->userId,
            'conversation_type' => $conversationType,
            ...$extra,
        ];

        $this->repository->execute(
            'INSERT INTO ' . $this->messageShardRouter->quote($messageTable) . '
                (organization, conversation_id, conversation_type, message_id, message_seq, client_msg_id, sender_id, message_type, content, status, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                $context->organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                'system-' . $messageId,
                self::SYSTEM_NOTIFICATION_USER_ID,
                MessageType::SYSTEM,
                json_encode($content, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                self::MESSAGE_NORMAL,
                $now,
                $now,
            ],
        );

        $message = $this->repository->fetchOne(
            'SELECT * FROM ' . $this->messageShardRouter->quote($messageTable) . ' WHERE organization = ? AND message_id = ? LIMIT 1',
            [$context->organization, $messageId],
        );
        if ($message === null) {
            throw new ImException('系统提示写入失败', 'SYSTEM_NOTICE_CREATE_FAILED');
        }
        $message['_message_table'] = $messageTable;
        $this->createMessageIndex($context->organization, $conversationId, $messageId, $messageSeq, $messageTable, $now);

        $conversationState = $this->refreshConversationLastMessage($context->organization, $conversationId, $now);

        return [
            'message' => $this->formatMessage($message),
            ...$conversationState,
        ];
    }

    private function refreshConversationLastMessage(int $organization, string $conversationId, string $now): array
    {
        $lastRows = [];
        foreach ($this->messageShardRouter->tablesForConversationNewestFirst($organization, $conversationId) as $table) {
            $rows = $this->repository->fetchAll(
                'SELECT * FROM ' . $this->messageShardRouter->quote($table) . '
                  WHERE organization = ? AND conversation_id = ? AND delete_time IS NULL
                  ORDER BY message_seq DESC LIMIT 1',
                [$organization, $conversationId],
            );
            foreach ($rows as $row) {
                $row['_message_table'] = $table;
                $lastRows[] = $row;
            }
        }
        usort($lastRows, static fn (array $left, array $right): int => (int) $right['message_seq'] <=> (int) $left['message_seq']);
        $lastMessage = $lastRows[0] ?? null;
        if ($lastMessage === null) {
            $this->repository->execute(
                'UPDATE im_conversation
                    SET last_message_id = "", last_message_seq = 0, last_message_time = NULL, last_message_summary = "", update_time = ?
                  WHERE organization = ? AND conversation_id = ?',
                [$now, $organization, $conversationId],
            );

            return [
                'last_message_id' => '',
                'last_message_seq' => 0,
                'last_message_time' => '',
                'last_message_summary' => '',
            ];
        }

        $content = json_decode((string) ($lastMessage['content'] ?? '{}'), true);
        $summary = $this->summary((int) $lastMessage['message_type'], is_array($content) ? $content : []);
        $this->repository->execute(
            'UPDATE im_conversation
                SET last_message_id = ?, last_message_seq = ?, last_message_time = ?, last_message_summary = ?, update_time = ?
              WHERE organization = ? AND conversation_id = ?',
            [
                (string) $lastMessage['message_id'],
                (int) $lastMessage['message_seq'],
                (string) $lastMessage['create_time'],
                $summary,
                $now,
                $organization,
                $conversationId,
            ],
        );

        return [
            'last_message_id' => (string) $lastMessage['message_id'],
            'last_message_seq' => (int) $lastMessage['message_seq'],
            'last_message_time' => (string) $lastMessage['create_time'],
            'last_message_summary' => $summary,
        ];
    }

    private function messageTableOf(array $message): string
    {
        return (string) ($message['_message_table'] ?? 'im_message');
    }

    private function formatMessage(array $message): array
    {
        $content = json_decode((string) ($message['content'] ?? '{}'), true);
        $senderId = (string) $message['sender_id'];

        return [
            'id' => (int) $message['id'],
            'organization' => (int) $message['organization'],
            'conversation_id' => (string) $message['conversation_id'],
            'conversation_type' => (int) $message['conversation_type'],
            'message_id' => (string) $message['message_id'],
            'message_seq' => (int) $message['message_seq'],
            'client_msg_id' => (string) $message['client_msg_id'],
            'sender_id' => $senderId,
            'sender_user' => $this->formatMessageSender((int) $message['organization'], $senderId),
            'message_type' => (int) $message['message_type'],
            'content' => is_array($content) ? $content : [],
            'status' => (int) $message['status'],
            'edit_time' => (string) ($message['edit_time'] ?? ''),
            'edit_count' => (int) ($message['edit_count'] ?? 0),
            'create_time' => (string) $message['create_time'],
            'update_time' => (string) ($message['update_time'] ?? ''),
        ];
    }

    private function formatMessageSender(int $organization, string $senderId): ?array
    {
        if ($senderId === '') {
            return null;
        }

        $row = $this->repository->fetchOne(
            'SELECT id,user_id,account,nickname,signature,avatar,mobile,im_short_no,gender,status,remark,login_time,is_system,system_code
               FROM im_user
              WHERE user_id = ?
                AND delete_time IS NULL
                AND ((organization = ? AND is_system = 2) OR (organization = 0 AND is_system = 1))
              LIMIT 1',
            [$senderId, $organization],
        );
        if ($row === null) {
            return null;
        }

        $status = (int) ($row['status'] ?? 1);

        return [
            'id' => (string) ($row['id'] ?? ''),
            'user_id' => (string) ($row['user_id'] ?? ''),
            'account' => (string) ($row['account'] ?? ''),
            'nickname' => (string) ($row['nickname'] ?? ''),
            'signature' => (string) ($row['signature'] ?? ''),
            'avatar' => (string) ($row['avatar'] ?? ''),
            'mobile' => (string) ($row['mobile'] ?? ''),
            'im_short_no' => (string) ($row['im_short_no'] ?? ''),
            'gender' => (int) ($row['gender'] ?? 0),
            'status' => $status,
            'status_text' => match ($status) {
                2 => '停用',
                3 => '封禁',
                default => '正常',
            },
            'remark' => '',
            'login_time' => (string) ($row['login_time'] ?? ''),
            'is_system' => (int) ($row['is_system'] ?? 2) === 1,
            'system_code' => (string) ($row['system_code'] ?? ''),
        ];
    }

    private function now(): string
    {
        return date('Y-m-d H:i:s');
    }

    private function newMessageId(): string
    {
        return date('YmdHis') . str_pad((string) random_int(0, 999999), 6, '0', STR_PAD_LEFT) . substr(bin2hex(random_bytes(4)), 0, 8);
    }

    private function messageOperationWindowSeconds(int $organization, string $key, int $fallback): int
    {
        $config = $this->messageOperationConfig($organization);
        $value = $config[$key] ?? $fallback;

        return max(0, (int) $value);
    }

    private function messageNoticeEnabled(int $organization, string $event, int $conversationType): bool
    {
        $scope = $conversationType === self::CONVERSATION_GROUP ? 'group' : 'single';
        $config = $this->messageOperationConfig($organization);
        $key = 'message_' . $event . '_notice_' . $scope . '_enabled';

        return (string) ($config[$key] ?? '1') === '1';
    }

    private function messageDeleteEnabled(int $organization, string $scope): bool
    {
        $config = $this->messageOperationConfig($organization);
        $key = $scope === 'both' ? 'message_delete_both_enabled' : 'message_delete_single_enabled';
        $default = $scope === 'both' ? '2' : '1';

        return (string) ($config[$key] ?? $default) === '1';
    }

    private function messageOperationConfig(int $organization): array
    {
        $cached = $this->messageConfigCache[$organization] ?? null;
        if (is_array($cached) && (int) ($cached['expire_at'] ?? 0) > time()) {
            return is_array($cached['data'] ?? null) ? $cached['data'] : [];
        }

        $config = [];
        $group = $this->repository->fetchOne(
            'SELECT id FROM sm_system_config_group WHERE code = ? AND delete_time IS NULL LIMIT 1',
            ['message_config'],
        );
        if ($group !== null) {
            $rows = $this->repository->fetchAll(
                'SELECT `key`, `value` FROM sm_system_config WHERE group_id = ? AND delete_time IS NULL',
                [(int) $group['id']],
            );
            foreach ($rows as $row) {
                $config[(string) $row['key']] = (string) ($row['value'] ?? '');
            }

            $tenant = $this->repository->fetchOne(
                'SELECT `value` FROM sm_tenant_config WHERE organization = ? AND group_id = ? AND delete_time IS NULL LIMIT 1',
                [$organization, (int) $group['id']],
            );
            if ($tenant !== null) {
                $tenantValue = json_decode((string) ($tenant['value'] ?? '{}'), true);
                if (is_array($tenantValue)) {
                    foreach ($tenantValue as $tenantKey => $tenantItemValue) {
                        if ($tenantItemValue !== '' && $tenantItemValue !== null) {
                            $config[(string) $tenantKey] = (string) $tenantItemValue;
                        }
                    }
                }
            }
        }

        $this->messageConfigCache[$organization] = [
            'expire_at' => time() + self::MESSAGE_CONFIG_CACHE_TTL,
            'data' => $config,
        ];

        return $config;
    }

    private static function singleConversationId(int $organization, string $left, string $right): string
    {
        $pair = [$left, $right];
        sort($pair, SORT_STRING);

        return 'single_' . substr(sha1($organization . ':' . implode(':', $pair)), 0, 40);
    }
}
