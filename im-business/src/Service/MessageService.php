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
use B8im\ImBusiness\Telemetry\Telemetry;
use OpenTelemetry\API\Trace\SpanKind;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Support\Constants;
use PDOException;

final class MessageService
{
    private const SYSTEM_NOTIFICATION_USER_ID = 'system_notification';
    private const NOTICE_RECALL = 'recall';
    private const NOTICE_SCREENSHOT = 'screenshot';

    private const CONVERSATION_SINGLE = 1;
    private const CONVERSATION_GROUP = 2;

    private const MESSAGE_NORMAL = 1;
    private const MESSAGE_RECALLED = 2;
    private const MESSAGE_DELETED_BOTH = 3;

    private const CHANGE_RECALL = 'recall';
    private const CHANGE_EDIT = 'edit';
    private const CHANGE_DELETE_BOTH = 'delete_both';
    private const CHANGE_DELETE_SELF = 'delete_self';

    private const RECEIPT_SENT = 1;
    private const RECEIPT_DELIVERED = 2;
    private const RECEIPT_READ = 3;

    private array $messageConfigCache = [];
    private MessageShardRouter $messageShardRouter;

    public function __construct(
        private readonly ImRepository $repository,
        private readonly Config $config,
        private readonly OutboxService $outbox,
        private readonly TenantImPolicyService $tenantImPolicies,
    ) {
        $this->messageShardRouter = new MessageShardRouter($repository, $config->messageShardBuckets);
    }

    public function preflight(): void
    {
        $this->messageShardRouter->preflight();
    }

    public function send(AuthContext $context, Packet $packet): array
    {
        $data = $packet->data;
        $clientMsgId = trim((string) ($packet->clientMsgId ?? $data['client_msg_id'] ?? ''));
        if ($clientMsgId === '') {
            throw new ImException('缺少 client_msg_id', 'SEND_CLIENT_MSG_ID_EMPTY');
        }

        $messageType = (int) ($data['message_type'] ?? 0);
        if (!MessageType::isClientSendable($messageType)) {
            throw new ImException('暂不支持该消息类型', 'SEND_MESSAGE_TYPE_INVALID');
        }

        $content = $this->normalizeContent($context, $messageType, $data['content'] ?? []);
        $conversationType = $this->normalizeConversationType($data);

        $this->messageShardRouter->assertIndexTableReady();
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
        $this->ensureOrganizationSequence($context->organization);
        $messageTable = $this->messageShardRouter->writeTable($context->organization, (string) $data['conversation_id'], $now);
        try {
            return Telemetry::run(
                'im.message.persist',
                function () use ($context, $clientMsgId, $messageType, $content, $conversationType, $data, $messageTable, $now): array {
                    return $this->repository->transaction(function () use ($context, $clientMsgId, $messageType, $content, $conversationType, $data, $messageTable, $now): array {
                        $conversation = $conversationType === self::CONVERSATION_SINGLE
                            ? $this->ensureSingleConversation($context, $data)
                            : $this->ensureGroupConversation($context, $data);

                        $messageSeq = $this->allocateMessageSeq($context->organization, $conversation['conversation_id']);
                        $messageId = $this->newMessageId();
                        $contentJson = json_encode($content, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
                        $summary = $this->summary($messageType, $content);

                        Telemetry::run(
                            'im.message.body.insert',
                            fn () => $this->repository->execute(
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
                            ),
                            SpanKind::KIND_CLIENT,
                            [
                                'operation' => 'im.message.body.insert',
                                'db.system.name' => 'mysql',
                                'db.operation.name' => 'INSERT',
                                'db.collection.name' => $messageTable,
                                'b8im.organization' => $context->organization,
                                'b8im.message_id' => $messageId,
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

                        $recipientUserIds = $conversation['recipient_user_ids'];
                        $this->createReceipts($context, $conversation['conversation_id'], $messageId, $recipientUserIds);
                        $this->increaseUnread($context->organization, $conversation['conversation_id'], $recipientUserIds);
                        $this->repository->execute(
                            'UPDATE im_conversation
                        SET last_message_id = ?, last_message_seq = ?, last_message_time = ?, last_message_summary = ?, update_time = ?
                      WHERE organization = ? AND conversation_id = ?',
                            [$messageId, $messageSeq, $now, $summary, $now, $context->organization, $conversation['conversation_id']],
                        );

                        $globalSeq = $this->allocateGlobalSeq($context->organization);
                        $this->createMessageIndex(
                            organization: $context->organization,
                            conversationId: $conversation['conversation_id'],
                            messageId: $messageId,
                            messageSeq: $messageSeq,
                            globalSeq: $globalSeq,
                            senderId: $context->userId,
                            clientMsgId: $clientMsgId,
                            messageTable: $messageTable,
                            now: $now,
                        );
                        $messageRow['global_seq'] = (string) $globalSeq;

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
                },
                attributes: [
                    'operation' => 'im.message.persist',
                    'b8im.organization' => $context->organization,
                    'b8im.client_msg_id' => $clientMsgId,
                    'b8im.conversation_id' => (string) $data['conversation_id'],
                ],
            );
        } catch (PDOException $exception) {
            if ($this->isClientMessageIdempotencyConflict($exception)) {
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
        $this->messageShardRouter->assertIndexTableReady();
        $message = $this->findVisibleMessage($context, $messageId);
        $now = $this->now();

        return $this->repository->transaction(function () use (
            $context,
            $message,
            $messageId,
            $receiptStatus,
            $now,
        ): array {
            $deliveredTime = $receiptStatus >= self::RECEIPT_DELIVERED ? $now : null;
            $readTime = $receiptStatus >= self::RECEIPT_READ ? $now : null;
            $this->repository->execute(
                'INSERT INTO im_message_receipt
                    (organization, conversation_id, message_id, user_id, status, delivered_time, read_time, create_time, update_time)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
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
            $highestReceipt = $this->repository->fetchOne(
                'SELECT MAX(status) AS status FROM im_message_receipt
                  WHERE organization = ? AND message_id = ? AND user_id = ?',
                [$context->organization, $messageId, $context->userId],
            );
            $effectiveReceiptStatus = max($receiptStatus, (int) ($highestReceipt['status'] ?? 0));

            $readState = null;
            if ($effectiveReceiptStatus >= self::RECEIPT_READ) {
                $readState = $this->advanceReadState(
                    $context,
                    (string) $message['conversation_id'],
                    $messageId,
                    (int) $message['message_seq'],
                    $now,
                );
            }

            return [
                'organization' => $context->organization,
                'message_id' => $messageId,
                'conversation_id' => $message['conversation_id'],
                'message_seq' => (int) $message['message_seq'],
                'global_seq' => (string) $message['global_seq'],
                'sender_id' => $message['sender_id'],
                'user_id' => $context->userId,
                'status' => $effectiveReceiptStatus,
                'last_read_message_id' => (string) ($readState['last_read_message_id'] ?? ''),
                'last_read_seq' => (int) ($readState['last_read_seq'] ?? 0),
                'unread_count' => (int) ($readState['unread_count'] ?? 0),
                'time' => $now,
            ];
        });
    }

    public function recall(AuthContext $context, array $data): array
    {
        $messageId = trim((string) ($data['message_id'] ?? ''));
        if ($messageId === '') {
            throw new ImException('缺少 message_id', 'RECALL_MESSAGE_ID_EMPTY');
        }

        $this->messageShardRouter->assertIndexTableReady();
        $previewMessage = $this->findVisibleMessage($context, $messageId);
        $this->messageShardRouter->writeTable($context->organization, (string) $previewMessage['conversation_id'], $this->now());
        return $this->repository->transaction(function () use ($context, $messageId): array {
            $message = $this->findVisibleMessage($context, $messageId, true);
            if ((string) $message['sender_id'] !== $context->userId) {
                throw new ImException('只能撤回自己发送的消息', 'RECALL_FORBIDDEN');
            }
            if ((int) $message['status'] === self::MESSAGE_RECALLED) {
                return [
                    'message_id' => $messageId,
                    'conversation_id' => $message['conversation_id'],
                    'recipient_user_ids' => $this->conversationMembers($context->organization, $message['conversation_id']),
                    'recalled' => true,
                    'change_seq' => $this->latestChangeSeq(
                        $context->organization,
                        (string) $message['conversation_id'],
                        $messageId,
                        self::CHANGE_RECALL,
                        null,
                    ),
                ];
            }
            $recallWindowSeconds = $this->tenantImPolicies
                ->policy($context->organization)
                ->messageRecallWindowSeconds;
            if ($recallWindowSeconds > 0 && strtotime((string) $message['create_time']) + $recallWindowSeconds < time()) {
                throw new ImException('消息已超过可撤回时间', 'RECALL_EXPIRED');
            }

            $now = $this->now();
            $affected = $this->repository->execute(
                'UPDATE ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . '
                    SET status = ?, update_time = ?
                  WHERE organization = ? AND message_id = ? AND status = ? AND delete_time IS NULL',
                [self::MESSAGE_RECALLED, $now, $context->organization, $messageId, self::MESSAGE_NORMAL],
            );
            if ($affected !== 1) {
                throw new ImException('消息状态已被并发修改', 'RECALL_CONFLICT');
            }
            $changeSeq = $this->recordMessageChange(
                context: $context,
                message: $message,
                changeType: self::CHANGE_RECALL,
                targetUserId: null,
                payload: ['status' => 'recalled'],
                eventType: Constants::MQ_ROUTING_MESSAGE_RECALLED,
                now: $now,
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
                'change_seq' => $changeSeq,
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
        $this->messageShardRouter->assertIndexTableReady();
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

        $this->messageShardRouter->assertIndexTableReady();

        return $this->repository->transaction(function () use ($context, $messageId, $scope): array {
            $message = $this->findVisibleMessage($context, $messageId, true);
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

                $changeSeq = $this->recordMessageChange(
                    context: $context,
                    message: $message,
                    changeType: self::CHANGE_DELETE_SELF,
                    targetUserId: $context->userId,
                    payload: ['scope' => 'self'],
                    eventType: Constants::MQ_ROUTING_MESSAGE_DELETED_SELF,
                    now: $now,
                );

                return [
                    'message_id' => $messageId,
                    'conversation_id' => (string) $message['conversation_id'],
                    'scope' => 'self',
                    'recipient_user_ids' => [],
                    'change_seq' => $changeSeq,
                    'time' => $now,
                ];
            }

            if (!$this->messageDeleteEnabled($context->organization, 'both')) {
                throw new ImException('当前租户未开启双向删除消息', 'DELETE_BOTH_DISABLED');
            }
            if ((string) $message['sender_id'] !== $context->userId) {
                throw new ImException('只能双向删除自己发送的消息', 'DELETE_BOTH_FORBIDDEN');
            }

            $affected = $this->repository->execute(
                'UPDATE ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . '
                    SET status = ?, update_time = ?, delete_time = ?
                  WHERE organization = ? AND message_id = ? AND status = ? AND delete_time IS NULL',
                [self::MESSAGE_DELETED_BOTH, $now, $now, $context->organization, $messageId, self::MESSAGE_NORMAL],
            );
            if ($affected !== 1) {
                throw new ImException('消息状态已被并发修改', 'DELETE_BOTH_CONFLICT');
            }
            $changeSeq = $this->recordMessageChange(
                context: $context,
                message: $message,
                changeType: self::CHANGE_DELETE_BOTH,
                targetUserId: null,
                payload: ['scope' => 'both', 'status' => 'deleted_both'],
                eventType: Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH,
                now: $now,
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
                'change_seq' => $changeSeq,
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

        $this->messageShardRouter->assertIndexTableReady();
        return $this->repository->transaction(function () use ($context, $messageId, $text): array {
            $message = $this->findVisibleMessage($context, $messageId, true);
            if ((string) $message['sender_id'] !== $context->userId) {
                throw new ImException('只能编辑自己发送的消息', 'EDIT_FORBIDDEN');
            }
            if ((int) $message['message_type'] !== MessageType::TEXT) {
                throw new ImException('只能编辑文本消息', 'EDIT_MESSAGE_TYPE_INVALID');
            }
            if ((int) $message['status'] !== self::MESSAGE_NORMAL) {
                throw new ImException('消息状态不允许编辑', 'EDIT_STATUS_INVALID');
            }

            $editWindowSeconds = $this->tenantImPolicies
                ->policy($context->organization)
                ->messageEditWindowSeconds;
            if ($editWindowSeconds > 0 && strtotime((string) $message['create_time']) + $editWindowSeconds < time()) {
                throw new ImException('消息已超过可编辑时间', 'EDIT_EXPIRED');
            }

            $now = $this->now();
            $messageContent = json_decode((string) ($message['content'] ?? '{}'), true);
            $messageContent = is_array($messageContent) ? $messageContent : [];
            $messageContent['text'] = $text;
            $contentJson = json_encode($messageContent, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);

            $affected = $this->repository->execute(
                'UPDATE ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . '
                    SET content = ?, edit_time = ?, edit_count = edit_count + 1, update_time = ?
                  WHERE organization = ? AND message_id = ? AND status = ? AND delete_time IS NULL',
                [$contentJson, $now, $now, $context->organization, $messageId, self::MESSAGE_NORMAL],
            );
            if ($affected !== 1) {
                throw new ImException('消息状态已被并发修改', 'EDIT_CONFLICT');
            }

            $updatedMessage = $this->repository->fetchOne(
                'SELECT * FROM ' . $this->messageShardRouter->quote($this->messageTableOf($message)) . ' WHERE organization = ? AND message_id = ? LIMIT 1',
                [$context->organization, $messageId],
            );
            if ($updatedMessage === null) {
                throw new ImException('消息编辑失败', 'EDIT_MESSAGE_NOT_FOUND');
            }
            $updatedMessage['_message_table'] = $this->messageTableOf($message);
            $updatedMessage['global_seq'] = (string) $message['global_seq'];
            $changeSeq = $this->recordMessageChange(
                context: $context,
                message: $updatedMessage,
                changeType: self::CHANGE_EDIT,
                targetUserId: null,
                payload: [
                    'content' => $messageContent,
                    'edit_time' => $now,
                    'edit_count' => (int) $updatedMessage['edit_count'],
                ],
                eventType: Constants::MQ_ROUTING_MESSAGE_EDITED,
                now: $now,
            );

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
                'change_seq' => $changeSeq,
                'time' => $now,
                ...$conversationState,
            ];
        });
    }

    public function sync(AuthContext $context, array $data): array
    {
        $limit = min(max((int) ($data['limit'] ?? 50), 1), $this->config->syncMaxLimit);
        $afterSeq = max((int) ($data['after_seq'] ?? 0), 0);
        $afterChangeSeq = max((int) ($data['after_change_seq'] ?? 0), 0);
        $afterGlobalSeq = $this->normalizeGlobalSeq($data['after_global_seq'] ?? '0');
        $conversationId = trim((string) ($data['conversation_id'] ?? ''));
        $this->messageShardRouter->assertIndexTableReady();

        if ($conversationId !== '') {
            $this->assertVisibleMember($context->organization, $conversationId, $context->userId);
            $messagePage = $this->fetchConversationMessages(
                $context->organization,
                $conversationId,
                $context->userId,
                $afterSeq,
                $limit,
            );
            $changePage = $this->fetchConversationChanges(
                $context->organization,
                $conversationId,
                $context->userId,
                $afterChangeSeq,
                $limit,
            );

            return [
                'organization' => $context->organization,
                'scope' => 'conversation',
                'conversation_id' => $conversationId,
                'messages' => array_map(fn (array $message): array => $this->formatMessage($message), $messagePage['messages']),
                'changes' => $changePage['changes'],
                'next_after_seq' => $messagePage['next_after_seq'],
                'next_after_change_seq' => $changePage['next_after_change_seq'],
                'messages_has_more' => $messagePage['has_more'],
                'changes_has_more' => $changePage['has_more'],
            ];
        }

        $page = $this->fetchGlobalMessages(
            $context->organization,
            $context->userId,
            $afterGlobalSeq,
            $limit,
        );

        return [
            'organization' => $context->organization,
            'scope' => 'global',
            'messages' => array_map(fn (array $message): array => $this->formatMessage($message), $page['messages']),
            'next_after_global_seq' => (string) $page['next_after_global_seq'],
            'has_more' => $page['has_more'],
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
            throw new ImException('发送群消息必须指定已存在的群聊会话', 'SEND_GROUP_CONVERSATION_REQUIRED');
        }

        return $data;
    }

    private function createMessageIndex(
        int $organization,
        string $conversationId,
        string $messageId,
        int $messageSeq,
        int $globalSeq,
        string $senderId,
        string $clientMsgId,
        string $messageTable,
        string $now,
    ): void {
        Telemetry::run(
            'im.message.index.insert',
            fn () => $this->repository->execute(
                'INSERT INTO `im_message_index`
                (organization, global_seq, message_id, conversation_id, message_seq,
                 sender_id, client_msg_id, storage_node, shard_table, create_time)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                $organization,
                $globalSeq,
                $messageId,
                $conversationId,
                $messageSeq,
                $senderId,
                $clientMsgId,
                'mysql-primary',
                $messageTable,
                $now,
                ],
            ),
            SpanKind::KIND_CLIENT,
            [
                'operation' => 'im.message.index.insert',
                'db.system.name' => 'mysql',
                'db.operation.name' => 'INSERT',
                'db.collection.name' => 'im_message_index',
                'b8im.organization' => $organization,
                'b8im.message_id' => $messageId,
                'b8im.conversation_id' => $conversationId,
            ],
        );
    }

    private function findMessageByClientMsg(int $organization, string $senderId, string $clientMsgId): ?array
    {
        $index = $this->repository->fetchOne(
            'SELECT message_id, global_seq, shard_table
               FROM im_message_index
              WHERE organization = ? AND sender_id = ? AND client_msg_id = ?
              LIMIT 1',
            [$organization, $senderId, $clientMsgId],
        );
        if ($index === null) {
            return null;
        }

        $table = (string) $index['shard_table'];
        $message = $this->repository->fetchOne(
            'SELECT * FROM ' . $this->messageShardRouter->quote($table) . '
              WHERE organization = ? AND message_id = ? AND delete_time IS NULL
              LIMIT 1',
            [$organization, (string) $index['message_id']],
        );
        if ($message === null) {
            throw new ImException('IM 消息索引存在但主体缺失', 'IM_INDEX_BODY_MISSING');
        }

        $message['_message_table'] = $table;
        $message['global_seq'] = (string) $index['global_seq'];

        return $message;
    }

    private function fetchConversationMessages(int $organization, string $conversationId, string $userId, int $afterSeq, int $limit): array
    {
        $candidates = $this->repository->fetchAll(
            'SELECT i.global_seq, i.message_id, i.message_seq, i.shard_table
               FROM im_message_index i
              WHERE i.organization = ?
                AND i.conversation_id = ?
                AND i.message_seq > ?
                AND EXISTS (
                    SELECT 1
                      FROM im_conversation_membership_period mp
                     WHERE mp.organization = i.organization
                       AND mp.conversation_id = i.conversation_id
                       AND mp.user_id = ?
                       AND mp.status = 1
                       AND i.message_seq >= mp.visible_from_message_seq
                       AND (mp.visible_until_message_seq IS NULL OR i.message_seq <= mp.visible_until_message_seq)
                )
              ORDER BY i.message_seq ASC
              LIMIT ' . ($limit + 1),
            [$organization, $conversationId, $afterSeq, $userId],
        );

        return $this->indexedMessagePage($organization, $userId, $candidates, $limit, $afterSeq, 0);
    }

    private function fetchGlobalMessages(int $organization, string $userId, int $afterGlobalSeq, int $limit): array
    {
        $candidates = $this->repository->fetchAll(
            'SELECT i.global_seq, i.message_id, i.message_seq, i.shard_table
               FROM im_message_index i
              WHERE i.organization = ?
                AND i.global_seq > ?
                AND EXISTS (
                    SELECT 1
                      FROM im_conversation_membership_period mp
                     WHERE mp.organization = i.organization
                       AND mp.conversation_id = i.conversation_id
                       AND mp.user_id = ?
                       AND mp.status = 1
                       AND i.message_seq >= mp.visible_from_message_seq
                       AND (mp.visible_until_message_seq IS NULL OR i.message_seq <= mp.visible_until_message_seq)
                )
              ORDER BY i.global_seq ASC
              LIMIT ' . ($limit + 1),
            [$organization, $afterGlobalSeq, $userId],
        );

        return $this->indexedMessagePage($organization, $userId, $candidates, $limit, 0, $afterGlobalSeq);
    }

    /**
     * Form the change_seq page before filtering target_user_id or membership
     * visibility so an invisible event can never pin a client's cursor.
     *
     * @return array{changes: list<array<string, mixed>>, next_after_change_seq: int, has_more: bool}
     */
    private function fetchConversationChanges(
        int $organization,
        string $conversationId,
        string $userId,
        int $afterChangeSeq,
        int $limit,
    ): array {
        $candidates = $this->repository->fetchAll(
            'SELECT conversation_id, change_seq, message_id, message_seq, change_type, target_user_id, payload_json, create_time
               FROM im_message_change
              WHERE organization = ? AND conversation_id = ? AND change_seq > ?
              ORDER BY change_seq ASC
              LIMIT ' . ($limit + 1),
            [$organization, $conversationId, $afterChangeSeq],
        );

        $hasMore = count($candidates) > $limit;
        $candidates = array_slice($candidates, 0, $limit);
        $lastCandidate = $candidates === [] ? null : end($candidates);
        $changes = [];

        foreach ($candidates as $candidate) {
            $targetUserId = trim((string) ($candidate['target_user_id'] ?? ''));
            if ($targetUserId !== '' && $targetUserId !== $userId) {
                continue;
            }
            if (!$this->isMessageSeqVisible(
                $organization,
                $conversationId,
                $userId,
                (int) $candidate['message_seq'],
            )) {
                continue;
            }

            $payload = json_decode((string) $candidate['payload_json'], true);
            if (!is_array($payload)) {
                throw new ImException('IM 变更流 payload 损坏', 'IM_CHANGE_PAYLOAD_INVALID');
            }
            $changes[] = [
                'conversation_id' => (string) $candidate['conversation_id'],
                'change_seq' => (int) $candidate['change_seq'],
                'change_type' => (string) $candidate['change_type'],
                'message_id' => (string) $candidate['message_id'],
                'message_seq' => (int) $candidate['message_seq'],
                'target_user_id' => $targetUserId === '' ? null : $targetUserId,
                'payload' => $payload,
                'create_time' => (string) $candidate['create_time'],
            ];
        }

        return [
            'changes' => $changes,
            'next_after_change_seq' => $lastCandidate === null
                ? $afterChangeSeq
                : (int) $lastCandidate['change_seq'],
            'has_more' => $hasMore,
        ];
    }

    /**
     * @param list<array<string, mixed>> $candidates
     * @return array{messages: list<array<string, mixed>>, next_after_seq: int, next_after_global_seq: int, has_more: bool}
     */
    private function indexedMessagePage(
        int $organization,
        string $userId,
        array $candidates,
        int $limit,
        int $afterSeq,
        int $afterGlobalSeq,
    ): array {
        $hasMore = count($candidates) > $limit;
        $candidates = array_slice($candidates, 0, $limit);
        $lastCandidate = $candidates === [] ? null : end($candidates);
        $messages = [];

        /** @var array<string, list<string>> $messageIdsByTable */
        $messageIdsByTable = [];
        foreach ($candidates as $candidate) {
            $table = (string) $candidate['shard_table'];
            $this->messageShardRouter->quote($table);
            $messageIdsByTable[$table][] = (string) $candidate['message_id'];
        }

        /** @var array<string, array<string, mixed>> $messageBodies */
        $messageBodies = [];
        foreach ($messageIdsByTable as $table => $messageIds) {
            $placeholders = implode(',', array_fill(0, count($messageIds), '?'));
            $rows = $this->repository->fetchAll(
                'SELECT * FROM ' . $this->messageShardRouter->quote($table) . '
                  WHERE organization = ? AND message_id IN (' . $placeholders . ')',
                array_merge([$organization], $messageIds),
            );
            foreach ($rows as $row) {
                $messageBodies[(string) $row['message_id']] = $row + ['_message_table' => $table];
            }
        }

        foreach ($candidates as $candidate) {
            $messageId = (string) $candidate['message_id'];
            $message = $messageBodies[$messageId] ?? null;
            if (!is_array($message)) {
                throw new ImException('IM 消息索引存在但主体缺失', 'IM_INDEX_BODY_MISSING');
            }

            if ((int) ($message['status'] ?? self::MESSAGE_NORMAL) === self::MESSAGE_DELETED_BOTH) {
                continue;
            }

            $deleted = $this->repository->fetchOne(
                'SELECT 1 AS deleted FROM im_message_user_delete
                  WHERE organization = ? AND message_id = ? AND user_id = ?
                  LIMIT 1',
                [$organization, $messageId, $userId],
            );
            if ($deleted !== null) {
                continue;
            }

            $message['global_seq'] = (string) $candidate['global_seq'];
            $messages[] = $message;
        }

        return [
            'messages' => $messages,
            'next_after_seq' => $lastCandidate === null ? $afterSeq : (int) $lastCandidate['message_seq'],
            'next_after_global_seq' => $lastCandidate === null ? $afterGlobalSeq : (int) $lastCandidate['global_seq'],
            'has_more' => $hasMore,
        ];
    }

    private function ensureSingleConversation(AuthContext $context, array $data): array
    {
        $toUserId = trim((string) ($data['to_user_id'] ?? $data['receiver_id'] ?? ''));
        if ($toUserId === '' || $toUserId === $context->userId) {
            throw new ImException('单聊接收人无效', 'SEND_SINGLE_RECEIVER_INVALID');
        }
        $this->assertActiveOrganizationUsers($context->organization, [$toUserId], 'SEND_SINGLE_RECEIVER_INVALID');
        $this->assertSingleRelationship($context->organization, $context->userId, $toUserId);

        $conversationId = self::singleConversationId($context->organization, $context->userId, $toUserId);
        $now = $this->now();
        $this->repository->execute(
            'INSERT INTO im_conversation
                (organization, conversation_id, conversation_type, title, owner_user_id, status, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, 1, ?, ?)
             ON DUPLICATE KEY UPDATE update_time = VALUES(update_time)',
            [$context->organization, $conversationId, self::CONVERSATION_SINGLE, '', $context->userId, $now, $now],
        );
        $conversation = $this->repository->fetchOne(
            'SELECT conversation_type, status FROM im_conversation
              WHERE organization = ? AND conversation_id = ? AND delete_time IS NULL
              LIMIT 1 FOR UPDATE',
            [$context->organization, $conversationId],
        );
        if ($conversation === null
            || (int) $conversation['conversation_type'] !== self::CONVERSATION_SINGLE
            || (int) $conversation['status'] !== 1) {
            throw new ImException('单聊会话不存在、已停用或类型无效', 'SEND_CONVERSATION_INACTIVE');
        }
        $this->ensureMember($context->organization, $conversationId, $context->userId, 'owner');
        $this->ensureMember($context->organization, $conversationId, $toUserId, 'member');

        return [
            'conversation_id' => $conversationId,
            'recipient_user_ids' => [$toUserId],
        ];
    }

    private function ensureGroupConversation(AuthContext $context, array $data): array
    {
        $conversationId = trim((string) ($data['conversation_id'] ?? ''));
        if ($conversationId === '') {
            throw new ImException('发送群消息必须指定已存在的群聊会话', 'SEND_GROUP_CONVERSATION_REQUIRED');
        }

        foreach (['member_ids', 'title', 'history_visibility', 'group_kind', 'display_member_count', 'description'] as $createField) {
            if (array_key_exists($createField, $data)) {
                throw new ImException('SEND 不允许创建或修改群聊', 'SEND_GROUP_CREATE_FORBIDDEN');
            }
        }

        $conversation = $this->repository->fetchOne(
            'SELECT * FROM im_conversation WHERE organization = ? AND conversation_id = ? AND delete_time IS NULL LIMIT 1 FOR UPDATE',
            [$context->organization, $conversationId],
        );
        if ($conversation === null) {
            throw new ImException('群聊会话不存在', 'SEND_GROUP_NOT_FOUND');
        }
        if ((int) $conversation['conversation_type'] !== self::CONVERSATION_GROUP) {
            throw new ImException('会话类型不是群聊', 'SEND_CONVERSATION_TYPE_MISMATCH');
        }
        if ((int) $conversation['status'] !== 1) {
            throw new ImException('群聊会话已停用', 'SEND_CONVERSATION_INACTIVE');
        }
        $this->assertMember($context->organization, $conversationId, $context->userId);
        $this->assertConversationWithoutSystemMember($context->organization, $conversationId);

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

    private function allocateGlobalSeq(int $organization): int
    {
        $row = $this->repository->fetchOne(
            'SELECT next_global_seq
               FROM im_organization_message_sequence
              WHERE organization = ?
              FOR UPDATE',
            [$organization],
        );
        if ($row === null) {
            throw new ImException('机构全局消息序号未初始化', 'GLOBAL_SEQ_NOT_INITIALIZED');
        }

        $globalSeq = max((int) $row['next_global_seq'], 1);
        $this->repository->execute(
            'UPDATE im_organization_message_sequence
                SET next_global_seq = ?, update_time = ?
              WHERE organization = ?',
            [$globalSeq + 1, $this->now(), $organization],
        );

        return $globalSeq;
    }

    private function ensureOrganizationSequence(int $organization): void
    {
        $now = $this->now();
        $this->repository->execute(
            'INSERT INTO im_organization_message_sequence
                (organization, next_global_seq, create_time, update_time)
             SELECT id, 1, ?, ?
               FROM sm_system_organization
              WHERE id = ? AND status = 1 AND delete_time IS NULL
             ON DUPLICATE KEY UPDATE organization = VALUES(organization)',
            [$now, $now, $organization],
        );

        $row = $this->repository->fetchOne(
            'SELECT organization FROM im_organization_message_sequence WHERE organization = ? LIMIT 1',
            [$organization],
        );
        if ($row === null) {
            throw new ImException('机构消息序号无法初始化', 'GLOBAL_SEQ_ORGANIZATION_INACTIVE');
        }
    }

    /**
     * @param array<string, mixed> $message
     * @param array<string, mixed> $payload
     */
    private function recordMessageChange(
        AuthContext $context,
        array $message,
        string $changeType,
        ?string $targetUserId,
        array $payload,
        string $eventType,
        string $now,
    ): int {
        $conversationId = (string) $message['conversation_id'];
        $conversation = $this->repository->fetchOne(
            'SELECT next_change_seq
               FROM im_conversation
              WHERE organization = ? AND conversation_id = ?
              FOR UPDATE',
            [$context->organization, $conversationId],
        );
        if ($conversation === null) {
            throw new ImException('会话不存在，无法分配变更序号', 'CONVERSATION_NOT_FOUND');
        }

        $changeSeq = max((int) $conversation['next_change_seq'], 1);
        $this->repository->execute(
            'UPDATE im_conversation
                SET next_change_seq = ?, last_change_seq = ?, update_time = ?
              WHERE organization = ? AND conversation_id = ?',
            [$changeSeq + 1, $changeSeq, $now, $context->organization, $conversationId],
        );
        $this->repository->execute(
            'INSERT INTO im_message_change
                (organization, conversation_id, change_seq, message_id, message_seq,
                 change_type, target_user_id, payload_json, create_time)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                $context->organization,
                $conversationId,
                $changeSeq,
                (string) $message['message_id'],
                (int) $message['message_seq'],
                $changeType,
                $targetUserId,
                json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                $now,
            ],
        );
        $realtimePayload = $payload;
        if ($eventType === Constants::MQ_ROUTING_MESSAGE_EDITED) {
            $realtimePayload['message'] = $this->formatMessage($message);
        }
        $this->outbox->createMessageChanged(
            context: $context,
            eventType: $eventType,
            messageId: (string) $message['message_id'],
            conversationId: $conversationId,
            conversationType: (int) $message['conversation_type'],
            messageSeq: (int) $message['message_seq'],
            changeSeq: $changeSeq,
            targetUserId: $targetUserId,
            payload: $realtimePayload,
        );

        return $changeSeq;
    }

    private function latestChangeSeq(
        int $organization,
        string $conversationId,
        string $messageId,
        string $changeType,
        ?string $targetUserId,
    ): int {
        $targetSql = $targetUserId === null ? 'target_user_id IS NULL' : 'target_user_id = ?';
        $params = [$organization, $conversationId, $messageId, $changeType];
        if ($targetUserId !== null) {
            $params[] = $targetUserId;
        }
        $row = $this->repository->fetchOne(
            'SELECT change_seq
               FROM im_message_change
              WHERE organization = ?
                AND conversation_id = ?
                AND message_id = ?
                AND change_type = ?
                AND ' . $targetSql . '
              ORDER BY change_seq DESC
              LIMIT 1',
            $params,
        );
        if ($row === null) {
            throw new ImException('消息状态与变更流不一致', 'IM_CHANGE_MISSING');
        }

        return (int) $row['change_seq'];
    }

    private function upsertReceipt(int $organization, string $conversationId, string $messageId, string $userId, int $status, ?string $deliveredTime, ?string $readTime): void
    {
        $now = $this->now();
        $this->repository->execute(
            'INSERT INTO im_message_receipt
                (organization, conversation_id, message_id, user_id, status, delivered_time, read_time, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
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

    /**
     * Caller must already run inside a transaction. The member row is locked
     * so old devices cannot regress the read cursor or overwrite the effective
     * unread count calculated from the canonical message index.
     *
     * @return array{last_read_message_id: string, last_read_seq: int, unread_count: int}
     */
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
                       AND mc.change_type = ?
                )',
            [$organization, $conversationId, $afterSeq, $userId, $userId, $userId, self::CHANGE_DELETE_BOTH],
        );

        return max(0, (int) ($row['aggregate'] ?? 0));
    }

    private function ensureMember(
        int $organization,
        string $conversationId,
        string $userId,
        string $memberRole,
        ?string $inviterUserId = null,
    ): void
    {
        $now = $this->now();
        $member = $this->repository->fetchOne(
            'SELECT id, status, delete_time
               FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ? AND user_id = ?
              LIMIT 1
              FOR UPDATE',
            [$organization, $conversationId, $userId],
        );

        if ($member === null) {
            $this->repository->execute(
                'INSERT INTO im_conversation_member
                    (organization, conversation_id, user_id, member_role, inviter_user_id, status,
                     mute_status, mute_until, access_version, join_at, create_time, update_time)
                 VALUES (?, ?, ?, ?, ?, 1, 0, NULL, 1, ?, ?, ?)',
                [$organization, $conversationId, $userId, $memberRole, $inviterUserId, $now, $now, $now],
            );
            $this->ensureMembershipPeriod($organization, $conversationId, $userId, $now);
            return;
        }

        if ((int) $member['status'] === 1 && empty($member['delete_time'])) {
            $this->ensureMembershipPeriod($organization, $conversationId, $userId, $now);
            return;
        }

        $this->repository->execute(
            'UPDATE im_conversation_member
                SET status = 1,
                    member_role = ?,
                    inviter_user_id = ?,
                    mute_status = 0,
                    mute_until = NULL,
                    access_version = access_version + 1,
                    join_at = ?,
                    delete_time = NULL,
                    update_time = ?
              WHERE id = ?',
            [$memberRole, $inviterUserId, $now, $now, (int) $member['id']],
        );
        $this->ensureMembershipPeriod($organization, $conversationId, $userId, $now);
    }

    private function ensureMembershipPeriod(int $organization, string $conversationId, string $userId, string $now): void
    {
        $openPeriod = $this->repository->fetchOne(
            'SELECT id
               FROM im_conversation_membership_period
              WHERE organization = ?
                AND conversation_id = ?
                AND user_id = ?
                AND status = 1
                AND visible_until_message_seq IS NULL
              LIMIT 1',
            [$organization, $conversationId, $userId],
        );
        if ($openPeriod !== null) {
            return;
        }

        $conversation = $this->repository->fetchOne(
            'SELECT c.next_message_seq, c.conversation_type, gp.history_visibility
               FROM im_conversation c
               LEFT JOIN im_group_profile gp
                 ON gp.organization = c.organization
                AND gp.conversation_id = c.conversation_id
                AND gp.status = 1
                AND gp.delete_time IS NULL
              WHERE c.organization = ? AND c.conversation_id = ?
              FOR UPDATE',
            [$organization, $conversationId],
        );
        if ($conversation === null) {
            throw new ImException('会话不存在，无法建立成员可见周期', 'CONVERSATION_NOT_FOUND');
        }
        $period = $this->repository->fetchOne(
            'SELECT COALESCE(MAX(period_no), 0) + 1 AS next_period_no
               FROM im_conversation_membership_period
              WHERE organization = ? AND conversation_id = ? AND user_id = ?',
            [$organization, $conversationId, $userId],
        );

        $this->repository->execute(
            'INSERT INTO im_conversation_membership_period
                (organization, conversation_id, user_id, period_no, visible_from_message_seq,
                 visible_until_message_seq, join_at, leave_at, status, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, 1, ?, ?)',
            [
                $organization,
                $conversationId,
                $userId,
                max((int) ($period['next_period_no'] ?? 1), 1),
                (int) $conversation['conversation_type'] === self::CONVERSATION_SINGLE
                    || (string) ($conversation['history_visibility'] ?? 'since_join') === 'all'
                    ? 1
                    : max((int) $conversation['next_message_seq'], 1),
                $now,
                $now,
                $now,
            ],
        );
    }

    private function assertMember(int $organization, string $conversationId, string $userId): void
    {
        $member = $this->repository->fetchOne(
            'SELECT id, mute_status, mute_until FROM im_conversation_member
              WHERE organization = ? AND conversation_id = ? AND user_id = ? AND status = 1 AND delete_time IS NULL LIMIT 1',
            [$organization, $conversationId, $userId],
        );
        if ($member === null) {
            throw new ImException('没有该会话的发送权限', 'CONVERSATION_MEMBER_FORBIDDEN');
        }
        if ((int) ($member['mute_status'] ?? 0) === 0) {
            return;
        }

        $muteUntil = trim((string) ($member['mute_until'] ?? ''));
        if ($muteUntil !== '' && strtotime($muteUntil) !== false && strtotime($muteUntil) <= time()) {
            $this->repository->execute(
                'UPDATE im_conversation_member
                    SET mute_status = 0, mute_until = NULL, update_time = ?
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
            'SELECT id FROM im_conversation_membership_period
              WHERE organization = ? AND conversation_id = ? AND user_id = ? AND status = 1 LIMIT 1',
            [$organization, $conversationId, $userId],
        );
        if ($member === null) {
            throw new ImException('没有该会话的访问权限', 'CONVERSATION_MEMBER_FORBIDDEN');
        }
    }

    private function isMessageSeqVisible(int $organization, string $conversationId, string $userId, int $messageSeq): bool
    {
        return $this->repository->fetchOne(
            'SELECT 1 AS visible
               FROM im_conversation_membership_period
              WHERE organization = ?
                AND conversation_id = ?
                AND user_id = ?
                AND status = 1
                AND ? >= visible_from_message_seq
                AND (visible_until_message_seq IS NULL OR ? <= visible_until_message_seq)
              LIMIT 1',
            [$organization, $conversationId, $userId, $messageSeq, $messageSeq],
        ) !== null;
    }

    private function findVisibleConversation(AuthContext $context, string $conversationId): array
    {
        $conversation = $this->repository->fetchOne(
            'SELECT c.* FROM im_conversation c
              INNER JOIN im_conversation_member cm
                ON cm.organization = c.organization
               AND cm.conversation_id = c.conversation_id
               AND cm.user_id = ?
               AND cm.status = 1
               AND cm.delete_time IS NULL
              WHERE c.organization = ?
                AND c.conversation_id = ?
                AND c.status = 1
                AND c.delete_time IS NULL
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

    /** @param list<string> $userIds */
    private function assertActiveOrganizationUsers(int $organization, array $userIds, string $errorCode): void
    {
        $userIds = array_values(array_unique(array_filter(
            array_map(static fn (mixed $userId): string => trim((string) $userId), $userIds),
            static fn (string $userId): bool => $userId !== '',
        )));
        if ($userIds === []) {
            throw new ImException('目标用户不能为空', $errorCode);
        }

        $placeholders = implode(',', array_fill(0, count($userIds), '?'));
        $rows = $this->repository->fetchAll(
            'SELECT user_id FROM im_user
              WHERE organization = ?
                AND user_id IN (' . $placeholders . ')
                AND status = 1
                AND delete_time IS NULL',
            array_merge([$organization], $userIds),
        );
        $active = array_fill_keys(array_map(
            static fn (array $row): string => (string) $row['user_id'],
            $rows,
        ), true);
        foreach ($userIds as $userId) {
            if (!isset($active[$userId])) {
                throw new ImException('目标用户不存在、已停用或不属于当前机构', $errorCode);
            }
        }
    }

    private function assertSingleRelationship(int $organization, string $senderId, string $recipientId): void
    {
        $target = $this->repository->fetchOne(
            'SELECT is_system FROM im_user
              WHERE organization = ? AND user_id = ? AND status = 1 AND delete_time IS NULL LIMIT 1',
            [$organization, $recipientId],
        );
        if ((int) ($target['is_system'] ?? 2) === 1) {
            return;
        }

        $relations = $this->repository->fetchAll(
            'SELECT user_id, friend_user_id, status
               FROM im_friend_relation
              WHERE organization = ?
                AND delete_time IS NULL
                AND ((user_id = ? AND friend_user_id = ?)
                  OR (user_id = ? AND friend_user_id = ?))',
            [$organization, $senderId, $recipientId, $recipientId, $senderId],
        );
        $directions = [];
        foreach ($relations as $relation) {
            if ((int) $relation['status'] !== 1) {
                throw new ImException('好友关系已被拉黑或停用', 'SEND_SINGLE_RELATION_BLOCKED');
            }
            $directions[(string) $relation['user_id'] . '>' . (string) $relation['friend_user_id']] = true;
        }
        if (!isset($directions[$senderId . '>' . $recipientId], $directions[$recipientId . '>' . $senderId])) {
            throw new ImException('建立双向好友关系后才能发送单聊消息', 'SEND_SINGLE_FRIEND_REQUIRED');
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
                AND cm.status = 1
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
            'SELECT cm.user_id
               FROM im_conversation_member cm
               INNER JOIN im_user u
                  ON u.organization = cm.organization
                 AND u.user_id = cm.user_id
                 AND u.status = 1
                 AND u.delete_time IS NULL
              WHERE cm.organization = ?
                AND cm.conversation_id = ?
                AND cm.status = 1
                AND cm.delete_time IS NULL',
            [$organization, $conversationId],
        );

        return array_values(array_map(static fn (array $row): string => (string) $row['user_id'], $rows));
    }

    private function findVisibleMessage(AuthContext $context, string $messageId, bool $forUpdate = false): array
    {
        $index = $this->repository->fetchOne(
            'SELECT shard_table, global_seq FROM im_message_index
              WHERE organization = ? AND message_id = ?
              LIMIT 1' . ($forUpdate ? ' FOR UPDATE' : ''),
            [$context->organization, $messageId],
        );
        if ($index === null) {
            throw new ImException('消息不存在或无权访问', 'MESSAGE_NOT_FOUND');
        }

        $table = (string) $index['shard_table'];
        $message = $this->repository->fetchOne(
            'SELECT m.* FROM ' . $this->messageShardRouter->quote($table) . ' m
              INNER JOIN im_conversation c
                 ON c.organization = m.organization
                AND c.conversation_id = m.conversation_id
                AND c.status = 1
                AND c.delete_time IS NULL
              INNER JOIN im_conversation_member cm
                ON cm.organization = m.organization
               AND cm.conversation_id = m.conversation_id
               AND cm.user_id = ?
               AND cm.status = 1
               AND cm.delete_time IS NULL
              WHERE m.organization = ? AND m.message_id = ? AND m.delete_time IS NULL
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
                       AND ud.message_id = m.message_id
                       AND ud.user_id = ?
                )
              LIMIT 1',
            [$context->userId, $context->organization, $messageId, $context->userId, $context->userId],
        );
        if ($message !== null) {
            $message['_message_table'] = $table;
            $message['global_seq'] = (string) $index['global_seq'];
            return $message;
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

    private function normalizeGlobalSeq(mixed $value): int
    {
        if (!is_string($value) || preg_match('/^(0|[1-9][0-9]*)$/', $value) !== 1) {
            throw new ImException('after_global_seq 必须是十进制字符串', 'SYNC_GLOBAL_SEQ_INVALID');
        }

        $normalized = ltrim($value, '0');
        $normalized = $normalized === '' ? '0' : $normalized;
        $max = (string) PHP_INT_MAX;
        if (strlen($normalized) > strlen($max) || (strlen($normalized) === strlen($max) && strcmp($normalized, $max) > 0)) {
            throw new ImException('after_global_seq 超出服务端整数范围', 'SYNC_GLOBAL_SEQ_INVALID');
        }

        return (int) $normalized;
    }

    private function isClientMessageIdempotencyConflict(PDOException $exception): bool
    {
        if ((string) ($exception->errorInfo[0] ?? '') !== '23000' || (int) ($exception->errorInfo[1] ?? 0) !== 1062) {
            return false;
        }

        return str_contains((string) ($exception->errorInfo[2] ?? $exception->getMessage()), 'uni_organization_client_msg');
    }

    private function normalizeContent(AuthContext $context, int $messageType, mixed $content): array
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

        $assetKind = match ($messageType) {
            MessageType::IMAGE => 'image',
            MessageType::FILE => 'file',
            MessageType::VOICE => 'voice',
            MessageType::VIDEO => 'video',
            default => null,
        };
        if ($assetKind !== null) {
            $fileId = trim((string) ($content['file_id'] ?? ''));
            if (preg_match('/^[a-f0-9]{40}$/', $fileId) !== 1) {
                throw new ImException('附件 file_id 无效', 'SEND_ASSET_FILE_ID_INVALID');
            }
            $asset = $this->repository->fetchOne(
                'SELECT file_id, kind, name, size_byte, mime_type, extension
                   FROM im_upload_asset
                  WHERE organization = ? AND user_id = ? AND file_id = ?
                    AND status = 1 AND delete_time IS NULL
                  LIMIT 1',
                [$context->organization, $context->userId, $fileId],
            );
            if ($asset === null || (string) $asset['kind'] !== $assetKind) {
                throw new ImException('附件不存在或不属于当前机构用户', 'SEND_ASSET_FORBIDDEN');
            }

            return [
                'file_id' => (string) $asset['file_id'],
                'name' => (string) $asset['name'],
                'size' => (int) $asset['size_byte'],
                'mime_type' => (string) $asset['mime_type'],
                'extension' => (string) $asset['extension'],
            ];
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
        $globalSeq = $this->allocateGlobalSeq($context->organization);
        $systemClientMsgId = 'system-' . $messageId;
        $this->createMessageIndex(
            organization: $context->organization,
            conversationId: $conversationId,
            messageId: $messageId,
            messageSeq: $messageSeq,
            globalSeq: $globalSeq,
            senderId: self::SYSTEM_NOTIFICATION_USER_ID,
            clientMsgId: $systemClientMsgId,
            messageTable: $messageTable,
            now: $now,
        );
        $message['global_seq'] = (string) $globalSeq;

        $recipientUserIds = array_values(array_filter(
            $this->conversationMembers($context->organization, $conversationId),
            static fn (string $userId): bool => $userId !== $context->userId,
        ));
        $this->createReceipts($context, $conversationId, $messageId, $recipientUserIds);
        $this->increaseUnread($context->organization, $conversationId, $recipientUserIds);
        $formattedMessage = $this->formatMessage($message);
        $this->outbox->createMessageCreated($context, $formattedMessage, $recipientUserIds);

        $conversationState = $this->refreshConversationLastMessage($context->organization, $conversationId, $now);

        return [
            'message' => $formattedMessage,
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
        $status = match ((int) $message['status']) {
            self::MESSAGE_NORMAL => 'normal',
            self::MESSAGE_RECALLED => 'recalled',
            self::MESSAGE_DELETED_BOTH => 'deleted_both',
            default => throw new ImException('IM 消息状态无效', 'IM_MESSAGE_STATUS_INVALID'),
        };

        return [
            'organization' => (int) $message['organization'],
            'conversation_id' => (string) $message['conversation_id'],
            'conversation_type' => (int) $message['conversation_type'],
            'message_id' => (string) $message['message_id'],
            'message_seq' => (int) $message['message_seq'],
            'global_seq' => (string) ($message['global_seq'] ?? throw new ImException(
                'IM 消息缺少 global_seq',
                'IM_GLOBAL_SEQ_MISSING',
            )),
            'client_msg_id' => (string) $message['client_msg_id'],
            'sender_id' => $senderId,
            'sender_user' => $this->formatMessageSender((int) $message['organization'], $senderId),
            'message_type' => (int) $message['message_type'],
            'content' => $status === 'normal' && is_array($content) ? $content : null,
            'status' => $status,
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
            'SELECT u.id, u.user_id, u.account, u.nickname, p.signature, u.avatar, u.mobile,
                    u.im_short_no, u.gender, u.status, u.remark, u.login_time, u.is_system, u.system_code
               FROM im_user u
               LEFT JOIN im_user_profile p
                 ON p.organization = u.organization
                AND p.user_id = u.user_id
                AND p.status = 1
                AND p.delete_time IS NULL
              WHERE u.user_id = ?
                AND u.delete_time IS NULL
                AND ((u.organization = ? AND u.is_system = 2) OR (u.organization = 0 AND u.is_system = 1))
              LIMIT 1',
            [$senderId, $organization],
        );
        if ($row === null) {
            return null;
        }

        $status = (int) ($row['status'] ?? 1);
        $avatarFileId = trim((string) ($row['avatar'] ?? ''));
        if ($avatarFileId !== '' && preg_match('/^[a-f0-9]{40}$/', $avatarFileId) !== 1) {
            throw new ImException('消息发送者头像引用无效', 'IM_AVATAR_FILE_ID_INVALID');
        }

        return [
            'id' => (string) ($row['id'] ?? ''),
            'user_id' => (string) ($row['user_id'] ?? ''),
            'account' => (string) ($row['account'] ?? ''),
            'nickname' => (string) ($row['nickname'] ?? ''),
            'signature' => (string) ($row['signature'] ?? ''),
            'avatar_file_id' => $avatarFileId,
            // 签名 URL 只能由受认证的 Server HTTP 投影生成，IM 不持有对象存储签名能力。
            'avatar_url' => '',
            'avatar_expires_at' => 0,
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

    private function messageNoticeEnabled(int $organization, string $event, int $conversationType): bool
    {
        if ($event === self::NOTICE_RECALL) {
            $policy = $this->tenantImPolicies->policy($organization);

            return $policy->recallNoticeEnabled
                && ($conversationType !== self::CONVERSATION_GROUP || $policy->groupRecallNoticeEnabled);
        }

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
            'expire_at' => time() + 30,
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
