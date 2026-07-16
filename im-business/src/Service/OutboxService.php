<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 消息 Outbox 事件
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Support\Constants;
use B8im\ImBusiness\Telemetry\Telemetry;
use OpenTelemetry\API\Trace\SpanKind;

final class OutboxService
{
    private const STATUS_PENDING = 1;
    private const STATUS_PROCESSING = 2;
    private const STATUS_PUBLISHED = 3;
    private const STATUS_FAILED = 4;

    public function __construct(
        private readonly ImRepository $repository,
        private readonly Config $config,
    ) {
    }

    /**
     * @param list<string> $recipientUserIds
     * @param array<string, int> $recipientHomes user_id => home organization
     */
    public function createMessageCreated(
        AuthContext $context,
        array $message,
        array $recipientUserIds,
        array $recipientHomes = [],
    ): void {
        $now = $this->now();
        $homes = [];
        foreach ($recipientUserIds as $userId) {
            $uid = (string) $userId;
            $homes[$uid] = (int) ($recipientHomes[$uid] ?? $context->organization);
        }
        $payload = [
            'event_type' => Constants::MQ_ROUTING_MESSAGE_CREATED,
            'organization' => $context->organization,
            'message_id' => (string) $message['message_id'],
            'message_seq' => (int) $message['message_seq'],
            'global_seq' => (string) $message['global_seq'],
            'conversation_id' => (string) $message['conversation_id'],
            'conversation_type' => (int) $message['conversation_type'],
            'sender_id' => (string) ($message['sender_id'] ?? $context->userId),
            'actor_user_id' => $context->userId,
            'origin_user_id' => $context->userId,
            'origin_client_id' => $context->clientId,
            'recipient_count' => count($recipientUserIds),
            'recipient_user_ids' => array_values(array_map('strval', $recipientUserIds)),
            'recipient_homes' => $homes,
            'message' => $message,
            'created_at' => (string) $message['create_time'],
        ];

        Telemetry::run(
            'im.outbox.insert',
            function () use ($context, $message, $payload, $now): int {
                $trace = Telemetry::currentTraceContext();
                return $this->repository->execute(
                    'INSERT INTO im_message_outbox
                        (organization, event_type, routing_key, message_id, change_seq, conversation_id, conversation_type, payload_json, traceparent, tracestate, status, retry_count, next_retry_at, create_time, update_time)
                     VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)',
                    [
                        $context->organization,
                        Constants::MQ_ROUTING_MESSAGE_CREATED,
                        Constants::MQ_ROUTING_MESSAGE_CREATED,
                        (string) $message['message_id'],
                        (string) $message['conversation_id'],
                        (int) $message['conversation_type'],
                        json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                        $trace?->traceparent,
                        $trace?->tracestate,
                        self::STATUS_PENDING,
                        $now,
                        $now,
                        $now,
                    ],
                );
            },
            SpanKind::KIND_CLIENT,
            [
                'operation' => 'im.outbox.insert',
                'db.system.name' => 'mysql',
                'db.operation.name' => 'INSERT',
                'db.collection.name' => 'im_message_outbox',
                'b8im.organization' => $context->organization,
                'b8im.message_id' => (string) $message['message_id'],
                'messaging.destination.name' => Constants::MQ_ROUTING_MESSAGE_CREATED,
            ],
        );
    }

    /**
     * Persist a reliable message mutation event in the same transaction as the
     * message body and im_message_change row. The payload intentionally carries
     * no recalled or deleted message body.
     *
     * @param array<string, mixed> $payload
     */
    public function createMessageChanged(
        AuthContext $context,
        string $eventType,
        string $messageId,
        string $conversationId,
        int $conversationType,
        int $messageSeq,
        int $changeSeq,
        ?string $targetUserId,
        array $payload,
    ): void {
        $now = $this->now();
        $eventPayload = [
            'event_type' => $eventType,
            'organization' => $context->organization,
            'conversation_id' => $conversationId,
            'conversation_type' => $conversationType,
            'message_id' => $messageId,
            'message_seq' => $messageSeq,
            'change_seq' => $changeSeq,
            'target_user_id' => $targetUserId,
            'actor_user_id' => $context->userId,
            'origin_user_id' => $context->userId,
            'origin_client_id' => $context->clientId,
            'payload' => $payload,
            'created_at' => $now,
        ];

        Telemetry::run(
            'im.outbox.insert',
            function () use ($context, $eventType, $messageId, $changeSeq, $conversationId, $conversationType, $eventPayload, $now): int {
                $trace = Telemetry::currentTraceContext();
                return $this->repository->execute(
                    'INSERT INTO im_message_outbox
                        (organization, event_type, routing_key, message_id, change_seq, conversation_id, conversation_type, payload_json, traceparent, tracestate, status, retry_count, next_retry_at, create_time, update_time)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)',
                    [
                        $context->organization,
                        $eventType,
                        $eventType,
                        $messageId,
                        $changeSeq,
                        $conversationId,
                        $conversationType,
                        json_encode($eventPayload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                        $trace?->traceparent,
                        $trace?->tracestate,
                        self::STATUS_PENDING,
                        $now,
                        $now,
                        $now,
                    ],
                );
            },
            SpanKind::KIND_CLIENT,
            [
                'operation' => 'im.outbox.insert',
                'db.system.name' => 'mysql',
                'db.operation.name' => 'INSERT',
                'db.collection.name' => 'im_message_outbox',
                'b8im.organization' => $context->organization,
                'b8im.message_id' => $messageId,
                'messaging.destination.name' => $eventType,
            ],
        );
    }

    public function claimPending(int $limit, string $workerId): array
    {
        $workerId = trim($workerId);
        if ($workerId === '' || strlen($workerId) > 64) {
            throw new \InvalidArgumentException('outbox worker_id must contain 1..64 bytes');
        }

        $now = $this->now();
        $lockedUntil = date('Y-m-d H:i:s', time() + $this->config->mqOutboxLockTtlSeconds);
        $limit = max(1, min(1000, $limit));
        $rows = $this->repository->fetchAll(
            'SELECT * FROM im_message_outbox
              WHERE retry_count < ?
                AND (
                    (status IN (?, ?) AND (next_retry_at IS NULL OR next_retry_at <= ?))
                    OR (status = ? AND locked_until <= ?)
                )
              ORDER BY id ASC
              LIMIT ' . $limit,
            [
                $this->config->mqOutboxMaxRetry,
                self::STATUS_PENDING,
                self::STATUS_FAILED,
                $now,
                self::STATUS_PROCESSING,
                $now,
            ],
        );

        $claimed = [];
        foreach ($rows as $row) {
            $claimToken = bin2hex(random_bytes(20));
            $affected = $this->repository->execute(
                'UPDATE im_message_outbox
                    SET status = ?, worker_id = ?, claim_token = ?, locked_until = ?, update_time = ?
                  WHERE id = ?
                    AND retry_count < ?
                    AND (
                        (status IN (?, ?) AND (next_retry_at IS NULL OR next_retry_at <= ?))
                        OR (status = ? AND locked_until <= ?)
                    )',
                [
                    self::STATUS_PROCESSING,
                    $workerId,
                    $claimToken,
                    $lockedUntil,
                    $now,
                    (int) $row['id'],
                    $this->config->mqOutboxMaxRetry,
                    self::STATUS_PENDING,
                    self::STATUS_FAILED,
                    $now,
                    self::STATUS_PROCESSING,
                    $now,
                ],
            );
            if ($affected !== 1) {
                continue;
            }

            $claimedRow = $this->repository->fetchOne('SELECT * FROM im_message_outbox WHERE id = ? LIMIT 1', [(int) $row['id']]);
            if ($claimedRow !== null) {
                $claimed[] = $claimedRow;
            }
        }

        return $claimed;
    }

    public function markPublished(int $id, string $claimToken): void
    {
        $now = $this->now();
        $affected = $this->repository->execute(
            'UPDATE im_message_outbox
                SET status = ?, published_at = ?, locked_until = NULL, worker_id = NULL,
                    claim_token = NULL, last_error = NULL, update_time = ?
              WHERE id = ? AND status = ? AND claim_token = ?',
            [self::STATUS_PUBLISHED, $now, $now, $id, self::STATUS_PROCESSING, $claimToken],
        );
        if ($affected !== 1) {
            throw new \RuntimeException('outbox publish result rejected because the claim is no longer current');
        }
    }

    public function markFailed(int $id, string $claimToken, string $error): void
    {
        $row = $this->repository->fetchOne(
            'SELECT retry_count FROM im_message_outbox
              WHERE id = ? AND status = ? AND claim_token = ? LIMIT 1',
            [$id, self::STATUS_PROCESSING, $claimToken],
        );
        if ($row === null) {
            throw new \RuntimeException('outbox failure result rejected because the claim is no longer current');
        }
        $retryCount = ((int) ($row['retry_count'] ?? 0)) + 1;
        $delay = min($this->config->mqOutboxRetryDelaySeconds * (2 ** min($retryCount - 1, 6)), 3600);
        $nextRetryAt = $retryCount >= $this->config->mqOutboxMaxRetry
            ? null
            : date('Y-m-d H:i:s', time() + $delay);

        $affected = $this->repository->execute(
            'UPDATE im_message_outbox
                SET status = ?, retry_count = ?, next_retry_at = ?, locked_until = NULL,
                    worker_id = NULL, claim_token = NULL, last_error = ?, update_time = ?
              WHERE id = ? AND status = ? AND claim_token = ?',
            [
                self::STATUS_FAILED,
                $retryCount,
                $nextRetryAt,
                mb_substr($error, 0, 500),
                $this->now(),
                $id,
                self::STATUS_PROCESSING,
                $claimToken,
            ],
        );
        if ($affected !== 1) {
            throw new \RuntimeException('outbox failure result lost its claim before commit');
        }
    }

    private function now(): string
    {
        return date('Y-m-d H:i:s');
    }
}
