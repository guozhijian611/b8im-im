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

    public function createMessageCreated(AuthContext $context, array $message, array $recipientUserIds): void
    {
        $now = $this->now();
        $payload = [
            'event_type' => Constants::MQ_ROUTING_MESSAGE_CREATED,
            'organization' => $context->organization,
            'message_id' => (string) $message['message_id'],
            'message_seq' => (int) $message['message_seq'],
            'conversation_id' => (string) $message['conversation_id'],
            'conversation_type' => (int) $message['conversation_type'],
            'sender_id' => $context->userId,
            'recipient_count' => count($recipientUserIds),
            'created_at' => (string) $message['create_time'],
        ];

        $this->repository->execute(
            'INSERT INTO im_message_outbox
                (organization, event_type, routing_key, message_id, conversation_id, conversation_type, payload, status, retry_count, next_retry_time, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
             ON DUPLICATE KEY UPDATE update_time = VALUES(update_time)',
            [
                $context->organization,
                Constants::MQ_ROUTING_MESSAGE_CREATED,
                Constants::MQ_ROUTING_MESSAGE_CREATED,
                (string) $message['message_id'],
                (string) $message['conversation_id'],
                (int) $message['conversation_type'],
                json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                self::STATUS_PENDING,
                $now,
                $now,
                $now,
            ],
        );
    }

    public function claimPending(int $limit): array
    {
        $now = $this->now();
        $staleTime = date('Y-m-d H:i:s', time() - $this->config->mqOutboxLockTtlSeconds);
        $rows = $this->repository->fetchAll(
            'SELECT * FROM im_message_outbox
              WHERE retry_count < ?
                AND (
                    (status IN (?, ?) AND (next_retry_time IS NULL OR next_retry_time <= ?))
                    OR (status = ? AND locked_time <= ?)
                )
              ORDER BY id ASC
              LIMIT ' . max(1, $limit),
            [
                $this->config->mqOutboxMaxRetry,
                self::STATUS_PENDING,
                self::STATUS_FAILED,
                $now,
                self::STATUS_PROCESSING,
                $staleTime,
            ],
        );

        $claimed = [];
        foreach ($rows as $row) {
            $affected = $this->repository->execute(
                'UPDATE im_message_outbox
                    SET status = ?, locked_time = ?, update_time = ?
                  WHERE id = ?
                    AND retry_count < ?
                    AND (
                        (status IN (?, ?) AND (next_retry_time IS NULL OR next_retry_time <= ?))
                        OR (status = ? AND locked_time <= ?)
                    )',
                [
                    self::STATUS_PROCESSING,
                    $now,
                    $now,
                    (int) $row['id'],
                    $this->config->mqOutboxMaxRetry,
                    self::STATUS_PENDING,
                    self::STATUS_FAILED,
                    $now,
                    self::STATUS_PROCESSING,
                    $staleTime,
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

    public function markPublished(int $id): void
    {
        $now = $this->now();
        $this->repository->execute(
            'UPDATE im_message_outbox
                SET status = ?, published_time = ?, locked_time = NULL, last_error = NULL, update_time = ?
              WHERE id = ?',
            [self::STATUS_PUBLISHED, $now, $now, $id],
        );
    }

    public function markFailed(int $id, string $error): void
    {
        $row = $this->repository->fetchOne('SELECT retry_count FROM im_message_outbox WHERE id = ? LIMIT 1', [$id]);
        $retryCount = ((int) ($row['retry_count'] ?? 0)) + 1;
        $delay = min($this->config->mqOutboxRetryDelaySeconds * (2 ** min($retryCount - 1, 6)), 3600);
        $nextRetryTime = $retryCount >= $this->config->mqOutboxMaxRetry
            ? null
            : date('Y-m-d H:i:s', time() + $delay);

        $this->repository->execute(
            'UPDATE im_message_outbox
                SET status = ?, retry_count = ?, next_retry_time = ?, locked_time = NULL, last_error = ?, update_time = ?
              WHERE id = ?',
            [
                self::STATUS_FAILED,
                $retryCount,
                $nextRetryTime,
                mb_substr($error, 0, 500),
                $this->now(),
                $id,
            ],
        );
    }

    private function now(): string
    {
        return date('Y-m-d H:i:s');
    }
}
