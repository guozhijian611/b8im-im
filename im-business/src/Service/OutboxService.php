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
use B8im\ImBusiness\Realtime\ConversationReadEventId;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Protocol\Dto\CanonicalDecimal;
use B8im\ImShared\Protocol\Dto\SearchProjectionEvent;
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

    /** @param list<array{organization:int,user_id:string}> $recipientIdentities */
    public function createMessageCreated(
        AuthContext $context,
        int $homeOrganization,
        array $message,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId = null,
    ): void {
        $now = $this->now();
        $eventId = $this->eventId([
            $homeOrganization,
            Constants::MQ_ROUTING_MESSAGE_CREATED,
            (string) $message['message_id'],
        ]);
        $payload = [
            'event_id' => $eventId,
            'event_type' => Constants::MQ_ROUTING_MESSAGE_CREATED,
            'organization' => $homeOrganization,
            'message_id' => (string) $message['message_id'],
            'message_seq' => (int) $message['message_seq'],
            'global_seq' => (string) $message['global_seq'],
            'conversation_id' => (string) $message['conversation_id'],
            'conversation_type' => (int) $message['conversation_type'],
            'sender_id' => (string) $message['sender_id'],
            'sender_organization' => (int) $message['sender_organization'],
            'actor_user_id' => $context->userId,
            'actor_organization' => $context->organization,
            'origin_user_id' => $context->userId,
            'origin_organization' => $context->organization,
            'origin_client_id' => $context->clientId,
            'recipient_count' => count($recipientIdentities),
            'recipient_identities' => $this->identities($recipientIdentities),
            'message' => $message,
            'created_at' => (string) $message['create_time'],
            ...$this->accessSnapshotPayload($crossOrgAccessSnapshotId),
        ];

        $this->insert(
            eventId: $eventId,
            organization: $homeOrganization,
            eventType: Constants::MQ_ROUTING_MESSAGE_CREATED,
            messageId: (string) $message['message_id'],
            changeSeq: 0,
            conversationId: (string) $message['conversation_id'],
            conversationType: (int) $message['conversation_type'],
            payload: $payload,
            now: $now,
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
        int $homeOrganization,
        string $eventType,
        string $messageId,
        string $conversationId,
        int $conversationType,
        int $messageSeq,
        int $changeSeq,
        ?int $targetOrganization,
        ?string $targetUserId,
        array $payload,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId = null,
    ): void {
        $now = $this->now();
        $eventId = $this->eventId([
            $homeOrganization,
            $eventType,
            $messageId,
            $changeSeq,
        ]);
        $eventPayload = [
            'event_id' => $eventId,
            'event_type' => $eventType,
            'organization' => $homeOrganization,
            'conversation_id' => $conversationId,
            'conversation_type' => $conversationType,
            'message_id' => $messageId,
            'message_seq' => $messageSeq,
            'change_seq' => $changeSeq,
            'target_organization' => $targetOrganization,
            'target_user_id' => $targetUserId,
            'actor_user_id' => $context->userId,
            'actor_organization' => $context->organization,
            'origin_user_id' => $context->userId,
            'origin_organization' => $context->organization,
            'origin_client_id' => $context->clientId,
            'recipient_count' => count($recipientIdentities),
            'recipient_identities' => $this->identities($recipientIdentities),
            'payload' => $payload,
            'created_at' => $now,
            ...$this->accessSnapshotPayload($crossOrgAccessSnapshotId),
        ];

        $this->insert(
            eventId: $eventId,
            organization: $homeOrganization,
            eventType: $eventType,
            messageId: $messageId,
            changeSeq: $changeSeq,
            conversationId: $conversationId,
            conversationType: $conversationType,
            payload: $eventPayload,
            now: $now,
        );
    }

    /** @param list<array{organization:int,user_id:string}> $recipientIdentities */
    public function createMessageReceipt(
        AuthContext $context,
        int $homeOrganization,
        array $receipt,
        int $conversationType,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId = null,
    ): void {
        $status = (string) $receipt['status'];
        $eventId = $this->eventId([
            $homeOrganization,
            Constants::MQ_ROUTING_MESSAGE_RECEIPT,
            (string) $receipt['message_id'],
            $context->organization,
            $context->userId,
            $status,
        ]);
        $payload = [
            'event_id' => $eventId,
            'event_type' => Constants::MQ_ROUTING_MESSAGE_RECEIPT,
            'organization' => $homeOrganization,
            'conversation_id' => (string) $receipt['conversation_id'],
            'conversation_type' => $conversationType,
            'message_id' => (string) $receipt['message_id'],
            'message_seq' => (int) $receipt['message_seq'],
            'change_seq' => 0,
            'sender_organization' => (int) $receipt['sender_organization'],
            'sender_id' => (string) $receipt['sender_id'],
            'user_organization' => $context->organization,
            'user_id' => $context->userId,
            'actor_organization' => $context->organization,
            'actor_user_id' => $context->userId,
            'origin_organization' => $context->organization,
            'origin_user_id' => $context->userId,
            'origin_client_id' => $context->clientId,
            'recipient_count' => count($recipientIdentities),
            'recipient_identities' => $this->identities($recipientIdentities),
            'receipt' => $receipt,
            'created_at' => (string) $receipt['time'],
            ...$this->accessSnapshotPayload($crossOrgAccessSnapshotId),
        ];
        $this->insert(
            eventId: $eventId,
            organization: $homeOrganization,
            eventType: Constants::MQ_ROUTING_MESSAGE_RECEIPT,
            messageId: (string) $receipt['message_id'],
            changeSeq: 0,
            conversationId: (string) $receipt['conversation_id'],
            conversationType: $conversationType,
            payload: $payload,
            now: (string) $receipt['time'],
        );
    }

    /** @param list<array{organization:int,user_id:string}> $recipientIdentities */
    public function createConversationRead(
        AuthContext $context,
        int $homeOrganization,
        array $readState,
        int $conversationType,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId = null,
    ): void {
        $eventId = ConversationReadEventId::generate(
            $homeOrganization,
            (string) $readState['conversation_id'],
            $context->organization,
            $context->userId,
            (int) $readState['last_read_seq'],
            $crossOrgAccessSnapshotId,
        );
        $payload = [
            'event_id' => $eventId,
            'event_type' => Constants::MQ_ROUTING_CONVERSATION_READ,
            'organization' => $homeOrganization,
            'conversation_id' => (string) $readState['conversation_id'],
            'conversation_type' => $conversationType,
            'message_id' => (string) $readState['last_read_message_id'],
            'message_seq' => (int) $readState['last_read_seq'],
            'change_seq' => 0,
            'user_organization' => $context->organization,
            'user_id' => $context->userId,
            'actor_organization' => $context->organization,
            'actor_user_id' => $context->userId,
            'origin_organization' => $context->organization,
            'origin_user_id' => $context->userId,
            'origin_client_id' => $context->clientId,
            'recipient_count' => count($recipientIdentities),
            'recipient_identities' => $this->identities($recipientIdentities),
            'read_state' => $readState,
            'created_at' => (string) $readState['time'],
            ...$this->accessSnapshotPayload($crossOrgAccessSnapshotId),
        ];
        $this->insert(
            eventId: $eventId,
            organization: $homeOrganization,
            eventType: Constants::MQ_ROUTING_CONVERSATION_READ,
            messageId: (string) $readState['last_read_message_id'],
            changeSeq: 0,
            conversationId: (string) $readState['conversation_id'],
            conversationType: $conversationType,
            payload: $payload,
            now: (string) $readState['time'],
        );
    }

    /**
     * Lock every organization search sequence before message body/change facts.
     * Callers may pass cross-home organizations in any order; the database lock
     * order is always ascending organization.
     *
     * @param list<int> $organizations
     */
    public function lockSearchProjectionSequences(array $organizations): void
    {
        if (!$this->repository->inTransaction()) {
            throw new \RuntimeException(
                'search projection sequence locks require the enclosing fact transaction',
            );
        }
        $organizations = array_values(array_unique(array_map('intval', $organizations)));
        sort($organizations, SORT_NUMERIC);
        if ($organizations === [] || $organizations[0] <= 0) {
            throw new \InvalidArgumentException('search projection organizations must be positive');
        }

        foreach ($organizations as $organization) {
            $row = $this->repository->fetchOne(
                'SELECT CAST(last_search_event_seq AS CHAR) AS last_search_event_seq
                   FROM im_organization_message_sequence
                  WHERE organization = ?
                  FOR UPDATE',
                [$organization],
            );
            if ($row === null) {
                throw new \RuntimeException(
                    'search projection sequence owner is missing for organization ' . $organization,
                );
            }
            CanonicalDecimal::nonNegative(
                (string) ($row['last_search_event_seq'] ?? ''),
                'last_search_event_seq',
            );
        }
    }

    /** @param array<string, mixed> $payload */
    private function insert(
        string $eventId,
        int $organization,
        string $eventType,
        string $messageId,
        int $changeSeq,
        string $conversationId,
        int $conversationType,
        array $payload,
        string $now,
    ): void {
        Telemetry::run(
            'im.outbox.insert',
            function () use (
                $eventId,
                $organization,
                $eventType,
                $messageId,
                $changeSeq,
                $conversationId,
                $conversationType,
                $payload,
                $now,
            ): int {
                $isSearchProjection = in_array(
                    $eventType,
                    SearchProjectionEvent::EVENT_TYPES,
                    true,
                );
                if ($isSearchProjection) {
                    // This is intentionally repeated even when MessageService
                    // already holds the row. It makes direct callers fail
                    // closed and serializes duplicate event_id checks before
                    // any source_event_seq allocation.
                    $this->lockSearchProjectionSequences([$organization]);

                    $existing = $this->existingEvent($eventId);
                    if ($existing !== null) {
                        $this->assertExistingIdentity(
                            $existing,
                            $organization,
                            $eventType,
                            $messageId,
                            $changeSeq,
                            $conversationId,
                            $conversationType,
                            true,
                        );
                        return 0;
                    }
                }

                $sourceEventSeq = $isSearchProjection
                    ? $this->allocateSearchProjectionSequence($organization)
                    : null;
                if ($sourceEventSeq !== null) {
                    $payload = array_replace($payload, (new SearchProjectionEvent(
                        $eventId,
                        $organization,
                        $eventType,
                        $sourceEventSeq,
                        $messageId,
                    ))->toArray());
                }
                $trace = Telemetry::currentTraceContext();
                $inserted = $this->repository->execute(
                    'INSERT INTO im_message_outbox
                        (event_id, organization, event_type, routing_key, message_id, change_seq, source_event_seq,
                         conversation_id, conversation_type, payload_json, traceparent, tracestate,
                         status, retry_count, next_retry_at, create_time, update_time)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)'
                    . ($isSearchProjection
                        ? ''
                        : ' ON DUPLICATE KEY UPDATE event_id = VALUES(event_id)'),
                    [
                        $eventId,
                        $organization,
                        $eventType,
                        $eventType,
                        $messageId,
                        $changeSeq,
                        $sourceEventSeq,
                        $conversationId,
                        $conversationType,
                        json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                        $trace?->traceparent,
                        $trace?->tracestate,
                        self::STATUS_PENDING,
                        $now,
                        $now,
                        $now,
                    ],
                );
                if ($isSearchProjection) {
                    return $inserted;
                }

                // The upsert takes the unique event_id record lock without
                // first acquiring a non-existent-key gap lock. Both concurrent
                // identical writers may therefore succeed, after which the
                // stored fact identity must still match exactly.
                $existing = $this->existingEvent($eventId);
                if ($existing === null) {
                    throw new \RuntimeException('ordinary outbox event disappeared after idempotent upsert');
                }
                $this->assertExistingIdentity(
                    $existing,
                    $organization,
                    $eventType,
                    $messageId,
                    $changeSeq,
                    $conversationId,
                    $conversationType,
                    false,
                );

                return $inserted;
            },
            SpanKind::KIND_CLIENT,
            [
                'operation' => 'im.outbox.insert',
                'db.system.name' => 'mysql',
                'db.operation.name' => 'INSERT',
                'db.collection.name' => 'im_message_outbox',
                'b8im.organization' => $organization,
                'b8im.message_id' => $messageId,
                'b8im.event_id' => $eventId,
                'messaging.destination.name' => $eventType,
            ],
        );
    }

    /** @return array<string,mixed>|null */
    private function existingEvent(string $eventId): ?array
    {
        return $this->repository->fetchOne(
            'SELECT event_id, organization, event_type, routing_key, message_id, change_seq,
                    CAST(source_event_seq AS CHAR) AS source_event_seq,
                    conversation_id, conversation_type, payload_json
               FROM im_message_outbox
              WHERE event_id = ?
              LIMIT 1 FOR UPDATE',
            [$eventId],
        );
    }

    /** @param array<string,mixed> $existing */
    private function assertExistingIdentity(
        array $existing,
        int $organization,
        string $eventType,
        string $messageId,
        int $changeSeq,
        string $conversationId,
        int $conversationType,
        bool $isSearchProjection,
    ): void {
        if (
            (int) ($existing['organization'] ?? 0) !== $organization
            || (string) ($existing['event_type'] ?? '') !== $eventType
            || (string) ($existing['routing_key'] ?? '') !== $eventType
            || (string) ($existing['message_id'] ?? '') !== $messageId
            || (int) ($existing['change_seq'] ?? -1) !== $changeSeq
            || (string) ($existing['conversation_id'] ?? '') !== $conversationId
            || (int) ($existing['conversation_type'] ?? 0) !== $conversationType
        ) {
            throw new \RuntimeException('outbox event_id is already bound to a different fact identity');
        }

        $sourceEventSeq = $existing['source_event_seq'] ?? null;
        if (!$isSearchProjection) {
            if ($sourceEventSeq !== null) {
                throw new \RuntimeException('non-search outbox event has a source_event_seq');
            }
            return;
        }
        if (!is_string($sourceEventSeq)) {
            throw new \RuntimeException('search projection event has no source_event_seq');
        }
        $payload = json_decode(
            (string) ($existing['payload_json'] ?? ''),
            true,
            flags: JSON_THROW_ON_ERROR,
        );
        if (!is_array($payload) || array_is_list($payload)) {
            throw new \RuntimeException('search projection event payload is not an object');
        }
        $identity = new SearchProjectionEvent(
            (string) $existing['event_id'],
            $organization,
            $eventType,
            $sourceEventSeq,
            $messageId,
        );
        foreach ($identity->toArray() as $field => $value) {
            if (!array_key_exists($field, $payload) || $payload[$field] !== $value) {
                throw new \RuntimeException(
                    'search projection event payload identity differs from outbox columns: ' . $field,
                );
            }
        }
    }

    private function allocateSearchProjectionSequence(int $organization): string
    {
        $row = $this->repository->fetchOne(
            'SELECT CAST(last_search_event_seq AS CHAR) AS last_search_event_seq
               FROM im_organization_message_sequence
              WHERE organization = ?
              FOR UPDATE',
            [$organization],
        );
        if ($row === null) {
            throw new \RuntimeException('search projection sequence owner is missing');
        }
        $current = CanonicalDecimal::nonNegative(
            (string) ($row['last_search_event_seq'] ?? ''),
            'last_search_event_seq',
        );
        try {
            $next = CanonicalDecimal::increment($current, 'source_event_seq');
        } catch (\InvalidArgumentException $exception) {
            throw new \RuntimeException(
                'search projection source_event_seq exhausted uint64',
                0,
                $exception,
            );
        }

        $affected = $this->repository->execute(
            'UPDATE im_organization_message_sequence
                SET last_search_event_seq = ?, update_time = ?
              WHERE organization = ? AND last_search_event_seq = ?',
            [$next, $this->now(), $organization, $current],
        );
        if ($affected !== 1) {
            throw new \RuntimeException('search projection sequence allocation lost its row lock');
        }

        return $next;
    }

    /** @param list<array{organization:int,user_id:string}> $identities */
    private function identities(array $identities): array
    {
        $normalized = [];
        foreach ($identities as $identity) {
            $organization = (int) ($identity['organization'] ?? 0);
            $userId = trim((string) ($identity['user_id'] ?? ''));
            if ($organization <= 0 || $userId === '') {
                throw new \InvalidArgumentException('outbox recipient identity is incomplete');
            }
            $normalized[$organization . ':' . $userId] = [
                'organization' => $organization,
                'user_id' => $userId,
            ];
        }

        return array_values($normalized);
    }

    /** @return array{cross_org_access_snapshot_id:string}|array{} */
    private function accessSnapshotPayload(?string $snapshotId): array
    {
        if ($snapshotId === null) {
            return [];
        }
        if (preg_match('/^[1-9][0-9]{0,19}$/D', $snapshotId) !== 1) {
            throw new \InvalidArgumentException('cross-org outbox access snapshot is invalid');
        }

        return ['cross_org_access_snapshot_id' => $snapshotId];
    }

    /** @param list<int|string> $parts */
    private function eventId(array $parts): string
    {
        return hash('sha256', implode('|', array_map('strval', $parts)));
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
