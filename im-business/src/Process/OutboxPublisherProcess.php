<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | Outbox 后台投递进程
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Process;

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Queue\RabbitMqPublisher;
use B8im\ImBusiness\Queue\RedisRealtimeControlPublisher;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\FriendRequestRealtimeEvent;
use B8im\ImBusiness\Service\OutboxService;
use B8im\ImBusiness\Service\RealtimeControlOutboxService;
use Throwable;
use Workerman\Timer;
use B8im\ImBusiness\Telemetry\Telemetry;
use B8im\ImShared\Telemetry\TraceContext;
use OpenTelemetry\API\Trace\SpanKind;

final class OutboxPublisherProcess
{
    private ?OutboxService $outbox = null;
    private ?RabbitMqPublisher $publisher = null;
    private ?RealtimeControlOutboxService $controlOutbox = null;
    private ?RedisRealtimeControlPublisher $controlPublisher = null;
    private ?OutboxPublisherTickScheduler $scheduler = null;
    private bool $running = false;

    private string $workerId = '';

    public function __construct(private readonly Config $config)
    {
    }

    public function start(): void
    {
        Telemetry::boot($this->config, 'b8im-im-outbox');
        $this->workerId = sprintf(
            '%s:%d:%s',
            substr((string) (gethostname() ?: 'unknown'), 0, 32),
            getmypid() ?: 0,
            bin2hex(random_bytes(6)),
        );
        $repository = ImRepository::connect($this->config);
        $this->outbox = new OutboxService($repository, $this->config);
        $this->publisher = new RabbitMqPublisher($this->config);
        $this->controlOutbox = new RealtimeControlOutboxService(
            $repository,
            $this->config->mqOutboxLockTtlSeconds,
        );
        $this->controlPublisher = RedisRealtimeControlPublisher::connect($this->config);
        $this->scheduler = new OutboxPublisherTickScheduler();

        Timer::add($this->config->mqOutboxIntervalMs / 1000, fn (): null => $this->tick());
        echo date('Y-m-d H:i:s') . " ImOutboxPublisher worker started\n";
    }

    private function tick(): null
    {
        if (
            $this->running
            || $this->outbox === null
            || $this->publisher === null
            || $this->controlOutbox === null
            || $this->controlPublisher === null
            || $this->scheduler === null
        ) {
            return null;
        }

        $this->running = true;
        $trace = Telemetry::start('im.outbox.claim', attributes: [
            'operation' => 'im.outbox.claim',
            'retry_count' => 0,
        ]);
        try {
            $result = $this->scheduler->run(
                function () use ($trace): void {
                    $rows = $this->outbox?->claimPending(
                        $this->config->mqOutboxBatchSize,
                        $this->workerId,
                    ) ?? [];
                    Telemetry::setAttributes($trace->span, [
                        'b8im.outbox.claimed_count' => count($rows),
                    ]);
                    foreach ($rows as $row) {
                        $this->publishRow($row);
                    }
                },
                function (): bool {
                    // One control row is the entire per-tick Redis budget. A
                    // slow/broken Redis socket therefore cannot multiply its
                    // bounded timeout by MQ_OUTBOX_BATCH_SIZE.
                    $rows = $this->controlOutbox?->claimPending(1, $this->workerId) ?? [];

                    return $rows === [] || $this->publishControlRow($rows[0]);
                },
            );
            if ($result['control_error'] instanceof Throwable) {
                echo date('Y-m-d H:i:s')
                    . ' IM control outbox claim failed: error_code=IM_CONTROL_OUTBOX_CLAIM_FAILED '
                    . Telemetry::logContext() . "\n";
            }
            $throwable = $result['rabbit_error'];
            if ($throwable instanceof Throwable) {
                throw $throwable;
            }
        } catch (Throwable $throwable) {
            Telemetry::recordError(
                $trace->span,
                $throwable,
                'IM_OUTBOX_CLAIM_FAILED',
                'infrastructure',
                'im.outbox.claim',
                ['retry_count' => 0],
            );
            echo date('Y-m-d H:i:s') . ' IM outbox claim failed: error_code=IM_OUTBOX_CLAIM_FAILED '
                . Telemetry::logContext() . "\n";
        } finally {
            $trace->end();
            $this->running = false;
        }

        return null;
    }

    private function publishControlRow(array $row): bool
    {
        $id = (int) ($row['id'] ?? 0);
        $token = (string) ($row['claim_token'] ?? '');
        try {
            if ($id <= 0 || preg_match('/^[a-f0-9]{40}$/D', $token) !== 1) {
                throw new \RuntimeException('control outbox claim identity is invalid');
            }
            $event = FriendRequestRealtimeEvent::fromRaw((string) ($row['payload_json'] ?? ''));
            if (
                ($row['aggregate_type'] ?? null) !== 'friend_request'
                || (int) ($row['aggregate_id'] ?? 0) !== $event->requestId
                || !hash_equals((string) ($row['event_id'] ?? ''), $event->eventId)
                || !hash_equals((string) ($row['event_type'] ?? ''), $event->event)
                || (int) ($row['organization'] ?? 0) !== $event->targetOrganization
                || !hash_equals((string) ($row['target_user_id'] ?? ''), $event->targetUserId)
            ) {
                throw new \RuntimeException('control outbox row differs from its immutable envelope');
            }

            // Renew immediately before the non-transactional RPUSH. A worker
            // whose lease was reclaimed cannot publish or write a result.
            $this->controlOutbox?->renew($id, $token);
            $this->controlPublisher?->publish($event->raw);
            $this->controlOutbox?->markPublished($id, $token);
            return true;
        } catch (Throwable $throwable) {
            try {
                $this->controlOutbox?->markFailed(
                    $id,
                    $token,
                    'IM_REALTIME_CONTROL_PUBLISH_FAILED: ' . $throwable->getMessage(),
                );
            } catch (Throwable) {
                echo date('Y-m-d H:i:s')
                    . ' IM control outbox claim result ignored: error_code=IM_CONTROL_OUTBOX_CLAIM_STALE id='
                    . $id . ' ' . Telemetry::logContext() . "\n";
            }
            echo date('Y-m-d H:i:s')
                . ' IM control outbox publish failed: error_code=IM_REALTIME_CONTROL_PUBLISH_FAILED id='
                . $id . ' ' . Telemetry::logContext() . "\n";
            return false;
        }
    }

    private function publishRow(array $row): void
    {
        $id = (int) $row['id'];
        $claimToken = (string) ($row['claim_token'] ?? '');
        $parent = null;
        try {
            $parent = TraceContext::fromCarrier(
                isset($row['traceparent']) ? (string) $row['traceparent'] : null,
                isset($row['tracestate']) ? (string) $row['tracestate'] : null,
            );
        } catch (\InvalidArgumentException $exception) {
            echo date('Y-m-d H:i:s') . ' IM outbox trace context ignored: id=' . $id . "\n";
        }
        $trace = Telemetry::start(
            'im.rabbitmq.publish ' . (string) ($row['routing_key'] ?? ''),
            SpanKind::KIND_PRODUCER,
            $parent,
            [
                'operation' => 'im.rabbitmq.publish',
                'messaging.system' => 'rabbitmq',
                'messaging.destination.name' => (string) ($row['routing_key'] ?? ''),
                'b8im.organization' => (int) ($row['organization'] ?? 0),
                'b8im.message_id' => (string) ($row['message_id'] ?? ''),
                'b8im.outbox_id' => $id,
                'retry_count' => (int) ($row['retry_count'] ?? 0),
            ],
        );
        try {
            if ($claimToken === '') {
                throw new \RuntimeException('outbox row has no claim_token');
            }
            $payload = json_decode((string) $row['payload_json'], true, flags: JSON_THROW_ON_ERROR);
            if (!is_array($payload)) {
                throw new \RuntimeException('outbox payload is not an object');
            }

            $searchIdentity = RabbitMqPublisher::searchProjectionIdentity($payload);
            $rowSourceEventSeq = $row['source_event_seq'] ?? null;
            if ($searchIdentity !== null) {
                if (
                    $searchIdentity->eventId !== (string) ($row['event_id'] ?? '')
                    || $searchIdentity->organization !== (int) ($row['organization'] ?? 0)
                    || $searchIdentity->eventType !== (string) ($row['event_type'] ?? '')
                    || $searchIdentity->eventType !== (string) ($row['routing_key'] ?? '')
                    || $searchIdentity->sourceEventSeq !== (string) $rowSourceEventSeq
                    || $searchIdentity->messageId !== (string) ($row['message_id'] ?? '')
                ) {
                    throw new \RuntimeException(
                        'search projection payload identity differs from the claimed outbox row',
                    );
                }
            } elseif ($rowSourceEventSeq !== null) {
                throw new \RuntimeException('non-search outbox row has a source_event_seq');
            }

            $messageId = RabbitMqPublisher::brokerMessageId(
                $payload,
                $id,
                (string) $row['message_id'],
            );
            $this->publisher?->publish(
                (string) $row['routing_key'],
                $payload,
                $messageId,
                Telemetry::currentTraceContext(),
            );
            $this->outbox?->markPublished($id, $claimToken);
        } catch (Throwable $throwable) {
            Telemetry::recordError(
                $trace->span,
                $throwable,
                'IM_RABBITMQ_PUBLISH_FAILED',
                'messaging',
                'im.rabbitmq.publish',
                [
                    'retry_count' => (int) ($row['retry_count'] ?? 0) + 1,
                    'b8im.outbox_id' => $id,
                    'b8im.message_id' => (string) ($row['message_id'] ?? ''),
                ],
            );
            try {
                $this->outbox?->markFailed($id, $claimToken, 'IM_RABBITMQ_PUBLISH_FAILED');
            } catch (Throwable $claimException) {
                echo date('Y-m-d H:i:s') . ' IM outbox claim result ignored: error_code=IM_OUTBOX_CLAIM_STALE id=' . $id . ' '
                    . Telemetry::logContext() . "\n";
            }
            echo date('Y-m-d H:i:s') . ' IM outbox publish failed: error_code=IM_RABBITMQ_PUBLISH_FAILED id=' . $id . ' '
                . Telemetry::logContext() . "\n";
        } finally {
            $trace->end();
        }
    }
}
