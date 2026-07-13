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
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\OutboxService;
use Throwable;
use Workerman\Timer;
use B8im\ImBusiness\Telemetry\Telemetry;
use B8im\ImShared\Telemetry\TraceContext;
use OpenTelemetry\API\Trace\SpanKind;

final class OutboxPublisherProcess
{
    private ?OutboxService $outbox = null;
    private ?RabbitMqPublisher $publisher = null;
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

        Timer::add($this->config->mqOutboxIntervalMs / 1000, fn (): null => $this->tick());
        echo date('Y-m-d H:i:s') . " ImOutboxPublisher worker started\n";
    }

    private function tick(): null
    {
        if ($this->running || $this->outbox === null || $this->publisher === null) {
            return null;
        }

        $this->running = true;
        $trace = Telemetry::start('im.outbox.claim', attributes: [
            'operation' => 'im.outbox.claim',
            'retry_count' => 0,
        ]);
        try {
            $rows = $this->outbox->claimPending($this->config->mqOutboxBatchSize, $this->workerId);
            Telemetry::setAttributes($trace->span, ['b8im.outbox.claimed_count' => count($rows)]);
            foreach ($rows as $row) {
                $this->publishRow($row);
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

            $messageId = 'im-outbox-' . $id . '-' . (string) $row['message_id'];
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
