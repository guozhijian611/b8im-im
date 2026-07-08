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

final class OutboxPublisherProcess
{
    private ?OutboxService $outbox = null;
    private ?RabbitMqPublisher $publisher = null;
    private bool $running = false;

    public function __construct(private readonly Config $config)
    {
    }

    public function start(): void
    {
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
        try {
            $rows = $this->outbox->claimPending($this->config->mqOutboxBatchSize);
            foreach ($rows as $row) {
                $this->publishRow($row);
            }
        } catch (Throwable $throwable) {
            echo date('Y-m-d H:i:s') . ' IM outbox tick error: ' . $throwable->getMessage() . "\n";
        } finally {
            $this->running = false;
        }

        return null;
    }

    private function publishRow(array $row): void
    {
        $id = (int) $row['id'];
        try {
            $payload = json_decode((string) $row['payload'], true, flags: JSON_THROW_ON_ERROR);
            if (!is_array($payload)) {
                throw new \RuntimeException('outbox payload is not an object');
            }

            $messageId = 'im-outbox-' . $id . '-' . (string) $row['message_id'];
            $this->publisher?->publish((string) $row['routing_key'], $payload, $messageId);
            $this->outbox?->markPublished($id);
        } catch (Throwable $throwable) {
            $this->outbox?->markFailed($id, $throwable->getMessage());
            echo date('Y-m-d H:i:s') . ' IM outbox publish failed: id=' . $id . ' ' . $throwable->getMessage() . "\n";
        }
    }
}
