<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Process;

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Queue\RabbitMqRealtimeConsumer;
use B8im\ImBusiness\Realtime\DatabaseRealtimeRecipientProvider;
use B8im\ImBusiness\Realtime\GatewayRealtimeGateway;
use B8im\ImBusiness\Realtime\RealtimeDeliveryHandler;
use B8im\ImBusiness\Realtime\RealtimeDeliveryService;
use B8im\ImBusiness\Realtime\RealtimeEventProjector;
use B8im\ImBusiness\Realtime\RedisRealtimeDeliveryCheckpoint;
use B8im\ImBusiness\Realtime\RedisRealtimeRetryCounter;
use B8im\ImBusiness\Repository\ImRepository;
use Throwable;
use Workerman\Timer;

final class RealtimeDeliveryProcess
{
    private ?RabbitMqRealtimeConsumer $consumer = null;
    private bool $polling = false;
    private float $retryAfter = 0.0;

    public function __construct(private readonly Config $config)
    {
    }

    public function start(): void
    {
        Timer::add($this->config->mqRealtimePollIntervalMs / 1000, function (): void {
            $this->tick();
        });
        // Workerman 会在守护进程 fork 后接管标准输出；此时 STDOUT/STDERR
        // 常量可能仍指向父进程已经关闭的资源。使用 echo 交给 Workerman 的
        // 输出缓冲处理，避免实时投递进程因无效 stream 循环退出。
        echo date('Y-m-d H:i:s') . " ImRealtimeDelivery worker started\n";
    }

    public function stop(): void
    {
        $this->consumer?->close();
    }

    private function tick(): void
    {
        if ($this->polling || microtime(true) < $this->retryAfter) {
            return;
        }

        $this->polling = true;
        try {
            $this->consumer ??= $this->buildConsumer();
            $this->consumer->poll();
            $this->retryAfter = 0.0;
        } catch (Throwable $throwable) {
            $this->consumer?->close();
            $this->consumer = null;
            $this->retryAfter = microtime(true) + 1.0;
            echo sprintf(
                "%s IM realtime consumer connection error: %s\n",
                date('Y-m-d H:i:s'),
                mb_substr($throwable->getMessage(), 0, 500),
            );
        } finally {
            $this->polling = false;
        }
    }

    private function buildConsumer(): RabbitMqRealtimeConsumer
    {
        $repository = ImRepository::connect($this->config);
        $deliverer = new RealtimeDeliveryService(
            new DatabaseRealtimeRecipientProvider($repository),
            new GatewayRealtimeGateway(),
            RedisRealtimeDeliveryCheckpoint::connect($this->config),
        );
        $handler = new RealtimeDeliveryHandler(
            new RealtimeEventProjector(),
            $deliverer,
            RedisRealtimeRetryCounter::connect($this->config),
            $this->config->mqRealtimeMaxRetry,
        );

        return new RabbitMqRealtimeConsumer($this->config, $handler);
    }
}
