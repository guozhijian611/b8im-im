<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | Outbox 发布进程 - MySQL 可靠事件投递到 RabbitMQ
// +----------------------------------------------------------------------
declare(strict_types=1);

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Process\OutboxPublisherProcess;
use Workerman\Worker;
use B8im\ImBusiness\Telemetry\Telemetry;

$outboxConfig = Config::fromEnv();
if ($outboxConfig->mqOutboxEnabled && $outboxConfig->mqOutboxProcessCount > 0) {
    $outboxWorker = new Worker();
    $outboxWorker->name = 'ImOutboxPublisher';
    $outboxWorker->count = $outboxConfig->mqOutboxProcessCount;
    $outboxWorker->onWorkerStart = static function () use ($outboxConfig): void {
        (new OutboxPublisherProcess($outboxConfig))->start();
    };
    $outboxWorker->onWorkerStop = static function (): void {
        Telemetry::shutdown();
    };
}
