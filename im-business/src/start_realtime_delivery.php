<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Process\RealtimeDeliveryProcess;
use B8im\ImShared\Support\RuntimeEnvironment;
use GatewayWorker\Lib\Gateway;
use Workerman\Worker;

$realtimeConfig = Config::fromEnv();
if ($realtimeConfig->mqRealtimeEnabled && $realtimeConfig->mqRealtimeProcessCount > 0) {
    $realtimeProcess = null;
    $realtimeWorker = new Worker();
    $realtimeWorker->name = 'ImRealtimeDelivery';
    $realtimeWorker->count = $realtimeConfig->mqRealtimeProcessCount;
    $realtimeWorker->onWorkerStart = static function () use (&$realtimeProcess, $realtimeConfig): void {
        // 该进程不是 BusinessWorker，必须显式指向 Register 并携带内部密钥。
        $registerAddress = RuntimeEnvironment::value('REGISTER_ADDRESS', '127.0.0.1:1238');
        $secretKey = RuntimeEnvironment::value('SECRET_KEY');
        Gateway::$registerAddress = $registerAddress;
        Gateway::$secretKey = RuntimeEnvironment::requireInternalSecret(
            $secretKey,
        );
        $realtimeProcess = new RealtimeDeliveryProcess($realtimeConfig);
        $realtimeProcess->start();
    };
    $realtimeWorker->onWorkerStop = static function () use (&$realtimeProcess): void {
        $realtimeProcess?->stop();
    };
}
