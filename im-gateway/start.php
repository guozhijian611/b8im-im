<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | im-gateway 入口 - 加载 GatewayWorker 进程
// +----------------------------------------------------------------------
declare(strict_types=1);

use Workerman\Worker;
use B8im\ImShared\Support\RuntimeEnvironment;

require_once __DIR__ . '/vendor/autoload.php';

if (is_file(__DIR__ . '/.env')) {
    Dotenv\Dotenv::createImmutable(__DIR__)->safeLoad();
}

RuntimeEnvironment::configureTimezone(
    RuntimeEnvironment::value('IM_TIMEZONE'),
);

$runtimeDir = trim((string) RuntimeEnvironment::value('IM_RUNTIME_DIR', ''));
if ($runtimeDir !== '') {
    if (!is_dir($runtimeDir) && !mkdir($runtimeDir, 0775, true) && !is_dir($runtimeDir)) {
        throw new RuntimeException('无法创建 IM_RUNTIME_DIR');
    }
    Worker::$pidFile = $runtimeDir . '/gateway.pid';
    Worker::$statusFile = $runtimeDir . '/gateway.status';
    Worker::$logFile = $runtimeDir . '/gateway.workerman.log';
    Worker::$stdoutFile = $runtimeDir . '/gateway.stdout.log';
}

require_once __DIR__ . '/src/start_gateway.php';

Worker::runAll();
