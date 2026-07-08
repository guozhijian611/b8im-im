<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | im-gateway 入口 - 加载 GatewayWorker 进程
// +----------------------------------------------------------------------
declare(strict_types=1);

use Workerman\Worker;

require_once __DIR__ . '/vendor/autoload.php';

if (is_file(__DIR__ . '/.env')) {
    Dotenv\Dotenv::createImmutable(__DIR__)->safeLoad();
}

require_once __DIR__ . '/src/start_gateway.php';

Worker::runAll();
