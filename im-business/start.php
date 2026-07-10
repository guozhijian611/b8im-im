<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | im-business 入口 - 加载 BusinessWorker 进程
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

require_once __DIR__ . '/src/start_business.php';
require_once __DIR__ . '/src/start_outbox_publisher.php';
require_once __DIR__ . '/src/start_realtime_delivery.php';

Worker::runAll();
