<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | im-register 入口 - 加载 RegisterWorker 进程
// +----------------------------------------------------------------------
declare(strict_types=1);

use Workerman\Worker;

require_once __DIR__ . '/vendor/autoload.php';

// 加载 .env（不存在则用默认值）
if (is_file(__DIR__ . '/.env')) {
    Dotenv\Dotenv::createImmutable(__DIR__)->safeLoad();
}

// 加载进程定义
require_once __DIR__ . '/src/start_register.php';

Worker::runAll();
