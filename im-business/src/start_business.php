<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | BusinessWorker - 业务进程
// +----------------------------------------------------------------------
// | 职责（readme §6.7 / §6.8 / §4.3）：
// |  - 处理 Gateway 转发来的连接事件和消息。
// |  - 可横向扩展，多进程/多机部署，处理逻辑必须幂等。
// |  - 核心状态（在线、连接映射）写 Redis，不留在单进程内存。
// |  - 事件回调统一在 Events 类里实现。
// +----------------------------------------------------------------------
declare(strict_types=1);

use GatewayWorker\BusinessWorker;
use B8im\ImBusiness\Events;

$worker = new BusinessWorker();
$worker->name = 'ImBusiness';
$worker->count = (int) (getenv('BUSINESS_PROCESS_COUNT') ?: 4);

// 指向 RegisterWorker（im-register），与 Gateway 用同一个。
$worker->registerAddress = getenv('REGISTER_ADDRESS') ?: '127.0.0.1:1238';

// 内部通信密钥，必须与 im-register / im-gateway 一致。
$worker->secretKey = getenv('SECRET_KEY') ?: '';

// 指定事件处理类。gateway-worker 4.x 支持用 eventHandler 指向一个类。
$worker->eventHandler = Events::class;
