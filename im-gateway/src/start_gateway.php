<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | GatewayWorker - 对外 WebSocket 接入
// +----------------------------------------------------------------------
// | 职责（readme §6.2 / §4.3）：
// |  - 对外承载 WebSocket 长连接，接入 App / Web。
// |  - 节点无状态，支持水平扩容。
// |  - 接到的消息原样转交 BusinessWorker；不在此处写业务逻辑。
// |  - 连接/断开事件也交由 BusinessWorker 处理（绑定 user_id、清理在线状态）。
// +----------------------------------------------------------------------
declare(strict_types=1);

use GatewayWorker\Gateway;

// 对外 WebSocket 监听地址，App/Web 连这里。
$wsListen = getenv('GATEWAY_LISTEN') ?: 'websocket://0.0.0.0:8787';

$gateway = new Gateway($wsListen);
$gateway->name = 'ImGateway';
$gateway->count = (int) (getenv('GATEWAY_PROCESS_COUNT') ?: 4);

// 本机对内通信端口（供 BusinessWorker 连接）。多机部署时必须是内网可达 IP。
$gateway->lanIp = getenv('GATEWAY_LAN_IP') ?: '127.0.0.1';
$gateway->startPort = (int) (getenv('GATEWAY_START_PORT') ?: 2900);

// 指向 RegisterWorker（im-register），集群所有节点共用同一个。
$gateway->registerAddress = getenv('REGISTER_ADDRESS') ?: '127.0.0.1:1238';

// 内部通信密钥，必须与 im-register / im-business 一致。
$gateway->secretKey = getenv('SECRET_KEY') ?: '';

// 心跳：超过该秒数未收到客户端数据则断开（readme §6.2 心跳保活）。
$gateway->pingInterval = (int) (getenv('PING_INTERVAL') ?: 55);
$gateway->pingNotResponseLimit = 1;
// 空字符串表示由客户端主动发心跳；服务端只做超时检测。
$gateway->pingData = '';

// TODO（后续）：
//  - onConnect 时可在此层做连接数限流 / 黑名单 IP 拦截。
//  - 鉴权由 BusinessWorker 收到 AUTH 帧后用 IM token 校验并 bindUid。
