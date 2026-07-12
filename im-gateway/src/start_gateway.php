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
use B8im\ImGateway\Security\OriginPolicy;
use B8im\ImShared\Support\RuntimeEnvironment;

// 对外 WebSocket 监听地址，App/Web 连这里。
$wsListen = RuntimeEnvironment::value('GATEWAY_LISTEN', 'websocket://0.0.0.0:8787');

$gateway = new Gateway($wsListen);
$gateway->name = 'ImGateway';
$gateway->count = (int) RuntimeEnvironment::value('GATEWAY_PROCESS_COUNT', '4');

// 本机对内通信端口（供 BusinessWorker 连接）。GatewayWorker 会把该地址同时
// 编入 client_id 和注册中心路由键，因此 Docker 服务名必须先解析成明确 IPv4。
$gatewayLanIp = trim((string) RuntimeEnvironment::value('GATEWAY_LAN_IP', '127.0.0.1'));
if (filter_var($gatewayLanIp, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) === false) {
    $resolvedLanIp = gethostbyname($gatewayLanIp);
    if (
        $resolvedLanIp === $gatewayLanIp
        || filter_var($resolvedLanIp, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) === false
    ) {
        throw new RuntimeException('GATEWAY_LAN_IP 必须是可解析的 IPv4 或主机名');
    }
    $gatewayLanIp = $resolvedLanIp;
}
$gateway->lanIp = $gatewayLanIp;
$gateway->startPort = (int) RuntimeEnvironment::value('GATEWAY_START_PORT', '2900');

// 指向 RegisterWorker（im-register），集群所有节点共用同一个。
$gateway->registerAddress = RuntimeEnvironment::value('REGISTER_ADDRESS', '127.0.0.1:1238');

// 内部通信密钥，必须与 im-register / im-business 一致。
$gateway->secretKey = RuntimeEnvironment::requireInternalSecret(
    RuntimeEnvironment::value('SECRET_KEY'),
);

// 心跳：超过该秒数未收到客户端数据则断开（readme §6.2 心跳保活）。
$gateway->pingInterval = (int) RuntimeEnvironment::value('PING_INTERVAL', '55');
$gateway->pingNotResponseLimit = 1;
// 空字符串表示由客户端主动发心跳；服务端只做超时检测。
$gateway->pingData = '';

$originPolicy = OriginPolicy::fromCsv((string) RuntimeEnvironment::value('IM_TRUSTED_ORIGINS', ''));
$gateway->onConnect = static function ($connection) use ($originPolicy): void {
    $connection->onWebSocketConnect = static function ($connection, $request) use ($originPolicy): void {
        $origin = is_object($request) && method_exists($request, 'header')
            ? $request->header('origin')
            : null;

        try {
            $originPolicy->assertAllowed(is_string($origin) ? $origin : null);
        } catch (\InvalidArgumentException) {
            $connection->close(
                "HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                true,
            );
        }
    };
};

// 无 Origin 的原生客户端不在 Gateway 被拒绝，但 BusinessWorker 仍会强制校验 IM JWT。
