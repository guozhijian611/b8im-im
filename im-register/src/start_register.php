<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | RegisterWorker - 服务注册发现
// +----------------------------------------------------------------------
// | 职责（readme §4.3）：Gateway 与 Business 启动后都连到这里登记，
// | Register 把彼此的地址互相告知，使两者建立内部连接。
// | 它只做注册/发现，不处理任何业务消息，通常一个实例即可。
// +----------------------------------------------------------------------
declare(strict_types=1);

use GatewayWorker\Register;
use B8im\ImShared\Support\RuntimeEnvironment;

// 监听地址：Gateway 和 Business 都用这个地址来注册/发现。
// 集群时所有节点指向同一个 Register。
$registerListen = RuntimeEnvironment::value('REGISTER_LISTEN', 'text://127.0.0.1:1238');

$register = new Register($registerListen);
$register->name = 'ImRegister';

// 内部通信密钥：必须与 Gateway / Business 的 SECRET_KEY 一致，否则拒绝注册。
// 生产环境务必通过 .env 配置一个强随机值。
$register->secretKey = RuntimeEnvironment::requireInternalSecret(
    RuntimeEnvironment::value('SECRET_KEY'),
);
