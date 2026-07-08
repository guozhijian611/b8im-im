<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | BusinessWorker 进程级依赖容器
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness;

use B8im\ImBusiness\Auth\ImToken;
use B8im\ImBusiness\Connection\ConnectionStore;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\DeviceService;
use B8im\ImBusiness\Service\MessageService;
use B8im\ImBusiness\Service\OutboxService;
use B8im\ImBusiness\Service\RealtimeEventConsumer;

final class Runtime
{
    private static ?Config $config = null;
    private static ?ImToken $token = null;
    private static ?ConnectionStore $connections = null;
    private static ?DeviceService $devices = null;
    private static ?MessageService $messages = null;
    private static ?RealtimeEventConsumer $realtimeEvents = null;

    public static function boot(): void
    {
        $config = Config::fromEnv();
        $repository = ImRepository::connect($config);

        self::$config = $config;
        self::$token = new ImToken($config);
        self::$connections = ConnectionStore::connect($config);
        self::$devices = new DeviceService($repository);
        self::$messages = new MessageService($repository, $config, new OutboxService($repository, $config));
        self::$realtimeEvents = RealtimeEventConsumer::connect($config);
    }

    public static function config(): Config
    {
        return self::$config ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function token(): ImToken
    {
        return self::$token ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function connections(): ConnectionStore
    {
        return self::$connections ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function messages(): MessageService
    {
        return self::$messages ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function realtimeEvents(): RealtimeEventConsumer
    {
        return self::$realtimeEvents ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function devices(): DeviceService
    {
        return self::$devices ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }
}
