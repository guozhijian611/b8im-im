<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | BusinessWorker 进程级依赖容器
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness;

use B8im\ImBusiness\Auth\ImToken;
use B8im\ImBusiness\Auth\AuthIdentityValidator;
use B8im\ImBusiness\Auth\ActiveSessionGuard;
use B8im\ImBusiness\Connection\ConnectionStore;
use B8im\ImBusiness\Module\ModuleRegistry;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\ConversationSyncService;
use B8im\ImBusiness\Service\DeviceService;
use B8im\ImBusiness\Service\MessageService;
use B8im\ImBusiness\Service\ModuleLicenseChecker;
use B8im\ImBusiness\Service\OutboxService;
use B8im\ImBusiness\Service\PresenceService;
use B8im\ImBusiness\Service\RealtimeEventConsumer;
use B8im\ImBusiness\Service\TypingService;
use B8im\ImBusiness\Service\TenantImPolicyService;

final class Runtime
{
    private static ?Config $config = null;
    private static ?ImToken $token = null;
    private static ?AuthIdentityValidator $authIdentities = null;
    private static ?ActiveSessionGuard $activeSessions = null;
    private static ?ConnectionStore $connections = null;
    private static ?DeviceService $devices = null;
    private static ?MessageService $messages = null;
    private static ?RealtimeEventConsumer $realtimeEvents = null;
    private static ?TypingService $typing = null;
    private static ?PresenceService $presence = null;
    private static ?ConversationSyncService $conversationSync = null;
    private static ?ModuleLicenseChecker $moduleLicense = null;
    private static ?TenantImPolicyService $tenantImPolicies = null;
    private static ?CmdDispatcher $cmdDispatcher = null;

    public static function boot(): void
    {
        $config = Config::fromEnv();
        $repository = ImRepository::connect($config);

        self::$config = $config;
        self::$token = new ImToken($config->tokenPolicy());
        self::$authIdentities = new AuthIdentityValidator($repository);
        self::$activeSessions = ActiveSessionGuard::connect($config, self::$authIdentities);
        self::$connections = ConnectionStore::connect($config);
        self::$devices = new DeviceService($repository);
        self::$tenantImPolicies = TenantImPolicyService::connect($config, $repository);
        self::$messages = new MessageService(
            $repository,
            $config,
            new OutboxService($repository, $config),
            self::$tenantImPolicies,
        );
        self::$messages->preflight();
        self::$realtimeEvents = RealtimeEventConsumer::connect($config, self::$tenantImPolicies);
        self::$typing = new TypingService($repository);
        self::$presence = PresenceService::connect($config);
        self::$conversationSync = new ConversationSyncService($repository);
        self::$moduleLicense = ModuleLicenseChecker::connect($config, $repository);

        // 模块 cmd 分发器：商业模块（客服/音视频等）在此注册自己的 cmd + license 门控
        self::$cmdDispatcher = new CmdDispatcher();
        ModuleRegistry::registerAll(self::$cmdDispatcher, $repository, $config, self::$moduleLicense);
    }

    public static function config(): Config
    {
        return self::$config ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function token(): ImToken
    {
        return self::$token ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function authIdentities(): AuthIdentityValidator
    {
        return self::$authIdentities ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function activeSessions(): ActiveSessionGuard
    {
        return self::$activeSessions ?? throw new \RuntimeException('IM Runtime 尚未启动');
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

    public static function typing(): TypingService
    {
        return self::$typing ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function presence(): PresenceService
    {
        return self::$presence ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function conversationSync(): ConversationSyncService
    {
        return self::$conversationSync ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function cmdDispatcher(): CmdDispatcher
    {
        return self::$cmdDispatcher ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function moduleLicense(): ModuleLicenseChecker
    {
        return self::$moduleLicense ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }

    public static function tenantImPolicies(): TenantImPolicyService
    {
        return self::$tenantImPolicies ?? throw new \RuntimeException('IM Runtime 尚未启动');
    }
}
