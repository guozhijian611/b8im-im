<?php

declare(strict_types=1);

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\ImRepositoryTenantImPolicyStore;
use B8im\ImBusiness\Service\TenantImPolicyService;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

$database = getenv('TENANT_IM_POLICY_TEST_DB_NAME');
if (!is_string($database) || !str_ends_with($database, '_im_policy_test')) {
    throw new RuntimeException('TENANT_IM_POLICY_TEST_DB_NAME 只允许使用 *_im_policy_test 临时库。');
}
$_ENV['DB_NAME'] = $database;
$_SERVER['DB_NAME'] = $database;
putenv('DB_NAME=' . $database);

$config = Config::fromEnv();
$repository = ImRepository::connect($config);
$selected = (string) ($repository->fetchOne('SELECT DATABASE() AS database_name')['database_name'] ?? '');
if ($config->dbName !== $database || $selected !== $database) {
    throw new RuntimeException(sprintf(
        'IM 策略集成测试库隔离失败: config=%s selected=%s expected=%s',
        $config->dbName,
        $selected,
        $database,
    ));
}

$requiredTables = ['sm_tenant_im_policy', 'sm_system_organization', 'im_user'];
foreach ($requiredTables as $table) {
    $exists = $repository->fetchOne(
        'SELECT 1 AS present
           FROM information_schema.TABLES
          WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
          LIMIT 1',
        [$table],
    );
    if ($exists === null) {
        throw new RuntimeException('isolated database is missing migration table: ' . $table);
    }
}

$redis = new Redis();
$redis->connect($config->redisHost, $config->redisPort, 2.0);
if ($config->redisPassword !== '') {
    $redis->auth($config->redisPassword);
}
if ($config->redisDb > 0) {
    $redis->select($config->redisDb);
}

$organization = random_int(700000000, 799999999);
$userId = 'policy-it-' . bin2hex(random_bytes(5));
$now = date('Y-m-d H:i:s');
$policyKey = 'tenant_im_policy:' . $organization;
$devicesKey = sprintf('im:%d:devices:%s', $organization, $userId);
$qpsKey = sprintf('rate:message:user:%d:%s:%d', $organization, $userId, 1783600000);
$concurrencyKey = sprintf('concurrency:message:user:%d:%s', $organization, $userId);
$authReservationKey = sprintf('auth:policy:reservation:%d:%s', $organization, $userId);
$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    $assertions++;
};

try {
    $repository->execute(
        'INSERT INTO sm_system_organization
            (id, enterprise_code, deployment_id, config_version, title,
             api_server_url, im_server_url, upload_server_url, web_server_url,
             organization_name, status, is_init, create_time, update_time)
         VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?)',
        [
            $organization,
            'policy_' . $organization,
            'policy-integration',
            '策略集成测试',
            'https://api.example.invalid',
            'wss://im.example.invalid',
            'https://upload.example.invalid',
            'https://web.example.invalid',
            '策略集成测试',
            $now,
            $now,
        ],
    );
    $repository->execute(
        'INSERT INTO sm_tenant_im_policy
            (organization, allowed_client_families_json, allow_multi_device_online,
             max_online_devices, same_device_login_policy, cross_device_login_policy,
             max_message_concurrency, max_message_qps, default_group_display_member_count,
             message_recall_window_seconds, message_edit_window_seconds,
             recall_notice_enabled, group_recall_notice_enabled, status, version,
             create_time, update_time)
         VALUES (?, ?, 1, 2, ?, ?, 1, 2, 50, 120, 120, 1, 1, ?, 1, ?, ?)',
        [$organization, '["web"]', 'replace', 'allow', 'ENABLED', $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_user
            (organization, user_id, account, password_hash, nickname, status, create_time, update_time)
         VALUES (?, ?, ?, ?, ?, 1, ?, ?)',
        [$organization, $userId, $userId, password_hash('integration-only', PASSWORD_DEFAULT), $userId, $now, $now],
    );

    $service = new TenantImPolicyService($redis, new ImRepositoryTenantImPolicyStore($repository), 30);
    $policy = $service->policy($organization);
    $assert($policy->version === 1 && $policy->allowedClientFamilies === ['web'], '真实 MySQL 策略未读取');
    $cached = json_decode((string) $redis->get($policyKey), true);
    $assert(is_array($cached) && $cached['organization'] === $organization, '真实 Redis JSON 策略快照未写入');

    $context = new AuthContext(
        organization: $organization,
        userId: $userId,
        deviceId: 'policy-device',
        clientId: 'policy-client',
        credentialSessionId: 'policy-credential',
        sessionId: str_repeat('b', 32),
        clientFamily: 'web',
        os: 'browser',
        issuer: 'policy-integration',
        audience: 'im',
        notBefore: time() - 5,
        expireAt: time() + 300,
    );
    $decision = $service->authorizeAuth($context);
    $assert($decision->clientIdsToDisconnect === [], '无在线设备时不应产生断开决策');
    $decision->release();
    $permit = $service->acquireSendPermit($organization, $userId, 'web', 1783600000);
    $permit->release();
    $assert(!$redis->exists($concurrencyKey), '真实 Redis 并发租约未释放');

    $repository->execute(
        'UPDATE sm_tenant_im_policy
            SET status = ?, version = version + 1, update_time = ?
          WHERE organization = ?',
        ['DISABLED', $now, $organization],
    );
    $service->invalidate($organization);
    try {
        $service->authorizeAuth($context);
        throw new RuntimeException('禁用策略未失败关闭 AUTH');
    } catch (ImException $exception) {
        $assert($exception->errorCode() === 'TENANT_POLICY_FORBIDDEN', '禁用策略返回错误码不正确');
    }
} finally {
    $redis->del($policyKey, $devicesKey, $qpsKey, $concurrencyKey, $authReservationKey);
    $repository->execute('DELETE FROM im_user WHERE organization = ?', [$organization]);
    $repository->execute('DELETE FROM sm_tenant_im_policy WHERE organization = ?', [$organization]);
    $repository->execute('DELETE FROM sm_system_organization WHERE id = ?', [$organization]);
}

fwrite(STDOUT, sprintf("Tenant IM policy MySQL/Redis integration (%s): %d assertions passed.\n", $database, $assertions));
