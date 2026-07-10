<?php

declare(strict_types=1);

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Service\TenantImPolicyService;
use B8im\ImBusiness\Service\TenantImPolicyStoreInterface;

require dirname(__DIR__) . '/vendor/autoload.php';

final class FakeImPolicyStore implements TenantImPolicyStoreInterface
{
    /** @var array<int, array<string, mixed>> */
    public array $policies = [];

    /** @var array<string, bool> */
    public array $users = [];

    public bool $failReads = false;

    public int $policyReads = 0;

    public function fetchPolicy(int $organization): ?array
    {
        $this->policyReads++;
        if ($this->failReads) {
            throw new RuntimeException('database unavailable');
        }

        return $this->policies[$organization] ?? null;
    }

    public function isActiveUser(int $organization, string $userId): bool
    {
        if ($this->failReads) {
            throw new RuntimeException('database unavailable');
        }

        return $this->users[$organization . ':' . $userId] ?? false;
    }
}

final class FakeImPolicyRedis
{
    /** @var array<string, string> */
    public array $strings = [];

    /** @var array<string, array<string, string>> */
    public array $hashes = [];

    /** @var array<string, array<string, int>> */
    public array $sortedSets = [];

    public bool $fail = false;

    public function get(string $key): string|false
    {
        $this->assertAvailable();

        return $this->strings[$key] ?? false;
    }

    public function setex(string $key, int $ttl, string $value): bool
    {
        $this->assertAvailable();
        $this->strings[$key] = $value;

        return true;
    }

    public function del(string $key): int
    {
        $this->assertAvailable();
        $present = isset($this->strings[$key]) || isset($this->hashes[$key]) || isset($this->sortedSets[$key]);
        unset($this->strings[$key], $this->hashes[$key], $this->sortedSets[$key]);

        return $present ? 1 : 0;
    }

    /** @return array<string, string> */
    public function hGetAll(string $key): array
    {
        $this->assertAvailable();

        return $this->hashes[$key] ?? [];
    }

    public function hDel(string $key, string $field): int
    {
        $this->assertAvailable();
        $present = isset($this->hashes[$key][$field]);
        unset($this->hashes[$key][$field]);

        return $present ? 1 : 0;
    }

    /** @param list<string> $arguments */
    public function eval(string $script, array $arguments, int $numberOfKeys): int
    {
        $this->assertAvailable();
        if ($numberOfKeys === 1 && str_contains($script, "'NX', 'PX'")) {
            [$key, $token] = $arguments;
            if (isset($this->strings[$key])) {
                return 0;
            }
            $this->strings[$key] = $token;

            return 1;
        }
        if ($numberOfKeys === 1 && str_contains($script, "redis.call('GET', KEYS[1]) == ARGV[1]")) {
            [$key, $token] = $arguments;
            if (($this->strings[$key] ?? null) !== $token) {
                return 0;
            }
            unset($this->strings[$key]);

            return 1;
        }
        if ($numberOfKeys === 2 && str_contains($script, "redis.call('INCR', KEYS[1])")) {
            [$qpsKey, $concurrencyKey, $maxQps, $maxConcurrency, $nowMilliseconds, $leaseMilliseconds, $token] = $arguments;
            $qps = (int) ($this->strings[$qpsKey] ?? 0);
            if ($qps >= (int) $maxQps) {
                return -1;
            }
            foreach ($this->sortedSets[$concurrencyKey] ?? [] as $member => $expiresAt) {
                if ($expiresAt <= (int) $nowMilliseconds) {
                    unset($this->sortedSets[$concurrencyKey][$member]);
                }
            }
            $concurrency = count($this->sortedSets[$concurrencyKey] ?? []);
            if ($concurrency >= (int) $maxConcurrency) {
                return -2;
            }
            $this->strings[$qpsKey] = (string) ($qps + 1);
            $this->sortedSets[$concurrencyKey][$token] = (int) $nowMilliseconds + (int) $leaseMilliseconds;

            return 1;
        }
        if ($numberOfKeys === 1 && str_contains($script, "redis.call('ZREM', KEYS[1]")) {
            [$key, $token] = $arguments;
            $present = isset($this->sortedSets[$key][$token]);
            unset($this->sortedSets[$key][$token]);
            if (($this->sortedSets[$key] ?? []) === []) {
                unset($this->sortedSets[$key]);
            }

            return $present ? 1 : 0;
        }

        throw new RuntimeException('unexpected Lua script');
    }

    private function assertAvailable(): void
    {
        if ($this->fail) {
            throw new RuntimeException('redis unavailable');
        }
    }
}

/** @return array<string, mixed> */
function policyRow(int $organization, array $overrides = []): array
{
    return array_merge([
        'organization' => $organization,
        'allowed_client_families_json' => '["web","app","desktop"]',
        'allow_multi_device_online' => 1,
        'max_online_devices' => 2,
        'same_device_login_policy' => 'replace',
        'cross_device_login_policy' => 'allow',
        'max_message_concurrency' => 2,
        'max_message_qps' => 2,
        'default_group_display_member_count' => 50,
        'message_recall_window_seconds' => 120,
        'message_edit_window_seconds' => 120,
        'recall_notice_enabled' => 1,
        'group_recall_notice_enabled' => 1,
        'status' => 'ENABLED',
        'version' => 1,
    ], $overrides);
}

function authContext(int $organization, string $userId, string $deviceId = 'device-new', string $clientFamily = 'web'): AuthContext
{
    return new AuthContext(
        organization: $organization,
        userId: $userId,
        deviceId: $deviceId,
        clientId: 'client-new',
        credentialSessionId: 'credential-new',
        sessionId: str_repeat('a', 32),
        clientFamily: $clientFamily,
        os: $clientFamily === 'web' ? 'browser' : 'android',
        issuer: 'policy-test',
        audience: 'im',
        notBefore: time() - 5,
        expireAt: time() + 300,
    );
}

/** @return string JSON */
function deviceSnapshot(int $organization, string $userId, string $deviceId, string $clientId, int $bindTime): string
{
    return json_encode([
        'organization' => $organization,
        'user_id' => $userId,
        'device_id' => $deviceId,
        'client_id' => $clientId,
        'session_id' => md5($clientId),
        'bind_time' => $bindTime,
    ], JSON_THROW_ON_ERROR);
}

function putOnlineDevice(
    FakeImPolicyRedis $redis,
    int $organization,
    string $userId,
    string $deviceId,
    string $clientId,
    int $bindTime,
): void {
    $snapshot = deviceSnapshot($organization, $userId, $deviceId, $clientId, $bindTime);
    $connectionSessionId = (string) json_decode($snapshot, true, flags: JSON_THROW_ON_ERROR)['session_id'];
    $redis->hashes[sprintf('im:%d:devices:%s', $organization, $userId)][$connectionSessionId] = $snapshot;
    $redis->strings[sprintf('im:%d:client:%s', $organization, $clientId)] = $snapshot;
}

$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    $assertions++;
};
$expectIm = static function (string $code, callable $callback) use ($assert): void {
    try {
        $callback();
    } catch (ImException $exception) {
        $assert($exception->errorCode() === $code, sprintf('expected %s, got %s', $code, $exception->errorCode()));
        return;
    }
    throw new RuntimeException('expected ImException ' . $code);
};

$store = new FakeImPolicyStore();
$redis = new FakeImPolicyRedis();
$store->policies[1] = policyRow(1);
$store->users['1:user-1'] = true;
$service = new TenantImPolicyService($redis, $store, 30);

$policy = $service->policy(1);
$assert($policy->maxMessageQps === 2 && $policy->allowedClientFamilies === ['web', 'app', 'desktop'], '数据库策略解析失败');
$assert(isset($redis->strings['tenant_im_policy:1']), '策略未写入 Redis JSON 短缓存');
$service->policy(1);
$assert($store->policyReads === 2, 'ENABLED 正缓存未回源 MySQL 重验');
$service->invalidate(1);
$assert(!isset($redis->strings['tenant_im_policy:1']), '策略变更未失效缓存');

$store->policies[2] = policyRow(2, ['status' => 'DISABLED']);
$store->users['2:user-2'] = true;
$expectIm('TENANT_POLICY_FORBIDDEN', static fn () => (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(2, 'user-2')));
$expectIm('TENANT_POLICY_FORBIDDEN', static fn () => (new TenantImPolicyService($redis, $store))->policy(99));
$store->failReads = true;
$expectIm('TENANT_POLICY_FORBIDDEN', static fn () => (new TenantImPolicyService($redis, $store))->policy(98));
$store->failReads = false;

$store->policies[15] = policyRow(15);
$store->users['15:user-15'] = true;
$staleStatusService = new TenantImPolicyService($redis, $store);
$staleStatusService->assertConnectionAllowed(15, 'web');
$store->policies[15] = policyRow(15, ['status' => 'DISABLED', 'version' => 2]);
$expectIm('TENANT_POLICY_FORBIDDEN', static fn () => $staleStatusService->assertConnectionAllowed(15, 'web'));

$store->policies[16] = policyRow(16);
$store->users['16:user-16'] = true;
$staleFamilyService = new TenantImPolicyService($redis, $store);
$staleFamilyService->assertConnectionAllowed(16, 'web');
$store->policies[16] = policyRow(16, [
    'allowed_client_families_json' => '["app"]',
    'version' => 2,
]);
$expectIm('TENANT_POLICY_FORBIDDEN', static fn () => $staleFamilyService->assertConnectionAllowed(16, 'web'));

$store->policies[17] = policyRow(17);
$store->users['17:user-17'] = true;
$staleFailureService = new TenantImPolicyService($redis, $store);
$staleFailureService->assertConnectionAllowed(17, 'web');
$store->failReads = true;
$expectIm('TENANT_POLICY_FORBIDDEN', static fn () => $staleFailureService->assertConnectionAllowed(17, 'web'));
$store->failReads = false;

$store->policies[18] = policyRow(18, ['status' => 'DISABLED']);
$cachedDenyService = new TenantImPolicyService($redis, $store);
$cachedDenyService->policy(18);
$denyReads = $store->policyReads;
$store->failReads = true;
$assert($cachedDenyService->policy(18)->status === 'DISABLED', 'cached DISABLED policy was not enforced');
$assert($store->policyReads === $denyReads, 'cached DISABLED policy unnecessarily queried MySQL');
$store->failReads = false;

$store->policies[3] = policyRow(3, ['allowed_client_families_json' => '["app"]']);
$store->users['3:user-3'] = true;
$expectIm('TENANT_POLICY_FORBIDDEN', static fn () => (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(3, 'user-3')));
$expectIm(
    'TENANT_POLICY_FORBIDDEN',
    static fn () => (new TenantImPolicyService($redis, $store))->assertConnectionAllowed(3, 'web'),
);
$expectIm(
    'TENANT_POLICY_FORBIDDEN',
    static fn () => (new TenantImPolicyService($redis, $store))->acquireSendPermit(3, 'user-3', 'web'),
);
(new TenantImPolicyService($redis, $store))->assertConnectionAllowed(3, 'app');
$assert(true, '允许的 app 客户端应通过逐命令策略校验');
$expectIm(
    'TENANT_POLICY_FORBIDDEN',
    static fn () => (new TenantImPolicyService($redis, $store))->assertConnectionAllowed(2, 'web'),
);

$store->policies[4] = policyRow(4, ['same_device_login_policy' => 'reject']);
$store->users['4:user-4'] = true;
putOnlineDevice($redis, 4, 'user-4', 'device-new', 'client-old', time() - 10);
$expectIm('SAME_DEVICE_LOGIN_REJECTED', static fn () => (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(4, 'user-4')));

$store->policies[5] = policyRow(5, ['cross_device_login_policy' => 'reject_new']);
$store->users['5:user-5'] = true;
putOnlineDevice($redis, 5, 'user-5', 'device-old', 'client-old', time() - 10);
$expectIm('CROSS_DEVICE_LOGIN_REJECTED', static fn () => (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(5, 'user-5')));

$store->policies[6] = policyRow(6, ['max_online_devices' => 1]);
$store->users['6:user-6'] = true;
putOnlineDevice($redis, 6, 'user-6', 'device-old', 'client-old', time() - 10);
$expectIm('DEVICE_LIMIT_EXCEEDED', static fn () => (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(6, 'user-6')));

$store->policies[7] = policyRow(7, [
    'allow_multi_device_online' => 0,
    'cross_device_login_policy' => 'kick_old',
]);
$store->users['7:user-7'] = true;
putOnlineDevice($redis, 7, 'user-7', 'device-new', 'client-same-old', time() - 20);
putOnlineDevice($redis, 7, 'user-7', 'device-other', 'client-cross-old', time() - 10);
$decision = (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(7, 'user-7'));
$disconnect = $decision->clientIdsToDisconnect;
sort($disconnect);
$assert($disconnect === ['client-cross-old', 'client-same-old'], 'replace/kick_old 未返回待断开连接决策');
$decision->release();

$store->policies[12] = policyRow(12, ['same_device_login_policy' => 'coexist']);
$store->users['12:user-12'] = true;
putOnlineDevice($redis, 12, 'user-12', 'device-new', 'client-coexist-1', time() - 20);
putOnlineDevice($redis, 12, 'user-12', 'device-new', 'client-coexist-2', time() - 10);
$coexistDecision = (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(12, 'user-12'));
$assert($coexistDecision->clientIdsToDisconnect === [], 'coexist 策略误踢同设备已有连接');
$coexistDecision->release();
$assert(count($redis->hashes['im:12:devices:user-12'] ?? []) === 2, 'coexist 策略扫描丢失同设备连接');

$store->policies[13] = policyRow(13, ['same_device_login_policy' => 'replace']);
$store->users['13:user-13'] = true;
putOnlineDevice($redis, 13, 'user-13', 'device-new', 'client-replace-1', time() - 20);
putOnlineDevice($redis, 13, 'user-13', 'device-new', 'client-replace-2', time() - 10);
$replaceDecision = (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(13, 'user-13'));
$replaceClients = $replaceDecision->clientIdsToDisconnect;
sort($replaceClients);
$assert(
    $replaceClients === ['client-replace-1', 'client-replace-2'],
    'replace 策略未精确返回同设备全部旧连接',
);
$replaceDecision->release();

$store->policies[14] = policyRow(14, ['max_online_devices' => 2, 'same_device_login_policy' => 'coexist']);
$store->users['14:user-14'] = true;
putOnlineDevice($redis, 14, 'user-14', 'device-old', 'client-old-1', time() - 20);
putOnlineDevice($redis, 14, 'user-14', 'device-old', 'client-old-2', time() - 10);
$distinctDeviceDecision = (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(14, 'user-14'));
$assert($distinctDeviceDecision->clientIdsToDisconnect === [], '同设备多连接被误计为多个在线设备');
$distinctDeviceDecision->release();

$store->policies[10] = policyRow(10, ['max_online_devices' => 1]);
$store->users['10:user-10'] = true;
$staleSnapshot = deviceSnapshot(
    10,
    'user-10',
    'stale-device',
    'stale-client',
    time() - 100,
);
$staleSessionId = (string) json_decode($staleSnapshot, true, flags: JSON_THROW_ON_ERROR)['session_id'];
$redis->hashes['im:10:devices:user-10'][$staleSessionId] = $staleSnapshot;
$staleDecision = (new TenantImPolicyService($redis, $store))->authorizeAuth(authContext(10, 'user-10'));
$assert($staleDecision->clientIdsToDisconnect === [], '进程退出残留设备被误认为在线');
$assert(($redis->hashes['im:10:devices:user-10'] ?? []) === [], '失效连接的设备 hash 残留未清理');
$staleDecision->release();

$store->policies[11] = policyRow(11);
$store->users['11:user-11'] = true;
$reservationService = new TenantImPolicyService($redis, $store);
$reserved = $reservationService->authorizeAuth(authContext(11, 'user-11'));
$expectIm('AUTH_POLICY_BUSY', static fn () => $reservationService->authorizeAuth(authContext(11, 'user-11')));
$reserved->release();
$released = $reservationService->authorizeAuth(authContext(11, 'user-11'));
$released->release();
$assert(!isset($redis->strings['auth:policy:reservation:11:user-11']), 'AUTH 成功绑定后未释放设备策略预留锁');

$expectIm('ACCOUNT_POLICY_BLOCKED', static fn () => $service->authorizeAuth(authContext(1, 'inactive-user')));

$first = $service->acquireSendPermit(1, 'user-1', 'web', 1783600000);
$second = $service->acquireSendPermit(1, 'user-1', 'web', 1783600000);
$expectIm('MESSAGE_QPS_EXCEEDED', static fn () => $service->acquireSendPermit(1, 'user-1', 'web', 1783600000));
$first->release();
$second->release();

$store->policies[8] = policyRow(8, ['max_message_qps' => 10, 'max_message_concurrency' => 1]);
$store->users['8:user-8'] = true;
$concurrencyService = new TenantImPolicyService($redis, $store);
$permit = $concurrencyService->acquireSendPermit(8, 'user-8', 'web', 1783600000);
$expectIm('MESSAGE_CONCURRENCY_EXCEEDED', static fn () => $concurrencyService->acquireSendPermit(8, 'user-8', 'web', 1783600000));
$permit->release();
$retry = $concurrencyService->acquireSendPermit(8, 'user-8', 'web', 1783600000);
$retry->release();
$assert(!isset($redis->sortedSets['concurrency:message:user:8:user-8']), '发送完成后未释放并发租约');

$store->policies[9] = policyRow(9, ['max_message_qps' => 10, 'max_message_concurrency' => 1]);
$store->users['9:user-9'] = true;
$leaseService = new TenantImPolicyService($redis, $store);
$stalePermit = $leaseService->acquireSendPermit(9, 'user-9', 'web', 1783600000);
$leaseKey = 'concurrency:message:user:9:user-9';
$staleToken = array_key_first($redis->sortedSets[$leaseKey]);
unset($redis->sortedSets[$leaseKey][$staleToken]);
$currentPermit = $leaseService->acquireSendPermit(9, 'user-9', 'web', 1783600000);
$stalePermit->release();
$assert(count($redis->sortedSets[$leaseKey] ?? []) === 1, '过期 permit 释放误删了新并发租约');
$currentPermit->release();

fwrite(STDOUT, sprintf("Tenant IM runtime policy: %d assertions passed.\n", $assertions));
