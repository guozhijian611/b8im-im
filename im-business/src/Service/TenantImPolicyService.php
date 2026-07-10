<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;
use Closure;
use Redis;
use Throwable;

final class TenantImPolicyService
{
    private const POLICY_CACHE_KEY = 'tenant_im_policy:%d';

    private const DEVICES_KEY = 'im:%d:devices:%s';

    private const CLIENT_KEY = 'im:%d:client:%s';

    private const AUTH_RESERVATION_KEY = 'auth:policy:reservation:%d:%s';

    private const QPS_KEY = 'rate:message:user:%d:%s:%d';

    private const CONCURRENCY_KEY = 'concurrency:message:user:%d:%s';

    private const ACQUIRE_SEND_LUA = <<<'LUA'
local qps = tonumber(redis.call('GET', KEYS[1]) or '0')
if qps >= tonumber(ARGV[1]) then
    return -1
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', tonumber(ARGV[3]))
local concurrent = tonumber(redis.call('ZCARD', KEYS[2]) or '0')
if concurrent >= tonumber(ARGV[2]) then
    return -2
end
redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], 2)
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]) + tonumber(ARGV[4]), ARGV[5])
redis.call('EXPIRE', KEYS[2], math.ceil(tonumber(ARGV[4]) / 1000) + 1)
return 1
LUA;

    private const RELEASE_SEND_LUA = <<<'LUA'
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('ZCARD', KEYS[1]) == 0 then
    redis.call('DEL', KEYS[1])
end
return removed
LUA;

    private const ACQUIRE_AUTH_RESERVATION_LUA = <<<'LUA'
local result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', tonumber(ARGV[2]))
if result then
    return 1
end
return 0
LUA;

    private const RELEASE_AUTH_RESERVATION_LUA = <<<'LUA'
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
LUA;

    private const MAX_CACHE_TTL_SECONDS = 60;

    private const CONCURRENCY_LEASE_SECONDS = 15;

    private const AUTH_RESERVATION_MILLISECONDS = 5000;

    public function __construct(
        private readonly object $redis,
        private readonly TenantImPolicyStoreInterface $store,
        private readonly int $cacheTtlSeconds = 30,
    ) {
        if ($cacheTtlSeconds < 1 || $cacheTtlSeconds > self::MAX_CACHE_TTL_SECONDS) {
            throw new \InvalidArgumentException('tenant IM policy cache TTL must be between 1 and 60 seconds');
        }
    }

    public static function connect(Config $config, ImRepository $repository): self
    {
        $redis = new Redis();
        $redis->connect($config->redisHost, $config->redisPort, 2.0);
        if ($config->redisPassword !== '') {
            $redis->auth($config->redisPassword);
        }
        if ($config->redisDb > 0) {
            $redis->select($config->redisDb);
        }

        return new self($redis, new ImRepositoryTenantImPolicyStore($repository));
    }

    public function policy(int $organization): TenantImPolicySnapshot
    {
        if ($organization <= 0) {
            throw new ImException('租户 IM 策略不可用', 'TENANT_POLICY_FORBIDDEN');
        }
        $key = sprintf(self::POLICY_CACHE_KEY, $organization);
        $cachedPositive = false;

        try {
            $cached = $this->redis->get($key);
            if (is_string($cached) && $cached !== '') {
                $decoded = json_decode($cached, true);
                if (is_array($decoded)) {
                    try {
                        $policy = TenantImPolicySnapshot::fromCache($decoded);
                        if ($policy->organization === $organization) {
                            if ($policy->status !== 'ENABLED') {
                                return $policy;
                            }
                            // Positive policy caches are only hints. A failed
                            // control-plane DEL/event must not extend access.
                            $cachedPositive = true;
                        }
                    } catch (Throwable) {
                    }
                }
                if (!isset($policy) || $policy->organization !== $organization) {
                    $this->redis->del($key);
                }
            }
        } catch (Throwable) {
            // Redis 不可用时必须回源 MySQL。
        }

        try {
            $row = $this->store->fetchPolicy($organization);
            if ($row === null) {
                throw new \UnexpectedValueException('tenant IM policy is missing');
            }
            $policy = TenantImPolicySnapshot::fromDatabaseRow($row);
            if ($policy->organization !== $organization) {
                throw new \UnexpectedValueException('tenant IM policy organization mismatch');
            }
        } catch (Throwable $exception) {
            if ($cachedPositive) {
                try {
                    $this->redis->del($key);
                } catch (Throwable) {
                }
            }
            throw new ImException('租户 IM 策略不可用', 'TENANT_POLICY_FORBIDDEN');
        }

        try {
            $this->redis->setex(
                $key,
                $this->cacheTtlSeconds,
                json_encode($policy->toCache(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
            );
        } catch (Throwable) {
            // 本次数据库事实仍然有效，缓存失败不改变判断。
        }

        return $policy;
    }

    public function authorizeAuth(AuthContext $context): TenantImAuthDecision
    {
        $policy = $this->policy($context->organization);
        $this->assertEnabled($policy);
        $this->assertClientFamilyAllowed($policy, $context->clientFamily);
        $this->assertActiveUser($context->organization, $context->userId);

        $releaseReservation = $this->acquireAuthReservation($context->organization, $context->userId);
        try {
            return $this->authorizeReserved($context, $policy, $releaseReservation);
        } catch (Throwable $exception) {
            $releaseReservation();
            throw $exception;
        }
    }

    private function authorizeReserved(
        AuthContext $context,
        TenantImPolicySnapshot $policy,
        Closure $releaseReservation,
    ): TenantImAuthDecision {

        $devicesKey = sprintf(self::DEVICES_KEY, $context->organization, $context->userId);
        try {
            $rawDevices = $this->redis->hGetAll($devicesKey);
        } catch (Throwable) {
            throw new ImException('无法校验多端在线策略', 'TENANT_POLICY_FORBIDDEN');
        }
        if (!is_array($rawDevices)) {
            throw new ImException('无法校验多端在线策略', 'TENANT_POLICY_FORBIDDEN');
        }

        /** @var array<string, array<string, array<string, mixed>>> $connectionsByDevice */
        $connectionsByDevice = [];
        foreach ($rawDevices as $connectionSessionId => $raw) {
            if (
                !is_string($connectionSessionId)
                || preg_match('/^[a-f0-9]{32}$/', $connectionSessionId) !== 1
                || !is_string($raw)
                || $raw === ''
            ) {
                throw new ImException('在线设备快照无效', 'TENANT_POLICY_FORBIDDEN');
            }
            $entry = json_decode($raw, true);
            if (!is_array($entry)) {
                throw new ImException('在线设备快照无效', 'TENANT_POLICY_FORBIDDEN');
            }
            $deviceId = (string) ($entry['device_id'] ?? '');
            if ((int) ($entry['organization'] ?? 0) !== $context->organization
                || (string) ($entry['user_id'] ?? '') !== $context->userId
                || $deviceId === ''
                || trim((string) ($entry['client_id'] ?? '')) === ''
                || !hash_equals($connectionSessionId, (string) ($entry['session_id'] ?? ''))) {
                throw new ImException('在线设备快照无效', 'TENANT_POLICY_FORBIDDEN');
            }
            try {
                $clientRaw = $this->redis->get(sprintf(
                    self::CLIENT_KEY,
                    $context->organization,
                    (string) $entry['client_id'],
                ));
                if (!is_string($clientRaw) || $clientRaw === '') {
                    // Hash 字段没有独立 TTL；进程异常退出后以连接 key
                    // 判定并精确清理该 connection session，不影响同设备其他连接。
                    $this->redis->hDel($devicesKey, $connectionSessionId);
                    continue;
                }
                $client = json_decode($clientRaw, true);
            } catch (Throwable) {
                throw new ImException('无法校验在线连接快照', 'TENANT_POLICY_FORBIDDEN');
            }
            if (!is_array($client)
                || (int) ($client['organization'] ?? 0) !== $context->organization
                || (string) ($client['user_id'] ?? '') !== $context->userId
                || (string) ($client['device_id'] ?? '') !== $deviceId
                || (string) ($client['client_id'] ?? '') !== (string) $entry['client_id']
                || !hash_equals($connectionSessionId, (string) ($client['session_id'] ?? ''))) {
                throw new ImException('在线连接与设备快照不一致', 'TENANT_POLICY_FORBIDDEN');
            }
            if ((string) $entry['client_id'] !== $context->clientId) {
                $connectionsByDevice[$deviceId][$connectionSessionId] = $entry;
            }
        }

        $disconnect = [];
        if (isset($connectionsByDevice[$context->deviceId])) {
            $sameDeviceConnections = $connectionsByDevice[$context->deviceId];
            if ($policy->sameDeviceLoginPolicy === 'reject') {
                throw new ImException('同一设备已在线', 'SAME_DEVICE_LOGIN_REJECTED');
            }
            if ($policy->sameDeviceLoginPolicy === 'replace') {
                foreach ($sameDeviceConnections as $connection) {
                    $disconnect[] = (string) $connection['client_id'];
                }
            }
            // coexist 保留所有同设备连接；设备上限只按不同 device_id 计数。
            unset($connectionsByDevice[$context->deviceId]);
        }

        if ($connectionsByDevice !== []) {
            if ($policy->crossDeviceLoginPolicy === 'reject_new') {
                throw new ImException('租户策略禁止新设备登录', 'CROSS_DEVICE_LOGIN_REJECTED');
            }
            if (!$policy->allowMultiDeviceOnline || $policy->crossDeviceLoginPolicy === 'kick_old') {
                foreach ($connectionsByDevice as $deviceConnections) {
                    foreach ($deviceConnections as $connection) {
                        $disconnect[] = (string) $connection['client_id'];
                    }
                }
            } elseif (count($connectionsByDevice) + 1 > $policy->maxOnlineDevices) {
                throw new ImException('在线设备数超过租户上限', 'DEVICE_LIMIT_EXCEEDED');
            }
        }

        return new TenantImAuthDecision(
            array_values(array_unique($disconnect)),
            $releaseReservation,
        );
    }

    public function assertConnectionAllowed(int $organization, string $clientFamily): void
    {
        $policy = $this->policy($organization);
        $this->assertEnabled($policy);
        $this->assertClientFamilyAllowed($policy, $clientFamily);
    }

    public function acquireSendPermit(
        int $organization,
        string $userId,
        string $clientFamily,
        ?int $now = null,
    ): TenantImSendPermit
    {
        $policy = $this->policy($organization);
        $this->assertEnabled($policy);
        $this->assertClientFamilyAllowed($policy, $clientFamily);
        $this->assertActiveUser($organization, $userId);

        $second = $now ?? time();
        $qpsKey = sprintf(self::QPS_KEY, $organization, $userId, $second);
        $concurrencyKey = sprintf(self::CONCURRENCY_KEY, $organization, $userId);
        $permitToken = bin2hex(random_bytes(16));
        $nowMilliseconds = (int) floor(microtime(true) * 1000);
        try {
            $result = (int) $this->redis->eval(self::ACQUIRE_SEND_LUA, [
                $qpsKey,
                $concurrencyKey,
                (string) $policy->maxMessageQps,
                (string) $policy->maxMessageConcurrency,
                (string) $nowMilliseconds,
                (string) (self::CONCURRENCY_LEASE_SECONDS * 1000),
                $permitToken,
            ], 2);
        } catch (Throwable) {
            throw new ImException('消息限流器不可用', 'TENANT_POLICY_FORBIDDEN');
        }
        if ($result === -1) {
            throw new ImException('消息发送频率超过租户上限', 'MESSAGE_QPS_EXCEEDED');
        }
        if ($result === -2) {
            throw new ImException('消息发送并发超过租户上限', 'MESSAGE_CONCURRENCY_EXCEEDED');
        }
        if ($result !== 1) {
            throw new ImException('消息限流器返回无效结果', 'TENANT_POLICY_FORBIDDEN');
        }

        return new TenantImSendPermit(Closure::fromCallable(function () use ($concurrencyKey, $permitToken): void {
            try {
                $this->redis->eval(self::RELEASE_SEND_LUA, [$concurrencyKey, $permitToken], 1);
            } catch (Throwable) {
                // 并发租约有短 TTL，释放失败不改变已成立的消息事实。
            }
        }));
    }

    public function invalidate(int $organization): void
    {
        try {
            $this->redis->del(sprintf(self::POLICY_CACHE_KEY, $organization));
        } catch (Throwable) {
            // 缓存 TTL 最长 60 秒，下次变更事件可重试。
        }
    }

    private function assertEnabled(TenantImPolicySnapshot $policy): void
    {
        if ($policy->status !== 'ENABLED') {
            throw new ImException('租户 IM 策略已停用', 'TENANT_POLICY_FORBIDDEN');
        }
    }

    private function assertClientFamilyAllowed(TenantImPolicySnapshot $policy, string $clientFamily): void
    {
        if (!in_array($clientFamily, ['web', 'app', 'desktop'], true)
            || !in_array($clientFamily, $policy->allowedClientFamilies, true)) {
            throw new ImException('当前客户端形态被租户策略禁止', 'TENANT_POLICY_FORBIDDEN');
        }
    }

    private function assertActiveUser(int $organization, string $userId): void
    {
        if ($organization <= 0 || trim($userId) === '') {
            throw new ImException('IM 席位无效', 'ACCOUNT_POLICY_BLOCKED');
        }
        try {
            $active = $this->store->isActiveUser($organization, $userId);
        } catch (Throwable) {
            $active = false;
        }
        if (!$active) {
            throw new ImException('IM 席位不存在或已停用', 'ACCOUNT_POLICY_BLOCKED');
        }
    }

    /** @return Closure(): void */
    private function acquireAuthReservation(int $organization, string $userId): Closure
    {
        $key = sprintf(self::AUTH_RESERVATION_KEY, $organization, $userId);
        $token = bin2hex(random_bytes(16));
        try {
            $acquired = (int) $this->redis->eval(self::ACQUIRE_AUTH_RESERVATION_LUA, [
                $key,
                $token,
                (string) self::AUTH_RESERVATION_MILLISECONDS,
            ], 1);
        } catch (Throwable) {
            throw new ImException('多端在线策略锁不可用', 'TENANT_POLICY_FORBIDDEN');
        }
        if ($acquired !== 1) {
            throw new ImException('当前账号正在处理其他登录', 'AUTH_POLICY_BUSY');
        }

        return Closure::fromCallable(function () use ($key, $token): void {
            try {
                $this->redis->eval(self::RELEASE_AUTH_RESERVATION_LUA, [$key, $token], 1);
            } catch (Throwable) {
                // 预留锁有 5 秒 TTL，释放失败不会形成永久锁。
            }
        });
    }
}
