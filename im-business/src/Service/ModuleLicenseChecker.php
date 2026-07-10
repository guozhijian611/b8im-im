<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 模块运行时启用校验
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImShared\Support\Constants;
use Redis;
use Throwable;

/**
 * 模块运行时启用校验
 *
 * 唯一执行边界：查 sm_tenant_module_license（叠加 sm_module 已安装且启用）。
 * 授权口径见 AGENTS.md：
 *   - 前端展示不作为权限依据
 *   - 套餐默认模块只表示权益，最终以 sm_tenant_module_license 的租户启用状态为准
 *
 * 性能：BusinessWorker 常驻进程，每条模块 cmd 都会校验，因此与 Server
 * 共用 module_license:{organization}:{module_key} JSON 快照。快照 TTL 有上限，
 * 且不会越过 effective_until；后台变更提交后删除同一 key 可立即生效。
 */
final class ModuleLicenseChecker
{
    /**
     * Server lifecycle mutations and IM cache misses may interleave. Reject a
     * late writer whose committed module/license version tuple is older than
     * the value already published by the control plane.
     */
    private const MONOTONIC_SET_LUA = <<<'LUA'
local currentRaw = redis.call('GET', KEYS[1])
if currentRaw then
    local currentOk, current = pcall(cjson.decode, currentRaw)
    local incomingOk, incoming = pcall(cjson.decode, ARGV[1])
    if currentOk and incomingOk then
        local currentModule = tonumber(current['module_lock_version']) or -1
        local incomingModule = tonumber(incoming['module_lock_version']) or -1
        local currentLicense = tonumber(current['version']) or -1
        local incomingLicense = tonumber(incoming['version']) or -1
        if incomingModule < currentModule
            or (incomingModule == currentModule and incomingLicense < currentLicense) then
            return 0
        end
    end
end
redis.call('SETEX', KEYS[1], tonumber(ARGV[2]), ARGV[1])
return 1
LUA;

    public function __construct(
        private readonly object $redis,
        private readonly ModuleLicenseRepositoryInterface $repository,
        private readonly int $cacheTtlSeconds,
    ) {
        if ($cacheTtlSeconds <= 0 || $cacheTtlSeconds > 300) {
            throw new \InvalidArgumentException('module license cache TTL must be between 1 and 300 seconds');
        }
    }

    public static function connect(Config $config, ModuleLicenseRepositoryInterface $repository): self
    {
        $redis = new Redis();
        $redis->connect($config->redisHost, $config->redisPort, 2.0);
        if ($config->redisPassword !== '') {
            $redis->auth($config->redisPassword);
        }
        if ($config->redisDb > 0) {
            $redis->select($config->redisDb);
        }

        return new self($redis, $repository, $config->moduleLicenseCacheTtlSeconds);
    }

    /**
     * 要求当前机构启用指定模块，未启用抛 ImException。
     *
     * @throws ImException
     */
    public function check(int $organization, string $moduleKey): void
    {
        if (!$this->isLicensed($organization, $moduleKey)) {
            throw new ImException('模块未启用', 'MODULE_NOT_LICENSED');
        }
    }

    /**
     * 判断机构是否启用模块（带缓存）。
     */
    public function isLicensed(int $organization, string $moduleKey): bool
    {
        if ($organization <= 0 || preg_match('/^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$/', $moduleKey) !== 1) {
            return false;
        }

        $cacheKey = sprintf(Constants::REDIS_MODULE_LICENSE, $organization, $moduleKey);
        $cachedPositive = false;
        try {
            $cached = $this->redis->get($cacheKey);
            if (is_string($cached) && $cached !== '') {
                $snapshot = json_decode($cached, true);
                if ($this->validSnapshot($snapshot)) {
                    if (!$this->snapshotAllows($snapshot)) {
                        return false;
                    }
                    // Redis positive snapshots are only a hint. A failed
                    // Server after-commit DEL must never extend authorization.
                    $cachedPositive = true;
                } else {
                    $this->redis->del($cacheKey);
                }
            }
        } catch (Throwable) {
            // Redis 不可用时回源 MySQL；MySQL 失败则在下方失败关闭。
        }

        try {
            $snapshot = $this->querySnapshot($organization, $moduleKey);
        } catch (Throwable) {
            if ($cachedPositive) {
                try {
                    $this->redis->del($cacheKey);
                } catch (Throwable) {
                }
            }
            return false;
        }

        try {
            $ttl = $this->cacheTtlSeconds;
            if ($snapshot['effective_until'] !== null) {
                $ttl = min($ttl, max(1, (int) $snapshot['effective_until'] - time()));
            }
            $encoded = json_encode($snapshot, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
            $this->redis->eval(
                self::MONOTONIC_SET_LUA,
                [$cacheKey, $encoded, (string) $ttl],
                1,
            );
        } catch (Throwable) {
            // 数据库快照仍是当次判断事实；缓存失败不改变结果。
        }

        return $this->snapshotAllows($snapshot);
    }

    /**
     * 主动失效某机构某模块的启用缓存（后台改能力边界后调用）。
     */
    public function invalidate(int $organization, string $moduleKey): void
    {
        $this->redis->del(sprintf(Constants::REDIS_MODULE_LICENSE, $organization, $moduleKey));
    }

    /**
     * 查库：模块已安装且启用 + 租户授权有效且未过期。
     */
    /**
     * @return array{
     *   enabled: bool,
     *   effective_until: int|null,
     *   version: int,
     *   module_version: string,
     *   module_lock_version: int,
     *   platforms: list<string>,
     *   capabilities: array<string, list<string>>
     * }
     */
    private function querySnapshot(int $organization, string $moduleKey): array
    {
        $row = $this->repository->fetchOne(
            'SELECT m.status AS module_status,
                    l.status AS license_status,
                    l.expire_at,
                    l.version AS license_version,
                    m.version AS module_version,
                    m.lock_version AS module_lock_version,
                    m.platforms_json,
                    m.capabilities_json
               FROM sm_tenant_module_license l
               INNER JOIN sm_module m
                  ON m.module_key = l.module_key
                 AND m.delete_time IS NULL
              WHERE l.organization = ?
                AND l.module_key = ?
                AND l.delete_time IS NULL
              LIMIT 1',
            [$organization, $moduleKey],
        );

        if ($row === null) {
            return [
                'enabled' => false,
                'effective_until' => null,
                'version' => 0,
                'module_version' => '',
                'module_lock_version' => 0,
                'platforms' => [],
                'capabilities' => [],
            ];
        }

        $effectiveUntil = empty($row['expire_at']) ? null : strtotime((string) $row['expire_at']);
        if ($effectiveUntil === false) {
            $effectiveUntil = 0;
        }

        return [
            'enabled' => (string) $row['module_status'] === 'ENABLED'
                && (string) $row['license_status'] === 'ENABLED',
            'effective_until' => $effectiveUntil,
            'version' => max((int) ($row['license_version'] ?? 0), 0),
            'module_version' => (string) ($row['module_version'] ?? ''),
            'module_lock_version' => max((int) ($row['module_lock_version'] ?? 0), 0),
            'platforms' => $this->decodeList($row['platforms_json'] ?? '[]'),
            'capabilities' => $this->decodeCapabilities($row['capabilities_json'] ?? '{}'),
        ];
    }

    /** @param array<string, mixed> $snapshot */
    private function snapshotAllows(array $snapshot): bool
    {
        if ($snapshot['enabled'] !== true) {
            return false;
        }

        if ($snapshot['effective_until'] !== null && (int) $snapshot['effective_until'] <= time()) {
            return false;
        }

        return in_array('im', $snapshot['platforms'], true);
    }

    private function validSnapshot(mixed $snapshot): bool
    {
        return is_array($snapshot)
            && isset($snapshot['enabled'], $snapshot['version'])
            && is_bool($snapshot['enabled'])
            && is_int($snapshot['version'])
            && array_key_exists('effective_until', $snapshot)
            && ($snapshot['effective_until'] === null || is_int($snapshot['effective_until']))
            && isset($snapshot['module_version'], $snapshot['module_lock_version'], $snapshot['platforms'], $snapshot['capabilities'])
            && is_string($snapshot['module_version'])
            && is_int($snapshot['module_lock_version'])
            && is_array($snapshot['platforms'])
            && is_array($snapshot['capabilities']);
    }

    /** @return list<string> */
    private function decodeList(mixed $value): array
    {
        $decoded = is_array($value) ? $value : json_decode((string) $value, true);
        if (!is_array($decoded)) {
            return [];
        }

        $result = [];
        foreach ($decoded as $item) {
            $item = trim((string) $item);
            if ($item !== '') {
                $result[$item] = $item;
            }
        }

        return array_values($result);
    }

    /** @return array<string, list<string>> */
    private function decodeCapabilities(mixed $value): array
    {
        $decoded = is_array($value) ? $value : json_decode((string) $value, true);
        if (!is_array($decoded)) {
            return [];
        }

        $result = [];
        foreach ($decoded as $platform => $capabilities) {
            if (!is_array($capabilities)) {
                continue;
            }
            $result[(string) $platform] = $this->decodeList($capabilities);
        }

        return $result;
    }
}
