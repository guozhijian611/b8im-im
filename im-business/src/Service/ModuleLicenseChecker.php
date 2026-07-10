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
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Support\Constants;
use Redis;

/**
 * 模块运行时启用校验
 *
 * 唯一执行边界：查 sm_tenant_module_license（叠加 sm_module 已安装且启用）。
 * 授权口径见 AGENTS.md：
 *   - 前端展示不作为权限依据
 *   - 套餐默认模块只表示权益，最终以 sm_tenant_module_license 的租户启用状态为准
 *
 * 性能：BusinessWorker 常驻进程，每条模块 cmd 都会校验，因此加 Redis 长缓存。
 * 后台改租户能力边界后必须主动调用失效/刷新逻辑，不能依赖固定过期时间兜底。
 */
final class ModuleLicenseChecker
{
    public function __construct(
        private readonly Redis $redis,
        private readonly ImRepository $repository,
    ) {
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

        return new self($redis, $repository);
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
        if ($organization <= 0 || $moduleKey === '') {
            return false;
        }

        $cacheKey = sprintf(Constants::REDIS_MODULE_LICENSE, $organization, $moduleKey);
        $cached = $this->redis->get($cacheKey);
        if ($cached !== false) {
            return $cached === '1';
        }

        $licensed = $this->queryLicensed($organization, $moduleKey);
        $this->redis->set($cacheKey, $licensed ? '1' : '0');

        return $licensed;
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
    private function queryLicensed(int $organization, string $moduleKey): bool
    {
        $row = $this->repository->fetchOne(
            'SELECT l.id
               FROM sm_tenant_module_license l
               INNER JOIN sm_module m
                  ON m.module_key = l.module_key
                 AND m.status = 1
                 AND m.delete_time IS NULL
              WHERE l.organization = ?
                AND l.module_key = ?
                AND l.status = 1
                AND l.delete_time IS NULL
                AND (l.expire_at IS NULL OR l.expire_at > NOW())
              LIMIT 1',
            [$organization, $moduleKey],
        );

        return $row !== null;
    }
}
