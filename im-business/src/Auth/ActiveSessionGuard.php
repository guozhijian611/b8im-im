<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 已鉴权连接的有界活性重验
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImShared\Support\Constants;
use Redis;
use Throwable;

final class ActiveSessionGuard
{
    public function __construct(
        private readonly AuthIdentityValidator $validator,
        private readonly Redis $redis,
        private readonly int $ttlSeconds,
    ) {
        if ($ttlSeconds <= 0 || $ttlSeconds > 30) {
            throw new \InvalidArgumentException('AUTH revalidation TTL must be between 1 and 30 seconds');
        }
    }

    public static function connect(
        Config $config,
        AuthIdentityValidator $validator,
    ): self {
        $redis = new Redis();
        $redis->connect($config->redisHost, $config->redisPort, 2.0);
        if ($config->redisPassword !== '') {
            $redis->auth($config->redisPassword);
        }
        if ($config->redisDb > 0) {
            $redis->select($config->redisDb);
        }

        return new self($validator, $redis, $config->authRevalidateTtlSeconds);
    }

    public function assertActive(AuthContext $context): void
    {
        try {
            if ($this->redis->get($this->organizationInactiveKey($context->organization)) === '1') {
                throw new ImException('目标 organization 已停用', 'AUTH_ORGANIZATION_INACTIVE');
            }
        } catch (ImException $exception) {
            throw $exception;
        } catch (Throwable) {
            // Redis 不可用时继续回源 MySQL，不能使用正缓存放行。
            $this->validator->assertActive($context);
            return;
        }

        $key = $this->key($context->organization, $context->credentialSessionId);
        try {
            if ($this->redis->get($key) === '1') {
                return;
            }
        } catch (Throwable) {
            // Redis 不可用时必须回源 MySQL，不能放行。
        }

        $this->validator->assertActive($context);

        try {
            $ttl = min($this->ttlSeconds, max(1, $context->expireAt - time()));
            $this->redis->setex($key, $ttl, '1');
        } catch (Throwable) {
            // MySQL 已确认当次活性；缓存写失败不改变本次结果。
        }
    }

    public function invalidate(int $organization, string $credentialSessionId): void
    {
        if ($organization <= 0 || trim($credentialSessionId) === '') {
            return;
        }
        try {
            $this->redis->del($this->key($organization, $credentialSessionId));
        } catch (Throwable) {
        }
    }

    private function key(int $organization, string $credentialSessionId): string
    {
        return sprintf(Constants::REDIS_AUTH_ACTIVE, $organization, $credentialSessionId);
    }

    private function organizationInactiveKey(int $organization): string
    {
        return sprintf(Constants::REDIS_AUTH_ORGANIZATION_INACTIVE, $organization);
    }
}
