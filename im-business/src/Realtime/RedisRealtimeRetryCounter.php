<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Config;
use B8im\ImShared\Support\Constants;
use Redis;

final class RedisRealtimeRetryCounter implements RealtimeRetryCounter
{
    public function __construct(
        private readonly Redis $redis,
        private readonly int $ttlSeconds,
    ) {
        if ($ttlSeconds < 1) {
            throw new \InvalidArgumentException('realtime retry TTL must be positive');
        }
    }

    public static function connect(Config $config): self
    {
        $redis = new Redis();
        $redis->connect($config->redisHost, $config->redisPort, 2.0);
        if ($config->redisPassword !== '') {
            $redis->auth($config->redisPassword);
        }
        if ($config->redisDb > 0) {
            $redis->select($config->redisDb);
        }

        return new self($redis, $config->mqRealtimeRetryTtlSeconds);
    }

    public function increment(RealtimeEvent $event): int
    {
        $script = <<<'LUA'
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return count
LUA;
        $count = $this->redis->eval($script, [$this->key($event), $this->ttlSeconds], 1);
        if (!is_int($count) || $count < 1) {
            throw new \RuntimeException('Redis did not persist the realtime retry counter');
        }

        return $count;
    }

    public function clear(RealtimeEvent $event): void
    {
        $this->redis->del($this->key($event));
    }

    private function key(RealtimeEvent $event): string
    {
        return sprintf(
            Constants::REDIS_REALTIME_RETRY,
            $event->organization,
            $event->messageId,
            $event->eventType,
            $event->changeSeq,
        );
    }
}
