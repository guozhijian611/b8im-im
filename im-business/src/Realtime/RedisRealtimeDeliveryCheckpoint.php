<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Config;
use Redis;

final class RedisRealtimeDeliveryCheckpoint implements RealtimeDeliveryCheckpoint
{
    public function __construct(
        private readonly Redis $redis,
        private readonly int $ttlSeconds,
    ) {
        if ($ttlSeconds < 1) {
            throw new \InvalidArgumentException('realtime delivery checkpoint TTL must be positive');
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

    public function wasDelivered(RealtimeEvent $event, string $clientId): bool
    {
        return $this->redis->exists($this->key($event, $clientId)) === 1;
    }

    public function markDelivered(RealtimeEvent $event, string $clientId): void
    {
        $stored = $this->redis->set(
            $this->key($event, $clientId),
            '1',
            ['ex' => $this->ttlSeconds],
        );
        if ($stored !== true) {
            throw new \RuntimeException('Redis did not persist the realtime delivery checkpoint');
        }
    }

    private function key(RealtimeEvent $event, string $clientId): string
    {
        return sprintf(
            'im:realtime:delivered:%s:%s',
            $event->eventId(),
            hash('sha256', $clientId),
        );
    }
}
