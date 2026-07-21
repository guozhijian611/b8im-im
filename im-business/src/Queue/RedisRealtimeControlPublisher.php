<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Queue;

use B8im\ImBusiness\Config;
use B8im\ImShared\Support\Constants;
use Closure;
use Throwable;

final class RedisRealtimeControlPublisher
{
    private ?object $redis;
    private readonly ?Closure $connector;

    public function __construct(?object $redis = null, ?callable $connector = null)
    {
        if (($redis === null) === ($connector === null)) {
            throw new \InvalidArgumentException('control Redis publisher requires exactly one connection source');
        }
        $this->redis = $redis;
        $this->connector = $connector === null ? null : Closure::fromCallable($connector);
    }

    public static function connect(Config $config): self
    {
        return new self(connector: static function () use ($config): object {
            return self::open($config);
        });
    }

    private static function open(Config $config): object
    {
        return RedisRealtimeControlSocket::connect(
            $config->redisHost,
            $config->redisPort,
            $config->redisPassword,
            $config->redisDb,
        );
    }

    public function publish(string $raw): void
    {
        try {
            $redis = $this->redis;
            if ($redis === null) {
                $connector = $this->connector;
                if ($connector === null) {
                    throw new \RuntimeException('control outbox Redis connector is missing');
                }
                $redis = $connector();
                $this->redis = $redis;
            }
            if ($redis->rPush(Constants::REDIS_REALTIME_EVENTS, $raw) === false) {
                throw new \RuntimeException('control outbox Redis RPUSH failed');
            }
        } catch (Throwable $error) {
            // A broken socket must not poison later ticks. The next publish
            // reconnects lazily while the independent Rabbit outbox proceeds.
            $this->redis = null;
            throw $error;
        }
    }
}
