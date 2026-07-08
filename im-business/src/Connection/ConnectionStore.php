<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | Redis 在线连接状态
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Connection;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImShared\Support\Constants;
use Redis;

final class ConnectionStore
{
    public function __construct(
        private readonly Redis $redis,
        private readonly Config $config,
    ) {
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

        return new self($redis, $config);
    }

    public function bind(string $clientId, AuthContext $context): void
    {
        $now = time();
        $payload = $context->toArray() + [
            'client_id' => $clientId,
            'bind_time' => $now,
            'last_seen' => $now,
        ];

        $this->redis->setex($this->clientIndexKey($clientId), $this->config->connectionTtl, (string) $context->organization);
        $this->redis->setex($this->clientKey($context->organization, $clientId), $this->config->connectionTtl, json_encode($payload, JSON_UNESCAPED_UNICODE));
        $this->redis->sAdd($this->onlineKey($context->organization, $context->userId), $clientId);
        $this->redis->expire($this->onlineKey($context->organization, $context->userId), $this->config->connectionTtl);
        $this->redis->hSet($this->devicesKey($context->organization, $context->userId), $context->deviceId, json_encode($payload, JSON_UNESCAPED_UNICODE));
        $this->redis->expire($this->devicesKey($context->organization, $context->userId), $this->config->connectionTtl);
    }

    public function touch(string $clientId): ?array
    {
        $connection = $this->get($clientId);
        if ($connection === null) {
            return null;
        }

        $connection['last_seen'] = time();
        $organization = (int) $connection['organization'];
        $this->redis->setex($this->clientIndexKey($clientId), $this->config->connectionTtl, (string) $organization);
        $this->redis->setex($this->clientKey($organization, $clientId), $this->config->connectionTtl, json_encode($connection, JSON_UNESCAPED_UNICODE));

        return $connection;
    }

    public function get(string $clientId): ?array
    {
        $organization = (int) $this->redis->get($this->clientIndexKey($clientId));
        if ($organization <= 0) {
            return null;
        }

        $raw = $this->redis->get($this->clientKey($organization, $clientId));
        if (!is_string($raw) || $raw === '') {
            return null;
        }

        $data = json_decode($raw, true);

        return is_array($data) ? $data : null;
    }

    public function unbind(string $clientId): ?array
    {
        $connection = $this->get($clientId);
        if ($connection === null) {
            $this->redis->del($this->clientIndexKey($clientId));
            return null;
        }

        $organization = (int) $connection['organization'];
        $userId = (string) $connection['user_id'];
        $deviceId = (string) $connection['device_id'];

        $this->redis->sRem($this->onlineKey($organization, $userId), $clientId);
        if ($deviceId !== '') {
            $this->redis->hDel($this->devicesKey($organization, $userId), $deviceId);
        }
        $this->redis->del($this->clientKey($organization, $clientId));
        $this->redis->del($this->clientIndexKey($clientId));

        return $connection;
    }

    private function clientIndexKey(string $clientId): string
    {
        return sprintf(Constants::REDIS_CLIENT_INDEX, $clientId);
    }

    private function clientKey(int $organization, string $clientId): string
    {
        return sprintf(Constants::REDIS_CLIENT, $organization, $clientId);
    }

    private function onlineKey(int $organization, string $userId): string
    {
        return sprintf(Constants::REDIS_ONLINE, $organization, $userId);
    }

    private function devicesKey(int $organization, string $userId): string
    {
        return sprintf(Constants::REDIS_DEVICES, $organization, $userId);
    }
}
