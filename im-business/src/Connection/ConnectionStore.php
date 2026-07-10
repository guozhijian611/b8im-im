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
    private const COMPARE_AND_DELETE_DEVICE_SESSION = <<<'LUA'
local current = redis.call('HGET', KEYS[1], ARGV[1])
if not current then
    return 0
end
local ok, payload = pcall(cjson.decode, current)
if not ok then
    return 0
end
if tostring(payload['client_id']) == ARGV[2]
    and tostring(payload['session_id']) == ARGV[1]
    and tostring(payload['device_id']) == ARGV[3] then
    return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
LUA;

    public function __construct(
        private readonly Redis $redis,
        private readonly int $connectionTtl,
    ) {
        if ($connectionTtl <= 0) {
            throw new \InvalidArgumentException('connection TTL must be positive');
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

        return new self($redis, $config->connectionTtl);
    }

    public function bind(string $clientId, AuthContext $context): void
    {
        if ($context->clientId !== $clientId || $context->sessionId === '') {
            throw new \InvalidArgumentException('connection context does not match client_id or has no session_id');
        }

        $now = time();
        $payload = $context->toArray() + [
            'bind_time' => $now,
            'last_seen' => $now,
        ];
        $encodedPayload = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
        $encodedIndex = json_encode([
            'organization' => $context->organization,
            'session_id' => $context->sessionId,
        ], JSON_THROW_ON_ERROR);

        $this->redis->multi();
        $this->redis->setex($this->clientIndexKey($clientId), $this->connectionTtl, $encodedIndex);
        $this->redis->setex($this->clientKey($context->organization, $clientId), $this->connectionTtl, $encodedPayload);
        $this->redis->sAdd($this->onlineKey($context->organization, $context->userId), $clientId);
        $this->redis->expire($this->onlineKey($context->organization, $context->userId), $this->connectionTtl);
        // 同一设备可以并存多条连接：设备 Hash 以每次连接的 session_id 为字段，
        // device_id 只用于策略分组，不得覆盖其他连接。
        $this->redis->hSet($this->devicesKey($context->organization, $context->userId), $context->sessionId, $encodedPayload);
        $this->redis->expire($this->devicesKey($context->organization, $context->userId), $this->connectionTtl);
        $result = $this->redis->exec();
        if ($result === false) {
            throw new \RuntimeException('failed to bind IM connection in Redis');
        }
    }

    public function touch(string $clientId): ?array
    {
        $connection = $this->get($clientId);
        if ($connection === null) {
            return null;
        }

        $connection['last_seen'] = time();
        $organization = (int) $connection['organization'];
        $encodedConnection = json_encode($connection, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
        $this->redis->multi();
        $this->redis->setex(
            $this->clientIndexKey($clientId),
            $this->connectionTtl,
            json_encode([
                'organization' => $organization,
                'session_id' => (string) $connection['session_id'],
            ], JSON_THROW_ON_ERROR),
        );
        $this->redis->setex(
            $this->clientKey($organization, $clientId),
            $this->connectionTtl,
            $encodedConnection,
        );
        $this->redis->expire(
            $this->onlineKey($organization, (string) $connection['user_id']),
            $this->connectionTtl,
        );
        $this->redis->hSet(
            $this->devicesKey($organization, (string) $connection['user_id']),
            (string) $connection['session_id'],
            $encodedConnection,
        );
        $this->redis->expire(
            $this->devicesKey($organization, (string) $connection['user_id']),
            $this->connectionTtl,
        );
        $result = $this->redis->exec();
        if ($result === false) {
            throw new \RuntimeException('failed to touch IM connection in Redis');
        }

        return $connection;
    }

    public function get(string $clientId): ?array
    {
        $indexRaw = $this->redis->get($this->clientIndexKey($clientId));
        if (!is_string($indexRaw) || $indexRaw === '') {
            return null;
        }
        $index = json_decode($indexRaw, true);
        if (!is_array($index)) {
            return null;
        }

        $organization = (int) ($index['organization'] ?? 0);
        $sessionId = (string) ($index['session_id'] ?? '');
        if ($organization <= 0) {
            return null;
        }

        $raw = $this->redis->get($this->clientKey($organization, $clientId));
        if (!is_string($raw) || $raw === '') {
            return null;
        }

        $data = json_decode($raw, true);

        if (
            !is_array($data)
            || (string) ($data['client_id'] ?? '') !== $clientId
            || $sessionId === ''
            || !hash_equals($sessionId, (string) ($data['session_id'] ?? ''))
        ) {
            return null;
        }

        return $data;
    }

    public function isBoundConnection(AuthContext $context): bool
    {
        $raw = $this->redis->hGet(
            $this->devicesKey($context->organization, $context->userId),
            $context->sessionId,
        );
        if (!is_string($raw) || $raw === '') {
            return false;
        }
        $deviceConnection = json_decode($raw, true);
        $clientConnection = $this->get($context->clientId);

        return is_array($deviceConnection)
            && is_array($clientConnection)
            && $this->matchesContext($deviceConnection, $context)
            && $this->matchesContext($clientConnection, $context);
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
        $sessionId = (string) ($connection['session_id'] ?? '');

        $this->redis->sRem($this->onlineKey($organization, $userId), $clientId);
        if ($deviceId !== '' && $sessionId !== '') {
            $this->redis->eval(
                self::COMPARE_AND_DELETE_DEVICE_SESSION,
                [
                    $this->devicesKey($organization, $userId),
                    $sessionId,
                    $clientId,
                    $deviceId,
                ],
                1,
            );
        }
        $this->redis->del($this->clientKey($organization, $clientId));
        $this->redis->del($this->clientIndexKey($clientId));

        return $connection;
    }

    /** @param array<string, mixed> $connection */
    private function matchesContext(array $connection, AuthContext $context): bool
    {
        return (int) ($connection['organization'] ?? 0) === $context->organization
            && hash_equals($context->userId, (string) ($connection['user_id'] ?? ''))
            && hash_equals($context->deviceId, (string) ($connection['device_id'] ?? ''))
            && hash_equals($context->clientId, (string) ($connection['client_id'] ?? ''))
            && hash_equals($context->sessionId, (string) ($connection['session_id'] ?? ''));
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
