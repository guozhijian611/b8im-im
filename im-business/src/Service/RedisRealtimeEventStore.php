<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Config;
use B8im\ImShared\Support\Constants;
use Redis;

final class RedisRealtimeEventStore implements RealtimeEventStoreInterface
{
    private const ORGANIZATION_INACTIVE_MARKER_TTL_SECONDS = 60;
    private const CLAIM_LEASE_SECONDS = 30;
    private const DONE_TTL_SECONDS = 86400;

    public function __construct(
        private readonly object $redis,
        private readonly int $organizationInactiveMarkerTtlSeconds = self::ORGANIZATION_INACTIVE_MARKER_TTL_SECONDS,
        private readonly int $claimLeaseSeconds = self::CLAIM_LEASE_SECONDS,
        private readonly int $doneTtlSeconds = self::DONE_TTL_SECONDS,
    ) {
        if ($organizationInactiveMarkerTtlSeconds < 1 || $organizationInactiveMarkerTtlSeconds > 300) {
            throw new \InvalidArgumentException('organization inactive marker TTL must be between 1 and 300 seconds');
        }
        if ($claimLeaseSeconds < 1 || $claimLeaseSeconds > 300) {
            throw new \InvalidArgumentException('realtime event claim lease must be between 1 and 300 seconds');
        }
        if ($doneTtlSeconds < 60 || $doneTtlSeconds > 604800) {
            throw new \InvalidArgumentException('realtime event deduplication TTL must be between 60 and 604800 seconds');
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

        return new self($redis);
    }

    public function claim(string $workerId): ?RealtimeEventClaim
    {
        if ($workerId === '' || trim($workerId) !== $workerId || strlen($workerId) > 160) {
            throw new \InvalidArgumentException('Realtime event worker id is invalid.');
        }

        $now = $this->nowMilliseconds();
        $claimToken = bin2hex(random_bytes(20));
        $result = $this->redis->eval($this->claimScript(), [
            Constants::REDIS_REALTIME_EVENTS,
            Constants::REDIS_REALTIME_EVENT_PROCESSING,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_IDS,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_WORKERS,
            Constants::REDIS_REALTIME_EVENT_INFLIGHT,
            Constants::REDIS_REALTIME_EVENT_LEASES,
            Constants::REDIS_REALTIME_EVENT_DONE,
            $claimToken,
            $workerId,
            (string) $now,
            (string) ($now + ($this->claimLeaseSeconds * 1000)),
        ], 7);

        if ($result === false) {
            throw new \RuntimeException('Redis failed to claim an IM realtime event.');
        }
        if (!is_array($result) || count($result) < 3) {
            return null;
        }

        $raw = $result[1] ?? null;
        $eventId = $result[2] ?? null;
        if (!is_string($raw) || !is_string($eventId)) {
            throw new \RuntimeException('Redis returned an invalid IM realtime event claim.');
        }

        return new RealtimeEventClaim(
            (string) ($result[0] ?? ''),
            $workerId,
            $raw,
            $eventId === '' ? null : $eventId,
        );
    }

    public function ack(RealtimeEventClaim $claim): void
    {
        $now = $this->nowMilliseconds();
        $result = $this->redis->eval($this->ackScript(), [
            Constants::REDIS_REALTIME_EVENT_PROCESSING,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_IDS,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_WORKERS,
            Constants::REDIS_REALTIME_EVENT_INFLIGHT,
            Constants::REDIS_REALTIME_EVENT_LEASES,
            Constants::REDIS_REALTIME_EVENT_DONE,
            $claim->claimToken,
            (string) ($now + ($this->doneTtlSeconds * 1000)),
        ], 6);
        if ($result === false) {
            throw new \RuntimeException('Redis failed to acknowledge an IM realtime event.');
        }
    }

    public function requeue(RealtimeEventClaim $claim): void
    {
        $result = $this->redis->eval($this->requeueScript(), [
            Constants::REDIS_REALTIME_EVENTS,
            Constants::REDIS_REALTIME_EVENT_PROCESSING,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_IDS,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_WORKERS,
            Constants::REDIS_REALTIME_EVENT_INFLIGHT,
            Constants::REDIS_REALTIME_EVENT_LEASES,
            $claim->claimToken,
        ], 6);
        if ($result === false) {
            throw new \RuntimeException('Redis failed to requeue an IM realtime event.');
        }
    }

    public function recoverExpired(int $limit = 100): int
    {
        if ($limit < 1 || $limit > 1000) {
            throw new \InvalidArgumentException('Realtime event recovery limit must be between 1 and 1000.');
        }

        $result = $this->redis->eval($this->recoverScript(), [
            Constants::REDIS_REALTIME_EVENTS,
            Constants::REDIS_REALTIME_EVENT_PROCESSING,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_IDS,
            Constants::REDIS_REALTIME_EVENT_PROCESSING_WORKERS,
            Constants::REDIS_REALTIME_EVENT_INFLIGHT,
            Constants::REDIS_REALTIME_EVENT_LEASES,
            (string) $this->nowMilliseconds(),
            (string) $limit,
        ], 6);
        if ($result === false || !is_int($result)) {
            throw new \RuntimeException('Redis failed to recover expired IM realtime event claims.');
        }

        return $result;
    }

    public function invalidateCredentialSessions(int $organization, array $credentialSessionIds): void
    {
        foreach (array_values(array_unique($credentialSessionIds)) as $credentialSessionId) {
            $result = $this->redis->del(sprintf(
                Constants::REDIS_AUTH_ACTIVE,
                $organization,
                $credentialSessionId,
            ));
            if ($result === false) {
                throw new \RuntimeException('Redis failed to invalidate an IM auth session cache');
            }
        }
    }

    public function setOrganizationInactive(int $organization, bool $inactive): void
    {
        $key = sprintf(Constants::REDIS_AUTH_ORGANIZATION_INACTIVE, $organization);
        if ($inactive) {
            if ($this->redis->setex($key, $this->organizationInactiveMarkerTtlSeconds, '1') !== true) {
                throw new \RuntimeException('Redis failed to mark an IM organization inactive');
            }
            return;
        }

        if ($this->redis->del($key) === false) {
            throw new \RuntimeException('Redis failed to clear the IM organization inactive marker');
        }
    }

    private function nowMilliseconds(): int
    {
        return (int) floor(microtime(true) * 1000);
    }

    private function claimScript(): string
    {
        return <<<'LUA'
redis.call('ZREMRANGEBYSCORE', KEYS[7], '-inf', ARGV[3])
for _ = 1, 100 do
    local raw = redis.call('LPOP', KEYS[1])
    if not raw then
        return {}
    end

    local event_id = ''
    local decoded_ok, decoded = pcall(cjson.decode, raw)
    if decoded_ok and type(decoded) == 'table' and type(decoded['event_id']) == 'string' then
        local candidate = decoded['event_id']
        if string.len(candidate) == 64 and string.match(candidate, '^[0-9a-f]+$') then
            event_id = candidate
        end
    end

    local duplicate = false
    if event_id ~= '' then
        duplicate = redis.call('ZSCORE', KEYS[7], event_id) ~= false
            or redis.call('HGET', KEYS[5], event_id) ~= false
    end
    if not duplicate then
        redis.call('HSET', KEYS[2], ARGV[1], raw)
        redis.call('HSET', KEYS[3], ARGV[1], event_id)
        redis.call('HSET', KEYS[4], ARGV[1], ARGV[2])
        if event_id ~= '' then
            redis.call('HSET', KEYS[5], event_id, ARGV[1])
        end
        redis.call('ZADD', KEYS[6], ARGV[4], ARGV[1])
        return {ARGV[1], raw, event_id}
    end
end
return {}
LUA;
    }

    private function ackScript(): string
    {
        return <<<'LUA'
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
    return 0
end
local event_id = redis.call('HGET', KEYS[2], ARGV[1]) or ''
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('ZREM', KEYS[5], ARGV[1])
if event_id ~= '' then
    if redis.call('HGET', KEYS[4], event_id) == ARGV[1] then
        redis.call('HDEL', KEYS[4], event_id)
    end
    redis.call('ZADD', KEYS[6], ARGV[2], event_id)
end
return 1
LUA;
    }

    private function requeueScript(): string
    {
        return <<<'LUA'
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 0 then
    return 0
end
local raw = redis.call('HGET', KEYS[2], ARGV[1])
local event_id = redis.call('HGET', KEYS[3], ARGV[1]) or ''
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[1])
redis.call('ZREM', KEYS[6], ARGV[1])
if event_id ~= '' and redis.call('HGET', KEYS[5], event_id) == ARGV[1] then
    redis.call('HDEL', KEYS[5], event_id)
end
redis.call('RPUSH', KEYS[1], raw)
return 1
LUA;
    }

    private function recoverScript(): string
    {
        return <<<'LUA'
local expired = redis.call('ZRANGEBYSCORE', KEYS[6], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local recovered = 0
for _, claim_token in ipairs(expired) do
    if redis.call('HEXISTS', KEYS[2], claim_token) == 1 then
        local raw = redis.call('HGET', KEYS[2], claim_token)
        local event_id = redis.call('HGET', KEYS[3], claim_token) or ''
        redis.call('HDEL', KEYS[2], claim_token)
        redis.call('HDEL', KEYS[3], claim_token)
        redis.call('HDEL', KEYS[4], claim_token)
        if event_id ~= '' and redis.call('HGET', KEYS[5], event_id) == claim_token then
            redis.call('HDEL', KEYS[5], event_id)
        end
        redis.call('RPUSH', KEYS[1], raw)
        recovered = recovered + 1
    end
    redis.call('ZREM', KEYS[6], claim_token)
end
return recovered
LUA;
    }
}
