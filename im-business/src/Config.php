<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | BusinessWorker 运行配置
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness;

final class Config
{
    public function __construct(
        public readonly string $dbHost,
        public readonly int $dbPort,
        public readonly string $dbName,
        public readonly string $dbUser,
        public readonly string $dbPassword,
        public readonly string $dbCharset,
        public readonly string $redisHost,
        public readonly int $redisPort,
        public readonly string $redisPassword,
        public readonly int $redisDb,
        public readonly string $rabbitmqHost,
        public readonly int $rabbitmqPort,
        public readonly string $rabbitmqUser,
        public readonly string $rabbitmqPassword,
        public readonly string $rabbitmqVhost,
        public readonly string $rabbitmqExchange,
        public readonly bool $mqOutboxEnabled,
        public readonly int $mqOutboxProcessCount,
        public readonly int $mqOutboxBatchSize,
        public readonly int $mqOutboxIntervalMs,
        public readonly int $mqOutboxMaxRetry,
        public readonly int $mqOutboxRetryDelaySeconds,
        public readonly int $mqOutboxLockTtlSeconds,
        public readonly string $imTokenSecret,
        public readonly bool $allowInsecureToken,
        public readonly int $connectionTtl,
        public readonly int $recallWindowSeconds,
        public readonly int $editWindowSeconds,
        public readonly int $syncMaxLimit,
        public readonly int $messageShardBuckets,
    ) {
    }

    public static function fromEnv(): self
    {
        return new self(
            dbHost: self::env('DB_HOST', '127.0.0.1'),
            dbPort: (int) self::env('DB_PORT', '3306'),
            dbName: self::env('DB_NAME', 'nb8im'),
            dbUser: self::env('DB_USER', 'root'),
            dbPassword: self::env('DB_PASSWORD', ''),
            dbCharset: self::env('DB_CHARSET', 'utf8mb4'),
            redisHost: self::env('REDIS_HOST', '127.0.0.1'),
            redisPort: (int) self::env('REDIS_PORT', '6379'),
            redisPassword: self::env('REDIS_PASSWORD', ''),
            redisDb: (int) self::env('REDIS_DB', '0'),
            rabbitmqHost: self::env('RABBITMQ_HOST', '127.0.0.1'),
            rabbitmqPort: (int) self::env('RABBITMQ_PORT', '5672'),
            rabbitmqUser: self::env('RABBITMQ_USER', 'guest'),
            rabbitmqPassword: self::env('RABBITMQ_PASSWORD', 'guest'),
            rabbitmqVhost: self::env('RABBITMQ_VHOST', '/'),
            rabbitmqExchange: self::env('RABBITMQ_EXCHANGE', 'im.message'),
            mqOutboxEnabled: self::boolEnv('MQ_OUTBOX_ENABLED', true),
            mqOutboxProcessCount: max(0, (int) self::env('MQ_OUTBOX_PROCESS_COUNT', '1')),
            mqOutboxBatchSize: max(1, (int) self::env('MQ_OUTBOX_BATCH_SIZE', '50')),
            mqOutboxIntervalMs: max(100, (int) self::env('MQ_OUTBOX_INTERVAL_MS', '1000')),
            mqOutboxMaxRetry: max(1, (int) self::env('MQ_OUTBOX_MAX_RETRY', '10')),
            mqOutboxRetryDelaySeconds: max(1, (int) self::env('MQ_OUTBOX_RETRY_DELAY_SECONDS', '30')),
            mqOutboxLockTtlSeconds: max(10, (int) self::env('MQ_OUTBOX_LOCK_TTL_SECONDS', '60')),
            imTokenSecret: self::env('IM_TOKEN_SECRET', ''),
            allowInsecureToken: self::boolEnv('IM_TOKEN_ALLOW_INSECURE', false),
            connectionTtl: (int) self::env('IM_CONNECTION_TTL', (string) (86400 * 7)),
            recallWindowSeconds: (int) self::env('IM_RECALL_WINDOW_SECONDS', '120'),
            editWindowSeconds: (int) self::env('IM_EDIT_WINDOW_SECONDS', self::env('IM_RECALL_WINDOW_SECONDS', '120')),
            syncMaxLimit: (int) self::env('IM_SYNC_MAX_LIMIT', '100'),
            messageShardBuckets: min(1024, max(1, (int) self::env('IM_MESSAGE_SHARD_BUCKETS', '64'))),
        );
    }

    private static function env(string $key, string $default): string
    {
        $value = $_ENV[$key] ?? $_SERVER[$key] ?? getenv($key);
        if ($value === false || $value === null) {
            return $default;
        }

        return trim((string) $value);
    }

    private static function boolEnv(string $key, bool $default): bool
    {
        $value = self::env($key, $default ? 'true' : 'false');

        return in_array(strtolower($value), ['1', 'true', 'yes', 'on'], true);
    }
}
