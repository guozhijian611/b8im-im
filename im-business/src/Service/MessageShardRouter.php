<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 消息月表分片路由
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Repository\MessageShardRepositoryInterface;
use B8im\ImBusiness\Exception\ImException;
use InvalidArgumentException;

final class MessageShardRouter
{
    private const BASE_TABLE = 'im_message';
    private const INDEX_TABLE = 'im_message_index';
    private const REQUIRED_RUNTIME_TABLES = [
        'im_runtime_config',
        'im_user',
        'im_user_profile',
        'im_user_privacy_setting',
        'im_user_security_policy',
        'im_friend_relation',
        'im_friend_request',
        'im_user_device',
        'im_user_login_audit',
        'im_web_access_session',
        'im_auth_session',
        'im_upload_asset',
        'im_conversation',
        'im_cross_organization_conversation',
        'im_group_profile',
        'im_conversation_member',
        'im_message_group',
        'im_conversation_membership_period',
        'im_organization_message_sequence',
        'im_message_index',
        'im_message_receipt',
        'im_message_user_delete',
        'im_message_change',
        'im_message_outbox',
        'sm_tenant_im_policy',
    ];

    private array $tableCache = [];
    private array $verifiedTables = [];
    private bool $indexVerified = false;

    public function __construct(
        private readonly MessageShardRepositoryInterface $repository,
        private readonly int $bucketCount,
    )
    {
    }

    public function writeTable(int $organization, string $conversationId, string $time): string
    {
        $timestamp = strtotime($time) ?: time();
        $table = sprintf('%s_%04d_%s', self::BASE_TABLE, $this->bucket($organization, $conversationId), date('Ym', $timestamp));
        $this->assertTableExists($table);

        return $table;
    }

    public function tablesForConversationNewestFirst(int $organization, string $conversationId): array
    {
        $bucket = $this->bucket($organization, $conversationId);
        $cacheKey = 'bucket:' . $bucket;
        if (isset($this->tableCache[$cacheKey])) {
            return $this->tableCache[$cacheKey];
        }

        $rows = $this->repository->fetchAll(
            "SELECT TABLE_NAME AS table_name
               FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME REGEXP ?
              ORDER BY TABLE_NAME DESC",
            ['^im_message_' . str_pad((string) $bucket, 4, '0', STR_PAD_LEFT) . '_[0-9]{6}$']
        );

        $tables = array_map(static fn (array $row): string => (string) $row['table_name'], $rows);
        $this->tableCache[$cacheKey] = array_values(array_unique(array_filter($tables, fn (string $table): bool => $this->isMessageTable($table))));

        return $this->tableCache[$cacheKey];
    }

    public function tablesNewestFirst(): array
    {
        if (isset($this->tableCache['all'])) {
            return $this->tableCache['all'];
        }

        $rows = $this->repository->fetchAll(
            "SELECT TABLE_NAME AS table_name
               FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME REGEXP '^im_message_[0-9]{4}_[0-9]{6}$'
              ORDER BY TABLE_NAME DESC"
        );

        $tables = array_map(static fn (array $row): string => (string) $row['table_name'], $rows);
        $this->tableCache['all'] = array_values(array_unique(array_filter($tables, fn (string $table): bool => $this->isMessageTable($table))));

        return $this->tableCache['all'];
    }

    public function indexTable(): string
    {
        return self::INDEX_TABLE;
    }

    public function assertIndexTableReady(): void
    {
        if ($this->indexVerified) {
            return;
        }

        if (!$this->tableExists(self::INDEX_TABLE)) {
            throw new ImException('IM 消息索引表未迁移，拒绝在请求路径建表', 'IM_SCHEMA_NOT_READY');
        }
        $this->indexVerified = true;
    }

    /**
     * Worker 启动时预检当月和下月所有分片，不得把 DDL 推迟到 SEND 请求。
     */
    public function preflight(): void
    {
        foreach (self::REQUIRED_RUNTIME_TABLES as $table) {
            if (!$this->tableExists($table)) {
                throw new ImException('IM 运行时表未迁移: ' . $table, 'IM_SCHEMA_NOT_READY');
            }
        }
        if (!$this->tableExists(self::BASE_TABLE)) {
            throw new ImException('IM 消息模板表未迁移: ' . self::BASE_TABLE, 'IM_SCHEMA_NOT_READY');
        }
        $configuredBuckets = $this->repository->fetchOne(
            'SELECT config_value FROM im_runtime_config
              WHERE config_key = ? LIMIT 1',
            ['message_shard_buckets'],
        );
        if (
            $configuredBuckets === null
            || (int) $configuredBuckets['config_value'] !== $this->bucketCount
        ) {
            throw new ImException(
                'IM_MESSAGE_SHARD_BUCKETS 与已迁移部署配置不一致',
                'IM_SHARD_BUCKET_CONFIG_MISMATCH',
            );
        }
        $this->indexVerified = true;

        foreach ([date('Ym'), date('Ym', strtotime('first day of next month'))] as $month) {
            for ($bucket = 0; $bucket < $this->bucketCount; $bucket++) {
                $table = sprintf('%s_%04d_%s', self::BASE_TABLE, $bucket, $month);
                if (!$this->tableExists($table)) {
                    throw new ImException('IM 消息分片未预建: ' . $table, 'IM_SHARD_NOT_PREBUILT');
                }
                $this->verifiedTables[$table] = true;
            }
        }
    }

    public function quote(string $table): string
    {
        if (!$this->isMessageTable($table)) {
            throw new InvalidArgumentException('invalid message table');
        }

        return '`' . $table . '`';
    }

    private function assertTableExists(string $table): void
    {
        if (isset($this->verifiedTables[$table])) {
            return;
        }

        if (!$this->tableExists($table)) {
            throw new ImException('IM 消息分片未预建: ' . $table, 'IM_SHARD_NOT_PREBUILT');
        }
        $this->verifiedTables[$table] = true;
    }

    private function tableExists(string $table): bool
    {
        if (preg_match('/^(?:im|sm)_[a-z0-9_]+$/', $table) !== 1) {
            throw new InvalidArgumentException('invalid IM table');
        }

        return $this->repository->fetchOne(
            'SELECT 1 AS present
               FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
              LIMIT 1',
            [$table],
        ) !== null;
    }

    private function isMessageTable(string $table): bool
    {
        return $table === self::BASE_TABLE || preg_match('/^im_message_\d{4}_\d{6}$/', $table) === 1;
    }

    private function bucket(int $organization, string $conversationId): int
    {
        return abs(crc32($organization . ':' . $conversationId)) % $this->bucketCount;
    }
}
