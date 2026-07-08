<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 消息月表分片路由
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Repository\ImRepository;
use InvalidArgumentException;

final class MessageShardRouter
{
    private const BASE_TABLE = 'im_message';
    private const INDEX_TABLE = 'im_message_index';

    private array $tableCache = [];
    private array $ensuredTables = [];
    private bool $indexEnsured = false;

    public function __construct(
        private readonly ImRepository $repository,
        private readonly int $bucketCount,
    )
    {
    }

    public function writeTable(int $organization, string $conversationId, string $time): string
    {
        $timestamp = strtotime($time) ?: time();
        $table = sprintf('%s_%04d_%s', self::BASE_TABLE, $this->bucket($organization, $conversationId), date('Ym', $timestamp));
        $this->ensureTable($table);

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

    public function ensureIndexTable(): void
    {
        if ($this->indexEnsured) {
            return;
        }

        $this->repository->execute(<<<'SQL'
CREATE TABLE IF NOT EXISTS `im_message_index` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `message_id` varchar(40) NOT NULL COMMENT '消息ID',
  `message_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT '会话内消息序号',
  `shard_table` varchar(64) NOT NULL COMMENT '消息分片表',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_message` (`organization`, `message_id`) USING BTREE,
  KEY `idx_conversation_seq` (`organization`, `conversation_id`, `message_seq`) USING BTREE,
  KEY `idx_shard_table` (`shard_table`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM消息分片索引表' ROW_FORMAT=DYNAMIC
SQL);
        $this->indexEnsured = true;
    }

    public function quote(string $table): string
    {
        if (!$this->isMessageTable($table)) {
            throw new InvalidArgumentException('invalid message table');
        }

        return '`' . $table . '`';
    }

    private function ensureTable(string $table): void
    {
        if (isset($this->ensuredTables[$table])) {
            return;
        }

        $this->repository->execute('CREATE TABLE IF NOT EXISTS ' . $this->quote($table) . ' LIKE `' . self::BASE_TABLE . '`');
        $this->ensuredTables[$table] = true;
        $this->tableCache = [];
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
