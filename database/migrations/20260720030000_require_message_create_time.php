<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

final class RequireMessageCreateTime extends AbstractMigration
{
    private const SHARD_LOCK_NAME = 'b8im:im:prebuild-message-shards';
    private const SHARD_TABLE_PATTERN = '/^im_message_[0-9]{4}_[0-9]{6}$/D';

    /** @var array{data_type:string,column_type:string,is_nullable:string,column_default:null,extra:string,column_comment:string} */
    private const BASELINE_SHAPE = [
        'data_type' => 'datetime',
        'column_type' => 'datetime',
        'is_nullable' => 'YES',
        'column_default' => null,
        'extra' => '',
        'column_comment' => '创建时间',
    ];

    /** @var array{data_type:string,column_type:string,is_nullable:string,column_default:null,extra:string,column_comment:string} */
    private const TARGET_SHAPE = [
        'data_type' => 'datetime',
        'column_type' => 'datetime',
        'is_nullable' => 'NO',
        'column_default' => null,
        'extra' => '',
        'column_comment' => '创建时间',
    ];

    public function up(): void
    {
        $connection = $this->requirePdoConnection();
        $this->withShardPrebuildLock(
            $connection,
            fn () => $this->migrate($connection, true),
        );
    }

    public function down(): void
    {
        $connection = $this->requirePdoConnection();
        $this->withShardPrebuildLock(
            $connection,
            fn () => $this->migrate($connection, false),
        );
    }

    private function requirePdoConnection(): PDO
    {
        $connection = $this->getAdapter()->getConnection();
        if (!$connection instanceof PDO) {
            throw new RuntimeException('message create_time migration requires a PDO adapter before DDL');
        }

        return $connection;
    }

    /** @param callable():void $operation */
    private function withShardPrebuildLock(PDO $connection, callable $operation): void
    {
        $statement = $connection->prepare('SELECT GET_LOCK(?, 30)');
        $statement->execute([self::SHARD_LOCK_NAME]);
        if ((int) $statement->fetchColumn() !== 1) {
            throw new RuntimeException('Unable to acquire the IM shard prebuild lock.');
        }

        $operationFailure = null;
        $releaseFailure = null;
        try {
            try {
                $operation();
            } catch (Throwable $exception) {
                $operationFailure = $exception;
            }
        } finally {
            try {
                $statement = $connection->prepare('SELECT RELEASE_LOCK(?)');
                $statement->execute([self::SHARD_LOCK_NAME]);
                if ((int) $statement->fetchColumn() !== 1) {
                    throw new RuntimeException('Unable to release the IM shard prebuild lock.');
                }
            } catch (Throwable $exception) {
                $releaseFailure = $exception;
            }
        }

        if ($operationFailure !== null) {
            if ($releaseFailure !== null) {
                throw new RuntimeException(
                    $operationFailure->getMessage() . '; additionally failed to release the IM shard prebuild lock',
                    0,
                    $operationFailure,
                );
            }
            throw $operationFailure;
        }
        if ($releaseFailure !== null) {
            throw $releaseFailure;
        }
    }

    private function migrate(PDO $connection, bool $up): void
    {
        $tables = $this->targetTables($connection);
        $tablesToAlter = $this->preflight($connection, $tables, $up);

        foreach ($tablesToAlter as $table) {
            $this->execute($up
                ? sprintf(
                    "ALTER TABLE `%s` MODIFY COLUMN `create_time` datetime NOT NULL COMMENT '创建时间'",
                    $table,
                )
                : sprintf(
                    "ALTER TABLE `%s` MODIFY COLUMN `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间'",
                    $table,
                ));
        }

        $this->assertFinalShape($connection, $this->targetTables($connection), $up);
    }

    /** @return list<string> */
    private function targetTables(PDO $connection): array
    {
        $rows = $connection->query(
            "SELECT TABLE_NAME AS table_name
               FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_TYPE = 'BASE TABLE'
              ORDER BY TABLE_NAME ASC",
        );
        if ($rows === false) {
            throw new RuntimeException('message create_time migration failed to enumerate physical shards');
        }

        $shards = [];
        foreach ($rows->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $table = (string) ($row['table_name'] ?? '');
            if (preg_match(self::SHARD_TABLE_PATTERN, $table) === 1) {
                $shards[] = $table;
            }
        }
        sort($shards, SORT_STRING);

        return array_merge(['im_message_index', 'im_message'], $shards);
    }

    /**
     * @param list<string> $tables
     * @return list<string>
     */
    private function preflight(PDO $connection, array $tables, bool $up): array
    {
        $metadata = $this->columnMetadata($connection, $tables);
        $desiredShape = $up ? self::TARGET_SHAPE : self::BASELINE_SHAPE;
        $sourceShape = $up ? self::BASELINE_SHAPE : self::TARGET_SHAPE;
        $missing = [];
        $drift = [];
        $nullRows = [];
        $tablesToAlter = [];

        foreach ($tables as $table) {
            if (!isset($metadata[$table])) {
                $missing[] = $table;
                continue;
            }

            $shape = $metadata[$table];
            if ($shape === $desiredShape) {
                continue;
            }
            if ($shape !== $sourceShape) {
                $drift[$table] = $shape;
                continue;
            }

            if ($up) {
                $count = $this->nullRowCount($connection, $table);
                if ($count > 0) {
                    $nullRows[$table] = $count;
                }
            }
            $tablesToAlter[] = $table;
        }

        if ($missing !== [] || $drift !== [] || $nullRows !== []) {
            throw new RuntimeException(sprintf(
                'message create_time migration %s preflight failed before DDL: %s',
                $up ? 'up' : 'down',
                $this->formatProblems($missing, $drift, $nullRows),
            ));
        }

        return $tablesToAlter;
    }

    /**
     * @param list<string> $tables
     * @return array<string,array{data_type:string,column_type:string,is_nullable:string,column_default:?string,extra:string,column_comment:string}>
     */
    private function columnMetadata(PDO $connection, array $tables): array
    {
        $wanted = [];
        foreach ($tables as $table) {
            $this->assertTargetTable($table);
            $wanted[$table] = true;
        }

        $rows = $connection->query(
            "SELECT c.TABLE_NAME AS table_name,
                    c.DATA_TYPE AS data_type,
                    c.COLUMN_TYPE AS column_type,
                    c.IS_NULLABLE AS is_nullable,
                    c.COLUMN_DEFAULT AS column_default,
                    c.EXTRA AS extra,
                    c.COLUMN_COMMENT AS column_comment
               FROM information_schema.COLUMNS c
               INNER JOIN information_schema.TABLES t
                 ON t.TABLE_SCHEMA = c.TABLE_SCHEMA
                AND t.TABLE_NAME = c.TABLE_NAME
                AND t.TABLE_TYPE = 'BASE TABLE'
              WHERE c.TABLE_SCHEMA = DATABASE()
                AND c.COLUMN_NAME = 'create_time'",
        );
        if ($rows === false) {
            throw new RuntimeException('message create_time migration failed to inspect column metadata');
        }

        $metadata = [];
        foreach ($rows->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $table = (string) ($row['table_name'] ?? '');
            if (!isset($wanted[$table])) {
                continue;
            }
            $metadata[$table] = [
                'data_type' => (string) ($row['data_type'] ?? ''),
                'column_type' => (string) ($row['column_type'] ?? ''),
                'is_nullable' => (string) ($row['is_nullable'] ?? ''),
                'column_default' => $row['column_default'] === null
                    ? null
                    : (string) $row['column_default'],
                'extra' => (string) ($row['extra'] ?? ''),
                'column_comment' => (string) ($row['column_comment'] ?? ''),
            ];
        }

        return $metadata;
    }

    private function nullRowCount(PDO $connection, string $table): int
    {
        $this->assertTargetTable($table);
        $statement = $connection->query(sprintf(
            'SELECT COUNT(*) FROM `%s` WHERE `create_time` IS NULL',
            $table,
        ));
        if ($statement === false) {
            throw new RuntimeException(
                'message create_time migration preflight count failed: table=' . $table,
            );
        }

        return (int) $statement->fetchColumn();
    }

    /** @param list<string> $tables */
    private function assertFinalShape(PDO $connection, array $tables, bool $up): void
    {
        $metadata = $this->columnMetadata($connection, $tables);
        $desiredShape = $up ? self::TARGET_SHAPE : self::BASELINE_SHAPE;
        $missing = [];
        $drift = [];
        foreach ($tables as $table) {
            if (!isset($metadata[$table])) {
                $missing[] = $table;
                continue;
            }
            if ($metadata[$table] !== $desiredShape) {
                $drift[$table] = $metadata[$table];
            }
        }

        if ($missing !== [] || $drift !== []) {
            throw new RuntimeException(sprintf(
                'message create_time migration %s final verification failed: %s',
                $up ? 'up' : 'down',
                $this->formatProblems($missing, $drift, []),
            ));
        }
    }

    /**
     * @param list<string> $missing
     * @param array<string,array{data_type:string,column_type:string,is_nullable:string,column_default:?string,extra:string,column_comment:string}> $drift
     * @param array<string,int> $nullRows
     */
    private function formatProblems(array $missing, array $drift, array $nullRows): string
    {
        $problems = [];
        if ($missing !== []) {
            $problems[] = 'missing_create_time=[' . implode(',', $missing) . ']';
        }
        if ($drift !== []) {
            $shapes = [];
            foreach ($drift as $table => $shape) {
                $shapes[] = $table . '=' . json_encode(
                    $shape,
                    JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR,
                );
            }
            $problems[] = 'illegal_create_time_shape=[' . implode(',', $shapes) . ']';
        }
        if ($nullRows !== []) {
            $counts = [];
            foreach ($nullRows as $table => $count) {
                $counts[] = $table . '=' . $count;
            }
            $problems[] = 'null_create_time_rows=[' . implode(',', $counts) . ']';
        }

        return implode('; ', $problems);
    }

    private function assertTargetTable(string $table): void
    {
        if (
            $table !== 'im_message_index'
            && $table !== 'im_message'
            && preg_match(self::SHARD_TABLE_PATTERN, $table) !== 1
        ) {
            throw new RuntimeException('message create_time migration refused invalid table: ' . $table);
        }
    }
}
