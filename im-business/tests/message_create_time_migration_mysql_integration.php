<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;

$enabled = strtolower(trim((string) (
    $_ENV['IM_MESSAGE_TIME_MIGRATION_MYSQL_TEST']
    ?? $_SERVER['IM_MESSAGE_TIME_MIGRATION_MYSQL_TEST']
    ?? getenv('IM_MESSAGE_TIME_MIGRATION_MYSQL_TEST')
)));
if (!in_array($enabled, ['1', 'true', 'yes', 'on'], true)) {
    fwrite(
        STDOUT,
        "SKIP message create_time migration MySQL integration: "
        . "set IM_MESSAGE_TIME_MIGRATION_MYSQL_TEST=1 to use isolated temporary databases.\n",
    );
    exit(0);
}

$businessRoot = dirname(__DIR__);
$repositoryRoot = dirname($businessRoot);
require $businessRoot . '/vendor/autoload.php';

if (is_file($businessRoot . '/.env')) {
    Dotenv\Dotenv::createImmutable($businessRoot)->safeLoad();
}

$config = Config::fromEnv();
$suffix = bin2hex(random_bytes(8));
$database = 'nb8im_message_time_' . $suffix . '_test';
$otherDatabase = 'nb8im_message_time_' . $suffix . '_other_test';
$safeDatabasePattern = '/^nb8im_message_time_[a-f0-9]{16}_(?:test|other_test)$/D';
foreach ([$database, $otherDatabase] as $candidate) {
    if (preg_match($safeDatabasePattern, $candidate) !== 1) {
        throw new RuntimeException('generated message create_time migration database name is unsafe');
    }
}

$admin = new PDO(
    sprintf('mysql:host=%s;port=%d;charset=%s', $config->dbHost, $config->dbPort, $config->dbCharset),
    $config->dbUser,
    $config->dbPassword,
    [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ],
);

$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    ++$assertions;
};
$previousEnvironment = [];
foreach (['DB_NAME', 'IM_EXPECT_DATABASE', 'IM_MESSAGE_SHARD_BUCKETS'] as $key) {
    $value = getenv($key);
    $previousEnvironment[$key] = $value === false ? null : $value;
}

$pdo = null;
$createdDatabases = [];
try {
    foreach ([$database, $otherDatabase] as $candidate) {
        $admin->exec('CREATE DATABASE ' . $candidate . ' CHARACTER SET utf8mb4 COLLATE utf8mb4_bin');
        $createdDatabases[] = $candidate;
    }
    putenv('DB_NAME=' . $database);
    putenv('IM_EXPECT_DATABASE=' . $database);
    putenv('IM_MESSAGE_SHARD_BUCKETS=1');

    $pdo = new PDO(
        sprintf(
            'mysql:host=%s;port=%d;dbname=%s;charset=%s',
            $config->dbHost,
            $config->dbPort,
            $database,
            $config->dbCharset,
        ),
        $config->dbUser,
        $config->dbPassword,
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => false,
        ],
    );
    $selectedDatabase = (string) $pdo->query('SELECT DATABASE()')->fetchColumn();
    $assert($selectedDatabase === $database, 'temporary database selection mismatch');
    $assert($selectedDatabase !== 'nb8im', 'integration test selected the local nb8im database');

    $pdo->exec(<<<'SQL'
CREATE TABLE sm_system_organization (
  id int(11) UNSIGNED NOT NULL,
  status tinyint(3) UNSIGNED NOT NULL DEFAULT 1,
  delete_time datetime NULL DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
SQL);

    $phinx = $businessRoot . '/vendor/bin/phinx';
    $phinxConfig = $repositoryRoot . '/phinx.php';
    $phinxCommand = static function (string $operation, string $target) use ($phinx, $phinxConfig): string {
        if (!in_array($operation, ['migrate', 'rollback'], true)) {
            throw new RuntimeException('unsupported Phinx operation');
        }
        if (preg_match('/^[0-9]{14}$/D', $target) !== 1) {
            throw new RuntimeException('unsafe Phinx migration target');
        }
        return sprintf(
            '%s %s -c %s %s -e development -t %s',
            escapeshellarg(PHP_BINARY),
            escapeshellarg($phinx),
            escapeshellarg($phinxConfig),
            $operation,
            escapeshellarg($target),
        );
    };
    $runPhinx = static function (string $operation, string $target) use ($phinxCommand): array {
        $command = $phinxCommand($operation, $target);
        $output = [];
        exec($command . ' 2>&1', $output, $exitCode);

        return [$exitCode, implode("\n", $output)];
    };
    $runSuccessfully = static function (string $operation, string $target, string $scope) use ($runPhinx): void {
        [$exitCode, $output] = $runPhinx($operation, $target);
        if ($exitCode !== 0) {
            throw new RuntimeException($scope . " failed\n" . $output);
        }
    };
    $startChild = static function (string $command, string $workingDirectory): array {
        $pipes = [];
        $process = proc_open(
            $command,
            [
                0 => ['pipe', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ],
            $pipes,
            $workingDirectory,
        );
        if (!is_resource($process) || count($pipes) !== 3) {
            throw new RuntimeException('failed to start migration race child process');
        }
        fclose($pipes[0]);

        return ['process' => $process, 'stdout' => $pipes[1], 'stderr' => $pipes[2]];
    };
    $finishChild = static function (array $child): array {
        $stdout = stream_get_contents($child['stdout']);
        $stderr = stream_get_contents($child['stderr']);
        fclose($child['stdout']);
        fclose($child['stderr']);
        $exitCode = proc_close($child['process']);

        return [$exitCode, trim((string) $stdout . PHP_EOL . (string) $stderr)];
    };

    $migrationVersion = '20260720030000';
    $previousVersion = '20260720020000';
    $hasMigration = static function () use ($pdo, $migrationVersion): bool {
        $statement = $pdo->prepare('SELECT 1 FROM im_phinxlog WHERE version = ? LIMIT 1');
        $statement->execute([$migrationVersion]);

        return $statement->fetchColumn() !== false;
    };
    $deleteMigrationLog = static function () use ($pdo, $migrationVersion): void {
        $pdo->prepare('DELETE FROM im_phinxlog WHERE version = ?')->execute([$migrationVersion]);
    };
    $baselineShape = [
        'data_type' => 'datetime',
        'column_type' => 'datetime',
        'is_nullable' => 'YES',
        'column_default' => null,
        'extra' => '',
        'column_comment' => '创建时间',
    ];
    $targetShape = $baselineShape;
    $targetShape['is_nullable'] = 'NO';
    $targetTables = static function () use ($pdo): array {
        $rows = $pdo->query(
            "SELECT TABLE_NAME AS table_name
               FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_TYPE = 'BASE TABLE'
              ORDER BY TABLE_NAME ASC",
        )->fetchAll();
        $shards = [];
        foreach ($rows as $row) {
            $table = (string) ($row['table_name'] ?? '');
            if (preg_match('/^im_message_[0-9]{4}_[0-9]{6}$/D', $table) !== 1) {
                continue;
            }
            $shards[] = $table;
        }
        sort($shards, SORT_STRING);

        return array_merge(['im_message_index', 'im_message'], $shards);
    };
    $columnShape = static function (string $schema, string $table) use ($pdo): ?array {
        $statement = $pdo->prepare(
            "SELECT DATA_TYPE AS data_type,
                    COLUMN_TYPE AS column_type,
                    IS_NULLABLE AS is_nullable,
                    COLUMN_DEFAULT AS column_default,
                    EXTRA AS extra,
                    COLUMN_COMMENT AS column_comment
               FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = 'create_time'
              LIMIT 1",
        );
        $statement->execute([$schema, $table]);
        $row = $statement->fetch(PDO::FETCH_ASSOC);
        if (!is_array($row)) {
            return null;
        }

        return [
            'data_type' => (string) ($row['data_type'] ?? ''),
            'column_type' => (string) ($row['column_type'] ?? ''),
            'is_nullable' => (string) ($row['is_nullable'] ?? ''),
            'column_default' => $row['column_default'] === null
                ? null
                : (string) $row['column_default'],
            'extra' => (string) ($row['extra'] ?? ''),
            'column_comment' => (string) ($row['column_comment'] ?? ''),
        ];
    };
    $nullability = static fn (string $schema, string $table): ?string =>
        $columnShape($schema, $table)['is_nullable'] ?? null;
    $assertTargetNullability = static function (string $expected, string $scope) use (
        $assert,
        $baselineShape,
        $columnShape,
        $database,
        $targetShape,
        $targetTables,
    ): void {
        $expectedShape = $expected === 'NO' ? $targetShape : $baselineShape;
        foreach ($targetTables() as $table) {
            $assert(
                $columnShape($database, $table) === $expectedShape,
                $scope . ': unexpected create_time shape for ' . $table,
            );
        }
    };
    $schemaSnapshot = static function () use ($pdo, $targetTables): array {
        $snapshot = [];
        foreach ($targetTables() as $table) {
            $row = $pdo->query('SHOW CREATE TABLE ' . $table)->fetch(PDO::FETCH_NUM);
            if (!is_array($row) || !isset($row[1])) {
                throw new RuntimeException('failed to snapshot target table ' . $table);
            }
            $snapshot[$table] = (string) $row[1];
        }

        return $snapshot;
    };
    $tableExists = static function (string $table) use ($pdo): bool {
        $statement = $pdo->prepare(
            'SELECT 1 FROM information_schema.TABLES '
            . 'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND TABLE_TYPE = ? LIMIT 1',
        );
        $statement->execute([$table, 'BASE TABLE']);

        return $statement->fetchColumn() !== false;
    };
    $lockOwner = static function () use ($pdo): ?string {
        $statement = $pdo->prepare('SELECT IS_USED_LOCK(?)');
        $statement->execute(['b8im:im:prebuild-message-shards']);
        $owner = $statement->fetchColumn();

        return $owner === false || $owner === null ? null : (string) $owner;
    };
    $waitForMigrationLock = static function (array $child) use ($lockOwner): string {
        $deadline = microtime(true) + 10.0;
        while (microtime(true) < $deadline) {
            $owner = $lockOwner();
            if ($owner !== null) {
                return $owner;
            }
            $status = proc_get_status($child['process']);
            if (($status['running'] ?? false) !== true) {
                throw new RuntimeException('migration child exited before acquiring the shard prebuild lock');
            }
            usleep(20_000);
        }

        throw new RuntimeException('timed out waiting for the migration shard prebuild lock');
    };
    $waitForPendingPrebuild = static function (array $child) use ($pdo): int {
        $statement = $pdo->prepare(
            "SELECT COUNT(*)
               FROM performance_schema.metadata_locks
              WHERE OBJECT_TYPE = 'USER LEVEL LOCK'
                AND OBJECT_NAME = ?
                AND LOCK_STATUS = 'PENDING'",
        );
        $deadline = microtime(true) + 10.0;
        while (microtime(true) < $deadline) {
            $statement->execute(['b8im:im:prebuild-message-shards']);
            $waiters = (int) $statement->fetchColumn();
            if ($waiters > 0) {
                return $waiters;
            }
            $status = proc_get_status($child['process']);
            if (($status['running'] ?? false) !== true) {
                throw new RuntimeException('prebuild child exited before waiting on the migration lock');
            }
            usleep(20_000);
        }

        throw new RuntimeException('timed out waiting for prebuild to block on the migration lock');
    };

    // Fresh migration creates the baseline shards and immediately tightens the
    // index, template, and every existing physical shard.
    $runSuccessfully('migrate', $migrationVersion, 'fresh message create_time migration');
    $assert($hasMigration(), 'fresh migration version was not recorded');
    $freshTables = $targetTables();
    $freshShards = array_slice($freshTables, 2);
    $assert(count($freshShards) >= 2, 'fresh migration did not create current and next-month shards');
    $sortedFreshShards = $freshShards;
    sort($sortedFreshShards, SORT_STRING);
    $assert($freshShards === $sortedFreshShards, 'fresh shard enumeration is not stably sorted');
    $assertTargetNullability('NO', 'fresh migration');

    // The canonical template must pass NOT NULL to every later CREATE LIKE.
    $lateShard = 'im_message_9999_209912';
    $decoyTable = 'im_message_9999_209912_extra';
    $externalShard = 'im_message_9999_209911';
    $pdo->exec('CREATE TABLE ' . $lateShard . ' LIKE im_message');
    $pdo->exec('CREATE TABLE ' . $decoyTable . ' LIKE im_message');
    $admin->exec('CREATE TABLE ' . $otherDatabase . '.' . $externalShard . ' LIKE ' . $database . '.im_message');
    $assert(
        $columnShape($database, $lateShard) === $targetShape,
        'CREATE LIKE did not inherit the complete target shape',
    );

    // Down restores all three target categories but ignores non-matching and
    // other-schema tables.
    $runSuccessfully('rollback', $previousVersion, 'message create_time down');
    $assert(!$hasMigration(), 'down left the migration version recorded');
    $assertTargetNullability('YES', 'down');
    $assert($nullability($database, $decoyTable) === 'NO', 'down changed a regex-decoy table');
    $admin->exec(
        'ALTER TABLE ' . $otherDatabase . '.' . $externalShard
        . " MODIFY COLUMN create_time datetime NULL DEFAULT NULL COMMENT '创建时间'",
    );

    // Missing columns fail before any target DDL and identify the exact table.
    $pdo->exec('ALTER TABLE ' . $lateShard . ' DROP COLUMN create_time');
    $missingSnapshot = $schemaSnapshot();
    [$missingExitCode, $missingOutput] = $runPhinx('migrate', $migrationVersion);
    $assert($missingExitCode !== 0, 'migration accepted a target shard without create_time');
    $assert(
        str_contains($missingOutput, 'missing_create_time=[' . $lateShard . ']'),
        'missing-column rejection did not name the exact target table',
    );
    $assert(!$hasMigration(), 'missing-column rejection was recorded');
    $assert($schemaSnapshot() === $missingSnapshot, 'missing-column preflight changed a target schema');
    $pdo->exec(
        'ALTER TABLE ' . $lateShard
        . " ADD COLUMN create_time datetime NULL DEFAULT NULL COMMENT '创建时间' AFTER edit_count",
    );

    // NULL data in more than one target must be reported in stable target
    // order, leave every schema byte-for-byte unchanged, and remain unrecorded.
    $firstShard = $targetTables()[2];
    $pdo->prepare(
        'INSERT INTO im_message_index '
        . '(organization, global_seq, message_id, conversation_id, message_seq, sender_id, '
        . 'sender_organization, client_msg_id, storage_node, shard_table, create_time) '
        . 'VALUES (1, 1, ?, ?, 1, ?, 1, ?, ?, ?, NULL)',
    )->execute(['null-index-message', 'null-index-conversation', 'sender', 'null-index-client', 'default', $firstShard]);
    $pdo->prepare(
        'INSERT INTO ' . $firstShard . ' '
        . '(organization, conversation_id, conversation_type, message_id, message_seq, client_msg_id, '
        . 'sender_id, sender_organization, message_type, content, status, create_time) '
        . 'VALUES (1, ?, 1, ?, 1, ?, ?, 1, 1, NULL, 1, NULL)',
    )->execute(['null-shard-conversation', 'null-shard-message', 'null-shard-client', 'sender']);
    $nullSnapshot = $schemaSnapshot();
    [$nullExitCode, $nullOutput] = $runPhinx('migrate', $migrationVersion);
    $assert($nullExitCode !== 0, 'migration accepted NULL create_time rows');
    $assert(
        str_contains(
            $nullOutput,
            'null_create_time_rows=[im_message_index=1,' . $firstShard . '=1]',
        ),
        'NULL rejection did not report stable table names and row counts',
    );
    $assert(!$hasMigration(), 'NULL rejection was recorded');
    $assert($schemaSnapshot() === $nullSnapshot, 'NULL preflight changed at least one target schema');

    $pdo->exec("DELETE FROM im_message_index WHERE message_id = 'null-index-message'");
    $pdo->exec("DELETE FROM " . $firstShard . " WHERE message_id = 'null-shard-message'");
    $runSuccessfully('migrate', $migrationVersion, 'post-NULL cleanup migration');
    $assert($hasMigration(), 'post-NULL cleanup migration was not recorded');
    $assertTargetNullability('NO', 'post-NULL cleanup');

    // A forced complete-schema rerun is a no-op for targets and does not cross
    // either the shard regex or current-database boundary.
    $pdo->exec(
        'ALTER TABLE ' . $decoyTable
        . " MODIFY COLUMN create_time datetime NULL DEFAULT NULL COMMENT '创建时间'",
    );
    $deleteMigrationLog();
    $runSuccessfully('migrate', $migrationVersion, 'complete-schema rerun');
    $assertTargetNullability('NO', 'complete-schema rerun');
    $assert($nullability($database, $decoyTable) === 'YES', 'up changed a regex-decoy table');
    $assert(
        $nullability($otherDatabase, $externalShard) === 'YES',
        'up changed a same-named shard in another database',
    );

    // Simulate interrupted DDL: already-tight tables stay untouched and only
    // the remaining nullable targets are repaired on rerun.
    $deleteMigrationLog();
    $pdo->exec(
        "ALTER TABLE im_message MODIFY COLUMN create_time datetime NULL DEFAULT NULL COMMENT '创建时间'",
    );
    $pdo->exec(
        'ALTER TABLE ' . $lateShard
        . " MODIFY COLUMN create_time datetime NULL DEFAULT NULL COMMENT '创建时间'",
    );
    $runSuccessfully('migrate', $migrationVersion, 'partial nullable up recovery');
    $assert($hasMigration(), 'partial nullable recovery was not recorded');
    $assertTargetNullability('NO', 'partial nullable up recovery');

    // Down is equally idempotent across a partial nullable state and restores
    // every currently existing physical shard without recreating data.
    $pdo->exec(
        "ALTER TABLE im_message MODIFY COLUMN create_time datetime NULL DEFAULT NULL COMMENT '创建时间'",
    );
    $runSuccessfully('rollback', $previousVersion, 'partial nullable down recovery');
    $assert(!$hasMigration(), 'partial nullable down left the migration recorded');
    $assertTargetNullability('YES', 'partial nullable down recovery');

    // Exact metadata is part of the contract. Type, default, comment, or extra
    // drift is never treated as an upgrade source and must fail before DDL.
    $pdo->exec(
        "ALTER TABLE im_message_index MODIFY COLUMN create_time timestamp NULL DEFAULT NULL COMMENT '创建时间'",
    );
    $pdo->exec(
        "ALTER TABLE im_message MODIFY COLUMN create_time datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'",
    );
    $pdo->exec(
        'ALTER TABLE ' . $firstShard
        . " MODIFY COLUMN create_time datetime NULL DEFAULT NULL COMMENT '漂移时间'",
    );
    $pdo->exec(
        'ALTER TABLE ' . $lateShard
        . " MODIFY COLUMN create_time datetime NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间'",
    );
    $assert(
        ($columnShape($database, 'im_message_index')['data_type'] ?? '') === 'timestamp',
        'type-drift fixture was not installed',
    );
    $assert(
        ($columnShape($database, 'im_message')['column_default'] ?? null) !== null,
        'default-drift fixture was not installed',
    );
    $assert(
        ($columnShape($database, $firstShard)['column_comment'] ?? '') === '漂移时间',
        'comment-drift fixture was not installed',
    );
    $assert(
        ($columnShape($database, $lateShard)['extra'] ?? '') !== '',
        'extra-drift fixture was not installed',
    );
    $upDriftSnapshot = $schemaSnapshot();
    [$upDriftExitCode, $upDriftOutput] = $runPhinx('migrate', $migrationVersion);
    $assert($upDriftExitCode !== 0, 'up accepted illegal create_time metadata drift');
    foreach (['im_message_index', 'im_message', $firstShard, $lateShard] as $driftTable) {
        $assert(
            str_contains($upDriftOutput, $driftTable . '='),
            'up drift rejection did not report ' . $driftTable,
        );
    }
    $assert(
        str_contains($upDriftOutput, 'illegal_create_time_shape=['),
        'up drift rejection did not report exact metadata',
    );
    $assert(!$hasMigration(), 'up drift rejection was recorded');
    $assert($schemaSnapshot() === $upDriftSnapshot, 'up drift preflight changed a target schema');
    $assert($lockOwner() === null, 'up drift failure leaked the shard prebuild lock');

    foreach (['im_message_index', 'im_message', $firstShard, $lateShard] as $table) {
        $pdo->exec(
            'ALTER TABLE ' . $table
            . " MODIFY COLUMN create_time datetime NULL DEFAULT NULL COMMENT '创建时间'",
        );
    }
    $runSuccessfully('migrate', $migrationVersion, 'post-drift legal up');
    $assertTargetNullability('NO', 'post-drift legal up');

    // Down accepts only the exact target shape. A target-like column with a
    // changed comment must leave both schema and migration record untouched.
    $pdo->exec(
        'ALTER TABLE ' . $lateShard
        . " MODIFY COLUMN create_time datetime NOT NULL COMMENT '漂移时间'",
    );
    $downDriftSnapshot = $schemaSnapshot();
    [$downDriftExitCode, $downDriftOutput] = $runPhinx('rollback', $previousVersion);
    $assert($downDriftExitCode !== 0, 'down accepted illegal create_time metadata drift');
    $assert(
        str_contains($downDriftOutput, $lateShard . '=')
        && str_contains($downDriftOutput, '漂移时间'),
        'down drift rejection did not identify the exact table metadata',
    );
    $assert($hasMigration(), 'down drift rejection removed the migration record');
    $assert($schemaSnapshot() === $downDriftSnapshot, 'down drift preflight changed a target schema');
    $assert($lockOwner() === null, 'down drift failure leaked the shard prebuild lock');
    $pdo->exec(
        'ALTER TABLE ' . $lateShard
        . " MODIFY COLUMN create_time datetime NOT NULL COMMENT '创建时间'",
    );
    $runSuccessfully('rollback', $previousVersion, 'post-drift legal down');
    $assertTargetNullability('YES', 'post-drift legal down');

    // Hold a metadata lock on the first ALTER target so the migration keeps
    // the named shard lock while a real prebuild child attempts to enter.
    $blocker = new PDO(
        sprintf(
            'mysql:host=%s;port=%d;dbname=%s;charset=%s',
            $config->dbHost,
            $config->dbPort,
            $database,
            $config->dbCharset,
        ),
        $config->dbUser,
        $config->dbPassword,
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
    );
    $prebuildCommand = escapeshellarg(PHP_BINARY) . ' '
        . escapeshellarg($repositoryRoot . '/bin/prebuild_message_shards.php');
    $currentPrebuildShard = 'im_message_0000_' . date('Ym');
    $nextPrebuildShard = 'im_message_0000_' . date('Ym', strtotime('first day of next month'));

    $assert($tableExists($currentPrebuildShard), 'current prebuild shard fixture is missing');
    $pdo->exec('DROP TABLE ' . $currentPrebuildShard);
    $blocker->exec('LOCK TABLES im_message_index READ');
    try {
        $upChild = $startChild($phinxCommand('migrate', $migrationVersion), $repositoryRoot);
        $upLockOwner = $waitForMigrationLock($upChild);
        $upPrebuildChild = $startChild($prebuildCommand, $repositoryRoot);
        $upPendingPrebuilds = $waitForPendingPrebuild($upPrebuildChild);
        $upPrebuildStatus = proc_get_status($upPrebuildChild['process']);
        $upShardMissingDuringLock = !$tableExists($currentPrebuildShard);
        $upOwnerDuringPrebuild = $lockOwner();
    } finally {
        $blocker->exec('UNLOCK TABLES');
    }
    [$upRaceExitCode, $upRaceOutput] = $finishChild($upChild);
    [$upPrebuildExitCode, $upPrebuildOutput] = $finishChild($upPrebuildChild);
    $assert($upRaceExitCode === 0, 'locked up migration failed: ' . $upRaceOutput);
    $assert($upPrebuildExitCode === 0, 'up-window prebuild failed: ' . $upPrebuildOutput);
    $assert(($upPrebuildStatus['running'] ?? false) === true, 'prebuild escaped the up migration window');
    $assert($upPendingPrebuilds >= 1, 'prebuild was not pending on the up migration named lock');
    $assert($upShardMissingDuringLock, 'prebuild created a shard before up released the named lock');
    $assert($upOwnerDuringPrebuild === $upLockOwner, 'prebuild replaced the up migration lock owner');
    $assert($hasMigration(), 'locked up migration was not recorded');
    $assert(
        $columnShape($database, $currentPrebuildShard) === $targetShape,
        'up-window CREATE LIKE did not inherit the exact target shape',
    );
    $assertTargetNullability('NO', 'locked up plus prebuild');
    $assert($lockOwner() === null, 'up/prebuild race leaked the shard prebuild lock');

    $assert($tableExists($nextPrebuildShard), 'next-month prebuild shard fixture is missing');
    $pdo->exec('DROP TABLE ' . $nextPrebuildShard);
    $blocker->exec('LOCK TABLES im_message_index READ');
    try {
        $downChild = $startChild($phinxCommand('rollback', $previousVersion), $repositoryRoot);
        $downLockOwner = $waitForMigrationLock($downChild);
        $downPrebuildChild = $startChild($prebuildCommand, $repositoryRoot);
        $downPendingPrebuilds = $waitForPendingPrebuild($downPrebuildChild);
        $downPrebuildStatus = proc_get_status($downPrebuildChild['process']);
        $downShardMissingDuringLock = !$tableExists($nextPrebuildShard);
        $downOwnerDuringPrebuild = $lockOwner();
    } finally {
        $blocker->exec('UNLOCK TABLES');
    }
    [$downRaceExitCode, $downRaceOutput] = $finishChild($downChild);
    [$downPrebuildExitCode, $downPrebuildOutput] = $finishChild($downPrebuildChild);
    $assert($downRaceExitCode === 0, 'locked down migration failed: ' . $downRaceOutput);
    $assert($downPrebuildExitCode === 0, 'down-window prebuild failed: ' . $downPrebuildOutput);
    $assert(($downPrebuildStatus['running'] ?? false) === true, 'prebuild escaped the down migration window');
    $assert($downPendingPrebuilds >= 1, 'prebuild was not pending on the down migration named lock');
    $assert($downShardMissingDuringLock, 'prebuild created a shard before down released the named lock');
    $assert($downOwnerDuringPrebuild === $downLockOwner, 'prebuild replaced the down migration lock owner');
    $assert(!$hasMigration(), 'locked down migration left the migration recorded');
    $assert(
        $columnShape($database, $nextPrebuildShard) === $baselineShape,
        'down-window CREATE LIKE did not inherit the exact baseline shape',
    );
    $assertTargetNullability('YES', 'locked down plus prebuild');
    $assert($lockOwner() === null, 'down/prebuild race leaked the shard prebuild lock');
    $blocker = null;

    fwrite(STDOUT, sprintf(
        "Message create_time migration MySQL integration (%s): %d assertions passed across %d targets.\n",
        $database,
        $assertions,
        count($targetTables()),
    ));
} finally {
    $pdo = null;
    foreach (array_reverse($createdDatabases) as $candidate) {
        if (preg_match($safeDatabasePattern, $candidate) !== 1) {
            throw new RuntimeException('refusing to drop an unsafe migration test database');
        }
        $admin->exec('DROP DATABASE IF EXISTS ' . $candidate);
    }
    foreach ($previousEnvironment as $key => $value) {
        putenv($value === null ? $key : $key . '=' . $value);
    }
}
