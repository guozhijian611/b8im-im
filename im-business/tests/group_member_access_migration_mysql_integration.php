<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;

$enabled = strtolower(trim((string) (
    $_ENV['IM_GROUP_ACCESS_MIGRATION_MYSQL_TEST']
    ?? $_SERVER['IM_GROUP_ACCESS_MIGRATION_MYSQL_TEST']
    ?? getenv('IM_GROUP_ACCESS_MIGRATION_MYSQL_TEST')
)));
if (!in_array($enabled, ['1', 'true', 'yes', 'on'], true)) {
    fwrite(
        STDOUT,
        "SKIP group member access migration MySQL integration: "
        . "set IM_GROUP_ACCESS_MIGRATION_MYSQL_TEST=1 to use an isolated temporary database.\n",
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
$database = 'nb8im_group_access_migration_' . bin2hex(random_bytes(8)) . '_test';
$safeDatabasePattern = '/^nb8im_group_access_migration_[a-f0-9]{16}_test$/D';
if (preg_match($safeDatabasePattern, $database) !== 1) {
    throw new RuntimeException('generated group access migration test database name is unsafe');
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
$created = false;
try {
    $admin->exec('CREATE DATABASE ' . $database . ' CHARACTER SET utf8mb4 COLLATE utf8mb4_bin');
    $created = true;
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

    // The first IM migration reads the Server-owned organization table when it
    // initializes global sequences. An empty minimal baseline is sufficient.
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
    $runPhinx = static function (string $operation, string $target) use ($phinx, $phinxConfig): array {
        if (!in_array($operation, ['migrate', 'rollback'], true)) {
            throw new RuntimeException('unsupported Phinx operation');
        }
        if (preg_match('/^[0-9]{14}$/D', $target) !== 1) {
            throw new RuntimeException('unsafe Phinx migration target');
        }
        $command = sprintf(
            '%s %s -c %s %s -e development -t %s',
            escapeshellarg(PHP_BINARY),
            escapeshellarg($phinx),
            escapeshellarg($phinxConfig),
            $operation,
            escapeshellarg($target),
        );
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

    $migrationVersion = '20260720020000';
    $previousVersion = '20260720010000';
    $runSuccessfully('migrate', $previousVersion, 'base IM migration');

    $exists = static function (string $informationSchemaTable, string $where, array $params) use ($pdo): bool {
        $statement = $pdo->prepare(
            'SELECT 1 FROM information_schema.' . $informationSchemaTable
            . ' WHERE TABLE_SCHEMA = DATABASE() AND ' . $where . ' LIMIT 1',
        );
        $statement->execute($params);

        return $statement->fetchColumn() !== false;
    };
    $hasTable = static fn (string $table): bool => $exists('TABLES', 'TABLE_NAME = ?', [$table]);
    $hasColumn = static fn (string $table, string $column): bool => $exists(
        'COLUMNS',
        'TABLE_NAME = ? AND COLUMN_NAME = ?',
        [$table, $column],
    );
    $hasIndex = static fn (string $table, string $index): bool => $exists(
        'STATISTICS',
        'TABLE_NAME = ? AND INDEX_NAME = ?',
        [$table, $index],
    );
    $hasMigration = static function () use ($pdo, $migrationVersion): bool {
        $statement = $pdo->prepare('SELECT 1 FROM im_phinxlog WHERE version = ? LIMIT 1');
        $statement->execute([$migrationVersion]);

        return $statement->fetchColumn() !== false;
    };
    $deleteMigrationLog = static function () use ($pdo, $migrationVersion): void {
        $pdo->prepare('DELETE FROM im_phinxlog WHERE version = ?')->execute([$migrationVersion]);
    };
    $assertComplete = static function (string $scope) use ($assert, $hasTable, $hasColumn, $hasIndex): void {
        $assert($hasTable('im_user_group_access_state'), $scope . ': state table is missing');
        $assert($hasTable('im_group_member_access_audit'), $scope . ': audit table is missing');
        $assert($hasColumn('im_conversation_member', 'access_state'), $scope . ': access_state is missing');
        $assert(
            $hasIndex('im_conversation_member', 'idx_group_access_snapshot_page'),
            $scope . ': member snapshot index is missing',
        );
        $assert(
            $hasIndex('im_conversation_membership_period', 'idx_group_access_period_page'),
            $scope . ': period snapshot index is missing',
        );
    };
    $assertPristine = static function (string $scope) use ($assert, $hasTable, $hasColumn, $hasIndex): void {
        $assert(!$hasTable('im_user_group_access_state'), $scope . ': state table remains');
        $assert(!$hasTable('im_group_member_access_audit'), $scope . ': audit table remains');
        $assert(!$hasColumn('im_conversation_member', 'access_state'), $scope . ': access_state remains');
        $assert(
            !$hasIndex('im_conversation_member', 'idx_group_access_snapshot_page'),
            $scope . ': member snapshot index remains',
        );
        $assert(
            !$hasIndex('im_conversation_membership_period', 'idx_group_access_period_page'),
            $scope . ': period snapshot index remains',
        );
    };

    $organization = 880001;
    $now = date('Y-m-d H:i:s');
    $insertUser = $pdo->prepare(
        'INSERT INTO im_user '
        . '(organization, user_id, account, password_hash, nickname, status, create_time, update_time) '
        . 'VALUES (?, ?, ?, ?, ?, 1, ?, ?)',
    );
    foreach (['group-active', 'group-history', 'single-only'] as $userId) {
        $insertUser->execute([$organization, $userId, $userId, 'integration-only', $userId, $now, $now]);
    }
    $pdo->prepare(
        'INSERT INTO im_conversation '
        . '(organization, conversation_id, conversation_type, title, last_message_seq, '
        . 'last_change_seq, status, create_time, update_time) '
        . 'VALUES (?, ?, 2, ?, 30, 7, 1, ?, ?), (?, ?, 1, ?, 11, 3, 1, ?, ?)',
    )->execute([
        $organization, 'group-access-it', 'group access integration', $now, $now,
        $organization, 'single-access-it', 'single access integration', $now, $now,
    ]);
    $insertMember = $pdo->prepare(
        'INSERT INTO im_conversation_member '
        . '(organization, conversation_id, user_id, member_organization, status, access_version, '
        . 'create_time, update_time, delete_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
    );
    $insertMember->execute([
        $organization, 'group-access-it', 'group-active', $organization, 1, 4, $now, $now, null,
    ]);
    $insertMember->execute([
        $organization, 'group-access-it', 'group-history', $organization, 2, 5, $now, $now, $now,
    ]);
    // This row deliberately resembles a revoked member, but conversation_type=1
    // must keep it out of group state derivation and baseline audit.
    $insertMember->execute([
        $organization, 'single-access-it', 'single-only', $organization, 2, 9, $now, $now, $now,
    ]);
    $insertPeriod = $pdo->prepare(
        'INSERT INTO im_conversation_membership_period '
        . '(organization, conversation_id, user_id, member_organization, period_no, '
        . 'visible_from_message_seq, visible_until_message_seq, join_at, leave_at, '
        . 'status, create_time, update_time) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, 1, ?, ?)',
    );
    $insertPeriod->execute([
        $organization, 'group-access-it', 'group-active', $organization, 1, null, $now, null, $now, $now,
    ]);
    $insertPeriod->execute([
        $organization, 'group-access-it', 'group-history', $organization, 3, 20, $now, $now, $now, $now,
    ]);

    // Legal up: derive active/history-only, create user epochs and immutable
    // audit, while leaving the single conversation at its column default.
    $runSuccessfully('migrate', $migrationVersion, 'legal group access up');
    $assert($hasMigration(), 'legal up was not recorded');
    $assertComplete('legal up');
    $assert(
        (int) $pdo->query('SELECT COUNT(*) FROM im_user_group_access_state')->fetchColumn() === 3,
        'legal up did not create one state row per user',
    );
    $audits = $pdo->query(
        'SELECT conversation_id, user_id, access_snapshot_id, access_version, access_state, periods_json '
        . 'FROM im_group_member_access_audit ORDER BY user_id',
    )->fetchAll();
    $assert(count($audits) === 2, 'single conversation entered the group audit baseline');
    $assert($audits[0]['access_state'] === 'active', 'active member baseline is wrong');
    $assert($audits[1]['access_state'] === 'history_only', 'history-only member baseline is wrong');
    $assert(
        json_decode((string) $audits[0]['periods_json'], true, flags: JSON_THROW_ON_ERROR)
        === [['period_no' => '1', 'from_seq' => '1', 'to_seq' => null]],
        'active member periods baseline is not canonical',
    );
    $assert(
        $pdo->query(
            "SELECT access_state FROM im_conversation_member WHERE conversation_id = 'single-access-it'",
        )->fetchColumn() === 'active',
        'single conversation entered group access-state derivation',
    );

    // Force Phinx to call up again: a complete schema must be a true no-op.
    $auditBeforeNoOp = json_encode($audits, JSON_THROW_ON_ERROR);
    $deleteMigrationLog();
    $runSuccessfully('migrate', $migrationVersion, 'duplicate complete-schema up');
    $assertComplete('duplicate up/no-op');
    $assert(
        json_encode($pdo->query(
            'SELECT conversation_id, user_id, access_snapshot_id, access_version, access_state, periods_json '
            . 'FROM im_group_member_access_audit ORDER BY user_id',
        )->fetchAll(), JSON_THROW_ON_ERROR) === $auditBeforeNoOp,
        'duplicate up changed immutable audit rows',
    );

    // A partial schema without published group events is rebuilt from scratch.
    $deleteMigrationLog();
    $pdo->exec('ALTER TABLE im_conversation_membership_period DROP INDEX idx_group_access_period_page');
    $runSuccessfully('migrate', $migrationVersion, 'no-event partial recovery');
    $assertComplete('no-event partial recovery');
    $assert(
        (int) $pdo->query('SELECT COUNT(*) FROM im_group_member_access_audit')->fetchColumn() === 2,
        'partial recovery duplicated or lost baseline audit rows',
    );

    $eventId = hash('sha256', 'group-access-migration-integration-event');
    $insertGroupEvent = static function () use ($pdo, $eventId, $organization, $now): void {
        $pdo->prepare(
            'INSERT INTO im_message_outbox '
            . '(event_id, organization, event_type, routing_key, message_id, change_seq, conversation_id, '
            . 'conversation_type, payload_json, status, retry_count, create_time, update_time) '
            . 'VALUES (?, ?, ?, ?, ?, 0, ?, 2, ?, 1, 0, ?, ?)',
        )->execute([
            $eventId,
            $organization,
            'group.member_access_changed',
            'group.member_access_changed',
            sha1('group-access-migration-integration-event'),
            'group-access-it',
            '{"event_type":"group.member_access_changed"}',
            $now,
            $now,
        ]);
    };

    // Once a group access event exists, partial auto-recovery must fail before
    // cleanup so operators can reconcile already-published state explicitly.
    $deleteMigrationLog();
    $pdo->exec('ALTER TABLE im_conversation_membership_period DROP INDEX idx_group_access_period_page');
    $insertGroupEvent();
    [$rejectedExitCode, $rejectedOutput] = $runPhinx('migrate', $migrationVersion);
    $assert($rejectedExitCode !== 0, 'partial schema with a group event was accepted');
    $assert(
        str_contains($rejectedOutput, 'refused recovery while group.member_access_changed outbox rows exist'),
        'partial rejection did not identify the group access event',
    );
    $assert(!$hasMigration(), 'rejected partial recovery was recorded');
    $assert(
        !$hasIndex('im_conversation_membership_period', 'idx_group_access_period_page')
        && $hasTable('im_group_member_access_audit'),
        'partial rejection mutated schema before refusing recovery',
    );
    $pdo->prepare('DELETE FROM im_message_outbox WHERE event_id = ?')->execute([$eventId]);
    $runSuccessfully('migrate', $migrationVersion, 'post-rejection recovery');

    // Down must tolerate a missing artifact and delete group access outbox rows
    // before removing the schema consumed by those events.
    $pdo->exec('ALTER TABLE im_conversation_membership_period DROP INDEX idx_group_access_period_page');
    $insertGroupEvent();
    $runSuccessfully('rollback', $previousVersion, 'partial schema down');
    $assert(!$hasMigration(), 'partial down left the migration recorded');
    $eventCheck = $pdo->prepare('SELECT 1 FROM im_message_outbox WHERE event_id = ? LIMIT 1');
    $eventCheck->execute([$eventId]);
    $assert($eventCheck->fetchColumn() === false, 'partial down left a group access event');
    $assertPristine('partial down');

    // The invalid group member is rejected before any target DDL is issued.
    $pdo->exec(
        "UPDATE im_conversation_member SET access_version = 0 "
        . "WHERE conversation_id = 'group-access-it' AND user_id = 'group-active'",
    );
    [$invalidExitCode, $invalidOutput] = $runPhinx('migrate', $migrationVersion);
    $assert($invalidExitCode !== 0, 'invalid group baseline was accepted');
    $assert(
        str_contains($invalidOutput, 'invalid identity, access_version, or user backing'),
        'invalid baseline rejection did not identify the member invariant',
    );
    $assert(!$hasMigration(), 'invalid baseline migration was recorded');
    $assertPristine('invalid baseline pre-DDL refusal');

    fwrite(STDOUT, sprintf(
        "Group member access migration MySQL integration (%s): %d assertions passed.\n",
        $database,
        $assertions,
    ));
} finally {
    $pdo = null;
    if ($created) {
        if (preg_match($safeDatabasePattern, $database) !== 1) {
            throw new RuntimeException('refusing to drop an unsafe migration test database');
        }
        $admin->exec('DROP DATABASE IF EXISTS ' . $database);
    }
    foreach ($previousEnvironment as $key => $value) {
        putenv($value === null ? $key : $key . '=' . $value);
    }
}
