<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\RealtimeControlOutboxService;
use B8im\ImBusiness\Service\FriendRequestRealtimeEvent;
use B8im\ImBusiness\Service\DatabaseFriendRequestRealtimeAuthorizer;
use B8im\ImBusiness\Service\CrossOrganizationConversationAccess;
use B8im\ImBusiness\Service\CrossOrganizationSocialPolicy;

$enabled = strtolower(trim((string) (getenv('IM_REALTIME_CONTROL_MYSQL_TEST') ?: '')));
if (!in_array($enabled, ['1', 'true', 'yes', 'on'], true)) {
    fwrite(STDOUT, "SKIP realtime control outbox MySQL integration: set IM_REALTIME_CONTROL_MYSQL_TEST=1.\n");
    exit(0);
}

$businessRoot = dirname(__DIR__);
$repoRoot = dirname($businessRoot);
require $businessRoot . '/vendor/autoload.php';
if (is_file($businessRoot . '/.env')) {
    Dotenv\Dotenv::createImmutable($businessRoot)->safeLoad();
}
$base = Config::fromEnv();
$database = 'nb8im_realtime_control_' . bin2hex(random_bytes(8)) . '_test';
if (preg_match('/^nb8im_realtime_control_[a-f0-9]{16}_test$/D', $database) !== 1) {
    throw new RuntimeException('unsafe realtime control database name');
}
$admin = new PDO(
    sprintf('mysql:host=%s;port=%d;charset=%s', $base->dbHost, $base->dbPort, $base->dbCharset),
    $base->dbUser,
    $base->dbPassword,
    [PDO::ATTR_ERRMODE=>PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE=>PDO::FETCH_ASSOC],
);
$previous = [];
foreach (['DB_NAME', 'IM_EXPECT_DATABASE', 'IM_MESSAGE_SHARD_BUCKETS'] as $key) {
    $value = getenv($key);
    $previous[$key] = $value === false ? null : $value;
}
$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    ++$assertions;
};
$pdo = null;
try {
    $admin->exec('CREATE DATABASE ' . $database . ' CHARACTER SET utf8mb4 COLLATE utf8mb4_bin');
    putenv('DB_NAME=' . $database);
    putenv('IM_EXPECT_DATABASE=' . $database);
    putenv('IM_MESSAGE_SHARD_BUCKETS=1');
    $pdo = new PDO(
        sprintf('mysql:host=%s;port=%d;dbname=%s;charset=%s', $base->dbHost, $base->dbPort, $database, $base->dbCharset),
        $base->dbUser,
        $base->dbPassword,
        [PDO::ATTR_ERRMODE=>PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE=>PDO::FETCH_ASSOC],
    );
    $assert((string) $pdo->query('SELECT DATABASE()')->fetchColumn() === $database, 'temporary database mismatch');
    $assert($database !== 'nb8im', 'test selected local development database');
    $pdo->exec(<<<'SQL'
CREATE TABLE sm_system_organization (
  id int(11) UNSIGNED NOT NULL,
  status tinyint(3) UNSIGNED NOT NULL DEFAULT 1,
  delete_time datetime NULL DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
SQL);
    $pdo->exec('INSERT INTO sm_system_organization (id,status) VALUES (7,1),(8,1)');

    $phinx = $businessRoot . '/vendor/bin/phinx';
    $config = $repoRoot . '/phinx.php';
    $run = static function (string $operation, string $target) use ($phinx, $config, $repoRoot): array {
        $command = sprintf(
            '%s %s -c %s %s -e development -t %s',
            escapeshellarg(PHP_BINARY), escapeshellarg($phinx), escapeshellarg($config),
            $operation, escapeshellarg($target),
        );
        $output = [];
        exec('cd ' . escapeshellarg($repoRoot) . ' && ' . $command . ' 2>&1', $output, $status);
        return [$status, implode("\n", $output)];
    };
    [$status, $output] = $run('migrate', '20260720040000');
    if ($status !== 0) {
        throw new RuntimeException("baseline migration failed\n" . $output);
    }

    // A hostile pre-existing table must be preserved and rejected. After an
    // operator removes it, the same migration is safely retryable.
    $pdo->exec('CREATE TABLE im_realtime_control_outbox (id bigint unsigned NOT NULL PRIMARY KEY)');
    [$status, $output] = $run('migrate', '20260721140000');
    $assert($status !== 0 && str_contains($output, 'shape drifted'), 'hostile schema was accepted');
    $assert((int) $pdo->query(
        "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() "
        . "AND TABLE_NAME='im_realtime_control_outbox'",
    )->fetchColumn() === 1, 'hostile schema was destructively replaced');
    $pdo->exec('DROP TABLE im_realtime_control_outbox');
    [$status, $output] = $run('migrate', '20260721140000');
    if ($status !== 0) {
        throw new RuntimeException("control outbox migration failed\n" . $output);
    }
    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox DROP CHECK chk_realtime_control_target, '
        . 'ADD CONSTRAINT chk_realtime_control_target CHECK (status=status)',
    );
    $pdo->exec('DELETE FROM im_phinxlog WHERE version=20260721140000');
    [$status, $output] = $run('migrate', '20260721140000');
    $assert($status !== 0 && str_contains($output, 'check shape drifted'), 'hostile CHECK body was accepted');
    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox DROP CHECK chk_realtime_control_target, '
        . 'ADD CONSTRAINT chk_realtime_control_target CHECK (organization > 0 '
        . 'AND target_user_id <> "" AND BINARY target_user_id = BINARY TRIM(target_user_id) '
        . 'AND LOCATE(CHAR(0),target_user_id)=0 AND LOCATE("|",target_user_id)=0)',
    );
    [$status, $output] = $run('migrate', '20260721140000');
    if ($status !== 0) {
        throw new RuntimeException("repaired control outbox retry failed\n" . $output);
    }
    ++$assertions;

    $rejectMetadataDrift = static function (string $message) use ($pdo, $run, $assert): void {
        $pdo->exec('DELETE FROM im_phinxlog WHERE version=20260721140000');
        [$status, $output] = $run('migrate', '20260721140000');
        $assert(
            $status !== 0 && str_contains($output, 'shape drifted'),
            $message . "\n" . $output,
        );
    };
    $acceptMetadataRepair = static function (string $message) use ($run): void {
        [$status, $output] = $run('migrate', '20260721140000');
        if ($status !== 0) {
            throw new RuntimeException($message . "\n" . $output);
        }
    };

    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'ALTER CHECK chk_realtime_control_target NOT ENFORCED',
    );
    $assert(
        (string) $pdo->query(
            "SELECT ENFORCED FROM information_schema.TABLE_CONSTRAINTS "
            . "WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME='im_realtime_control_outbox' "
            . "AND CONSTRAINT_NAME='chk_realtime_control_target'",
        )->fetchColumn() === 'NO',
        'hostile CHECK did not become NOT ENFORCED',
    );
    $rejectMetadataDrift('NOT ENFORCED CHECK was accepted');
    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'ALTER CHECK chk_realtime_control_target ENFORCED',
    );
    $acceptMetadataRepair('ENFORCED CHECK repair was rejected');

    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'DROP INDEX uni_realtime_control_event, '
        . 'ADD UNIQUE KEY uni_realtime_control_event (event_id(32)) USING BTREE',
    );
    $assert(
        (int) $pdo->query(
            "SELECT SUB_PART FROM information_schema.STATISTICS "
            . "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='im_realtime_control_outbox' "
            . "AND INDEX_NAME='uni_realtime_control_event'",
        )->fetchColumn() === 32,
        'hostile prefix index did not expose SUB_PART=32',
    );
    $rejectMetadataDrift('prefix unique index was accepted as full-width');
    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'DROP INDEX uni_realtime_control_event, '
        . 'ADD UNIQUE KEY uni_realtime_control_event (event_id) USING BTREE',
    );
    $acceptMetadataRepair('full-width unique index repair was rejected');

    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'ALTER INDEX idx_realtime_control_claim INVISIBLE',
    );
    $assert(
        (string) $pdo->query(
            "SELECT IS_VISIBLE FROM information_schema.STATISTICS "
            . "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='im_realtime_control_outbox' "
            . "AND INDEX_NAME='idx_realtime_control_claim' LIMIT 1",
        )->fetchColumn() === 'NO',
        'hostile index did not become invisible',
    );
    $rejectMetadataDrift('invisible claim index was accepted');
    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'ALTER INDEX idx_realtime_control_claim VISIBLE',
    );
    $acceptMetadataRepair('visible claim index repair was rejected');

    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'DROP INDEX idx_realtime_control_claim, '
        . 'ADD FULLTEXT KEY idx_realtime_control_claim (target_user_id)',
    );
    $assert(
        (string) $pdo->query(
            "SELECT INDEX_TYPE FROM information_schema.STATISTICS "
            . "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='im_realtime_control_outbox' "
            . "AND INDEX_NAME='idx_realtime_control_claim' LIMIT 1",
        )->fetchColumn() === 'FULLTEXT',
        'hostile index did not become non-BTREE',
    );
    $rejectMetadataDrift('non-BTREE claim index was accepted');
    $pdo->exec(
        'ALTER TABLE im_realtime_control_outbox '
        . 'DROP INDEX idx_realtime_control_claim, '
        . 'ADD KEY idx_realtime_control_claim (status,next_retry_at,locked_until,id) USING BTREE',
    );
    $acceptMetadataRepair('BTREE claim index repair was rejected');

    $envelope = static function (
        int $requestId,
        string $transition = 'created',
        int $fromOrganization = 7,
        string $fromUserId = 'from-user',
        int $toOrganization = 8,
        string $toUserId = 'to-user',
        ?string $snapshot = '42',
    ): array {
        $handled = $transition !== 'created';
        $targetOrganization = $handled ? $fromOrganization : $toOrganization;
        $targetUserId = $handled ? $fromUserId : $toUserId;
        $actorOrganization = $handled ? $toOrganization : $fromOrganization;
        $actorUserId = $handled ? $toUserId : $fromUserId;
        $eventType = 'friend_request.' . $transition;
        $eventId = hash('sha256', json_encode([
            'friend_request.v1', $requestId, $eventType,
            (string) $fromOrganization, $fromUserId, (string) $toOrganization, $toUserId,
            (string) $targetOrganization, $targetUserId, $snapshot,
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
        return [$eventId, $eventType, $targetOrganization, $targetUserId, json_encode([
            'event_id'=>$eventId, 'type'=>'friend_request', 'organization'=>(string) $targetOrganization,
            'data'=>[
                'event'=>$transition, 'request_id'=>$requestId,
                'status'=>['created'=>1,'accepted'=>2,'rejected'=>3][$transition],
                'from_organization'=>(string) $fromOrganization, 'from_user_id'=>$fromUserId,
                'to_organization'=>(string) $toOrganization, 'to_user_id'=>$toUserId,
                'target_organization'=>(string) $targetOrganization, 'target_user_id'=>$targetUserId,
                'actor_organization'=>(string) $actorOrganization, 'actor_user_id'=>$actorUserId,
                'cross_org_access_snapshot_id'=>$snapshot, 'create_time'=>'2026-07-21 14:00:00',
                'handle_time'=>$handled ? '2026-07-21 14:01:00' : null,
            ],
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR)];
    };
    $insert = $pdo->prepare(
        'INSERT INTO im_realtime_control_outbox '
        . '(event_id,aggregate_type,aggregate_id,event_type,organization,target_user_id,payload_json,create_time,update_time) '
        . 'VALUES (?,"friend_request",?,?,?,?,?,?,?)',
    );
    $now = date('Y-m-d H:i:s');
    // CHECK constraints are enforced, not merely named.
    try {
        $pdo->exec("INSERT INTO im_realtime_control_outbox "
            . "(event_id,aggregate_type,aggregate_id,event_type,organization,target_user_id,payload_json,create_time,update_time) "
            . "VALUES ('bad','friend_request',1,'friend_request.created',8,'to-user','{}',NOW(),NOW())");
        throw new RuntimeException('invalid event_id passed CHECK');
    } catch (PDOException) {
        ++$assertions;
    }

    // Concurrent workers partition claims without duplicate ownership.
    for ($i = 100; $i < 108; $i++) {
        [$eventId, $eventType, $organization, $targetUserId, $raw] = $envelope($i);
        $insert->execute([$eventId, $i, $eventType, $organization, $targetUserId, $raw, $now, $now]);
    }
    $claimDir = sys_get_temp_dir() . '/b8im-control-claim-' . bin2hex(random_bytes(8));
    if (!mkdir($claimDir, 0700) && !is_dir($claimDir)) {
        throw new RuntimeException('unable to create claim result directory');
    }
    $barrier = $claimDir . '/go';
    $insert = null;
    $pdo = null;
    $admin = null;
    $children = [];
    foreach (['worker-a', 'worker-b'] as $index => $worker) {
        $pid = pcntl_fork();
        if ($pid === 0) {
            while (!is_file($barrier)) {
                usleep(1000);
            }
            $service = new RealtimeControlOutboxService(ImRepository::connect(Config::fromEnv()), 10);
            $ids = array_map(
                static fn (array $row): int => (int) $row['id'],
                $service->claimPending(4, $worker),
            );
            file_put_contents(
                $claimDir . '/result-' . $index . '.json',
                json_encode($ids, JSON_THROW_ON_ERROR),
                LOCK_EX,
            );
            exit(0);
        }
        if ($pid < 0) {
            throw new RuntimeException('unable to fork claim contender');
        }
        $children[] = $pid;
    }
    touch($barrier);
    foreach ($children as $pid) {
        pcntl_waitpid($pid, $childStatus);
        $assert(pcntl_wexitstatus($childStatus) === 0, 'claim contender failed');
    }
    $claimedIds = [];
    foreach ([0, 1] as $index) {
        $claimedIds = [
            ...$claimedIds,
            ...json_decode(
                (string) file_get_contents($claimDir . '/result-' . $index . '.json'),
                true,
                flags: JSON_THROW_ON_ERROR,
            ),
        ];
        unlink($claimDir . '/result-' . $index . '.json');
    }
    unlink($barrier);
    rmdir($claimDir);
    $assert(count($claimedIds) === 8, 'concurrent claim did not make full progress');
    $assert(count(array_unique($claimedIds)) === 8, 'concurrent workers claimed the same row');

    $admin = new PDO(
        sprintf('mysql:host=%s;port=%d;charset=%s', $base->dbHost, $base->dbPort, $base->dbCharset),
        $base->dbUser,
        $base->dbPassword,
        [PDO::ATTR_ERRMODE=>PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE=>PDO::FETCH_ASSOC],
    );
    $pdo = new PDO(
        sprintf('mysql:host=%s;port=%d;dbname=%s;charset=%s', $base->dbHost, $base->dbPort, $database, $base->dbCharset),
        $base->dbUser,
        $base->dbPassword,
        [PDO::ATTR_ERRMODE=>PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE=>PDO::FETCH_ASSOC],
    );
    $insert = $pdo->prepare(
        'INSERT INTO im_realtime_control_outbox '
        . '(event_id,aggregate_type,aggregate_id,event_type,organization,target_user_id,payload_json,create_time,update_time) '
        . 'VALUES (?,"friend_request",?,?,?,?,?,?,?)',
    );

    // Lease recovery changes the token; the stale owner cannot write a result.
    $service = new RealtimeControlOutboxService(ImRepository::connect(Config::fromEnv()), 10);
    [$eventId, $eventType, $organization, $targetUserId, $raw] = $envelope(200);
    $insert->execute([$eventId, 200, $eventType, $organization, $targetUserId, $raw, $now, $now]);
    $first = $service->claimPending(1, 'lease-old')[0];
    $pdo->prepare('UPDATE im_realtime_control_outbox SET locked_until=DATE_SUB(NOW(),INTERVAL 1 SECOND) WHERE id=?')
        ->execute([(int) $first['id']]);
    $second = $service->claimPending(1, 'lease-new')[0];
    $assert($first['claim_token'] !== $second['claim_token'], 'lease recovery reused a claim token');
    try {
        $service->markPublished((int) $first['id'], (string) $first['claim_token']);
        throw new RuntimeException('stale token marked a row published');
    } catch (RuntimeException $error) {
        $assert(str_contains($error->getMessage(), 'stale'), 'wrong stale token failure');
    }
    $service->markPublished((int) $second['id'], (string) $second['claim_token']);

    // Ten failures become a durable dead letter with exponential retry state.
    [$eventId, $eventType, $organization, $targetUserId, $raw] = $envelope(201);
    $insert->execute([$eventId, 201, $eventType, $organization, $targetUserId, $raw, $now, $now]);
    for ($attempt = 1; $attempt <= 10; $attempt++) {
        $claim = $service->claimPending(1, 'retry-worker')[0];
        $service->markFailed((int) $claim['id'], (string) $claim['claim_token'], 'redis unavailable');
        $row = $pdo->query(
            'SELECT status,retry_count,next_retry_at,last_error FROM im_realtime_control_outbox WHERE aggregate_id=201',
        )->fetch();
        $assert((int) $row['retry_count'] === $attempt, 'retry counter drifted');
        if ($attempt < 10) {
            $assert((int) $row['status'] === 4 && $row['next_retry_at'] !== null, 'retry state is invalid');
            $pdo->exec('UPDATE im_realtime_control_outbox SET next_retry_at=DATE_SUB(NOW(),INTERVAL 1 SECOND) WHERE aggregate_id=201');
        } else {
            $assert((int) $row['status'] === 5 && $row['next_retry_at'] === null, 'tenth failure was not dead');
            $assert($row['last_error'] === 'redis unavailable', 'dead row lost its error');
        }
    }

    // Consumer authorization reads terminal request state and requires both
    // composite friend directions only for acceptance.
    $repository = ImRepository::connect(Config::fromEnv());
    $requestCreateTime = '2026-07-21 14:00:00';
    $requestHandleTime = '2026-07-21 14:01:00';
    foreach (['same-a', 'same-b'] as $userId) {
        $repository->execute(
            'INSERT INTO im_user '
            . '(organization,user_id,account,password_hash,nickname,status,create_time,update_time) '
            . 'VALUES (7,?,?,?,?,1,?,?)',
            [$userId, $userId, 'test-only', $userId, $now, $now],
        );
    }
    $repository->execute(
        'INSERT INTO im_friend_request '
        . '(organization,from_organization,to_organization,from_user_id,to_user_id,add_method,status,handle_time,create_time,update_time) '
        . 'VALUES (7,7,7,"same-a","same-b","username",2,?,?,?)',
        [$requestHandleTime, $requestCreateTime, $now],
    );
    $acceptedRequestId = $repository->lastInsertId();
    foreach ([['same-a','same-b'], ['same-b','same-a']] as [$owner, $friend]) {
        $repository->execute(
            'INSERT INTO im_friend_relation '
            . '(organization,user_id,friend_user_id,friend_organization,add_method,added_at,status,create_time,update_time) '
            . 'VALUES (7,?,?,7,"username",?,1,?,?)',
            [$owner, $friend, $now, $now, $now],
        );
    }
    $authorizer = new DatabaseFriendRequestRealtimeAuthorizer(
        $repository,
        new CrossOrganizationConversationAccess(
            $repository,
            new CrossOrganizationSocialPolicy($repository),
        ),
    );
    $acceptedRaw = $envelope(
        $acceptedRequestId,
        'accepted',
        7,
        'same-a',
        7,
        'same-b',
        null,
    )[4];
    $acceptedEvent = FriendRequestRealtimeEvent::fromRaw($acceptedRaw);
    $delivered = false;
    $authorizer->withCurrentEvent($acceptedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert($delivered, 'accepted request with both composite relations was dropped');

    $repository->execute(
        'UPDATE im_friend_request SET create_time=? WHERE id=?',
        ['2026-07-21 14:00:01', $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($acceptedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'accepted event with stale create_time was delivered');
    $repository->execute(
        'UPDATE im_friend_request SET create_time=?,handle_time=? WHERE id=?',
        [$requestCreateTime, '2026-07-21 14:01:01', $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($acceptedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'accepted event with stale handle_time was delivered');
    $repository->execute(
        'UPDATE im_friend_request SET handle_time=? WHERE id=?',
        [$requestHandleTime, $acceptedRequestId],
    );

    $repository->execute(
        'UPDATE im_friend_relation SET friend_user_id=? '
        . 'WHERE organization=7 AND BINARY user_id=BINARY ? '
        . 'AND friend_organization=7 AND BINARY friend_user_id=BINARY ?',
        ['SAME-A', 'same-b', 'same-a'],
    );
    $assert(
        (int) ($repository->fetchOne(
            'SELECT COUNT(*) AS aggregate FROM im_friend_relation '
            . 'WHERE organization=7 AND user_id=? AND friend_organization=7 AND friend_user_id=?',
            ['same-b', 'same-a'],
        )['aggregate'] ?? 0) === 1,
        'fixture did not demonstrate general_ci case folding',
    );
    $assert(
        (int) ($repository->fetchOne(
            'SELECT COUNT(*) AS aggregate FROM im_friend_relation '
            . 'WHERE organization=7 AND BINARY user_id=BINARY ? '
            . 'AND friend_organization=7 AND BINARY friend_user_id=BINARY ?',
            ['same-b', 'same-a'],
        )['aggregate'] ?? 0) === 0,
        'fixture unexpectedly retained an exact reverse relation',
    );
    $delivered = false;
    $authorizer->withCurrentEvent($acceptedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'accepted request used a case-folded reverse relation');
    $repository->execute(
        'UPDATE im_friend_relation SET friend_user_id=? '
        . 'WHERE organization=7 AND BINARY user_id=BINARY ? '
        . 'AND friend_organization=7 AND BINARY friend_user_id=BINARY ?',
        ['same-a', 'same-b', 'SAME-A'],
    );

    $repository->execute(
        'UPDATE im_friend_relation SET delete_time=? '
        . 'WHERE organization=7 AND BINARY user_id=BINARY ? AND BINARY friend_user_id=BINARY ?',
        [$now, 'same-b', 'same-a'],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($acceptedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'accepted request without reverse composite relation was delivered');

    $repository->execute(
        'UPDATE im_friend_request SET status=3,handle_time=?,create_time=? WHERE id=?',
        [$requestHandleTime, $requestCreateTime, $acceptedRequestId],
    );
    $rejectedEvent = FriendRequestRealtimeEvent::fromRaw($envelope(
        $acceptedRequestId,
        'rejected',
        7,
        'same-a',
        7,
        'same-b',
        null,
    )[4]);
    $delivered = false;
    $authorizer->withCurrentEvent($rejectedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert($delivered, 'authoritative rejected request was dropped');

    $repository->execute(
        'UPDATE im_friend_request SET handle_time=? WHERE id=?',
        ['2026-07-21 14:01:01', $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($rejectedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'rejected event with stale handle_time was delivered');
    $repository->execute(
        'UPDATE im_friend_request SET handle_time=?,create_time=? WHERE id=?',
        [$requestHandleTime, '2026-07-21 14:00:01', $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($rejectedEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'rejected event with stale create_time was delivered');

    $createdEvent = FriendRequestRealtimeEvent::fromRaw($envelope(
        $acceptedRequestId,
        'created',
        7,
        'same-a',
        7,
        'same-b',
        null,
    )[4]);
    $repository->execute(
        'UPDATE im_friend_request SET status=1,handle_time=NULL,create_time=? WHERE id=?',
        [$requestCreateTime, $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($createdEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert($delivered, 'authoritative created request was dropped');

    $repository->execute(
        'UPDATE im_friend_request SET handle_time=? WHERE id=?',
        [$requestHandleTime, $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($createdEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'created event with a database handle_time was delivered');
    $repository->execute(
        'UPDATE im_friend_request SET handle_time=NULL,create_time=? WHERE id=?',
        ['2026-07-21 14:00:01', $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($createdEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'created event with stale create_time was delivered');
    $repository->execute(
        'UPDATE im_friend_request SET status=3,handle_time=?,create_time=? WHERE id=?',
        [$requestHandleTime, $requestCreateTime, $acceptedRequestId],
    );
    $delivered = false;
    $authorizer->withCurrentEvent($createdEvent, static function () use (&$delivered): void {
        $delivered = true;
    });
    $assert(!$delivered, 'created event was delivered after request reached terminal state');

    // Rollback owns only the new table and a fresh migrate recreates it.
    [$status, $output] = $run('rollback', '20260720040000');
    if ($status !== 0) {
        throw new RuntimeException("control outbox rollback failed\n" . $output);
    }
    $assert((int) $pdo->query(
        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() "
        . "AND TABLE_NAME='im_realtime_control_outbox'",
    )->fetchColumn() === 0, 'rollback left the control outbox table');
    [$status, $output] = $run('migrate', '20260721140000');
    if ($status !== 0) {
        throw new RuntimeException("control outbox retry migration failed\n" . $output);
    }
    ++$assertions;
} finally {
    $pdo = null;
    $admin->exec('DROP DATABASE IF EXISTS ' . $database);
    foreach ($previous as $key => $value) {
        putenv($value === null ? $key : $key . '=' . $value);
    }
}

fwrite(STDOUT, sprintf("Realtime control outbox MySQL integration: %d assertions passed.\n", $assertions));
