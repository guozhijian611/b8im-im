<?php

declare(strict_types=1);

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Queue\RabbitMqPublisher;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\OutboxService;
use B8im\ImShared\Protocol\Dto\CanonicalDecimal;
use B8im\ImShared\Protocol\Dto\SearchProjectionEvent;
use B8im\ImShared\Support\Constants;

$enabled = strtolower(trim((string) (
    $_ENV['IM_SEARCH_SEQUENCE_MYSQL_TEST']
    ?? $_SERVER['IM_SEARCH_SEQUENCE_MYSQL_TEST']
    ?? getenv('IM_SEARCH_SEQUENCE_MYSQL_TEST')
)));
if (!in_array($enabled, ['1', 'true', 'yes', 'on'], true)) {
    fwrite(
        STDOUT,
        "SKIP search projection sequence MySQL integration: "
        . "set IM_SEARCH_SEQUENCE_MYSQL_TEST=1 to use an isolated temporary database.\n",
    );
    exit(0);
}

$businessRoot = dirname(__DIR__);
$repositoryRoot = dirname($businessRoot);
require $businessRoot . '/vendor/autoload.php';
if (is_file($businessRoot . '/.env')) {
    Dotenv\Dotenv::createImmutable($businessRoot)->safeLoad();
}

$baseConfig = Config::fromEnv();
$suffix = bin2hex(random_bytes(8));
$database = 'nb8im_search_seq_' . $suffix . '_test';
if (preg_match('/^nb8im_search_seq_[a-f0-9]{16}_test$/D', $database) !== 1) {
    throw new RuntimeException('generated search sequence database name is unsafe');
}
$admin = new PDO(
    sprintf(
        'mysql:host=%s;port=%d;charset=%s',
        $baseConfig->dbHost,
        $baseConfig->dbPort,
        $baseConfig->dbCharset,
    ),
    $baseConfig->dbUser,
    $baseConfig->dbPassword,
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
foreach ([
    'DB_HOST',
    'DB_PORT',
    'DB_NAME',
    'DB_USER',
    'DB_PASSWORD',
    'DB_CHARSET',
    'IM_EXPECT_DATABASE',
    'IM_MESSAGE_SHARD_BUCKETS',
] as $key) {
    $value = getenv($key);
    $previousEnvironment[$key] = $value === false ? null : $value;
}

$pdo = null;
try {
    $admin->exec('CREATE DATABASE ' . $database . ' CHARACTER SET utf8mb4 COLLATE utf8mb4_bin');
    foreach ([
        'DB_HOST' => $baseConfig->dbHost,
        'DB_PORT' => (string) $baseConfig->dbPort,
        'DB_NAME' => $database,
        'DB_USER' => $baseConfig->dbUser,
        'DB_PASSWORD' => $baseConfig->dbPassword,
        'DB_CHARSET' => $baseConfig->dbCharset,
        'IM_EXPECT_DATABASE' => $database,
        'IM_MESSAGE_SHARD_BUCKETS' => '1',
    ] as $key => $value) {
        putenv($key . '=' . $value);
    }
    $config = Config::fromEnv();
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
    $assert((string) $pdo->query('SELECT DATABASE()')->fetchColumn() === $database, 'isolated database mismatch');
    $assert($database !== 'nb8im', 'search sequence test selected the local development database');

    $pdo->exec(<<<'SQL'
CREATE TABLE sm_system_organization (
  id int(11) UNSIGNED NOT NULL,
  status tinyint(3) UNSIGNED NOT NULL DEFAULT 1,
  delete_time datetime NULL DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
SQL);
    $pdo->exec('INSERT INTO sm_system_organization (id, status) VALUES (1,1),(2,1),(3,1),(4,1)');

    $phinx = $businessRoot . '/vendor/bin/phinx';
    $phinxConfig = $repositoryRoot . '/phinx.php';
    $runPhinx = static function (string $operation, string $target) use (
        $phinx,
        $phinxConfig,
        $repositoryRoot,
    ): array {
        if (!in_array($operation, ['migrate', 'rollback'], true)) {
            throw new RuntimeException('unsupported Phinx operation');
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
        exec('cd ' . escapeshellarg($repositoryRoot) . ' && ' . $command . ' 2>&1', $output, $exitCode);

        return [$exitCode, implode("\n", $output)];
    };
    $runSuccessfully = static function (string $operation, string $target, string $label) use ($runPhinx): void {
        [$exitCode, $output] = $runPhinx($operation, $target);
        if ($exitCode !== 0) {
            throw new RuntimeException($label . " failed\n" . $output);
        }
    };
    $runSuccessfully('migrate', '20260720030000', 'baseline migration');

    $now = date('Y-m-d H:i:s');
    $insertHistory = $pdo->prepare(
        'INSERT INTO im_message_outbox
            (event_id, organization, event_type, routing_key, message_id, change_seq,
             conversation_id, conversation_type, payload_json, status, retry_count,
             next_retry_at, create_time, update_time)
         VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, 1, 0, ?, ?, ?)',
    );
    $history = [
        [1, SearchProjectionEvent::EVENT_CREATED, 'history-1', 0],
        [1, SearchProjectionEvent::EVENT_EDITED, 'history-1', 1],
        [2, SearchProjectionEvent::EVENT_RECALLED, 'history-2', 1],
        [2, 'message.receipt', 'history-2', 0],
        [2, SearchProjectionEvent::EVENT_DELETED_BOTH, 'history-3', 2],
    ];
    foreach ($history as $index => [$organization, $eventType, $messageId, $changeSeq]) {
        $eventId = hash('sha256', 'history|' . $index);
        $insertHistory->execute([
            $eventId,
            $organization,
            $eventType,
            $eventType,
            $messageId,
            $changeSeq,
            'conversation-history-' . $organization,
            json_encode([
                'event_id' => $eventId,
                'event_type' => $eventType,
                'organization' => $organization,
                'message_id' => $messageId,
            ], JSON_THROW_ON_ERROR),
            $now,
            $now,
            $now,
        ]);
    }
    $historicalMaxId = (int) $pdo->query('SELECT MAX(id) FROM im_message_outbox')->fetchColumn();
    $runSuccessfully('migrate', '20260720040000', 'search sequence migration');

    $rows = $pdo->query(
        'SELECT id, organization, event_type, event_id, message_id,
                CAST(source_event_seq AS CHAR) AS source_event_seq, payload_json
           FROM im_message_outbox ORDER BY organization, id',
    )->fetchAll();
    $expectedByOrganization = [1 => 0, 2 => 0];
    foreach ($rows as $row) {
        $eventType = (string) $row['event_type'];
        if (!in_array($eventType, SearchProjectionEvent::EVENT_TYPES, true)) {
            $assert($row['source_event_seq'] === null, 'non-search history received a source_event_seq');
            continue;
        }
        $organization = (int) $row['organization'];
        $expected = (string) (++$expectedByOrganization[$organization]);
        $assert((string) $row['source_event_seq'] === $expected, 'history backfill is not continuous by organization,id');
        $payload = json_decode((string) $row['payload_json'], true, flags: JSON_THROW_ON_ERROR);
        $identity = new SearchProjectionEvent(
            (string) $row['event_id'],
            $organization,
            $eventType,
            $expected,
            (string) $row['message_id'],
        );
        foreach ($identity->toArray() as $field => $value) {
            $assert(($payload[$field] ?? null) === $value, 'history payload identity was not patched: ' . $field);
        }
    }
    $assert($expectedByOrganization === [1 => 2, 2 => 2], 'history backfill counters diverged');

    $context = static fn (int $organization): AuthContext => new AuthContext(
        organization: $organization,
        userId: 'search-sequence-user',
        deviceId: 'search-sequence-device',
        clientId: 'search-sequence-client',
        credentialSessionId: 'search-sequence-credential',
        sessionId: str_repeat('a', 32),
        clientFamily: 'web',
        os: 'browser',
        issuer: 'b8im-test',
        audience: 'im',
        notBefore: time() - 10,
        expireAt: time() + 600,
    );
    $create = static function (
        ImRepository $repository,
        Config $config,
        array $organizations,
        string $messageId,
        int $holdMicroseconds = 0,
        bool $rollback = false,
    ) use ($context): void {
        $outbox = new OutboxService($repository, $config);
        $repository->transaction(function () use (
            $outbox,
            $organizations,
            $messageId,
            $holdMicroseconds,
            $rollback,
            $context,
        ): void {
            $outbox->lockSearchProjectionSequences($organizations);
            foreach ($organizations as $organization) {
                $outbox->createMessageCreated(
                    $context((int) $organization),
                    (int) $organization,
                    [
                        'message_id' => $messageId,
                        'message_seq' => 1,
                        'global_seq' => '1',
                        'conversation_id' => 'conversation-' . $messageId,
                        'conversation_type' => 1,
                        'sender_id' => 'search-sequence-user',
                        'sender_organization' => (int) $organization,
                        'create_time' => date('Y-m-d H:i:s'),
                    ],
                    [[
                        'organization' => (int) $organization,
                        'user_id' => 'search-sequence-user',
                    ]],
                );
            }
            if ($holdMicroseconds > 0) {
                usleep($holdMicroseconds);
            }
            if ($rollback) {
                throw new RuntimeException('intentional search sequence rollback');
            }
        });
    };
    $forkCreate = static function (
        array $organizations,
        string $messageId,
        int $holdMicroseconds = 0,
    ) use ($config, $create): int {
        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new RuntimeException('unable to fork search sequence contender');
        }
        if ($pid === 0) {
            try {
                $create(ImRepository::connect($config), $config, $organizations, $messageId, $holdMicroseconds);
                exit(0);
            } catch (Throwable $exception) {
                fwrite(STDERR, $exception->getMessage() . "\n");
                exit(1);
            }
        }

        return $pid;
    };
    $waitChildren = static function (array $pids, string $label) use ($assert): void {
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
            $assert(pcntl_wifexited($status) && pcntl_wexitstatus($status) === 0, $label . ' child failed');
        }
    };

    $repository = ImRepository::connect($config);
    try {
        (new OutboxService($repository, $config))->lockSearchProjectionSequences([1]);
        throw new RuntimeException('search sequence lock was accepted outside a fact transaction');
    } catch (RuntimeException $exception) {
        $assert(
            str_contains($exception->getMessage(), 'enclosing fact transaction'),
            'out-of-transaction search sequence lock did not fail closed',
        );
    }
    $beforeRollback = (string) $repository->fetchOne(
        'SELECT CAST(last_search_event_seq AS CHAR) AS seq FROM im_organization_message_sequence WHERE organization = 1',
    )['seq'];
    try {
        $create($repository, $config, [1], 'rollback-event', rollback: true);
        throw new RuntimeException('rollback fixture unexpectedly committed');
    } catch (RuntimeException $exception) {
        $assert($exception->getMessage() === 'intentional search sequence rollback', 'rollback fixture failed for an unexpected reason');
    }
    $afterRollback = (string) $repository->fetchOne(
        'SELECT CAST(last_search_event_seq AS CHAR) AS seq FROM im_organization_message_sequence WHERE organization = 1',
    )['seq'];
    $assert($afterRollback === $beforeRollback, 'rolled-back event left a source_event_seq hole');
    $assert($repository->fetchOne(
        'SELECT 1 FROM im_message_outbox WHERE message_id = ? LIMIT 1',
        ['rollback-event'],
    ) === null, 'rolled-back outbox event remained committed');

    $create($repository, $config, [1], 'duplicate-event');
    $afterFirstDuplicate = (string) $repository->fetchOne(
        'SELECT CAST(last_search_event_seq AS CHAR) AS seq FROM im_organization_message_sequence WHERE organization = 1',
    )['seq'];
    $create($repository, $config, [1], 'duplicate-event');
    $afterSecondDuplicate = (string) $repository->fetchOne(
        'SELECT CAST(last_search_event_seq AS CHAR) AS seq FROM im_organization_message_sequence WHERE organization = 1',
    )['seq'];
    $assert($afterSecondDuplicate === $afterFirstDuplicate, 'duplicate event_id consumed a new source_event_seq');
    $assert((int) $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate FROM im_message_outbox WHERE organization = 1 AND message_id = ?',
        ['duplicate-event'],
    )['aggregate'] === 1, 'duplicate event_id created a second outbox row');
    try {
        $repository->transaction(function () use ($repository, $config, $context): void {
            $outbox = new OutboxService($repository, $config);
            $outbox->lockSearchProjectionSequences([1]);
            $outbox->createMessageCreated(
                $context(1),
                1,
                [
                    'message_id' => 'duplicate-event',
                    'message_seq' => 1,
                    'global_seq' => '1',
                    'conversation_id' => 'different-conversation',
                    'conversation_type' => 1,
                    'sender_id' => 'search-sequence-user',
                    'sender_organization' => 1,
                    'create_time' => date('Y-m-d H:i:s'),
                ],
                [['organization' => 1, 'user_id' => 'search-sequence-user']],
            );
        });
        throw new RuntimeException('conflicting search event_id identity was silently reused');
    } catch (RuntimeException $exception) {
        $assert(
            str_contains($exception->getMessage(), 'different fact identity'),
            'conflicting search event_id did not fail closed',
        );
    }
    $afterConflictingDuplicate = (string) $repository->fetchOne(
        'SELECT CAST(last_search_event_seq AS CHAR) AS seq FROM im_organization_message_sequence WHERE organization = 1',
    )['seq'];
    $assert(
        $afterConflictingDuplicate === $afterSecondDuplicate,
        'conflicting duplicate event_id consumed a source_event_seq',
    );

    $mutationTypes = [
        SearchProjectionEvent::EVENT_EDITED,
        SearchProjectionEvent::EVENT_RECALLED,
        SearchProjectionEvent::EVENT_DELETED_BOTH,
    ];
    foreach ($mutationTypes as $offset => $eventType) {
        $repository->transaction(function () use (
            $repository,
            $config,
            $context,
            $eventType,
            $offset,
        ): void {
            $outbox = new OutboxService($repository, $config);
            $outbox->lockSearchProjectionSequences([1]);
            $outbox->createMessageChanged(
                $context(1),
                1,
                $eventType,
                'runtime-mutation-message',
                'runtime-mutation-conversation',
                1,
                1,
                $offset + 1,
                null,
                null,
                ['status' => $eventType],
                [['organization' => 1, 'user_id' => 'search-sequence-user']],
            );
        });
    }
    $runtimeMutations = $repository->fetchAll(
        'SELECT event_id, event_type, organization, message_id,
                CAST(source_event_seq AS CHAR) AS source_event_seq, payload_json
           FROM im_message_outbox
          WHERE organization = 1 AND message_id = ?
          ORDER BY source_event_seq',
        ['runtime-mutation-message'],
    );
    $assert(count($runtimeMutations) === 3, 'runtime did not persist all search mutation event types');
    foreach ($runtimeMutations as $index => $row) {
        $payload = json_decode((string) $row['payload_json'], true, flags: JSON_THROW_ON_ERROR);
        $identity = RabbitMqPublisher::searchProjectionIdentity($payload);
        $assert(
            $identity !== null
            && $identity->eventType === $mutationTypes[$index]
            && $identity->eventId === (string) $row['event_id']
            && $identity->organization === (int) $row['organization']
            && $identity->sourceEventSeq === (string) $row['source_event_seq']
            && $identity->messageId === (string) $row['message_id'],
            'runtime mutation outbox body does not match its immutable identity',
        );
    }

    $receipt = [
        'status' => 'delivered',
        'message_id' => 'ordinary-receipt-message',
        'conversation_id' => 'ordinary-receipt-conversation',
        'message_seq' => 1,
        'sender_organization' => 1,
        'sender_id' => 'sender',
        'time' => date('Y-m-d H:i:s'),
    ];
    $createReceipt = static function (
        ImRepository $receiptRepository,
        array $value,
        int $holdMicroseconds = 0,
    ) use ($config, $context): void {
        $receiptRepository->transaction(function () use (
            $receiptRepository,
            $config,
            $context,
            $value,
            $holdMicroseconds,
        ): void {
            (new OutboxService($receiptRepository, $config))->createMessageReceipt(
                $context(1),
                1,
                $value,
                1,
                [['organization' => 1, 'user_id' => 'search-sequence-user']],
            );
            if ($holdMicroseconds > 0) {
                usleep($holdMicroseconds);
            }
        });
    };
    $forkReceipt = static function (
        array $value,
        int $holdMicroseconds = 0,
    ) use ($config, $createReceipt): int {
        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new RuntimeException('unable to fork ordinary outbox contender');
        }
        if ($pid === 0) {
            try {
                $createReceipt(
                    ImRepository::connect($config),
                    $value,
                    $holdMicroseconds,
                );
                exit(0);
            } catch (Throwable $exception) {
                fwrite(STDERR, $exception->getMessage() . "\n");
                exit(1);
            }
        }

        return $pid;
    };
    $firstOrdinary = $forkReceipt($receipt, 250000);
    usleep(50000);
    $secondOrdinary = $forkReceipt($receipt);
    $waitChildren(
        [$firstOrdinary, $secondOrdinary],
        'same ordinary event_id atomic idempotency race',
    );
    $ordinary = $repository->fetchAll(
        'SELECT source_event_seq FROM im_message_outbox
          WHERE organization = 1 AND message_id = ?',
        ['ordinary-receipt-message'],
    );
    $assert(
        count($ordinary) === 1,
        'concurrent ordinary duplicate event_id did not reuse its exact identity',
    );
    $assert($ordinary[0]['source_event_seq'] === null, 'ordinary outbox event received a source_event_seq');
    try {
        $createReceipt(
            $repository,
            [...$receipt, 'conversation_id' => 'ordinary-receipt-conflict'],
        );
        throw new RuntimeException('conflicting ordinary event_id identity was silently reused');
    } catch (RuntimeException $exception) {
        $assert(
            str_contains($exception->getMessage(), 'different fact identity'),
            'conflicting ordinary event_id did not fail closed',
        );
    }

    $beforeConcurrentDuplicate = (string) $repository->fetchOne(
        'SELECT CAST(last_search_event_seq AS CHAR) AS seq
           FROM im_organization_message_sequence WHERE organization = 1',
    )['seq'];
    $firstDuplicate = $forkCreate([1], 'concurrent-duplicate-event', 250000);
    usleep(50000);
    $secondDuplicate = $forkCreate([1], 'concurrent-duplicate-event');
    $waitChildren(
        [$firstDuplicate, $secondDuplicate],
        'same search event_id sequence lock race',
    );
    $afterConcurrentDuplicate = (string) $repository->fetchOne(
        'SELECT CAST(last_search_event_seq AS CHAR) AS seq
           FROM im_organization_message_sequence WHERE organization = 1',
    )['seq'];
    $assert(
        $afterConcurrentDuplicate === CanonicalDecimal::increment(
            $beforeConcurrentDuplicate,
            'concurrent_duplicate_source_event_seq',
        ),
        'concurrent search duplicate event_id consumed more than one source_event_seq',
    );
    $assert((int) $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate FROM im_message_outbox
          WHERE organization = 1 AND message_id = ?',
        ['concurrent-duplicate-event'],
    )['aggregate'] === 1, 'concurrent search duplicate event_id created a second outbox row');

    $first = $forkCreate([1], 'commit-race-a', 250000);
    usleep(50000);
    $second = $forkCreate([1], 'commit-race-b');
    $waitChildren([$first, $second], 'same-organization commit race');
    $raceRows = $repository->fetchAll(
        'SELECT message_id, CAST(source_event_seq AS CHAR) AS source_event_seq
           FROM im_message_outbox
          WHERE organization = 1 AND message_id IN (?, ?)
          ORDER BY source_event_seq',
        ['commit-race-a', 'commit-race-b'],
    );
    $assert(count($raceRows) === 2, 'same-organization commit race lost an event');
    $assert(
        (int) $raceRows[1]['source_event_seq'] === (int) $raceRows[0]['source_event_seq'] + 1,
        'same-organization commit race created a sequence gap',
    );

    $orgOne = $forkCreate([1], 'different-org-a', 150000);
    $orgThree = $forkCreate([3], 'different-org-b', 150000);
    $waitChildren([$orgOne, $orgThree], 'different-organization concurrency');
    $assert($repository->fetchOne(
        'SELECT 1 FROM im_message_outbox WHERE organization = 1 AND message_id = ?',
        ['different-org-a'],
    ) !== null, 'organization 1 concurrent event is missing');
    $assert($repository->fetchOne(
        'SELECT 1 FROM im_message_outbox WHERE organization = 3 AND message_id = ?',
        ['different-org-b'],
    ) !== null, 'organization 3 concurrent event is missing');

    $crossA = $forkCreate([2, 1], 'cross-home-a', 150000);
    usleep(30000);
    $crossB = $forkCreate([1, 2], 'cross-home-b');
    $waitChildren([$crossA, $crossB], 'cross-home canonical lock order');
    foreach ([1, 2] as $organization) {
        $crossRows = $repository->fetchAll(
            'SELECT CAST(source_event_seq AS CHAR) AS source_event_seq
               FROM im_message_outbox
              WHERE organization = ? AND message_id IN (?, ?)
              ORDER BY source_event_seq',
            [$organization, 'cross-home-a', 'cross-home-b'],
        );
        $assert(count($crossRows) === 2, 'cross-home transaction lost an organization projection');
        $assert(
            (int) $crossRows[1]['source_event_seq'] === (int) $crossRows[0]['source_event_seq'] + 1,
            'cross-home transaction created a sequence gap',
        );
    }

    $repository->execute(
        'UPDATE im_organization_message_sequence SET last_search_event_seq = ? WHERE organization = 4',
        ['18446744073709551614'],
    );
    $create($repository, $config, [4], 'uint64-max-event');
    $maxRow = $repository->fetchOne(
        'SELECT CAST(source_event_seq AS CHAR) AS source_event_seq, payload_json
           FROM im_message_outbox WHERE organization = 4 AND message_id = ?',
        ['uint64-max-event'],
    );
    $assert(
        (string) ($maxRow['source_event_seq'] ?? '') === CanonicalDecimal::UNSIGNED_BIGINT_MAX,
        'uint64 maximum source_event_seq was not preserved as a decimal string',
    );
    try {
        $create($repository, $config, [4], 'uint64-overflow-event');
        throw new RuntimeException('uint64 overflow event unexpectedly committed');
    } catch (RuntimeException $exception) {
        $assert(
            str_contains($exception->getMessage(), 'exhausted uint64'),
            'uint64 exhaustion did not fail closed',
        );
    }
    $assert($repository->fetchOne(
        'SELECT 1 FROM im_message_outbox WHERE message_id = ? LIMIT 1',
        ['uint64-overflow-event'],
    ) === null, 'uint64 overflow left an outbox artifact');
    $maxPayload = json_decode((string) $maxRow['payload_json'], true, flags: JSON_THROW_ON_ERROR);
    $headers = RabbitMqPublisher::applicationHeaders($maxPayload, null);
    $assert($headers === [
        'event_contract' => SearchProjectionEvent::CONTRACT,
        'event_id' => (string) $maxPayload['event_id'],
        'organization' => 4,
        'event_type' => SearchProjectionEvent::EVENT_CREATED,
        'source_event_seq' => CanonicalDecimal::UNSIGNED_BIGINT_MAX,
        'message_id' => 'uint64-max-event',
    ], 'RabbitMQ search identity headers differ from the outbox body');

    $reverseRows = $repository->fetchAll(
        'SELECT id, CAST(source_event_seq AS CHAR) AS source_event_seq
           FROM im_message_outbox
          WHERE organization = 1 AND message_id IN (?, ?)
          ORDER BY source_event_seq',
        ['commit-race-a', 'commit-race-b'],
    );
    $repository->execute('UPDATE im_message_outbox SET status = 3, next_retry_at = NULL');
    $repository->execute(
        'UPDATE im_message_outbox SET status = 4, next_retry_at = ? WHERE id = ?',
        [date('Y-m-d H:i:s', time() + 3600), (int) $reverseRows[0]['id']],
    );
    $repository->execute(
        'UPDATE im_message_outbox SET status = 1, next_retry_at = NULL WHERE id = ?',
        [(int) $reverseRows[1]['id']],
    );
    $claimed = (new OutboxService($repository, $config))->claimPending(10, 'reverse-order-worker');
    $assert(
        count($claimed) === 1
        && (int) $claimed[0]['id'] === (int) $reverseRows[1]['id']
        && (string) $claimed[0]['source_event_seq'] === (string) $reverseRows[1]['source_event_seq'],
        'retry readiness did not demonstrate the supported reverse publication order',
    );

    $repository->execute('DELETE FROM im_message_outbox WHERE id > ?', [$historicalMaxId]);
    foreach ([1 => 2, 2 => 2, 3 => 0, 4 => 0] as $organization => $historicalSeq) {
        $repository->execute(
            'UPDATE im_organization_message_sequence SET last_search_event_seq = ? WHERE organization = ?',
            [$historicalSeq, $organization],
        );
    }

    $historyRow = $repository->fetchOne(
        'SELECT id, payload_json FROM im_message_outbox
          WHERE event_type = ? ORDER BY id LIMIT 1',
        [SearchProjectionEvent::EVENT_CREATED],
    );
    $corruptPayload = json_decode((string) $historyRow['payload_json'], true, flags: JSON_THROW_ON_ERROR);
    $corruptPayload['event_contract'] = 'im.search-projection.v0';
    $repository->execute(
        'UPDATE im_message_outbox SET payload_json = ? WHERE id = ?',
        [json_encode($corruptPayload, JSON_THROW_ON_ERROR), (int) $historyRow['id']],
    );
    [$downExit, $downOutput] = $runPhinx('rollback', '20260720030000');
    $assert($downExit !== 0, 'strict down accepted a corrupted search projection identity');
    $assert(
        str_contains($downOutput, 'payload identity differs'),
        'strict down corruption failure was not explicit',
    );
    $corruptPayload['event_contract'] = SearchProjectionEvent::CONTRACT;
    $repository->execute(
        'UPDATE im_message_outbox SET payload_json = ? WHERE id = ?',
        [json_encode($corruptPayload, JSON_THROW_ON_ERROR), (int) $historyRow['id']],
    );
    $runSuccessfully('rollback', '20260720030000', 'strict search sequence down');
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
    $columns = $pdo->query(
        "SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name
           FROM information_schema.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE()
            AND ((TABLE_NAME = 'im_message_outbox' AND COLUMN_NAME = 'source_event_seq')
              OR (TABLE_NAME = 'im_organization_message_sequence' AND COLUMN_NAME = 'last_search_event_seq'))",
    )->fetchAll();
    $assert($columns === [], 'strict down retained search sequence columns');
    $rolledBackPayload = json_decode((string) $pdo->query(
        'SELECT payload_json FROM im_message_outbox WHERE id = ' . (int) $historyRow['id'],
    )->fetchColumn(), true, flags: JSON_THROW_ON_ERROR);
    $assert(
        !array_key_exists('event_contract', $rolledBackPayload)
        && !array_key_exists('source_event_seq', $rolledBackPayload),
        'strict down retained search projection-only payload fields',
    );

    fwrite(STDOUT, sprintf(
        "Search projection sequence MySQL integration (%s): %d assertions passed.\n",
        $database,
        $assertions,
    ));
} finally {
    $pdo = null;
    if (preg_match('/^nb8im_search_seq_[a-f0-9]{16}_test$/D', $database) !== 1) {
        throw new RuntimeException('refusing to drop an unsafe search sequence database');
    }
    try {
        $admin->exec('DROP DATABASE IF EXISTS ' . $database);
    } catch (PDOException) {
        // pcntl children inherit open descriptors. Reconnect the administrative
        // handle before cleanup if a child exit invalidated the parent socket.
        $admin = new PDO(
            sprintf(
                'mysql:host=%s;port=%d;charset=%s',
                $baseConfig->dbHost,
                $baseConfig->dbPort,
                $baseConfig->dbCharset,
            ),
            $baseConfig->dbUser,
            $baseConfig->dbPassword,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $admin->exec('DROP DATABASE IF EXISTS ' . $database);
    }
    foreach ($previousEnvironment as $key => $value) {
        putenv($value === null ? $key : $key . '=' . $value);
    }
}
