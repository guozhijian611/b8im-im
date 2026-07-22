<?php

declare(strict_types=1);

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\GroupMemberAccessRepository;
use B8im\ImBusiness\Service\GroupMemberAccessSnapshotService;
use B8im\ImBusiness\Service\GroupMemberAccessSnapshotSession;

require dirname(__DIR__) . '/vendor/autoload.php';

final class GroupAccessRepositoryProbe implements GroupMemberAccessRepository
{
    public int $transactionCalls = 0;
    public int $lockedSnapshotReads = 0;

    /** @var list<string|null> */
    public array $snapshotReads = ['7'];
    private int $snapshotIndex = 0;

    /** @var list<array<string,mixed>> */
    public array $members = [
        [
            'conversation_id' => 'group:A',
            'access_version' => '3',
            'access_state' => 'active',
            'last_message_seq' => '10',
            'last_change_seq' => '2',
        ],
        [
            'conversation_id' => 'group:a',
            'access_version' => '4',
            'access_state' => 'history_only',
            'last_message_seq' => '20',
            'last_change_seq' => '5',
        ],
    ];

    /** @var list<array<string,mixed>> */
    public array $periods = [
        [
            'conversation_id' => 'group:A',
            'period_no' => '1',
            'visible_from_message_seq' => '1',
            'visible_until_message_seq' => null,
        ],
        [
            'conversation_id' => 'group:a',
            'period_no' => '1',
            'visible_from_message_seq' => '5',
            'visible_until_message_seq' => '12',
        ],
    ];

    public function transaction(callable $callback): mixed
    {
        ++$this->transactionCalls;

        return $callback($this);
    }

    public function fetchOne(string $sql, array $params = []): ?array
    {
        if (str_contains($sql, 'im_user_group_access_state')) {
            if (str_contains($sql, 'FOR UPDATE')) {
                ++$this->lockedSnapshotReads;
            }
            $index = min($this->snapshotIndex, count($this->snapshotReads) - 1);
            ++$this->snapshotIndex;
            $value = $this->snapshotReads[$index] ?? null;

            return $value === null ? null : ['access_snapshot_id' => $value];
        }
        if (str_contains($sql, 'SELECT cm.access_version')) {
            $conversationId = (string) ($params[1] ?? '');
            foreach ($this->members as $member) {
                if ($member['conversation_id'] === $conversationId) {
                    return $member;
                }
            }
        }

        return null;
    }

    public function fetchAll(string $sql, array $params = []): array
    {
        if (str_contains($sql, 'FROM im_conversation_member cm')) {
            $after = (string) ($params[3] ?? '');
            $rows = array_values(array_filter(
                $this->members,
                static fn (array $row): bool => strcmp((string) $row['conversation_id'], $after) > 0,
            ));
            usort($rows, static fn (array $left, array $right): int => strcmp(
                (string) $left['conversation_id'],
                (string) $right['conversation_id'],
            ));
            preg_match('/LIMIT ([0-9]+)/', $sql, $match);

            return array_slice($rows, 0, (int) ($match[1] ?? count($rows)));
        }
        if (str_contains($sql, 'FROM im_conversation_membership_period')) {
            $conversationIds = array_slice($params, 3);

            return array_values(array_filter(
                $this->periods,
                static fn (array $period): bool => in_array(
                    $period['conversation_id'],
                    $conversationIds,
                    true,
                ),
            ));
        }

        return [];
    }
}

$passed = 0;
$test = static function (string $name, callable $callback) use (&$passed): void {
    try {
        $callback();
        ++$passed;
        echo "PASS {$name}\n";
    } catch (Throwable $throwable) {
        fwrite(STDERR, "FAIL {$name}: {$throwable->getMessage()}\n");
        exit(1);
    }
};
$assert = static function (bool $condition, string $message): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
};
$expectCode = static function (string $code, callable $callback) use ($assert): void {
    try {
        $callback();
    } catch (ImException $exception) {
        $assert($exception->errorCode() === $code, 'expected ' . $code . ', got ' . $exception->errorCode());
        return;
    }
    throw new RuntimeException('expected ImException ' . $code);
};
$secret = str_repeat('cursor-secret-', 4);

$test('snapshot pages use binary order and a signed identity/epoch/limit cursor', static function () use ($assert, $expectCode, $secret): void {
    $repository = new GroupAccessRepositoryProbe();
    $service = new GroupMemberAccessSnapshotService($repository, $secret);
    $first = $service->page(7, 'user-1', null, null, 1);
    $assert($first['access_snapshot_id'] === '7', 'first page snapshot mismatch');
    $assert($first['entries'][0]['conversation_id'] === 'group:A', 'binary first row mismatch');
    $assert($first['entries'][0]['conversation_type'] === 2, 'conversation_type missing');
    $assert($first['entries'][0]['last_message_seq'] === '10', 'last_message_seq missing');
    $assert($first['has_more'] === true && is_string($first['next_cursor']), 'continuation missing');

    $second = $service->page(7, 'user-1', '7', $first['next_cursor'], 1);
    $assert($second['entries'][0]['conversation_id'] === 'group:a', 'binary continuation row mismatch');
    $assert($second['entries'][0]['access_state'] === 'history_only', 'history_only state missing');
    $assert($second['has_more'] === false && $second['next_cursor'] === null, 'terminal page mismatch');

    $tampered = substr((string) $first['next_cursor'], 0, -1) . '0';
    $expectCode(
        'ACCESS_SNAPSHOT_CURSOR_INVALID',
        static fn () => $service->page(7, 'user-1', '7', $tampered, 1),
    );
    $expectCode(
        'ACCESS_SNAPSHOT_CURSOR_INVALID',
        static fn () => $service->page(7, 'user-1', '7', $first['next_cursor'], 2),
    );
    $expectCode(
        'ACCESS_SNAPSHOT_CURSOR_INVALID',
        static fn () => $service->page(7, 'user-2', '7', $first['next_cursor'], 1),
    );
    $expectCode(
        'ACCESS_SNAPSHOT_CURSOR_INVALID',
        static fn () => $service->page(8, 'user-1', '7', $first['next_cursor'], 1),
    );
});

$test('snapshot page commit is serialized by the user access-state lock', static function () use ($assert, $expectCode, $secret): void {
    $repository = new GroupAccessRepositoryProbe();
    $service = new GroupMemberAccessSnapshotService($repository, $secret);
    $committed = false;
    $result = $service->commitPageIfCurrent(7, 'user-1', '7', static function () use (&$committed): string {
        $committed = true;

        return 'committed';
    });
    $assert($result === 'committed' && $committed, 'current snapshot did not commit the page state');
    $assert($repository->transactionCalls === 1, 'page commit did not use one transaction');
    $assert($repository->lockedSnapshotReads === 1, 'page commit did not lock user access state');

    $staleRepository = new GroupAccessRepositoryProbe();
    $staleRepository->snapshotReads = ['8'];
    $staleService = new GroupMemberAccessSnapshotService($staleRepository, $secret);
    $staleCommitted = false;
    $expectCode(
        'ACCESS_SNAPSHOT_STALE',
        static fn () => $staleService->commitPageIfCurrent(
            7,
            'user-1',
            '7',
            static function () use (&$staleCommitted): void {
                $staleCommitted = true;
            },
        ),
    );
    $assert(!$staleCommitted, 'stale snapshot executed the page commit callback');
    $assert($staleRepository->lockedSnapshotReads === 1, 'stale commit bypassed the state lock');
});

$test('snapshot and conversation reads fail closed on epoch/version changes', static function () use ($assert, $expectCode, $secret): void {
    $staleRepository = new GroupAccessRepositoryProbe();
    $staleRepository->snapshotReads = ['7', '8'];
    $staleService = new GroupMemberAccessSnapshotService($staleRepository, $secret);
    $expectCode(
        'ACCESS_SNAPSHOT_STALE',
        static fn () => $staleService->page(7, 'user-1', null, null, 2),
    );

    $missingRepository = new GroupAccessRepositoryProbe();
    $missingRepository->snapshotReads = [null];
    $missingService = new GroupMemberAccessSnapshotService($missingRepository, $secret);
    $expectCode(
        'ACCESS_STATE_NOT_INITIALIZED',
        static fn () => $missingService->currentSnapshotId(7, 'user-1'),
    );

    $repository = new GroupAccessRepositoryProbe();
    $service = new GroupMemberAccessSnapshotService($repository, $secret);
    $history = $service->assertConversationVersion(7, 'user-1', 'group:a', '4');
    $assert($history->accessState === 'history_only' && $history->periods[0]->toSeq === '12', 'history periods missing');
    $expectCode(
        'ACCESS_VERSION_STALE',
        static fn () => $service->assertConversationVersion(7, 'user-1', 'group:a', '3'),
    );
});

$test('a connection must traverse the exact snapshot page chain before SYNC pinning', static function () use ($assert, $expectCode): void {
    $session = ['user_id' => 'user-1'];
    $session = GroupMemberAccessSnapshotSession::begin($session);
    $session = GroupMemberAccessSnapshotSession::advance($session, [
        'access_snapshot_id' => '7',
        'next_cursor' => 'cursor-1',
        'has_more' => true,
    ], 50);
    GroupMemberAccessSnapshotSession::assertContinuation($session, '7', 'cursor-1', 50);
    foreach ([
        [['user_id' => 'user-1'], '7', 'cursor-1', 50],
        [$session, '8', 'cursor-1', 50],
        [$session, '7', 'cursor-from-another-chain', 50],
        [$session, '7', 'cursor-1', 100],
    ] as [$candidate, $snapshotId, $cursor, $limit]) {
        $expectCode(
            'ACCESS_SNAPSHOT_NOT_COMPLETED',
            static fn () => GroupMemberAccessSnapshotSession::assertContinuation(
                $candidate,
                $snapshotId,
                $cursor,
                $limit,
            ),
        );
    }
    $session = GroupMemberAccessSnapshotSession::advance($session, [
        'access_snapshot_id' => '7',
        'next_cursor' => null,
        'has_more' => false,
    ], 50);
    $assert(($session['access_snapshot_id'] ?? null) === '7', 'terminal page did not pin the completed snapshot');
    $assert(!array_key_exists(GroupMemberAccessSnapshotSession::STAGING_KEY, $session), 'terminal page retained staging state');

    $aborted = GroupMemberAccessSnapshotSession::abort($session);
    $assert(!array_key_exists('access_snapshot_id', $aborted), 'snapshot error retained a completed pin');
    $assert(!array_key_exists(GroupMemberAccessSnapshotSession::STAGING_KEY, $aborted), 'snapshot error retained staging');

    $stale = GroupMemberAccessSnapshotSession::invalidate($session, '8');
    $assert(!array_key_exists('access_snapshot_id', $stale), 'new epoch retained an old completed pin');
    $same = GroupMemberAccessSnapshotSession::invalidate($session, '7');
    $assert(($same['access_snapshot_id'] ?? null) === '7', 'same epoch invalidated a completed pin');
});

$test('history-only SYNC uses periods while writes and realtime remain active-only', static function () use ($assert): void {
    $messageSource = (string) file_get_contents(dirname(__DIR__) . '/src/Service/MessageService.php');
    $recipientSource = (string) file_get_contents(dirname(__DIR__) . '/src/Realtime/DatabaseRealtimeRecipientProvider.php');
    $snapshotSource = (string) file_get_contents(dirname(__DIR__) . '/src/Service/GroupMemberAccessSnapshotService.php');
    $assert(str_contains($messageSource, 'private function assertVisibleMember'), 'visible-member boundary missing');
    $assert(str_contains($messageSource, 'FROM im_conversation_membership_period'), 'SYNC no longer uses periods');
    $assert(str_contains($messageSource, 'c.conversation_type <> 2 OR cm.access_state'), 'group write boundary is not active-only');
    $assert(str_contains($recipientSource, 'c.conversation_type <> 2 OR cm.access_state'), 'group realtime boundary is not active-only');
    $assert(str_contains($snapshotSource, 'ORDER BY cm.conversation_id COLLATE utf8mb4_bin ASC'), 'snapshot order is not binary');
    $assert(str_contains($snapshotSource, 'ORDER BY conversation_id COLLATE utf8mb4_bin ASC, period_no ASC'), 'period batch order is not binary');
});

$test('RabbitMQ topology and gateway handlers expose the group access contract', static function () use ($assert): void {
    $publisherSource = (string) file_get_contents(dirname(__DIR__) . '/src/Queue/RabbitMqPublisher.php');
    $consumerSource = (string) file_get_contents(dirname(__DIR__) . '/src/Queue/RabbitMqRealtimeConsumer.php');
    $eventsSource = (string) file_get_contents(dirname(__DIR__) . '/src/Events.php');
    $assert(
        str_contains(
            $publisherSource,
            '$channel->queue_bind(Constants::MQ_MESSAGE_AFTER, $exchange, Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED);',
        ),
        'publisher topology does not bind group access routing to message.after',
    );
    $assert(
        str_contains($consumerSource, 'Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,'),
        'realtime consumer topology does not bind group access routing to message.after',
    );
    $assert(str_contains($eventsSource, 'Command::GROUP_MEMBER_ACCESS_SNAPSHOT'), 'snapshot command handler missing');
    $assert(str_contains($eventsSource, 'Command::GROUP_MEMBER_ACCESS_SNAPSHOT_ACK'), 'snapshot ACK missing');
    $assert(str_contains($eventsSource, 'GroupMemberAccessSnapshotSession::assertContinuation'), 'snapshot page chain is not connection-bound');
    $assert(str_contains($eventsSource, 'GroupMemberAccessSnapshotSession::advance'), 'terminal snapshot does not pin the session');
    $assert(str_contains($eventsSource, 'commitPageIfCurrent('), 'snapshot session commit is not serialized with access events');
    $assert(str_contains($eventsSource, 'assertConnectionAccessSnapshot('), 'SYNC session pin check missing');
});

$test('migration preflights group-only baselines and safely recovers partial DDL', static function () use ($assert): void {
    $migrationSource = (string) file_get_contents(
        dirname(__DIR__, 2) . '/database/migrations/20260720020000_add_group_member_access_snapshots.php',
    );
    foreach ([
        'CREATE TABLE im_user_group_access_state',
        'CREATE TABLE im_group_member_access_audit',
        'ADD COLUMN access_state',
        'period_no = 0',
        'visible_from_message_seq = 0',
        'refused overlapping membership periods',
        'refused reverse-ordered membership periods',
        'refused multiple open membership periods',
        'refused member/open-period state mismatch',
        'FROM im_user user_state_backing',
        'user_state_backing.organization = 0',
        "LOCATE('|', user_state_backing.user_id) > 0",
        "LOCATE('|', member.conversation_id) > 0",
        "LOCATE('|', member.user_id) > 0",
        "LOCATE('|', period.conversation_id) > 0",
        "LOCATE('|', period.user_id) > 0",
        'invalid user identity before state backfill',
        'member.member_organization <> member.organization',
        'member.access_version = 0',
        'LEFT JOIN im_user user_backing',
        'OR user_backing.id IS NULL',
        "if (\$schemaState === 'complete')",
        "if (\$schemaState === 'partial')",
        'hasGroupAccessOutboxEvents',
        'refused recovery while group.member_access_changed outbox rows exist',
        'deleteGroupAccessOutboxEvents',
        'cleanupSchemaArtifacts',
        'DROP TABLE im_group_member_access_audit',
        'DROP TABLE im_user_group_access_state',
        'DROP COLUMN access_state',
    ] as $requiredFragment) {
        $assert(str_contains($migrationSource, $requiredFragment), 'migration contract missing: ' . $requiredFragment);
    }
    $pdoPreflight = strpos($migrationSource, '$connection = $this->requirePdoConnection()');
    $baselinePreflight = strpos($migrationSource, '$this->assertGroupBaselineIsCanonical($connection)');
    $firstCreate = strpos($migrationSource, 'CREATE TABLE im_user_group_access_state');
    $assert(
        is_int($pdoPreflight) && is_int($baselinePreflight) && is_int($firstCreate)
        && $pdoPreflight < $firstCreate && $baselinePreflight < $firstCreate,
        'PDO and baseline preflights must finish before the first DDL',
    );
    $assert(
        substr_count($migrationSource, 'conversation.conversation_type = 2') >= 8,
        'single conversations can enter a group access baseline query',
    );
    $periodIdentityStart = strpos($migrationSource, '$invalidPeriodIdentity =');
    $periodScalarStart = strpos($migrationSource, '$invalidScalar =');
    $periodOverlapStart = strpos($migrationSource, '$overlap =');
    $assert(
        is_int($periodIdentityStart) && is_int($periodScalarStart) && is_int($periodOverlapStart)
        && $periodIdentityStart < $periodScalarStart && $periodScalarStart < $periodOverlapStart,
        'group period preflight sections are incomplete',
    );
    $allStatusPeriodPreflight = substr(
        $migrationSource,
        $periodIdentityStart,
        $periodOverlapStart - $periodIdentityStart,
    );
    $assert(
        !str_contains($allStatusPeriodPreflight, 'period.status = 1'),
        'revoked group periods bypass identity or scalar preflight',
    );
    $assert(
        str_contains(
            $migrationSource,
            "UPDATE im_conversation_member cm\nINNER JOIN im_conversation conversation",
        ),
        'single conversation access_state can be rewritten by the group migration',
    );
    $assert(!str_contains($migrationSource, 'GROUP_CONCAT'), 'audit backfill may truncate periods');
});

echo "{$passed} group member access tests passed.\n";
