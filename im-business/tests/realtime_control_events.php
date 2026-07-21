<?php

declare(strict_types=1);

use B8im\ImBusiness\Service\RealtimeEventConsumer;
use B8im\ImBusiness\Service\RealtimeEventClaim;
use B8im\ImBusiness\Service\RealtimeEventGatewayInterface;
use B8im\ImBusiness\Service\RealtimeEventStoreInterface;
use B8im\ImBusiness\Service\FriendRequestRealtimeAuthorizerInterface;
use B8im\ImBusiness\Service\FriendRequestRealtimeEvent;
use B8im\ImBusiness\Service\RedisRealtimeEventStore;
use B8im\ImBusiness\Queue\RedisRealtimeControlPublisher;

require dirname(__DIR__) . '/vendor/autoload.php';

$tests = [];

function controlTest(string $name, callable $callback): void
{
    global $tests;
    $tests[$name] = $callback;
}

function controlAssert(bool $condition, string $message = 'assertion failed'): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

final class ControlFriendRequestAuthorizer implements FriendRequestRealtimeAuthorizerInterface
{
    /** @var list<array<string,int|string|null>> */
    public array $calls = [];

    public function __construct(public bool $deliver)
    {
    }

    public function withCurrentEvent(FriendRequestRealtimeEvent $event, callable $delivery): void
    {
        $this->calls[] = [
            'event' => $event->event,
            'event_organization' => $event->targetOrganization,
            'request_id' => $event->requestId,
            'from_organization' => $event->fromOrganization,
            'from_user_id' => $event->fromUserId,
            'to_organization' => $event->toOrganization,
            'to_user_id' => $event->toUserId,
            'cross_org_access_snapshot_id' => $event->crossOrgAccessSnapshotId,
        ];
        if ($this->deliver) {
            $delivery();
        }
    }
}

function friendEnvelope(
    string $transition,
    int $requestId,
    int $fromOrganization,
    string $fromUserId,
    int $toOrganization,
    string $toUserId,
    ?string $snapshotId,
): string {
    $handled = $transition !== 'created';
    $targetOrganization = $handled ? $fromOrganization : $toOrganization;
    $targetUserId = $handled ? $fromUserId : $toUserId;
    $actorOrganization = $handled ? $toOrganization : $fromOrganization;
    $actorUserId = $handled ? $toUserId : $fromUserId;
    $eventType = 'friend_request.' . $transition;
    $eventId = hash('sha256', json_encode([
        'friend_request.v1', $requestId, $eventType,
        (string) $fromOrganization, $fromUserId,
        (string) $toOrganization, $toUserId,
        (string) $targetOrganization, $targetUserId, $snapshotId,
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));

    return json_encode([
        'event_id' => $eventId,
        'type' => 'friend_request',
        'organization' => (string) $targetOrganization,
        'data' => [
            'event' => $transition,
            'request_id' => $requestId,
            'status' => ['created' => 1, 'accepted' => 2, 'rejected' => 3][$transition],
            'from_organization' => (string) $fromOrganization,
            'from_user_id' => $fromUserId,
            'to_organization' => (string) $toOrganization,
            'to_user_id' => $toUserId,
            'target_organization' => (string) $targetOrganization,
            'target_user_id' => $targetUserId,
            'actor_organization' => (string) $actorOrganization,
            'actor_user_id' => $actorUserId,
            'cross_org_access_snapshot_id' => $snapshotId,
            'create_time' => '2026-07-21 14:00:00',
            'handle_time' => $handled ? '2026-07-21 14:01:00' : null,
        ],
    ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
}

/** @return array<string, int|string> */
function controlSession(
    int|string $organization,
    string $userId,
    string $deviceId,
    string $clientId,
    string $credentialSessionId,
    string $connectionSessionId,
): array {
    return [
        'organization' => $organization,
        'user_id' => $userId,
        'device_id' => $deviceId,
        'client_id' => $clientId,
        'credential_session_id' => $credentialSessionId,
        'session_id' => $connectionSessionId,
    ];
}

/** @param array<string, mixed> $data */
function controlEnvelope(string $type, int|string $organization, array $data): string
{
    $identity = [
        'type' => $type,
        'organization' => $organization,
        'data' => $data,
    ];

    return json_encode([
        'event_id' => hash('sha256', json_encode($identity, JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE)),
        ...$identity,
    ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE);
}

final class ControlTestStore implements RealtimeEventStoreInterface
{
    /** @var list<string> */
    public array $queue = [];

    /** @var list<array{0: int, 1: list<string>}> */
    public array $invalidations = [];

    /** @var list<array{0: int, 1: bool}> */
    public array $organizationMarkers = [];

    /** @var array<string, RealtimeEventClaim> */
    public array $processing = [];

    /** @var array<string, string> */
    public array $inflight = [];

    /** @var array<string, true> */
    public array $done = [];

    /** @var array<string, true> */
    public array $expired = [];

    /** @var list<string> */
    public array $acked = [];

    /** @var list<string> */
    public array $requeued = [];

    public int $recovered = 0;

    private int $claimSequence = 0;

    public function claim(string $workerId): ?RealtimeEventClaim
    {
        while (($raw = array_shift($this->queue)) !== null) {
            $eventId = null;
            try {
                $event = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
                $candidate = is_array($event) ? ($event['event_id'] ?? null) : null;
                if (is_string($candidate) && preg_match('/^[a-f0-9]{64}$/', $candidate) === 1) {
                    $eventId = $candidate;
                }
            } catch (Throwable) {
                // Malformed events are still claimed so the consumer can explicitly ack/discard them.
            }

            if ($eventId !== null && (isset($this->done[$eventId]) || isset($this->inflight[$eventId]))) {
                continue;
            }

            $token = sha1('control-claim-' . (++$this->claimSequence));
            $claim = new RealtimeEventClaim($token, $workerId, $raw, $eventId);
            $this->processing[$token] = $claim;
            if ($eventId !== null) {
                $this->inflight[$eventId] = $token;
            }

            return $claim;
        }

        return null;
    }

    public function ack(RealtimeEventClaim $claim): void
    {
        if (!isset($this->processing[$claim->claimToken])) {
            return;
        }
        unset($this->processing[$claim->claimToken], $this->expired[$claim->claimToken]);
        if ($claim->eventId !== null) {
            unset($this->inflight[$claim->eventId]);
            $this->done[$claim->eventId] = true;
        }
        $this->acked[] = $claim->raw;
    }

    public function requeue(RealtimeEventClaim $claim): void
    {
        if (!isset($this->processing[$claim->claimToken])) {
            return;
        }
        unset($this->processing[$claim->claimToken], $this->expired[$claim->claimToken]);
        if ($claim->eventId !== null) {
            unset($this->inflight[$claim->eventId]);
        }
        $this->queue[] = $claim->raw;
        $this->requeued[] = $claim->raw;
    }

    public function recoverExpired(int $limit = 100): int
    {
        $recovered = 0;
        foreach (array_keys($this->expired) as $claimToken) {
            if ($recovered >= $limit) {
                break;
            }
            $claim = $this->processing[$claimToken] ?? null;
            if (!$claim instanceof RealtimeEventClaim) {
                unset($this->expired[$claimToken]);
                continue;
            }
            unset($this->processing[$claimToken], $this->expired[$claimToken]);
            if ($claim->eventId !== null) {
                unset($this->inflight[$claim->eventId]);
            }
            $this->queue[] = $claim->raw;
            ++$recovered;
        }
        $this->recovered += $recovered;

        return $recovered;
    }

    public function expire(RealtimeEventClaim $claim): void
    {
        $this->expired[$claim->claimToken] = true;
    }

    public function invalidateCredentialSessions(int $organization, array $credentialSessionIds): void
    {
        $this->invalidations[] = [$organization, array_values($credentialSessionIds)];
    }

    public function setOrganizationInactive(int $organization, bool $inactive): void
    {
        $this->organizationMarkers[] = [$organization, $inactive];
    }
}

final class ControlRedisHandler
{
    /** @var list<array{0: string, 1: int, 2: string}> */
    public array $expiring = [];

    /** @var list<string> */
    public array $deleted = [];

    public function setex(string $key, int $ttl, string $value): bool
    {
        $this->expiring[] = [$key, $ttl, $value];

        return true;
    }

    public function del(string $key): int
    {
        $this->deleted[] = $key;

        return 1;
    }
}

final class ControlTestGateway implements RealtimeEventGatewayInterface
{
    /** @var array<string, array<string, array<string, mixed>>> */
    public array $userSessions = [];

    /** @var array<string, array<string, mixed>> */
    public array $organizationSessions = [];

    /** @var list<array{0: int, 1: string}> */
    public array $sessionQueries = [];

    /** @var list<string> */
    public array $closed = [];

    /** @var list<array{0: int, 1: string, 2: array<string, mixed>}> */
    public array $sent = [];

    public int $allSessionQueries = 0;

    /** @var array<string, int> */
    public array $closeFailuresRemaining = [];

    public function sendToUser(int $organization, string $userId, string $packet): void
    {
        $decoded = json_decode($packet, true, flags: JSON_THROW_ON_ERROR);
        $this->sent[] = [$organization, $userId, is_array($decoded) ? $decoded : []];
    }

    public function sessionsForUser(int $organization, string $userId): array
    {
        $this->sessionQueries[] = [$organization, $userId];

        return $this->userSessions[$organization . ':' . $userId] ?? [];
    }

    public function allSessions(): array
    {
        ++$this->allSessionQueries;

        return $this->organizationSessions;
    }

    public function close(string $clientId): void
    {
        if (($this->closeFailuresRemaining[$clientId] ?? 0) > 0) {
            --$this->closeFailuresRemaining[$clientId];
            throw new RuntimeException('synthetic Gateway close failure for ' . $clientId);
        }
        $this->closed[] = $clientId;
        unset($this->organizationSessions[$clientId]);
        foreach ($this->userSessions as &$sessions) {
            unset($sessions[$clientId]);
        }
        unset($sessions);
    }
}

controlTest('session_revoked invalidates and closes only the exact scoped connection', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue[] = controlEnvelope('auth.session_revoked', 7, [
        'user_id' => 'user-7',
        'device_id' => 'device-7',
        'client_id' => 'client-exact',
        'credential_session_ids' => ['credential-exact'],
        'occurred_at' => '2026-07-10 14:00:00',
    ]);
    $gateway->userSessions['7:user-7'] = [
        'client-exact' => controlSession(7, 'user-7', 'device-7', 'client-exact', 'credential-exact', str_repeat('a', 32)),
        'client-other-credential' => controlSession(7, 'user-7', 'device-7', 'client-other-credential', 'credential-other', str_repeat('b', 32)),
        'client-other-device' => controlSession(7, 'user-7', 'device-other', 'client-other-device', 'credential-exact', str_repeat('c', 32)),
        'client-other-organization' => controlSession(8, 'user-7', 'device-7', 'client-other-organization', 'credential-exact', str_repeat('d', 32)),
        'client-field-mismatch' => controlSession(7, 'user-7', 'device-7', 'different-client-field', 'credential-exact', str_repeat('e', 32)),
    ];

    (new RealtimeEventConsumer($store, $gateway))->consume();

    controlAssert($store->invalidations === [[7, ['credential-exact']]]);
    controlAssert($gateway->sessionQueries === [[7, 'user-7']]);
    controlAssert($gateway->closed === ['client-exact']);
});

controlTest('device_disabled closes only matching device credential sessions in one organization', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue[] = controlEnvelope('auth.device_disabled', 7, [
        'user_id' => 'user-7',
        'device_id' => 'device-7',
        'client_id' => null,
        'connection_session_id' => null,
        'credential_session_ids' => ['credential-1', 'credential-2'],
    ]);
    $gateway->userSessions['7:user-7'] = [
        'client-1' => controlSession(7, 'user-7', 'device-7', 'client-1', 'credential-1', str_repeat('1', 32)),
        'client-2' => controlSession(7, 'user-7', 'device-7', 'client-2', 'credential-2', str_repeat('2', 32)),
        'client-new-session' => controlSession(7, 'user-7', 'device-7', 'client-new-session', 'credential-new', str_repeat('3', 32)),
        'client-other-device' => controlSession(7, 'user-7', 'device-8', 'client-other-device', 'credential-1', str_repeat('4', 32)),
        'client-other-org' => controlSession(8, 'user-7', 'device-7', 'client-other-org', 'credential-1', str_repeat('5', 32)),
    ];

    (new RealtimeEventConsumer($store, $gateway))->consume();

    controlAssert($store->invalidations === [[7, ['credential-1', 'credential-2']]]);
    controlAssert($gateway->closed === ['client-1', 'client-2']);
});

controlTest('device disable closes every revoked credential on the device despite one representative client', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $connectionSession = str_repeat('a', 32);
    $store->queue[] = controlEnvelope('auth.device_disabled', 7, [
        'user_id' => 'user-7',
        'device_id' => 'device-7',
        'client_id' => 'client-1',
        'connection_session_id' => $connectionSession,
        'credential_session_ids' => ['credential-1', 'credential-2'],
    ]);
    $gateway->userSessions['7:user-7'] = [
        'client-1' => controlSession(7, 'user-7', 'device-7', 'client-1', 'credential-1', $connectionSession),
        'client-2' => controlSession(7, 'user-7', 'device-7', 'client-2', 'credential-2', str_repeat('b', 32)),
    ];

    (new RealtimeEventConsumer($store, $gateway))->consume();

    controlAssert($gateway->closed === ['client-1', 'client-2']);
});

controlTest('65 to 100 byte device ids dispatch revoke and disable events', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $device65 = str_repeat('d', 65);
    $device100 = str_repeat('e', 100);
    $store->queue[] = controlEnvelope('auth.session_revoked', 7, [
        'user_id' => 'user-65',
        'device_id' => $device65,
        'client_id' => 'client-65',
        'credential_session_ids' => ['credential-65'],
    ]);
    $store->queue[] = controlEnvelope('auth.device_disabled', 7, [
        'user_id' => 'user-100',
        'device_id' => $device100,
        'client_id' => null,
        'connection_session_id' => null,
        'credential_session_ids' => ['credential-100'],
    ]);
    $gateway->userSessions['7:user-65'] = [
        'client-65' => controlSession(7, 'user-65', $device65, 'client-65', 'credential-65', str_repeat('6', 32)),
    ];
    $gateway->userSessions['7:user-100'] = [
        'client-100' => controlSession(7, 'user-100', $device100, 'client-100', 'credential-100', str_repeat('a', 32)),
    ];

    (new RealtimeEventConsumer($store, $gateway))->consume();

    controlAssert($store->invalidations === [
        [7, ['credential-65']],
        [7, ['credential-100']],
    ]);
    controlAssert($gateway->sessionQueries === [[7, 'user-65'], [7, 'user-100']]);
    controlAssert($gateway->closed === ['client-65', 'client-100']);
});

controlTest('organization_disabled marks first and closes only canonical sessions in that organization', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue[] = controlEnvelope('auth.organization_disabled', 7, [
        'occurred_at' => '2026-07-10 14:01:00',
        'reason' => 'operator_disabled',
    ]);
    $gateway->organizationSessions = [
        'org7-client-1' => controlSession(7, 'user-1', 'device-1', 'org7-client-1', 'credential-1', str_repeat('1', 32)),
        'org7-client-2' => controlSession(7, 'user-2', 'device-2', 'org7-client-2', 'credential-2', str_repeat('2', 32)),
        'org8-client' => controlSession(8, 'user-1', 'device-1', 'org8-client', 'credential-8', str_repeat('8', 32)),
        'string-org-client' => controlSession('7', 'user-3', 'device-3', 'string-org-client', 'credential-3', str_repeat('3', 32)),
        'client-key-mismatch' => controlSession(7, 'user-4', 'device-4', 'different-client', 'credential-4', str_repeat('4', 32)),
        'malformed-session' => ['organization' => 7, 'client_id' => 'malformed-session'],
    ];

    (new RealtimeEventConsumer($store, $gateway))->consume();

    controlAssert($store->organizationMarkers === [[7, true]]);
    controlAssert($store->invalidations === [[7, ['credential-1', 'credential-2']]]);
    controlAssert($gateway->closed === ['org7-client-1', 'org7-client-2']);
    controlAssert($gateway->allSessionQueries === 1);
});

controlTest('organization_enabled only clears its own marker', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue[] = controlEnvelope('auth.organization_enabled', 9, ['occurred_at' => '2026-07-10 14:02:00']);

    (new RealtimeEventConsumer($store, $gateway))->consume();

    controlAssert($store->organizationMarkers === [[9, false]]);
    controlAssert($store->invalidations === [] && $gateway->closed === []);
    controlAssert($gateway->allSessionQueries === 0);
});

controlTest('tenant policy change invalidates policy and reconnects only canonical organization sessions', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $invalidatedPolicies = [];
    $store->queue[] = controlEnvelope('tenant.policy.changed', 7, ['version' => 3, 'actor' => ['type' => 'tenant']]);
    $gateway->organizationSessions = [
        'org7-client' => controlSession(7, 'user-7', 'device-7', 'org7-client', 'credential-7', str_repeat('7', 32)),
        'org8-client' => controlSession(8, 'user-8', 'device-8', 'org8-client', 'credential-8', str_repeat('8', 32)),
        'malformed-client' => controlSession('7', 'user-x', 'device-x', 'malformed-client', 'credential-x', str_repeat('9', 32)),
    ];

    (new RealtimeEventConsumer(
        $store,
        $gateway,
        static function (int $organization) use (&$invalidatedPolicies): void {
            $invalidatedPolicies[] = $organization;
        },
    ))->consume();

    controlAssert($invalidatedPolicies === [7]);
    controlAssert($store->invalidations === [[7, ['credential-7']]]);
    controlAssert($gateway->closed === ['org7-client']);
});

controlTest('dispatch failure requeues the claim and a later consume retries it', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $raw = controlEnvelope('auth.session_revoked', 7, [
        'user_id' => 'user-7',
        'device_id' => 'device-7',
        'client_id' => 'client-retry',
        'credential_session_ids' => ['credential-retry'],
    ]);
    $store->queue[] = $raw;
    $gateway->userSessions['7:user-7'] = [
        'client-retry' => controlSession(
            7,
            'user-7',
            'device-7',
            'client-retry',
            'credential-retry',
            str_repeat('a', 32),
        ),
    ];
    $gateway->closeFailuresRemaining['client-retry'] = 1;
    $consumer = new RealtimeEventConsumer($store, $gateway, workerId: 'worker-retry');

    $failed = false;
    try {
        $consumer->consume();
    } catch (RuntimeException $exception) {
        $failed = str_contains($exception->getMessage(), 'synthetic Gateway close failure');
    }
    controlAssert($failed, 'the first dispatch must expose the Gateway exception');
    controlAssert($store->queue === [$raw] && $store->requeued === [$raw], 'failed claim must be requeued');
    controlAssert($store->acked === [], 'failed dispatch must not be acknowledged');

    $consumer->consume();

    controlAssert($gateway->closed === ['client-retry']);
    controlAssert(count($store->acked) === 1 && count($store->done) === 1);
});

controlTest('poison event is requeued at the tail and does not block the next control event', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $poison = controlEnvelope('auth.session_revoked', 7, [
        'user_id' => 'user-7',
        'device_id' => 'device-7',
        'client_id' => 'client-poison',
        'credential_session_ids' => ['credential-poison'],
    ]);
    $success = controlEnvelope('auth.organization_enabled', 9, [
        'occurred_at' => '2026-07-10 14:04:00',
    ]);
    $store->queue = [$poison, $success];
    $gateway->userSessions['7:user-7'] = [
        'client-poison' => controlSession(7, 'user-7', 'device-7', 'client-poison', 'credential-poison', str_repeat('d', 32)),
    ];
    $gateway->closeFailuresRemaining['client-poison'] = 100;
    $consumer = new RealtimeEventConsumer($store, $gateway, workerId: 'worker-poison');

    try {
        $consumer->consume();
        throw new RuntimeException('expected poison dispatch to fail');
    } catch (RuntimeException $exception) {
        controlAssert(str_contains($exception->getMessage(), 'synthetic Gateway close failure'));
    }
    controlAssert($store->queue === [$success, $poison], 'failed claim must move to the queue tail');

    $consumer->consume(1);

    controlAssert($store->organizationMarkers === [[9, false]], 'later control event must make progress');
    controlAssert($store->queue === [$poison], 'poison event must remain available for a later retry');
});

controlTest('partial Gateway close failure retries only the still-live organization session', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue[] = controlEnvelope('auth.organization_disabled', 7, [
        'occurred_at' => '2026-07-10 14:05:00',
        'reason' => 'operator_disabled',
    ]);
    $gateway->organizationSessions = [
        'client-closed-first' => controlSession(7, 'user-1', 'device-1', 'client-closed-first', 'credential-1', str_repeat('1', 32)),
        'client-fails-once' => controlSession(7, 'user-2', 'device-2', 'client-fails-once', 'credential-2', str_repeat('2', 32)),
    ];
    $gateway->closeFailuresRemaining['client-fails-once'] = 1;
    $consumer = new RealtimeEventConsumer($store, $gateway, workerId: 'worker-partial');

    try {
        $consumer->consume();
        throw new RuntimeException('expected the first partial close to fail');
    } catch (RuntimeException $exception) {
        controlAssert(str_contains($exception->getMessage(), 'synthetic Gateway close failure'));
    }

    $consumer->consume();

    controlAssert($gateway->closed === ['client-closed-first', 'client-fails-once']);
    controlAssert($store->organizationMarkers === [[7, true], [7, true]], 'retry must repeat the fail-closed marker');
    controlAssert($store->invalidations === [
        [7, ['credential-1', 'credential-2']],
        [7, ['credential-2']],
    ]);
    controlAssert(count($store->acked) === 1 && count($store->requeued) === 1);
});

controlTest('expired crash claim is recovered and delivered by another worker', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue[] = controlEnvelope('auth.session_revoked', 7, [
        'user_id' => 'user-7',
        'device_id' => 'device-7',
        'client_id' => 'client-recovered',
        'credential_session_ids' => ['credential-recovered'],
    ]);
    $gateway->userSessions['7:user-7'] = [
        'client-recovered' => controlSession(
            7,
            'user-7',
            'device-7',
            'client-recovered',
            'credential-recovered',
            str_repeat('b', 32),
        ),
    ];
    $crashedClaim = $store->claim('worker-crashed');
    controlAssert($crashedClaim instanceof RealtimeEventClaim);
    $store->expire($crashedClaim);

    (new RealtimeEventConsumer($store, $gateway, workerId: 'worker-recovery'))->consume();

    controlAssert($store->recovered === 1 && $store->processing === []);
    controlAssert($gateway->closed === ['client-recovered']);
    controlAssert(count($store->acked) === 1);
});

controlTest('duplicate event id is dispatched exactly once across workers', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $raw = controlEnvelope('auth.session_revoked', 7, [
        'user_id' => 'user-7',
        'device_id' => 'device-7',
        'client_id' => 'client-once',
        'credential_session_ids' => ['credential-once'],
    ]);
    $store->queue = [$raw, $raw];
    $gateway->userSessions['7:user-7'] = [
        'client-once' => controlSession(7, 'user-7', 'device-7', 'client-once', 'credential-once', str_repeat('c', 32)),
    ];

    (new RealtimeEventConsumer($store, $gateway, workerId: 'worker-dedup-a'))->consume(1);
    (new RealtimeEventConsumer($store, $gateway, workerId: 'worker-dedup-b'))->consume(2);

    controlAssert($gateway->closed === ['client-once']);
    controlAssert($store->invalidations === [[7, ['credential-once']]]);
    controlAssert(count($store->acked) === 1 && count($store->done) === 1);
});

controlTest('malformed and legacy auth events are dropped without cross-tenant effects', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue = [
        '{bad-json',
        controlEnvelope('auth.session_revoked', '7', [
            'user_id' => 'user-7',
            'device_id' => 'device-7',
            'credential_session_ids' => ['credential-7'],
        ]),
        json_encode(['type' => 'auth.session_revoked', 'organization' => 7, 'data' => ['not-an-object']], JSON_THROW_ON_ERROR),
        controlEnvelope('auth.session_revoked', 7, [
            'user_id' => 'user-7',
            'device_id' => 'device-7',
            'credential_session_ids' => [],
        ]),
        controlEnvelope('auth.device_disabled', 7, [
            'organization' => 8,
            'user_id' => 'user-7',
            'device_id' => 'device-7',
            'credential_session_ids' => ['credential-7'],
        ]),
        controlEnvelope('auth.organization_disabled', 7, [
            'user_id' => 'user-7',
            'device_id' => 'device-7',
            'credential_session_ids' => ['credential-7'],
        ]),
        controlEnvelope('auth.session_revoked.v0', 7, [
            'user_id' => 'user-7',
            'device_id' => 'device-7',
            'credential_session_ids' => ['credential-7'],
        ]),
        controlEnvelope('auth.device_disabled', 7, [
            'user_id' => 'user-7',
            'device_id' => str_repeat('d', 101),
            'credential_session_ids' => ['credential-7'],
        ]),
    ];

    (new RealtimeEventConsumer($store, $gateway))->consume(20);

    controlAssert($store->invalidations === [] && $store->organizationMarkers === []);
    controlAssert($gateway->closed === [] && $gateway->sessionQueries === [] && $gateway->allSessionQueries === 0);
    controlAssert(count($store->acked) === 8, 'malformed and legacy events must be explicitly acknowledged');
    controlAssert($store->processing === [] && $store->queue === []);
});

controlTest('created accepted and rejected use composite target identity', static function (): void {
    foreach ([
        ['created', 8, 'to-user', 'created'],
        ['accepted', 7, 'from-user', 'accepted'],
        ['rejected', 7, 'from-user', 'rejected'],
    ] as [$transition, $targetOrganization, $targetUserId, $packetEvent]) {
        $store = new ControlTestStore();
        $gateway = new ControlTestGateway();
        $authorizer = new ControlFriendRequestAuthorizer(true);
        $store->queue[] = friendEnvelope(
            $transition,
            88,
            7,
            'from-user',
            8,
            'to-user',
            '42',
        );

        (new RealtimeEventConsumer(
            $store,
            $gateway,
            friendRequestAuthorizer: $authorizer,
        ))->consume();

        controlAssert(count($authorizer->calls) === 1);
        controlAssert(count($gateway->sent) === 1);
        controlAssert($gateway->sent[0][0] === $targetOrganization);
        controlAssert($gateway->sent[0][1] === $targetUserId);
        controlAssert($gateway->sent[0][2]['organization'] === $targetOrganization);
        controlAssert($gateway->sent[0][2]['data']['event'] === $packetEvent);
        controlAssert(preg_match('/^[a-f0-9]{64}$/D', $gateway->sent[0][2]['data']['event_id']) === 1);
        controlAssert(count($store->acked) === 1 && $store->requeued === []);
    }
});

controlTest('same organization requires null snapshot and current database authority', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $authorizer = new ControlFriendRequestAuthorizer(true);
    $store->queue[] = friendEnvelope('created', 89, 7, 'from-user', 7, 'to-user', null);

    (new RealtimeEventConsumer(
        $store,
        $gateway,
        friendRequestAuthorizer: $authorizer,
    ))->consume();

    controlAssert(count($authorizer->calls) === 1);
    controlAssert($authorizer->calls[0]['cross_org_access_snapshot_id'] === null);
    controlAssert($gateway->sent[0][0] === 7 && $gateway->sent[0][1] === 'to-user');
    controlAssert(array_key_exists('cross_org_access_snapshot_id', $gateway->sent[0][2]['data']));
    controlAssert($gateway->sent[0][2]['data']['cross_org_access_snapshot_id'] === null);
});

controlTest('stale authoritative friend event is acknowledged without delivery or retry', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $authorizer = new ControlFriendRequestAuthorizer(false);
    $store->queue[] = friendEnvelope('accepted', 90, 7, 'from-user', 8, 'to-user', '43');

    (new RealtimeEventConsumer(
        $store,
        $gateway,
        friendRequestAuthorizer: $authorizer,
    ))->consume();

    controlAssert(count($authorizer->calls) === 1);
    controlAssert($gateway->sent === []);
    controlAssert(count($store->acked) === 1 && $store->requeued === [] && $store->processing === []);
});

controlTest('hostile friend envelopes fail closed before database authorization', static function (): void {
    $valid = json_decode(
        friendEnvelope('created', 91, 7, 'from-user', 8, 'to-user', '44'),
        true,
        flags: JSON_THROW_ON_ERROR,
    );
    $hostile = [
        'wrong envelope home' => [static function (array $event): array {
            $event['organization'] = '7';
            return $event;
        }, 'home or time'],
        'wrong created target' => [static function (array $event): array {
            $event['data']['target_user_id'] = 'from-user';
            return $event;
        }, 'target or actor'],
        'missing cross-org snapshot' => [static function (array $event): array {
            $event['data']['cross_org_access_snapshot_id'] = null;
            return $event;
        }, 'no access snapshot'],
        'extra data key' => [static function (array $event): array {
            $event['data']['extra'] = true;
            return $event;
        }, 'shape is invalid'],
        'forged event id' => [static function (array $event): array {
            $event['event_id'] = str_repeat('0', 64);
            return $event;
        }, 'canonical tuple'],
        'unsafe request id' => [static function (array $event): array {
            $event['data']['request_id'] = 9007199254740992;
            return $event;
        }, 'transition is invalid'],
        'overflow snapshot id' => [static function (array $event): array {
            $event['data']['cross_org_access_snapshot_id'] = '18446744073709551616';
            return $event;
        }, 'no access snapshot'],
        'pre-MySQL datetime year' => [static function (array $event): array {
            $event['data']['create_time'] = '0000-01-01 00:00:00';
            return $event;
        }, 'home or time'],
        'invalid calendar datetime' => [static function (array $event): array {
            $event['data']['create_time'] = '2026-02-30 00:00:00';
            return $event;
        }, 'home or time'],
        'same composite identity' => [static function (array $event): array {
            $event['organization'] = '7';
            $event['data']['to_organization'] = '7';
            $event['data']['to_user_id'] = 'from-user';
            $event['data']['target_organization'] = '7';
            $event['data']['target_user_id'] = 'from-user';
            $event['data']['cross_org_access_snapshot_id'] = null;
            return $event;
        }, 'identities must be distinct'],
    ];

    foreach ($hostile as $name => [$mutate, $expectedError]) {
        $raw = json_encode($mutate($valid), JSON_THROW_ON_ERROR);
        try {
            FriendRequestRealtimeEvent::fromRaw($raw);
            throw new RuntimeException($name . ' unexpectedly passed the strict friend parser');
        } catch (InvalidArgumentException $error) {
            controlAssert(
                str_contains($error->getMessage(), $expectedError),
                $name . ' failed at an unexpected parser boundary: ' . $error->getMessage(),
            );
        }

        // Each hostile vector gets an isolated store. Reusing one store would
        // let the first ACK put the shared event_id in the done set and hide
        // later vectors behind Redis deduplication.
        $store = new ControlTestStore();
        $store->queue[] = $raw;
        $gateway = new ControlTestGateway();
        $authorizer = new ControlFriendRequestAuthorizer(true);
        (new RealtimeEventConsumer(
            $store,
            $gateway,
            friendRequestAuthorizer: $authorizer,
        ))->consume();

        controlAssert($authorizer->calls === [], $name . ' reached database authorization');
        controlAssert($gateway->sent === [], $name . ' reached Gateway delivery');
        controlAssert(
            count($store->acked) === 1
            && $store->queue === []
            && $store->processing === []
            && $store->requeued === [],
            $name . ' was not independently ACK-dropped',
        );
    }
});

controlTest('friend event without database authorizer fails closed', static function (): void {
    $store = new ControlTestStore();
    $gateway = new ControlTestGateway();
    $store->queue[] = friendEnvelope('created', 92, 7, 'from-user', 7, 'to-user', null);

    (new RealtimeEventConsumer($store, $gateway))->consume();

    controlAssert($gateway->sent === []);
    controlAssert(count($store->acked) === 1 && $store->requeued === []);
});

controlTest('Redis control publisher exposes RPUSH failure for durable retry', static function (): void {
    $redis = new class() {
        public function rPush(string $key, string $raw): false
        {
            return false;
        }
    };
    try {
        (new RedisRealtimeControlPublisher($redis))->publish(
            friendEnvelope('created', 93, 7, 'from-user', 7, 'to-user', null),
        );
        throw new RuntimeException('Redis publisher silently accepted RPUSH failure');
    } catch (RuntimeException $error) {
        controlAssert(str_contains($error->getMessage(), 'RPUSH failed'));
    }
});

controlTest('Redis control publisher connects lazily and reconnects without startup coupling', static function (): void {
    $connects = 0;
    $publisher = new RedisRealtimeControlPublisher(connector: static function () use (&$connects): object {
        ++$connects;
        throw new RuntimeException('synthetic Redis connect failure');
    });
    controlAssert($connects === 0, 'constructor attempted a Redis connection');
    foreach ([1, 2] as $attempt) {
        try {
            $publisher->publish(
                friendEnvelope('created', 100 + $attempt, 7, 'from-user', 7, 'to-user', null),
            );
            throw new RuntimeException('lazy publisher hid a connection failure');
        } catch (RuntimeException $error) {
            controlAssert(str_contains($error->getMessage(), 'synthetic Redis connect failure'));
        }
        controlAssert($connects === $attempt, 'failed Redis connection was not retried on the next publish');
    }
});

controlTest('organization inactive marker is bounded and enable clears it', static function (): void {
    $redis = new ControlRedisHandler();
    $store = new RedisRealtimeEventStore($redis, 60);

    $store->setOrganizationInactive(7, true);
    controlAssert(
        $redis->expiring === [['im:auth:organization_inactive:7', 60, '1']],
        'organization inactive marker must use a bounded TTL',
    );

    $store->setOrganizationInactive(7, false);
    controlAssert(
        $redis->deleted === ['im:auth:organization_inactive:7'],
        'organization enable must clear the inactive marker',
    );
});

$failed = 0;
foreach ($tests as $name => $callback) {
    try {
        $callback();
        fwrite(STDOUT, "[PASS] {$name}\n");
    } catch (Throwable $throwable) {
        ++$failed;
        fwrite(STDERR, sprintf("[FAIL] %s\n       %s: %s\n", $name, $throwable::class, $throwable->getMessage()));
    }
}

fwrite(STDOUT, sprintf("\n%d realtime control tests, %d failed.\n", count($tests), $failed));
exit($failed === 0 ? 0 : 1);
