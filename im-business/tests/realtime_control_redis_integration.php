<?php

declare(strict_types=1);

use B8im\ImBusiness\Service\RealtimeEventClaim;
use B8im\ImBusiness\Service\RedisRealtimeEventStore;
use B8im\ImShared\Support\Constants;

require dirname(__DIR__) . '/vendor/autoload.php';

if ((string) getenv('IM_REALTIME_REDIS_ISOLATED') !== '1') {
    throw new RuntimeException('Realtime Redis integration requires an explicitly isolated Redis process.');
}
$host = trim((string) (getenv('IM_REALTIME_REDIS_HOST') ?: '127.0.0.1'));
$port = (int) (getenv('IM_REALTIME_REDIS_PORT') ?: 0);
if ($host !== '127.0.0.1' || $port < 1024 || $port > 65535) {
    throw new RuntimeException('Realtime Redis integration target must be an isolated loopback port.');
}

$redis = new Redis();
if (!$redis->connect($host, $port, 2.0)) {
    throw new RuntimeException('Unable to connect to isolated Redis.');
}
if ((int) $redis->dbSize() !== 0) {
    throw new RuntimeException('Isolated Redis must start empty.');
}

$store = new RedisRealtimeEventStore($redis, 60, 1, 60);
$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    ++$assertions;
};
$envelope = static function (string $name): string {
    $identity = [
        'type' => 'auth.organization_enabled',
        'organization' => 7,
        'data' => ['name' => $name],
    ];

    return json_encode([
        'event_id' => hash('sha256', json_encode($identity, JSON_THROW_ON_ERROR)),
        ...$identity,
    ], JSON_THROW_ON_ERROR);
};

try {
    $firstRaw = $envelope('dedup');
    $redis->rPush(Constants::REDIS_REALTIME_EVENTS, $firstRaw, $firstRaw);
    $first = $store->claim('redis-worker-a');
    $assert($first instanceof RealtimeEventClaim && $first->raw === $firstRaw, 'first event was not claimed');
    $assert($store->claim('redis-worker-b') === null, 'concurrent duplicate event was dispatched');
    $store->requeue($first);
    $retried = $store->claim('redis-worker-b');
    $assert($retried instanceof RealtimeEventClaim && $retried->eventId === $first->eventId, 'requeued event was not retried');
    $store->ack($retried);
    $redis->rPush(Constants::REDIS_REALTIME_EVENTS, $firstRaw);
    $assert($store->claim('redis-worker-c') === null, 'acknowledged event id was dispatched twice');

    $crashRaw = $envelope('crash-recovery');
    $redis->rPush(Constants::REDIS_REALTIME_EVENTS, $crashRaw);
    $crashed = $store->claim('redis-worker-crashed');
    $assert($crashed instanceof RealtimeEventClaim, 'crash candidate was not claimed');
    usleep(1_100_000);
    $assert($store->recoverExpired() === 1, 'expired claim lease was not recovered');
    $recovered = $store->claim('redis-worker-recovery');
    $assert($recovered instanceof RealtimeEventClaim && $recovered->raw === $crashRaw, 'recovered claim payload changed');
    $store->ack($recovered);

    $poisonRaw = $envelope('poison');
    $successRaw = $envelope('success-after-poison');
    $redis->rPush(Constants::REDIS_REALTIME_EVENTS, $poisonRaw, $successRaw);
    $poison = $store->claim('redis-worker-poison');
    $assert($poison instanceof RealtimeEventClaim && $poison->raw === $poisonRaw, 'poison queue order changed');
    $store->requeue($poison);
    $success = $store->claim('redis-worker-success');
    $assert($success instanceof RealtimeEventClaim && $success->raw === $successRaw, 'requeue did not move poison event to the tail');
    $store->ack($success);
    $poisonRetry = $store->claim('redis-worker-poison-retry');
    $assert($poisonRetry instanceof RealtimeEventClaim && $poisonRetry->raw === $poisonRaw, 'poison event was lost after tail requeue');
    $store->ack($poisonRetry);

    $redis->rPush(Constants::REDIS_REALTIME_EVENTS, '{malformed-json');
    $malformed = $store->claim('redis-worker-malformed');
    $assert($malformed instanceof RealtimeEventClaim && $malformed->eventId === null, 'malformed event was not explicitly claimable');
    $store->ack($malformed);

    $assert((int) $redis->lLen(Constants::REDIS_REALTIME_EVENTS) === 0, 'pending queue was not drained');
    $assert((int) $redis->hLen(Constants::REDIS_REALTIME_EVENT_PROCESSING) === 0, 'processing claims leaked');
    $assert((int) $redis->hLen(Constants::REDIS_REALTIME_EVENT_INFLIGHT) === 0, 'inflight ids leaked');
    $assert((int) $redis->zCard(Constants::REDIS_REALTIME_EVENT_LEASES) === 0, 'claim leases leaked');
} finally {
    $redis->flushDB();
    $redis->close();
}

fwrite(STDOUT, sprintf("Realtime Redis claim integration: %d assertions passed.\n", $assertions));
