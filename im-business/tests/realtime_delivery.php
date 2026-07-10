<?php

declare(strict_types=1);

use B8im\ImBusiness\Realtime\InvalidRealtimeEvent;
use B8im\ImBusiness\Realtime\RealtimeDeliveryCheckpoint;
use B8im\ImBusiness\Realtime\RealtimeDeliveryHandler;
use B8im\ImBusiness\Realtime\RealtimeDeliveryResult;
use B8im\ImBusiness\Realtime\RealtimeDeliveryService;
use B8im\ImBusiness\Realtime\RealtimeEvent;
use B8im\ImBusiness\Realtime\RealtimeEventDeliverer;
use B8im\ImBusiness\Realtime\RealtimeEventProjector;
use B8im\ImBusiness\Realtime\RealtimeGateway;
use B8im\ImBusiness\Realtime\RealtimeRecipientProvider;
use B8im\ImBusiness\Realtime\RealtimeRetryCounter;
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Support\Constants;

require dirname(__DIR__) . '/vendor/autoload.php';

$tests = [];

function realtimeTest(string $name, callable $callback): void
{
    global $tests;
    $tests[$name] = $callback;
}

function realtimeAssert(bool $condition, string $message = 'assertion failed'): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function expectInvalidRealtime(callable $callback): void
{
    try {
        $callback();
    } catch (InvalidRealtimeEvent) {
        return;
    }

    throw new RuntimeException('expected InvalidRealtimeEvent');
}

/** @return array<string, mixed> */
function createdRealtimePayload(): array
{
    return [
        'event_type' => Constants::MQ_ROUTING_MESSAGE_CREATED,
        'organization' => 7,
        'message_id' => '01JZMESSAGE00000000000000001',
        'message_seq' => 12,
        'global_seq' => '99',
        'conversation_id' => 'group:7:alpha',
        'conversation_type' => 2,
        'sender_id' => 'sender-1',
        'actor_user_id' => 'sender-1',
        'origin_user_id' => 'sender-1',
        'origin_client_id' => 'origin-client',
        'recipient_count' => 2,
        'recipient_user_ids' => ['recipient-1', 'left-member'],
        'message' => [
            'organization' => 7,
            'conversation_id' => 'group:7:alpha',
            'conversation_type' => 2,
            'message_id' => '01JZMESSAGE00000000000000001',
            'message_seq' => 12,
            'global_seq' => '99',
            'client_msg_id' => 'web-message-1',
            'sender_id' => 'sender-1',
            'sender_user' => ['user_id' => 'sender-1', 'nickname' => 'Sender'],
            'message_type' => 1,
            'content' => ['text' => 'hello'],
            'status' => 'normal',
            'edit_time' => '',
            'edit_count' => 0,
            'create_time' => '2026-07-10 12:00:00',
            'update_time' => '2026-07-10 12:00:00',
        ],
        'created_at' => '2026-07-10 12:00:00',
    ];
}

/** @return array<string, mixed> */
function mutationRealtimePayload(string $eventType): array
{
    $base = [
        'event_type' => $eventType,
        'organization' => 7,
        'conversation_id' => 'group:7:alpha',
        'conversation_type' => 2,
        'message_id' => '01JZMESSAGE00000000000000001',
        'message_seq' => 12,
        'change_seq' => 3,
        'target_user_id' => null,
        'actor_user_id' => 'sender-1',
        'origin_user_id' => 'sender-1',
        'origin_client_id' => 'origin-client',
        'created_at' => '2026-07-10 12:01:00',
    ];

    $base['payload'] = match ($eventType) {
        Constants::MQ_ROUTING_MESSAGE_RECALLED => ['status' => 'recalled'],
        Constants::MQ_ROUTING_MESSAGE_EDITED => [
            'content' => ['text' => 'edited'],
            'edit_time' => '2026-07-10 12:01:00',
            'edit_count' => 1,
            'message' => [
                'organization' => 7,
                'conversation_id' => 'group:7:alpha',
                'conversation_type' => 2,
                'message_id' => '01JZMESSAGE00000000000000001',
                'message_seq' => 12,
                'global_seq' => '99',
                'client_msg_id' => 'web-message-1',
                'sender_id' => 'sender-1',
                'sender_user' => ['user_id' => 'sender-1', 'nickname' => 'Sender'],
                'message_type' => 1,
                'content' => ['text' => 'edited'],
                'status' => 'normal',
                'edit_time' => '2026-07-10 12:01:00',
                'edit_count' => 1,
                'create_time' => '2026-07-10 12:00:00',
                'update_time' => '2026-07-10 12:01:00',
            ],
        ],
        Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH => ['scope' => 'both', 'status' => 'deleted_both'],
        Constants::MQ_ROUTING_MESSAGE_DELETED_SELF => ['scope' => 'self'],
        default => [],
    };
    if ($eventType === Constants::MQ_ROUTING_MESSAGE_DELETED_SELF) {
        $base['target_user_id'] = 'target-1';
    }

    return $base;
}

realtimeTest('created event projects the safe message and idempotency sequences', static function (): void {
    $payload = createdRealtimePayload();
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    );

    realtimeAssert($event->packetCommand === Command::PUSH);
    realtimeAssert($event->messageSeq === 12 && $event->changeSeq === 0);
    realtimeAssert($event->originUserId === 'sender-1' && $event->originClientId === 'origin-client');
    realtimeAssert($event->recipientUserIds === ['recipient-1', 'left-member']);
    $packet = json_decode($event->encodedPacket(), true, flags: JSON_THROW_ON_ERROR);
    realtimeAssert($packet['organization'] === 7);
    realtimeAssert($packet['data']['message_seq'] === 12);
    realtimeAssert($packet['data']['change_seq'] === 0);
    realtimeAssert($packet['data']['event_id'] === $event->eventId());
    realtimeAssert(preg_match('/^[a-f0-9]{64}$/', $event->eventId()) === 1);
    realtimeAssert($packet['data']['message']['content']['text'] === 'hello');
});

realtimeTest('mutation events map to client commands and always carry both sequences', static function (): void {
    $projector = new RealtimeEventProjector();
    $commands = [
        Constants::MQ_ROUTING_MESSAGE_RECALLED => Command::RECALL,
        Constants::MQ_ROUTING_MESSAGE_EDITED => Command::EDIT,
        Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH => Command::DELETE,
        Constants::MQ_ROUTING_MESSAGE_DELETED_SELF => Command::DELETE,
    ];

    foreach ($commands as $eventType => $command) {
        $event = $projector->project($eventType, json_encode(mutationRealtimePayload($eventType), JSON_THROW_ON_ERROR));
        realtimeAssert($event->packetCommand === $command, $eventType . ' command mismatch');
        realtimeAssert($event->messageSeq === 12 && $event->changeSeq === 3, $eventType . ' sequence mismatch');
        $packet = json_decode($event->encodedPacket(), true, flags: JSON_THROW_ON_ERROR);
        realtimeAssert($packet['data']['message_seq'] === 12, $eventType . ' packet message_seq missing');
        realtimeAssert($packet['data']['change_seq'] === 3, $eventType . ' packet change_seq missing');
        if ($eventType === Constants::MQ_ROUTING_MESSAGE_EDITED) {
            realtimeAssert($packet['data']['message']['global_seq'] === '99');
            realtimeAssert($packet['data']['message']['client_msg_id'] === 'web-message-1');
            realtimeAssert($packet['data']['message']['create_time'] === '2026-07-10 12:00:00');
            realtimeAssert($packet['data']['message']['sender_user']['nickname'] === 'Sender');
        }
    }
});

realtimeTest('bad JSON, routing conflicts and cross-envelope identities are rejected', static function (): void {
    $projector = new RealtimeEventProjector();
    expectInvalidRealtime(static fn () => $projector->project(Constants::MQ_ROUTING_MESSAGE_CREATED, '{'));
    expectInvalidRealtime(static fn () => $projector->project('message.unknown', '{}'));

    $payload = createdRealtimePayload();
    $payload['event_type'] = Constants::MQ_ROUTING_MESSAGE_EDITED;
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));

    $payload = createdRealtimePayload();
    $payload['message']['organization'] = 8;
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));

    $payload = createdRealtimePayload();
    $payload['recipient_user_ids'][] = 'recipient-1';
    $payload['recipient_count'] = 3;
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));

    $payload = createdRealtimePayload();
    $payload['origin_user_id'] = 'another-user';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));

    $payload = mutationRealtimePayload(Constants::MQ_ROUTING_MESSAGE_EDITED);
    $payload['payload']['message']['conversation_id'] = 'group:7:other';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_MESSAGE_EDITED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));
});

realtimeTest('created recipients are intersected with current organization members', static function (): void {
    $provider = new class() implements RealtimeRecipientProvider {
        public array $calls = [];

        public function activeUserIds(int $organization, string $conversationId, int $messageSeq): array
        {
            $this->calls[] = [$organization, $conversationId, $messageSeq];
            return ['sender-1', 'recipient-1', 'current-not-in-event'];
        }
    };
    $gateway = new class() implements RealtimeGateway {
        public array $clientIds = [
            'sender-1' => ['origin-client', 'sender-secondary'],
            'recipient-1' => ['recipient-client'],
        ];
        public array $deliveries = [];

        public function clientIdsForOrganizationUser(int $organization, string $userId): array
        {
            return $this->clientIds[$userId] ?? [];
        }

        public function sendToClient(string $clientId, string $packet): void
        {
            $this->deliveries[] = [$clientId, json_decode($packet, true, flags: JSON_THROW_ON_ERROR)];
        }
    };
    $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
        public array $delivered = [];

        public function wasDelivered(RealtimeEvent $event, string $clientId): bool
        {
            return isset($this->delivered[$event->eventId() . ':' . $clientId]);
        }

        public function markDelivered(RealtimeEvent $event, string $clientId): void
        {
            $this->delivered[$event->eventId() . ':' . $clientId] = true;
        }
    };
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode(createdRealtimePayload(), JSON_THROW_ON_ERROR),
    );
    (new RealtimeDeliveryService($provider, $gateway, $checkpoints))->deliver($event);

    realtimeAssert($provider->calls === [[7, 'group:7:alpha', 12]]);
    realtimeAssert(array_column($gateway->deliveries, 0) === ['sender-secondary', 'recipient-client']);
    realtimeAssert($gateway->deliveries[0][1]['data']['event_id'] === $event->eventId());
});

realtimeTest('system notices reach recipients without becoming an unread origin echo', static function (): void {
    $payload = createdRealtimePayload();
    $payload['sender_id'] = 'system_notification';
    $payload['message']['sender_id'] = 'system_notification';
    $payload['message']['message_type'] = 5;
    $payload['message']['content'] = [
        'event' => 'screenshot',
        'actor_user_id' => 'sender-1',
        'text' => '截屏提示',
    ];
    $provider = new class() implements RealtimeRecipientProvider {
        public function activeUserIds(int $organization, string $conversationId, int $messageSeq): array
        {
            return ['sender-1', 'recipient-1'];
        }
    };
    $gateway = new class() implements RealtimeGateway {
        public array $lookups = [];
        public array $deliveries = [];

        public function clientIdsForOrganizationUser(int $organization, string $userId): array
        {
            $this->lookups[] = $userId;
            return ['client-' . $userId];
        }

        public function sendToClient(string $clientId, string $packet): void
        {
            $this->deliveries[] = $clientId;
        }
    };
    $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
        public function wasDelivered(RealtimeEvent $event, string $clientId): bool
        {
            return false;
        }

        public function markDelivered(RealtimeEvent $event, string $clientId): void
        {
        }
    };
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
    );
    (new RealtimeDeliveryService($provider, $gateway, $checkpoints))->deliver($event);

    realtimeAssert($event->actorUserId === 'sender-1');
    realtimeAssert($gateway->lookups === ['recipient-1']);
    realtimeAssert($gateway->deliveries === ['client-recipient-1']);
});

realtimeTest('client checkpoints suppress redelivery and resume only failed clients', static function (): void {
    $provider = new class() implements RealtimeRecipientProvider {
        public function activeUserIds(int $organization, string $conversationId, int $messageSeq): array
        {
            return ['recipient-1', 'left-member'];
        }
    };
    $gateway = new class() implements RealtimeGateway {
        public array $attempts = [];
        private bool $failSecondClient = true;

        public function clientIdsForOrganizationUser(int $organization, string $userId): array
        {
            return match ($userId) {
                'recipient-1' => ['client-a'],
                'left-member' => ['client-b'],
                default => [],
            };
        }

        public function sendToClient(string $clientId, string $packet): void
        {
            $this->attempts[] = $clientId;
            if ($clientId === 'client-b' && $this->failSecondClient) {
                $this->failSecondClient = false;
                throw new RuntimeException('gateway unavailable');
            }
        }
    };
    $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
        public array $delivered = [];

        public function wasDelivered(RealtimeEvent $event, string $clientId): bool
        {
            return isset($this->delivered[$event->eventId() . ':' . $clientId]);
        }

        public function markDelivered(RealtimeEvent $event, string $clientId): void
        {
            $this->delivered[$event->eventId() . ':' . $clientId] = true;
        }
    };
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode(createdRealtimePayload(), JSON_THROW_ON_ERROR),
    );
    $deliverer = new RealtimeDeliveryService($provider, $gateway, $checkpoints);

    try {
        $deliverer->deliver($event);
        throw new RuntimeException('expected first delivery to fail');
    } catch (RuntimeException $exception) {
        realtimeAssert($exception->getMessage() === 'gateway unavailable');
    }
    $deliverer->deliver($event);
    $deliverer->deliver($event);

    realtimeAssert($gateway->attempts === ['client-a', 'client-b', 'client-b']);
});

realtimeTest('delete_self bypasses member fanout and targets only its organization user', static function (): void {
    $provider = new class() implements RealtimeRecipientProvider {
        public int $calls = 0;

        public function activeUserIds(int $organization, string $conversationId, int $messageSeq): array
        {
            ++$this->calls;
            return ['target-1', 'other-1'];
        }
    };
    $gateway = new class() implements RealtimeGateway {
        public array $deliveries = [];

        public function clientIdsForOrganizationUser(int $organization, string $userId): array
        {
            return ['client-' . $userId];
        }

        public function sendToClient(string $clientId, string $packet): void
        {
            $this->deliveries[] = $clientId;
        }
    };
    $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
        public function wasDelivered(RealtimeEvent $event, string $clientId): bool
        {
            return false;
        }

        public function markDelivered(RealtimeEvent $event, string $clientId): void
        {
        }
    };
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_DELETED_SELF,
        json_encode(mutationRealtimePayload(Constants::MQ_ROUTING_MESSAGE_DELETED_SELF), JSON_THROW_ON_ERROR),
    );
    (new RealtimeDeliveryService($provider, $gateway, $checkpoints))->deliver($event);

    realtimeAssert($provider->calls === 0);
    realtimeAssert($gateway->deliveries === ['client-target-1']);
});

realtimeTest('durable message handlers do not perform a second direct fanout', static function (): void {
    $source = file_get_contents(dirname(__DIR__) . '/src/Events.php');
    realtimeAssert(is_string($source) && $source !== '');

    foreach (['handleSend', 'handleRecall', 'handleEdit', 'handleDelete', 'handleScreenshot'] as $handler) {
        $pattern = sprintf(
            '/private static function %s\\b.*?(?=\\n    private static function |\\n})/s',
            preg_quote($handler, '/'),
        );
        realtimeAssert(preg_match($pattern, $source, $match) === 1, $handler . ' source not found');
        realtimeAssert(!str_contains($match[0], 'Gateway::sendToUid'), $handler . ' still directly fans out to a user');
        realtimeAssert(!str_contains($match[0], 'Gateway::getClientIdByUid'), $handler . ' still directly fans out to sibling clients');
    }
});

realtimeTest('delivery handler ACKs success only after delivery and retry cleanup', static function (): void {
    $deliverer = new class() implements RealtimeEventDeliverer {
        public int $calls = 0;

        public function deliver(RealtimeEvent $event): void
        {
            ++$this->calls;
        }
    };
    $counter = new class() implements RealtimeRetryCounter {
        public int $increments = 0;
        public int $clears = 0;

        public function increment(RealtimeEvent $event): int
        {
            return ++$this->increments;
        }

        public function clear(RealtimeEvent $event): void
        {
            ++$this->clears;
        }
    };
    $handler = new RealtimeDeliveryHandler(new RealtimeEventProjector(), $deliverer, $counter, 2);
    $result = $handler->handle(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode(createdRealtimePayload(), JSON_THROW_ON_ERROR),
    );

    realtimeAssert($result->outcome === RealtimeDeliveryResult::ACK);
    realtimeAssert($deliverer->calls === 1 && $counter->clears === 1 && $counter->increments === 0);
});

realtimeTest('runtime failures requeue a bounded number of times and then dead-letter', static function (): void {
    $deliverer = new class() implements RealtimeEventDeliverer {
        public int $calls = 0;

        public function deliver(RealtimeEvent $event): void
        {
            ++$this->calls;
            throw new RuntimeException('gateway unavailable');
        }
    };
    $counter = new class() implements RealtimeRetryCounter {
        public int $attempt = 0;

        public function increment(RealtimeEvent $event): int
        {
            return ++$this->attempt;
        }

        public function clear(RealtimeEvent $event): void
        {
        }
    };
    $handler = new RealtimeDeliveryHandler(new RealtimeEventProjector(), $deliverer, $counter, 2);
    $body = json_encode(createdRealtimePayload(), JSON_THROW_ON_ERROR);

    realtimeAssert($handler->handle(Constants::MQ_ROUTING_MESSAGE_CREATED, $body)->outcome === RealtimeDeliveryResult::REQUEUE);
    realtimeAssert($handler->handle(Constants::MQ_ROUTING_MESSAGE_CREATED, $body)->outcome === RealtimeDeliveryResult::REQUEUE);
    $final = $handler->handle(Constants::MQ_ROUTING_MESSAGE_CREATED, $body);
    realtimeAssert($final->outcome === RealtimeDeliveryResult::DEAD_LETTER);
    realtimeAssert($final->attempt === 3 && $deliverer->calls === 3);
});

realtimeTest('invalid payload is dead-lettered without delivery or retry accounting', static function (): void {
    $deliverer = new class() implements RealtimeEventDeliverer {
        public int $calls = 0;

        public function deliver(RealtimeEvent $event): void
        {
            ++$this->calls;
        }
    };
    $counter = new class() implements RealtimeRetryCounter {
        public int $calls = 0;

        public function increment(RealtimeEvent $event): int
        {
            ++$this->calls;
            return $this->calls;
        }

        public function clear(RealtimeEvent $event): void
        {
            ++$this->calls;
        }
    };
    $handler = new RealtimeDeliveryHandler(new RealtimeEventProjector(), $deliverer, $counter, 2);
    $result = $handler->handle(Constants::MQ_ROUTING_MESSAGE_CREATED, '{bad-json');

    realtimeAssert($result->outcome === RealtimeDeliveryResult::DEAD_LETTER);
    realtimeAssert($deliverer->calls === 0 && $counter->calls === 0);
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

fwrite(STDOUT, sprintf("\n%d realtime tests, %d failed.\n", count($tests), $failed));
exit($failed === 0 ? 0 : 1);
