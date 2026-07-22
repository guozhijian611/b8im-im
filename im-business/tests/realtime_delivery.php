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
use B8im\ImBusiness\Realtime\GroupMemberAccessRealtimeAuthorizer;
use B8im\ImBusiness\Realtime\GroupMemberAccessSessionInvalidator;
use B8im\ImBusiness\Realtime\DatabaseGroupMemberAccessRealtimeAuthorizer;
use B8im\ImBusiness\Repository\GroupMemberAccessRepository;
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

trait PassThroughRealtimeRecipientBoundary
{
    public function withDeliverableIdentities(RealtimeEvent $event, callable $delivery): void
    {
        $delivery($this->activeIdentities(
            $event->organization,
            $event->conversationId,
            $event->messageSeq,
        ));
    }
}

/** @return array<string, mixed> */
function createdRealtimePayload(): array
{
    return [
        'event_id' => hash('sha256', 'created-home-7'),
        'event_type' => Constants::MQ_ROUTING_MESSAGE_CREATED,
        'organization' => 7,
        'message_id' => '01JZMESSAGE00000000000000001',
        'message_seq' => 12,
        'global_seq' => '99',
        'conversation_id' => 'group:7:alpha',
        'conversation_type' => 2,
        'sender_id' => 'sender-1',
        'sender_organization' => 7,
        'actor_user_id' => 'sender-1',
        'actor_organization' => 7,
        'origin_user_id' => 'sender-1',
        'origin_organization' => 7,
        'origin_client_id' => 'origin-client',
        'recipient_count' => 3,
        'recipient_identities' => [
            ['organization' => 7, 'user_id' => 'sender-1'],
            ['organization' => 7, 'user_id' => 'recipient-1'],
            ['organization' => 7, 'user_id' => 'left-member'],
        ],
        'message' => [
            'organization' => 7,
            'conversation_id' => 'group:7:alpha',
            'conversation_type' => 2,
            'message_id' => '01JZMESSAGE00000000000000001',
            'message_seq' => 12,
            'global_seq' => '99',
            'client_msg_id' => 'web-message-1',
            'sender_id' => 'sender-1',
            'sender_organization' => 7,
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
        'event_id' => hash('sha256', 'mutation-' . $eventType),
        'event_type' => $eventType,
        'organization' => 7,
        'conversation_id' => 'group:7:alpha',
        'conversation_type' => 2,
        'message_id' => '01JZMESSAGE00000000000000001',
        'message_seq' => 12,
        'change_seq' => 3,
        'target_user_id' => null,
        'target_organization' => null,
        'actor_user_id' => 'sender-1',
        'actor_organization' => 7,
        'origin_user_id' => 'sender-1',
        'origin_organization' => 7,
        'origin_client_id' => 'origin-client',
        'recipient_count' => 2,
        'recipient_identities' => [
            ['organization' => 7, 'user_id' => 'recipient-1'],
            ['organization' => 7, 'user_id' => 'left-member'],
        ],
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
                'sender_organization' => 7,
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
        $base['target_organization'] = 7;
        $base['recipient_count'] = 1;
        $base['recipient_identities'] = [['organization' => 7, 'user_id' => 'target-1']];
    }

    return $base;
}

/** @return array<string,mixed> */
function groupAccessRealtimePayload(): array
{
    return [
        'event_id' => 'c0be0c0c4fcabe60b7253511adcc1f07d327744ddd545889dfaa27930d95d686',
        'event_type' => Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
        'organization' => 7,
        'conversation_id' => 'group:7:alpha',
        'conversation_type' => 2,
        'target_organization' => 7,
        'target_user_id' => 'member-1',
        'access_snapshot_id' => '12',
        'access_version' => '5',
        'access_state' => 'revoked',
        'last_message_seq' => '20',
        'last_change_seq' => '3',
        'periods' => [],
        'reason' => 'history_revoke',
        'actor_organization' => 7,
        'actor_user_id' => 'admin-1',
        'recipient_count' => 1,
        'recipient_identities' => [['organization' => 7, 'user_id' => 'member-1']],
        'created_at' => '2026-07-20 19:30:00',
    ];
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
    realtimeAssert($event->recipientIdentities === [
        ['organization' => 7, 'user_id' => 'sender-1'],
        ['organization' => 7, 'user_id' => 'recipient-1'],
        ['organization' => 7, 'user_id' => 'left-member'],
    ]);
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

realtimeTest('receipt and conversation read events preserve composite actor identity', static function (): void {
    $base = [
        'organization' => 7,
        'conversation_id' => 'single_2118193dd11825a86050c3575d1f9aa52849d5e3',
        'conversation_type' => 1,
        'message_id' => '01JZMESSAGE00000000000000001',
        'message_seq' => 12,
        'change_seq' => 0,
        'actor_organization' => 8,
        'actor_user_id' => 'reader-1',
        'origin_organization' => 8,
        'origin_user_id' => 'reader-1',
        'origin_client_id' => 'reader-client',
        'cross_org_access_snapshot_id' => '41',
        'recipient_count' => 1,
        'recipient_identities' => [['organization' => 7, 'user_id' => 'sender-1']],
        'created_at' => '2026-07-10 12:02:00',
    ];
    $receiptPayload = $base + [
        'event_id' => hash('sha256', 'receipt-home-7'),
        'event_type' => Constants::MQ_ROUTING_MESSAGE_RECEIPT,
        'sender_organization' => 7,
        'sender_id' => 'sender-1',
        'user_organization' => 8,
        'user_id' => 'reader-1',
        'receipt' => [
            'organization' => 7,
            'message_id' => '01JZMESSAGE00000000000000001',
            'conversation_id' => 'single_2118193dd11825a86050c3575d1f9aa52849d5e3',
            'message_seq' => 12,
            'global_seq' => '21',
            'sender_organization' => 7,
            'sender_id' => 'sender-1',
            'user_organization' => 8,
            'user_id' => 'reader-1',
            'status' => 'read',
            'last_read_message_id' => '01JZMESSAGE00000000000000001',
            'last_read_seq' => 12,
            'unread_count' => 0,
            'time' => '2026-07-10 12:02:00',
        ],
    ];
    $receipt = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_RECEIPT,
        json_encode($receiptPayload, JSON_THROW_ON_ERROR),
    );
    realtimeAssert($receipt->packetCommand === Command::ACK);
    realtimeAssert($receipt->actorOrganization === 8 && $receipt->organization === 7);
    realtimeAssert($receipt->crossOrgAccessSnapshotId === '41');
    $receiptPacket = json_decode($receipt->encodedPacket(), true, flags: JSON_THROW_ON_ERROR);
    realtimeAssert($receiptPacket['data']['cross_org_access_snapshot_id'] === '41');

    $readPayload = $base + [
        'event_id' => hash(
            'sha256',
            '7|' . Constants::MQ_ROUTING_CONVERSATION_READ
                . '|single_2118193dd11825a86050c3575d1f9aa52849d5e3|8|reader-1|12|41',
        ),
        'event_type' => Constants::MQ_ROUTING_CONVERSATION_READ,
        'user_organization' => 8,
        'user_id' => 'reader-1',
        'read_state' => [
            'conversation_id' => 'single_2118193dd11825a86050c3575d1f9aa52849d5e3',
            'last_read_message_id' => '01JZMESSAGE00000000000000001',
            'last_read_seq' => 12,
            'unread_count' => 0,
            'user_organization' => 8,
            'user_id' => 'reader-1',
            'time' => '2026-07-10 12:02:00',
            'cross_org_access_snapshot_id' => '41',
        ],
    ];
    $read = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_CONVERSATION_READ,
        json_encode($readPayload, JSON_THROW_ON_ERROR),
    );
    realtimeAssert($read->packetCommand === Command::CONVERSATION_READ);
    realtimeAssert($read->actorOrganization === 8 && $read->organization === 7);
    realtimeAssert($read->crossOrgAccessSnapshotId === '41');
    $readPacket = json_decode($read->encodedPacket(), true, flags: JSON_THROW_ON_ERROR);
    realtimeAssert($readPacket['data']['cross_org_access_snapshot_id'] === '41');
});

realtimeTest('conversation read epoch identity is stable and isolates delivery checkpoints', static function (): void {
    $conversationId = 'single_2118193dd11825a86050c3575d1f9aa52849d5e3';
    $messageId = '01JZMESSAGE00000000000000001';
    $payload = [
        'event_id' => hash(
            'sha256',
            '7|' . Constants::MQ_ROUTING_CONVERSATION_READ . '|' . $conversationId . '|8|reader-1|12|41',
        ),
        'event_type' => Constants::MQ_ROUTING_CONVERSATION_READ,
        'organization' => 7,
        'conversation_id' => $conversationId,
        'conversation_type' => 1,
        'message_id' => $messageId,
        'message_seq' => 12,
        'change_seq' => 0,
        'actor_organization' => 8,
        'actor_user_id' => 'reader-1',
        'origin_organization' => 8,
        'origin_user_id' => 'reader-1',
        'origin_client_id' => 'reader-epoch-41',
        'cross_org_access_snapshot_id' => '41',
        'user_organization' => 8,
        'user_id' => 'reader-1',
        'recipient_count' => 1,
        'recipient_identities' => [['organization' => 7, 'user_id' => 'sender-1']],
        'read_state' => [
            'conversation_id' => $conversationId,
            'last_read_message_id' => $messageId,
            'last_read_seq' => 12,
            'unread_count' => 0,
            'user_organization' => 8,
            'user_id' => 'reader-1',
            'time' => '2026-07-10 12:02:00',
            'cross_org_access_snapshot_id' => '41',
        ],
        'created_at' => '2026-07-10 12:02:00',
    ];
    $projector = new RealtimeEventProjector();
    $epochFortyOne = $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_READ,
        json_encode($payload, JSON_THROW_ON_ERROR),
    );
    $sameEpochRetryPayload = $payload;
    $sameEpochRetryPayload['origin_client_id'] = 'reader-epoch-41-retry';
    $sameEpochRetry = $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_READ,
        json_encode($sameEpochRetryPayload, JSON_THROW_ON_ERROR),
    );
    realtimeAssert(
        $sameEpochRetry->eventId() === $epochFortyOne->eventId()
            && $sameEpochRetry->originClientId === 'reader-epoch-41-retry',
        'origin client must remain payload data outside the stable event identity',
    );

    $mismatchedPayload = $payload;
    $mismatchedPayload['cross_org_access_snapshot_id'] = '43';
    $mismatchedPayload['read_state']['cross_org_access_snapshot_id'] = '43';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_READ,
        json_encode($mismatchedPayload, JSON_THROW_ON_ERROR),
    ));

    $sameOrganizationPayload = $payload;
    unset($sameOrganizationPayload['cross_org_access_snapshot_id']);
    unset($sameOrganizationPayload['read_state']['cross_org_access_snapshot_id']);
    $sameOrganizationPayload['actor_organization'] = 7;
    $sameOrganizationPayload['origin_organization'] = 7;
    $sameOrganizationPayload['user_organization'] = 7;
    $sameOrganizationPayload['read_state']['user_organization'] = 7;
    $sameOrganizationPayload['event_id'] = hash(
        'sha256',
        '7|' . Constants::MQ_ROUTING_CONVERSATION_READ . '|' . $conversationId . '|7|reader-1|12',
    );
    $sameOrganizationEvent = $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_READ,
        json_encode($sameOrganizationPayload, JSON_THROW_ON_ERROR),
    );
    realtimeAssert(
        $sameOrganizationEvent->eventId() === $sameOrganizationPayload['event_id']
            && $sameOrganizationEvent->crossOrgAccessSnapshotId === null,
        'snapshot-free conversation.read keeps the legacy identity formula',
    );
    $sameOrganizationSnapshotLeak = $sameOrganizationPayload;
    $sameOrganizationSnapshotLeak['read_state']['cross_org_access_snapshot_id'] = '41';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_READ,
        json_encode($sameOrganizationSnapshotLeak, JSON_THROW_ON_ERROR),
    ));

    $epochFortyThreePayload = $payload;
    $epochFortyThreePayload['event_id'] = hash(
        'sha256',
        '7|' . Constants::MQ_ROUTING_CONVERSATION_READ . '|' . $conversationId . '|8|reader-1|12|43',
    );
    $epochFortyThreePayload['origin_client_id'] = 'reader-epoch-43';
    $epochFortyThreePayload['cross_org_access_snapshot_id'] = '43';
    $epochFortyThreePayload['read_state']['cross_org_access_snapshot_id'] = '43';
    $epochFortyThree = $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_READ,
        json_encode($epochFortyThreePayload, JSON_THROW_ON_ERROR),
    );
    realtimeAssert($epochFortyThree->eventId() !== $epochFortyOne->eventId());

    $provider = new class() implements RealtimeRecipientProvider {
        use PassThroughRealtimeRecipientBoundary;

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
        {
            return [['organization' => 7, 'user_id' => 'sender-1']];
        }
    };
    $gateway = new class() implements RealtimeGateway {
        public array $eventIds = [];

        public function clientIdsForOrganizationUser(int $organization, string $userId): array
        {
            return ['sender-client'];
        }

        public function sendToClient(string $clientId, string $packet): void
        {
            $decoded = json_decode($packet, true, flags: JSON_THROW_ON_ERROR);
            $this->eventIds[] = (string) $decoded['data']['event_id'];
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
    $delivery = new RealtimeDeliveryService($provider, $gateway, $checkpoints);
    $delivery->deliver($epochFortyOne);
    $delivery->deliver($epochFortyOne);
    $delivery->deliver($epochFortyThree);
    $delivery->deliver($epochFortyThree);

    realtimeAssert($gateway->eventIds === [$epochFortyOne->eventId(), $epochFortyThree->eventId()]);
    realtimeAssert(count($checkpoints->delivered) === 2);
});

realtimeTest('conversation access changes project both allowed states without member-policy gating', static function (): void {
    $projector = new RealtimeEventProjector();
    $conversationId = 'single_5dd996df2b82554f8a914976e78535f6f6de3384';
    foreach ([false, true] as $allowed) {
        $snapshotId = $allowed ? '43' : '42';
        $payload = [
            'event_id' => hash('sha256', '7|conversation.access_changed|' . $conversationId . '|' . $snapshotId),
            'event_type' => Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
            'organization' => 7,
            'conversation_id' => $conversationId,
            'conversation_type' => 1,
            'cross_org_access_snapshot_id' => $snapshotId,
            'allowed' => $allowed,
            'target_organization' => 7,
            'target_user_id' => 'sender-1',
            'peer_organization' => 8,
            'peer_user_id' => 'reader-1',
            'recipient_count' => 1,
            'recipient_identities' => [['organization' => 7, 'user_id' => 'sender-1']],
            'created_at' => '2026-07-20 12:00:00',
        ];
        $event = $projector->project(
            Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
            json_encode($payload, JSON_THROW_ON_ERROR),
        );
        realtimeAssert($event->packetCommand === Command::CONVERSATION_ACCESS_CHANGED);
        realtimeAssert($event->targetOrganization === 7 && $event->targetUserId === 'sender-1');
        $packet = json_decode($event->encodedPacket(), true, flags: JSON_THROW_ON_ERROR);
        realtimeAssert($packet['data']['cross_org_access_snapshot_id'] === $snapshotId);
        realtimeAssert($packet['data']['allowed'] === $allowed);
        realtimeAssert($packet['data']['target_organization'] === 7);
        realtimeAssert($packet['data']['target_user_id'] === 'sender-1');
        realtimeAssert($packet['data']['peer_organization'] === 8);
        realtimeAssert($packet['data']['peer_user_id'] === 'reader-1');

        $provider = new class() implements RealtimeRecipientProvider {
            use PassThroughRealtimeRecipientBoundary;

            public int $calls = 0;
            public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
            {
                ++$this->calls;
                return [];
            }
        };
        $gateway = new class() implements RealtimeGateway {
            public array $packets = [];
            public function clientIdsForOrganizationUser(int $organization, string $userId): array
            {
                return ['access-client'];
            }
            public function sendToClient(string $clientId, string $packet): void
            {
                $this->packets[] = json_decode($packet, true, flags: JSON_THROW_ON_ERROR);
            }
        };
        $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
            public function wasDelivered(RealtimeEvent $event, string $clientId): bool { return false; }
            public function markDelivered(RealtimeEvent $event, string $clientId): void {}
        };
        (new RealtimeDeliveryService($provider, $gateway, $checkpoints))->deliver($event);
        realtimeAssert($provider->calls === 0, 'access event must not be gated by current membership/policy');
        realtimeAssert(count($gateway->packets) === 1);
    }

    $legacy = $payload;
    unset($legacy['cross_org_access_snapshot_id'], $legacy['allowed']);
    $legacy['access_snapshot_id'] = '43';
    $legacy['access_allowed'] = true;
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
        json_encode($legacy, JSON_THROW_ON_ERROR),
    ));

    $invalid = [
        'event_id' => hash('sha256', '7|conversation.access_changed|' . $conversationId . '|01'),
        'event_type' => Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
        'organization' => 7,
        'conversation_id' => $conversationId,
        'conversation_type' => 1,
        'cross_org_access_snapshot_id' => '01',
        'allowed' => false,
        'target_organization' => 7,
        'target_user_id' => 'sender-1',
        'peer_organization' => 8,
        'peer_user_id' => 'reader-1',
        'recipient_count' => 1,
        'recipient_identities' => [['organization' => 7, 'user_id' => 'sender-1']],
        'created_at' => '2026-07-20 12:00:00',
    ];
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
        json_encode($invalid, JSON_THROW_ON_ERROR),
    ));
    $invalid['cross_org_access_snapshot_id'] = '44';
    $invalid['event_id'] = hash('sha256', '7|conversation.access_changed|' . $conversationId . '|44');
    $invalid['recipient_identities'] = [['organization' => 8, 'user_id' => 'reader-1']];
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
        json_encode($invalid, JSON_THROW_ON_ERROR),
    ));
    $invalid['recipient_identities'] = [['organization' => 7, 'user_id' => 'sender-1']];
    $invalid['peer_user_id'] = 'another-peer';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
        json_encode($invalid, JSON_THROW_ON_ERROR),
    ));
});

realtimeTest('group access fixed vector is current-authorized, invalidates session pins and deduplicates', static function (): void {
    $payload = groupAccessRealtimePayload();
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    );
    realtimeAssert($event->eventId() === 'c0be0c0c4fcabe60b7253511adcc1f07d327744ddd545889dfaa27930d95d686');
    realtimeAssert($event->messageId === '86e88de917a2db16a4c35e82747aded6c42ac69d');
    realtimeAssert($event->packetCommand === Command::GROUP_MEMBER_ACCESS_CHANGED);
    $packet = json_decode($event->encodedPacket(), true, flags: JSON_THROW_ON_ERROR);
    realtimeAssert($packet['data']['conversation_type'] === 2);
    realtimeAssert($packet['data']['last_message_seq'] === '20');
    realtimeAssert($packet['data']['last_change_seq'] === '3');
    realtimeAssert($packet['data']['changed_at'] === '2026-07-20 19:30:00');
    realtimeAssert(!array_key_exists('actor_organization', $packet['data']));
    realtimeAssert(!array_key_exists('actor_user_id', $packet['data']));

    $provider = new class() implements RealtimeRecipientProvider {
        public int $calls = 0;
        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array { ++$this->calls; return []; }
        public function withDeliverableIdentities(RealtimeEvent $event, callable $delivery): void { ++$this->calls; $delivery([]); }
    };
    $gateway = new class() implements RealtimeGateway, GroupMemberAccessSessionInvalidator {
        public array $invalidations = [];
        public array $packets = [];
        public function clientIdsForOrganizationUser(int $organization, string $userId): array { return ['group-access-client']; }
        public function invalidateGroupAccessSnapshot(string $clientId, string $currentSnapshotId): void { $this->invalidations[] = [$clientId, $currentSnapshotId]; }
        public function sendToClient(string $clientId, string $packet): void { $this->packets[] = $packet; }
    };
    $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
        public array $seen = [];
        public function wasDelivered(RealtimeEvent $event, string $clientId): bool { return isset($this->seen[$event->eventId() . ':' . $clientId]); }
        public function markDelivered(RealtimeEvent $event, string $clientId): void { $this->seen[$event->eventId() . ':' . $clientId] = true; }
    };
    $authorizer = new class() implements GroupMemberAccessRealtimeAuthorizer {
        public int $calls = 0;
        public function withCurrentEvent(RealtimeEvent $event, callable $delivery): void { ++$this->calls; $delivery(); }
    };
    $delivery = new RealtimeDeliveryService($provider, $gateway, $checkpoints, $authorizer);
    $delivery->deliver($event);
    $delivery->deliver($event);
    realtimeAssert($provider->calls === 0, 'group access event touched active-member gate');
    realtimeAssert($authorizer->calls === 2);
    realtimeAssert(count($gateway->invalidations) === 2, 'duplicate event did not keep session pin invalidated');
    realtimeAssert(count($gateway->packets) === 1, 'delivery checkpoint did not suppress duplicate packet');

    $staleAuthorizer = new class() implements GroupMemberAccessRealtimeAuthorizer {
        public function withCurrentEvent(RealtimeEvent $event, callable $delivery): void {}
    };
    $staleGateway = new class() implements RealtimeGateway, GroupMemberAccessSessionInvalidator {
        public int $calls = 0;
        public function clientIdsForOrganizationUser(int $organization, string $userId): array { ++$this->calls; return []; }
        public function invalidateGroupAccessSnapshot(string $clientId, string $currentSnapshotId): void { ++$this->calls; }
        public function sendToClient(string $clientId, string $packet): void { ++$this->calls; }
    };
    (new RealtimeDeliveryService($provider, $staleGateway, $checkpoints, $staleAuthorizer))->deliver($event);
    realtimeAssert($staleGateway->calls === 0, 'stale access event reached Gateway');

    $missingInvalidator = new class() implements RealtimeGateway {
        public function clientIdsForOrganizationUser(int $organization, string $userId): array { return []; }
        public function sendToClient(string $clientId, string $packet): void {}
    };
    try {
        (new RealtimeDeliveryService($provider, $missingInvalidator, $checkpoints, $authorizer))->deliver($event);
        throw new RuntimeException('expected missing session invalidator failure');
    } catch (RuntimeException $exception) {
        realtimeAssert(str_contains($exception->getMessage(), 'session invalidator'));
    }
});

realtimeTest('group access authorizer holds the authority lock and rejects stale epochs', static function (): void {
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
        json_encode(groupAccessRealtimePayload(), JSON_THROW_ON_ERROR),
    );
    $repository = new class() implements GroupMemberAccessRepository {
        public bool $inTransaction = false;
        public array $queries = [];
        public array $state = ['access_snapshot_id' => '12'];
        public array $member = [
            'access_version' => '5',
            'access_state' => 'revoked',
        ];
        public function transaction(callable $callback): mixed {
            $this->inTransaction = true;
            try { return $callback($this); } finally { $this->inTransaction = false; }
        }
        public function fetchOne(string $sql, array $params = []): ?array {
            $this->queries[] = $sql;
            if (str_contains($sql, 'FROM im_conversation_member')) { return $this->member; }
            if (str_contains($sql, 'FROM im_user_group_access_state')) { return $this->state; }
            if (str_contains($sql, 'FROM im_conversation')) { return ['conversation_id' => 'group:7:alpha']; }
            return null;
        }
        public function fetchAll(string $sql, array $params = []): array { return []; }
    };
    $authorizer = new DatabaseGroupMemberAccessRealtimeAuthorizer($repository);
    $deliveredInsideLock = false;
    $authorizer->withCurrentEvent($event, static function () use ($repository, &$deliveredInsideLock): void {
        $deliveredInsideLock = $repository->inTransaction;
    });
    realtimeAssert($deliveredInsideLock, 'current access event was not delivered inside its database lock');
    realtimeAssert(count($repository->queries) === 3, 'access authorizer did not lock exactly three authority rows');
    realtimeAssert(str_contains($repository->queries[0], 'FROM im_conversation') && str_contains($repository->queries[0], 'FOR UPDATE'), 'conversation was not locked first');
    realtimeAssert(str_contains($repository->queries[1], 'FROM im_conversation_member') && str_contains($repository->queries[1], 'FOR UPDATE'), 'member was not locked second');
    realtimeAssert(str_contains($repository->queries[2], 'FROM im_user_group_access_state') && str_contains($repository->queries[2], 'FOR UPDATE'), 'user snapshot was not locked last');
    $repository->state['access_snapshot_id'] = '13';
    $staleDelivered = false;
    $authorizer->withCurrentEvent($event, static function () use (&$staleDelivered): void {
        $staleDelivered = true;
    });
    realtimeAssert(!$staleDelivered, 'stale access epoch remained deliverable');
});

realtimeTest('group access projector rejects malformed state, bigint and identity tuples', static function (): void {
    $projector = new RealtimeEventProjector();
    foreach (['01', '18446744073709551616'] as $invalidDecimal) {
        $payload = groupAccessRealtimePayload();
        $payload['access_snapshot_id'] = $invalidDecimal;
        expectInvalidRealtime(static fn () => $projector->project(
            Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
            json_encode($payload, JSON_THROW_ON_ERROR),
        ));
    }
    $payload = groupAccessRealtimePayload();
    $payload['access_state'] = 'active';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));
    $payload = groupAccessRealtimePayload();
    $payload['target_organization'] = 8;
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));
    $payload = groupAccessRealtimePayload();
    $payload['reason'] = 'unknown';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));
});

realtimeTest('recipient organizations outside the event home are rejected before intersection', static function (): void {
    $payload = createdRealtimePayload();
    $payload['recipient_identities'][] = ['organization' => 8, 'user_id' => 'foreign'];
    $payload['recipient_count'] = 4;
    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    );
    $provider = new class() implements RealtimeRecipientProvider {
        use PassThroughRealtimeRecipientBoundary;

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array { return []; }
    };
    $gateway = new class() implements RealtimeGateway {
        public function clientIdsForOrganizationUser(int $organization, string $userId): array { return []; }
        public function sendToClient(string $clientId, string $packet): void {}
    };
    $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
        public function wasDelivered(RealtimeEvent $event, string $clientId): bool { return false; }
        public function markDelivered(RealtimeEvent $event, string $clientId): void {}
    };
    try {
        (new RealtimeDeliveryService($provider, $gateway, $checkpoints))->deliver($event);
        throw new RuntimeException('expected foreign recipient rejection');
    } catch (RuntimeException $exception) {
        realtimeAssert(str_contains($exception->getMessage(), 'outside its home projection'));
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
    $payload['recipient_identities'][] = ['organization' => 7, 'user_id' => 'recipient-1'];
    $payload['recipient_count'] = 4;
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

    foreach (['0', '01', 1, '', str_repeat('9', 21)] as $invalidSnapshotId) {
        $payload = createdRealtimePayload();
        $payload['cross_org_access_snapshot_id'] = $invalidSnapshotId;
        expectInvalidRealtime(static fn () => $projector->project(
            Constants::MQ_ROUTING_MESSAGE_CREATED,
            json_encode($payload, JSON_THROW_ON_ERROR),
        ));
    }

    $payload = mutationRealtimePayload(Constants::MQ_ROUTING_MESSAGE_EDITED);
    $payload['payload']['message']['conversation_id'] = 'group:7:other';
    expectInvalidRealtime(static fn () => $projector->project(
        Constants::MQ_ROUTING_MESSAGE_EDITED,
        json_encode($payload, JSON_THROW_ON_ERROR),
    ));
});

realtimeTest('created recipients are intersected with current organization members', static function (): void {
    $provider = new class() implements RealtimeRecipientProvider {
        use PassThroughRealtimeRecipientBoundary;

        public array $calls = [];

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
        {
            $this->calls[] = [$organization, $conversationId, $messageSeq];
            return [
                ['organization' => 7, 'user_id' => 'sender-1'],
                ['organization' => 7, 'user_id' => 'recipient-1'],
                ['organization' => 7, 'user_id' => 'current-not-in-event'],
            ];
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
    $payload['recipient_count'] = 2;
    $payload['recipient_identities'] = [
        ['organization' => 7, 'user_id' => 'recipient-1'],
        ['organization' => 7, 'user_id' => 'left-member'],
    ];
    $provider = new class() implements RealtimeRecipientProvider {
        use PassThroughRealtimeRecipientBoundary;

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
        {
            return [
                ['organization' => 7, 'user_id' => 'sender-1'],
                ['organization' => 7, 'user_id' => 'recipient-1'],
            ];
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
        use PassThroughRealtimeRecipientBoundary;

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
        {
            return [
                ['organization' => 7, 'user_id' => 'recipient-1'],
                ['organization' => 7, 'user_id' => 'left-member'],
            ];
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
        use PassThroughRealtimeRecipientBoundary;

        public int $calls = 0;

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
        {
            ++$this->calls;
            return [
                ['organization' => 7, 'user_id' => 'target-1'],
                ['organization' => 7, 'user_id' => 'other-1'],
            ];
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

    realtimeAssert($provider->calls === 1);
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

realtimeTest('Gateway fanout stays inside the authorization boundary callback', static function (): void {
    $provider = new class() implements RealtimeRecipientProvider {
        public bool $insideBoundary = false;

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
        {
            throw new RuntimeException('legacy recipient lookup must not be used for delivery');
        }

        public function withDeliverableIdentities(RealtimeEvent $event, callable $delivery): void
        {
            $this->insideBoundary = true;
            try {
                $delivery([['organization' => 7, 'user_id' => 'recipient-1']]);
            } finally {
                $this->insideBoundary = false;
            }
        }
    };
    $gateway = new class($provider) implements RealtimeGateway {
        public int $deliveries = 0;

        public function __construct(private readonly object $provider)
        {
        }

        public function clientIdsForOrganizationUser(int $organization, string $userId): array
        {
            realtimeAssert($this->provider->insideBoundary, 'Gateway lookup escaped authorization boundary');
            return ['recipient-client'];
        }

        public function sendToClient(string $clientId, string $packet): void
        {
            realtimeAssert($this->provider->insideBoundary, 'Gateway send escaped authorization boundary');
            ++$this->deliveries;
        }
    };
    $checkpoints = new class($provider) implements RealtimeDeliveryCheckpoint {
        public function __construct(private readonly object $provider)
        {
        }

        public function wasDelivered(RealtimeEvent $event, string $clientId): bool
        {
            realtimeAssert($this->provider->insideBoundary, 'checkpoint read escaped authorization boundary');
            return false;
        }

        public function markDelivered(RealtimeEvent $event, string $clientId): void
        {
            realtimeAssert($this->provider->insideBoundary, 'checkpoint write escaped authorization boundary');
        }
    };

    $event = (new RealtimeEventProjector())->project(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode(createdRealtimePayload(), JSON_THROW_ON_ERROR),
    );
    (new RealtimeDeliveryService($provider, $gateway, $checkpoints))->deliver($event);

    realtimeAssert($gateway->deliveries === 1 && !$provider->insideBoundary);
});

realtimeTest('stale realtime event is ACKed and retry state is cleared without Gateway access', static function (): void {
    $provider = new class() implements RealtimeRecipientProvider {
        public int $boundaries = 0;

        public function activeIdentities(int $organization, string $conversationId, int $messageSeq): array
        {
            throw new RuntimeException('legacy recipient lookup must not be used for delivery');
        }

        public function withDeliverableIdentities(RealtimeEvent $event, callable $delivery): void
        {
            ++$this->boundaries;
            $delivery([]);
        }
    };
    $gateway = new class() implements RealtimeGateway {
        public int $calls = 0;

        public function clientIdsForOrganizationUser(int $organization, string $userId): array
        {
            ++$this->calls;
            return [];
        }

        public function sendToClient(string $clientId, string $packet): void
        {
            ++$this->calls;
        }
    };
    $checkpoints = new class() implements RealtimeDeliveryCheckpoint {
        public int $calls = 0;

        public function wasDelivered(RealtimeEvent $event, string $clientId): bool
        {
            ++$this->calls;
            return false;
        }

        public function markDelivered(RealtimeEvent $event, string $clientId): void
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
    $handler = new RealtimeDeliveryHandler(
        new RealtimeEventProjector(),
        new RealtimeDeliveryService($provider, $gateway, $checkpoints),
        $counter,
        2,
    );
    $result = $handler->handle(
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        json_encode(createdRealtimePayload(), JSON_THROW_ON_ERROR),
    );

    realtimeAssert($result->outcome === RealtimeDeliveryResult::ACK);
    realtimeAssert($provider->boundaries === 1);
    realtimeAssert($gateway->calls === 0 && $checkpoints->calls === 0);
    realtimeAssert($counter->clears === 1 && $counter->increments === 0);
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
