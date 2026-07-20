<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Service\CrossOrganizationSocialPolicy;
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Support\Constants;
use JsonException;

final class RealtimeEventProjector
{
    private const MAX_USER_ID_BYTES = 64;
    private const MAX_MESSAGE_ID_BYTES = 40;
    private const MAX_CONVERSATION_ID_BYTES = 64;
    private const MAX_CLIENT_ID_BYTES = 128;

    private const SUPPORTED_EVENTS = [
        Constants::MQ_ROUTING_MESSAGE_CREATED,
        Constants::MQ_ROUTING_MESSAGE_RECALLED,
        Constants::MQ_ROUTING_MESSAGE_EDITED,
        Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH,
        Constants::MQ_ROUTING_MESSAGE_DELETED_SELF,
        Constants::MQ_ROUTING_MESSAGE_RECEIPT,
        Constants::MQ_ROUTING_CONVERSATION_READ,
        Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
    ];

    public function project(string $routingKey, string $body): RealtimeEvent
    {
        if (!in_array($routingKey, self::SUPPORTED_EVENTS, true)) {
            throw new InvalidRealtimeEvent('unsupported RabbitMQ routing key');
        }

        try {
            $payload = json_decode($body, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new InvalidRealtimeEvent('realtime event is not valid JSON', previous: $exception);
        }
        if (!is_array($payload) || array_is_list($payload)) {
            throw new InvalidRealtimeEvent('realtime event must be a JSON object');
        }

        $eventId = $this->string($payload, 'event_id', 64);
        if (preg_match('/^[a-f0-9]{64}$/', $eventId) !== 1) {
            throw new InvalidRealtimeEvent('event_id must be a lowercase sha256');
        }
        $eventType = $this->string($payload, 'event_type', 64);
        if ($eventType !== $routingKey) {
            throw new InvalidRealtimeEvent('event_type does not match routing key');
        }
        $organization = $this->integer($payload, 'organization', 1);
        $conversationId = $this->string($payload, 'conversation_id', self::MAX_CONVERSATION_ID_BYTES);
        $conversationType = $this->integer($payload, 'conversation_type', 1, 2);
        if ($eventType === Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED) {
            return $this->conversationAccessChanged(
                $payload,
                $eventId,
                $organization,
                $conversationId,
                $conversationType,
            );
        }
        $messageId = $this->string($payload, 'message_id', self::MAX_MESSAGE_ID_BYTES);
        $messageSeq = $this->integer($payload, 'message_seq', 1);
        $originOrganization = $this->integer($payload, 'origin_organization', 1);
        $originUserId = $this->string($payload, 'origin_user_id', self::MAX_USER_ID_BYTES);
        $originClientId = $this->string($payload, 'origin_client_id', self::MAX_CLIENT_ID_BYTES);
        $actorOrganization = $this->integer($payload, 'actor_organization', 1);
        $actorUserId = $this->string($payload, 'actor_user_id', self::MAX_USER_ID_BYTES);
        if ($actorOrganization !== $originOrganization || $actorUserId !== $originUserId) {
            throw new InvalidRealtimeEvent('actor identity must equal origin identity');
        }
        $this->string($payload, 'created_at', 32);
        $recipientIdentities = $this->identities($payload);
        $crossOrgAccessSnapshotId = $this->optionalPositiveDecimal(
            $payload,
            'cross_org_access_snapshot_id',
        );

        return match ($eventType) {
            Constants::MQ_ROUTING_MESSAGE_CREATED => $this->created(
                $payload,
                $eventId,
                $organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                $actorOrganization,
                $actorUserId,
                $originOrganization,
                $originUserId,
                $originClientId,
                $recipientIdentities,
                $crossOrgAccessSnapshotId,
            ),
            Constants::MQ_ROUTING_MESSAGE_RECALLED,
            Constants::MQ_ROUTING_MESSAGE_EDITED,
            Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH,
            Constants::MQ_ROUTING_MESSAGE_DELETED_SELF => $this->mutation(
                $payload,
                $eventId,
                $eventType,
                $organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                $actorOrganization,
                $actorUserId,
                $originOrganization,
                $originUserId,
                $originClientId,
                $recipientIdentities,
                $crossOrgAccessSnapshotId,
            ),
            Constants::MQ_ROUTING_MESSAGE_RECEIPT => $this->receipt(
                $payload,
                $eventId,
                $organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                $actorOrganization,
                $actorUserId,
                $originOrganization,
                $originUserId,
                $originClientId,
                $recipientIdentities,
                $crossOrgAccessSnapshotId,
            ),
            Constants::MQ_ROUTING_CONVERSATION_READ => $this->conversationRead(
                $payload,
                $eventId,
                $organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                $actorOrganization,
                $actorUserId,
                $originOrganization,
                $originUserId,
                $originClientId,
                $recipientIdentities,
                $crossOrgAccessSnapshotId,
            ),
            default => throw new InvalidRealtimeEvent('unsupported realtime event'),
        };
    }

    /** @param array<string,mixed> $payload */
    private function conversationAccessChanged(
        array $payload,
        string $eventId,
        int $organization,
        string $conversationId,
        int $conversationType,
    ): RealtimeEvent {
        if ($conversationType !== 1) {
            throw new InvalidRealtimeEvent('conversation.access_changed only supports single chat');
        }
        $snapshotId = $this->string($payload, 'cross_org_access_snapshot_id', 20);
        if (preg_match('/^[1-9][0-9]*$/', $snapshotId) !== 1) {
            throw new InvalidRealtimeEvent('cross_org_access_snapshot_id must be a positive canonical decimal string');
        }
        $allowed = $payload['allowed'] ?? null;
        if (!is_bool($allowed)) {
            throw new InvalidRealtimeEvent('allowed must be a boolean');
        }
        $targetOrganization = $this->integer($payload, 'target_organization', 1);
        $targetUserId = $this->string($payload, 'target_user_id', self::MAX_USER_ID_BYTES);
        $peerOrganization = $this->integer($payload, 'peer_organization', 1);
        $peerUserId = $this->string($payload, 'peer_user_id', self::MAX_USER_ID_BYTES);
        if (
            $targetOrganization !== $organization
            || $targetOrganization === $peerOrganization
        ) {
            throw new InvalidRealtimeEvent('access event target and peer identities are invalid');
        }
        if (
            CrossOrganizationSocialPolicy::singleConversationId(
                $targetOrganization,
                $targetUserId,
                $peerOrganization,
                $peerUserId,
            ) !== $conversationId
        ) {
            throw new InvalidRealtimeEvent('access event identity pair differs from conversation_id');
        }
        if (
            hash(
                'sha256',
                $organization . '|' . Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED
                    . '|' . $conversationId . '|' . $snapshotId,
            ) !== $eventId
        ) {
            throw new InvalidRealtimeEvent('access event_id differs from its canonical projection identity');
        }
        $this->string($payload, 'created_at', 32);
        $recipientIdentities = $this->identities($payload);
        if ($recipientIdentities !== [[
            'organization' => $targetOrganization,
            'user_id' => $targetUserId,
        ]]) {
            throw new InvalidRealtimeEvent('access event recipient differs from its target identity');
        }

        return $this->event(
            eventId: $eventId,
            eventType: Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: 1,
            messageId: '',
            messageSeq: 0,
            changeSeq: 0,
            actorOrganization: $organization,
            actorUserId: 'system',
            originOrganization: $organization,
            originUserId: 'system',
            originClientId: '',
            targetOrganization: $targetOrganization,
            targetUserId: $targetUserId,
            crossOrgAccessSnapshotId: $snapshotId,
            recipientIdentities: $recipientIdentities,
            packetCommand: Command::CONVERSATION_ACCESS_CHANGED,
            packetData: [
                'event_type' => Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
                'conversation_id' => $conversationId,
                'conversation_type' => 1,
                'cross_org_access_snapshot_id' => $snapshotId,
                'allowed' => $allowed,
                'target_organization' => $targetOrganization,
                'target_user_id' => $targetUserId,
                'peer_organization' => $peerOrganization,
                'peer_user_id' => $peerUserId,
            ],
        );
    }

    /** @param array<string,mixed> $payload */
    private function created(
        array $payload,
        string $eventId,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        int $actorOrganization,
        string $actorUserId,
        int $originOrganization,
        string $originUserId,
        string $originClientId,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId,
    ): RealtimeEvent {
        $senderOrganization = $this->integer($payload, 'sender_organization', 1);
        $senderId = $this->string($payload, 'sender_id', self::MAX_USER_ID_BYTES);
        $message = $this->message(
            $payload['message'] ?? null,
            $organization,
            $conversationId,
            $conversationType,
            $messageId,
            $messageSeq,
            $senderOrganization,
            $senderId,
        );
        if (($message['status'] ?? null) !== 'normal' || !is_array($message['content'] ?? null)) {
            throw new InvalidRealtimeEvent('message.created body must be a normal decoded message');
        }
        $globalSeq = $this->positiveDecimal($message, 'global_seq');
        if ($this->positiveDecimal($payload, 'global_seq') !== $globalSeq) {
            throw new InvalidRealtimeEvent('message.created global_seq differs from message');
        }

        return $this->event(
            eventId: $eventId,
            eventType: Constants::MQ_ROUTING_MESSAGE_CREATED,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: $conversationType,
            messageId: $messageId,
            messageSeq: $messageSeq,
            changeSeq: 0,
            actorOrganization: $actorOrganization,
            actorUserId: $actorUserId,
            originOrganization: $originOrganization,
            originUserId: $originUserId,
            originClientId: $originClientId,
            targetOrganization: null,
            targetUserId: null,
            crossOrgAccessSnapshotId: $crossOrgAccessSnapshotId,
            recipientIdentities: $recipientIdentities,
            packetCommand: Command::PUSH,
            packetData: [
                'event_type' => Constants::MQ_ROUTING_MESSAGE_CREATED,
                'message_id' => $messageId,
                'conversation_id' => $conversationId,
                'message_seq' => $messageSeq,
                'change_seq' => 0,
                'message' => $message,
                ...$this->accessSnapshotPacketData($crossOrgAccessSnapshotId),
            ],
        );
    }

    /** @param array<string,mixed> $payload */
    private function mutation(
        array $payload,
        string $eventId,
        string $eventType,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        int $actorOrganization,
        string $actorUserId,
        int $originOrganization,
        string $originUserId,
        string $originClientId,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId,
    ): RealtimeEvent {
        $changeSeq = $this->integer($payload, 'change_seq', 1);
        $targetUserId = $payload['target_user_id'] ?? null;
        $targetOrganization = $payload['target_organization'] ?? null;
        if (($targetUserId === null) !== ($targetOrganization === null)) {
            throw new InvalidRealtimeEvent('target identity must contain both organization and user_id');
        }
        if ($targetUserId !== null) {
            if (!is_string($targetUserId) || !is_int($targetOrganization) || $targetOrganization <= 0) {
                throw new InvalidRealtimeEvent('target identity is invalid');
            }
            $targetUserId = $this->boundedString(
                $targetUserId,
                'target_user_id',
                self::MAX_USER_ID_BYTES,
            );
        }
        $mutation = $payload['payload'] ?? null;
        if (!is_array($mutation) || array_is_list($mutation)) {
            throw new InvalidRealtimeEvent('mutation payload must be an object');
        }
        $packetCommand = match ($eventType) {
            Constants::MQ_ROUTING_MESSAGE_RECALLED => $this->validateRecall($mutation, $targetUserId),
            Constants::MQ_ROUTING_MESSAGE_EDITED => $this->validateEdit(
                $mutation,
                $targetUserId,
                $organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                $actorOrganization,
                $actorUserId,
            ),
            Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH => $this->validateDeletedBoth($mutation, $targetUserId),
            Constants::MQ_ROUTING_MESSAGE_DELETED_SELF => $this->validateDeletedSelf($mutation, $targetUserId),
            default => throw new InvalidRealtimeEvent('unsupported mutation'),
        };
        $packetData = array_merge($mutation, [
            'event_type' => $eventType,
            'message_id' => $messageId,
            'conversation_id' => $conversationId,
            'message_seq' => $messageSeq,
            'change_seq' => $changeSeq,
            'actor_organization' => $actorOrganization,
            'actor_user_id' => $actorUserId,
            'target_organization' => $targetOrganization,
            'target_user_id' => $targetUserId,
            ...$this->accessSnapshotPacketData($crossOrgAccessSnapshotId),
        ]);

        return $this->event(
            eventId: $eventId,
            eventType: $eventType,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: $conversationType,
            messageId: $messageId,
            messageSeq: $messageSeq,
            changeSeq: $changeSeq,
            actorOrganization: $actorOrganization,
            actorUserId: $actorUserId,
            originOrganization: $originOrganization,
            originUserId: $originUserId,
            originClientId: $originClientId,
            targetOrganization: $targetOrganization,
            targetUserId: $targetUserId,
            crossOrgAccessSnapshotId: $crossOrgAccessSnapshotId,
            recipientIdentities: $recipientIdentities,
            packetCommand: $packetCommand,
            packetData: $packetData,
        );
    }

    /** @param array<string,mixed> $payload */
    private function receipt(
        array $payload,
        string $eventId,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        int $actorOrganization,
        string $actorUserId,
        int $originOrganization,
        string $originUserId,
        string $originClientId,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId,
    ): RealtimeEvent {
        $receipt = $payload['receipt'] ?? null;
        if (!is_array($receipt) || array_is_list($receipt)) {
            throw new InvalidRealtimeEvent('message.receipt receipt must be an object');
        }
        $senderOrganization = $this->integer($payload, 'sender_organization', 1);
        $senderId = $this->string($payload, 'sender_id', self::MAX_USER_ID_BYTES);
        $userOrganization = $this->integer($payload, 'user_organization', 1);
        $userId = $this->string($payload, 'user_id', self::MAX_USER_ID_BYTES);
        if (
            $this->integer($receipt, 'sender_organization', 1) !== $senderOrganization
            || $this->string($receipt, 'sender_id', self::MAX_USER_ID_BYTES) !== $senderId
            || $this->integer($receipt, 'user_organization', 1) !== $userOrganization
            || $this->string($receipt, 'user_id', self::MAX_USER_ID_BYTES) !== $userId
            || $this->string($receipt, 'message_id', self::MAX_MESSAGE_ID_BYTES) !== $messageId
            || $this->string($receipt, 'conversation_id', self::MAX_CONVERSATION_ID_BYTES) !== $conversationId
            || $this->integer($receipt, 'message_seq', 1) !== $messageSeq
            || !in_array($receipt['status'] ?? null, ['delivered', 'read'], true)
        ) {
            throw new InvalidRealtimeEvent('message.receipt identity or state mismatch');
        }

        return $this->event(
            eventId: $eventId,
            eventType: Constants::MQ_ROUTING_MESSAGE_RECEIPT,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: $conversationType,
            messageId: $messageId,
            messageSeq: $messageSeq,
            changeSeq: 0,
            actorOrganization: $actorOrganization,
            actorUserId: $actorUserId,
            originOrganization: $originOrganization,
            originUserId: $originUserId,
            originClientId: $originClientId,
            targetOrganization: null,
            targetUserId: null,
            crossOrgAccessSnapshotId: $crossOrgAccessSnapshotId,
            recipientIdentities: $recipientIdentities,
            packetCommand: Command::ACK,
            packetData: $receipt + [
                'event_type' => Constants::MQ_ROUTING_MESSAGE_RECEIPT,
                ...$this->accessSnapshotPacketData($crossOrgAccessSnapshotId),
            ],
        );
    }

    /** @param array<string,mixed> $payload */
    private function conversationRead(
        array $payload,
        string $eventId,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        int $actorOrganization,
        string $actorUserId,
        int $originOrganization,
        string $originUserId,
        string $originClientId,
        array $recipientIdentities,
        ?string $crossOrgAccessSnapshotId,
    ): RealtimeEvent {
        $readState = $payload['read_state'] ?? null;
        if (!is_array($readState) || array_is_list($readState)) {
            throw new InvalidRealtimeEvent('conversation.read read_state must be an object');
        }
        $userOrganization = $this->integer($payload, 'user_organization', 1);
        $userId = $this->string($payload, 'user_id', self::MAX_USER_ID_BYTES);
        if (
            $this->integer($readState, 'user_organization', 1) !== $userOrganization
            || $this->string($readState, 'user_id', self::MAX_USER_ID_BYTES) !== $userId
            || $this->string($readState, 'conversation_id', self::MAX_CONVERSATION_ID_BYTES) !== $conversationId
            || $this->string($readState, 'last_read_message_id', self::MAX_MESSAGE_ID_BYTES) !== $messageId
            || $this->integer($readState, 'last_read_seq', 1) !== $messageSeq
        ) {
            throw new InvalidRealtimeEvent('conversation.read identity or cursor mismatch');
        }

        return $this->event(
            eventId: $eventId,
            eventType: Constants::MQ_ROUTING_CONVERSATION_READ,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: $conversationType,
            messageId: $messageId,
            messageSeq: $messageSeq,
            changeSeq: 0,
            actorOrganization: $actorOrganization,
            actorUserId: $actorUserId,
            originOrganization: $originOrganization,
            originUserId: $originUserId,
            originClientId: $originClientId,
            targetOrganization: null,
            targetUserId: null,
            crossOrgAccessSnapshotId: $crossOrgAccessSnapshotId,
            recipientIdentities: $recipientIdentities,
            packetCommand: Command::CONVERSATION_READ,
            packetData: $readState + [
                'event_type' => Constants::MQ_ROUTING_CONVERSATION_READ,
                ...$this->accessSnapshotPacketData($crossOrgAccessSnapshotId),
            ],
        );
    }

    private function event(
        string $eventId,
        string $eventType,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        int $changeSeq,
        int $actorOrganization,
        string $actorUserId,
        int $originOrganization,
        string $originUserId,
        string $originClientId,
        ?int $targetOrganization,
        ?string $targetUserId,
        ?string $crossOrgAccessSnapshotId,
        array $recipientIdentities,
        string $packetCommand,
        array $packetData,
    ): RealtimeEvent {
        return new RealtimeEvent(
            eventType: $eventType,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: $conversationType,
            messageId: $messageId,
            messageSeq: $messageSeq,
            changeSeq: $changeSeq,
            actorOrganization: $actorOrganization,
            actorUserId: $actorUserId,
            originOrganization: $originOrganization,
            originUserId: $originUserId,
            originClientId: $originClientId,
            targetOrganization: $targetOrganization,
            targetUserId: $targetUserId,
            crossOrgAccessSnapshotId: $crossOrgAccessSnapshotId,
            recipientIdentities: $recipientIdentities,
            packetCommand: $packetCommand,
            packetData: $packetData,
            stableEventId: $eventId,
        );
    }

    private function validateRecall(array $payload, ?string $targetUserId): string
    {
        if ($targetUserId !== null || ($payload['status'] ?? null) !== 'recalled') {
            throw new InvalidRealtimeEvent('message.recalled schema is invalid');
        }

        return Command::RECALL;
    }

    private function validateEdit(
        array $payload,
        ?string $targetUserId,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        int $actorOrganization,
        string $actorUserId,
    ): string {
        if ($targetUserId !== null || !is_array($payload['content'] ?? null)) {
            throw new InvalidRealtimeEvent('message.edited content or target is invalid');
        }
        $this->string($payload, 'edit_time', 32);
        $this->integer($payload, 'edit_count', 1);
        $message = $this->message(
            $payload['message'] ?? null,
            $organization,
            $conversationId,
            $conversationType,
            $messageId,
            $messageSeq,
            $actorOrganization,
            $actorUserId,
        );
        if (
            ($message['status'] ?? null) !== 'normal'
            || ($message['content'] ?? null) !== $payload['content']
            || ($message['edit_time'] ?? null) !== $payload['edit_time']
            || ($message['edit_count'] ?? null) !== $payload['edit_count']
        ) {
            throw new InvalidRealtimeEvent('message.edited body differs from mutation');
        }

        return Command::EDIT;
    }

    private function validateDeletedBoth(array $payload, ?string $targetUserId): string
    {
        if (
            $targetUserId !== null
            || ($payload['scope'] ?? null) !== 'both'
            || ($payload['status'] ?? null) !== 'deleted_both'
        ) {
            throw new InvalidRealtimeEvent('message.deleted_both schema is invalid');
        }

        return Command::DELETE;
    }

    private function validateDeletedSelf(array $payload, ?string $targetUserId): string
    {
        if ($targetUserId === null || ($payload['scope'] ?? null) !== 'self') {
            throw new InvalidRealtimeEvent('message.deleted_self schema is invalid');
        }

        return Command::DELETE;
    }

    /** @return array<string,mixed> */
    private function message(
        mixed $message,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        int $senderOrganization,
        string $senderId,
    ): array {
        if (!is_array($message) || array_is_list($message)) {
            throw new InvalidRealtimeEvent('message must be an object');
        }
        if (
            $this->integer($message, 'organization', 1) !== $organization
            || $this->string($message, 'conversation_id', self::MAX_CONVERSATION_ID_BYTES) !== $conversationId
            || $this->integer($message, 'conversation_type', 1, 2) !== $conversationType
            || $this->string($message, 'message_id', self::MAX_MESSAGE_ID_BYTES) !== $messageId
            || $this->integer($message, 'message_seq', 1) !== $messageSeq
            || $this->integer($message, 'sender_organization', 1) !== $senderOrganization
            || $this->string($message, 'sender_id', self::MAX_USER_ID_BYTES) !== $senderId
        ) {
            throw new InvalidRealtimeEvent('message envelope and identity differ');
        }
        $this->positiveDecimal($message, 'global_seq');
        $this->string($message, 'client_msg_id', 80);
        $this->integer($message, 'message_type', 1);
        $this->string($message, 'create_time', 32);

        return $message;
    }

    /** @return list<array{organization:int,user_id:string}> */
    private function identities(array $payload): array
    {
        $raw = $payload['recipient_identities'] ?? null;
        if (!is_array($raw) || !array_is_list($raw)) {
            throw new InvalidRealtimeEvent('recipient_identities must be a list');
        }
        $identities = [];
        foreach ($raw as $identity) {
            if (!is_array($identity) || array_is_list($identity)) {
                throw new InvalidRealtimeEvent('recipient identity must be an object');
            }
            $organization = $this->integer($identity, 'organization', 1);
            $userId = $this->string($identity, 'user_id', self::MAX_USER_ID_BYTES);
            $key = $organization . ':' . $userId;
            if (isset($identities[$key])) {
                throw new InvalidRealtimeEvent('recipient identity list contains duplicates');
            }
            $identities[$key] = ['organization' => $organization, 'user_id' => $userId];
        }
        if ($this->integer($payload, 'recipient_count', 0) !== count($identities)) {
            throw new InvalidRealtimeEvent('recipient_count does not match recipient_identities');
        }

        return array_values($identities);
    }

    private function integer(array $payload, string $key, int $min, int $max = PHP_INT_MAX): int
    {
        $value = $payload[$key] ?? null;
        if (!is_int($value) || $value < $min || $value > $max) {
            throw new InvalidRealtimeEvent($key . ' must be an integer in the allowed range');
        }

        return $value;
    }

    private function string(array $payload, string $key, int $maxBytes): string
    {
        $value = $payload[$key] ?? null;
        if (!is_string($value)) {
            throw new InvalidRealtimeEvent($key . ' must be a string');
        }

        return $this->boundedString($value, $key, $maxBytes);
    }

    private function boundedString(string $value, string $key, int $maxBytes): string
    {
        if ($value === '' || strlen($value) > $maxBytes || str_contains($value, "\0")) {
            throw new InvalidRealtimeEvent($key . ' is empty or exceeds its byte limit');
        }

        return $value;
    }

    private function positiveDecimal(array $payload, string $key): string
    {
        $value = $this->string($payload, $key, 20);
        if (preg_match('/^[1-9][0-9]*$/', $value) !== 1) {
            throw new InvalidRealtimeEvent($key . ' must be a positive decimal string');
        }

        return $value;
    }

    private function optionalPositiveDecimal(array $payload, string $key): ?string
    {
        if (!array_key_exists($key, $payload)) {
            return null;
        }

        return $this->positiveDecimal($payload, $key);
    }

    /** @return array{cross_org_access_snapshot_id:string}|array{} */
    private function accessSnapshotPacketData(?string $snapshotId): array
    {
        return $snapshotId === null ? [] : ['cross_org_access_snapshot_id' => $snapshotId];
    }
}
