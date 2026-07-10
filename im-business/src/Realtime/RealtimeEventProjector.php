<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

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

        $eventType = $this->string($payload, 'event_type', 64);
        if ($eventType !== $routingKey || !in_array($eventType, self::SUPPORTED_EVENTS, true)) {
            throw new InvalidRealtimeEvent('event_type does not match the supported routing key');
        }

        $organization = $this->integer($payload, 'organization', 1);
        $conversationId = $this->string($payload, 'conversation_id', self::MAX_CONVERSATION_ID_BYTES);
        $conversationType = $this->integer($payload, 'conversation_type', 1, 2);
        $messageId = $this->string($payload, 'message_id', self::MAX_MESSAGE_ID_BYTES);
        $messageSeq = $this->integer($payload, 'message_seq', 1);
        $originUserId = $this->string($payload, 'origin_user_id', self::MAX_USER_ID_BYTES);
        $originClientId = $this->string($payload, 'origin_client_id', self::MAX_CLIENT_ID_BYTES);
        $this->string($payload, 'created_at', 32);

        if ($eventType === Constants::MQ_ROUTING_MESSAGE_CREATED) {
            return $this->created(
                $payload,
                $eventType,
                $organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                $originUserId,
                $originClientId,
            );
        }

        return $this->mutation(
            $payload,
            $eventType,
            $organization,
            $conversationId,
            $conversationType,
            $messageId,
            $messageSeq,
            $originUserId,
            $originClientId,
        );
    }

    /** @param array<string, mixed> $payload */
    private function created(
        array $payload,
        string $eventType,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        string $originUserId,
        string $originClientId,
    ): RealtimeEvent {
        $senderId = $this->string($payload, 'sender_id', self::MAX_USER_ID_BYTES);
        $actorUserId = $this->string($payload, 'actor_user_id', self::MAX_USER_ID_BYTES);
        if ($actorUserId !== $originUserId) {
            throw new InvalidRealtimeEvent('message.created origin_user_id must equal actor_user_id');
        }
        $message = $payload['message'] ?? null;
        if (!is_array($message) || array_is_list($message)) {
            throw new InvalidRealtimeEvent('message.created message must be an object');
        }

        if (
            $this->integer($message, 'organization', 1) !== $organization
            || $this->string($message, 'conversation_id', self::MAX_CONVERSATION_ID_BYTES) !== $conversationId
            || $this->integer($message, 'conversation_type', 1, 2) !== $conversationType
            || $this->string($message, 'message_id', self::MAX_MESSAGE_ID_BYTES) !== $messageId
            || $this->integer($message, 'message_seq', 1) !== $messageSeq
            || $this->string($message, 'sender_id', self::MAX_USER_ID_BYTES) !== $senderId
        ) {
            throw new InvalidRealtimeEvent('message.created envelope and message identity differ');
        }
        $globalSeq = $this->string($message, 'global_seq', 20);
        $envelopeGlobalSeq = $this->string($payload, 'global_seq', 20);
        if (preg_match('/^[1-9][0-9]*$/', $globalSeq) !== 1 || $envelopeGlobalSeq !== $globalSeq) {
            throw new InvalidRealtimeEvent('message.created global_seq must be a positive decimal string');
        }
        $this->string($message, 'client_msg_id', 80);
        $this->integer($message, 'message_type', 1);
        if (!is_array($message['content'] ?? null)) {
            throw new InvalidRealtimeEvent('message.created content must be a safe decoded object');
        }
        if (($message['status'] ?? null) !== 'normal') {
            throw new InvalidRealtimeEvent('message.created status must be normal');
        }
        $this->string($message, 'create_time', 32);

        $recipientUserIds = $this->userIdList($payload, 'recipient_user_ids');
        if ($this->integer($payload, 'recipient_count', 0) !== count($recipientUserIds)) {
            throw new InvalidRealtimeEvent('message.created recipient_count does not match recipient_user_ids');
        }
        $packetData = [
            'event_type' => $eventType,
            'message_id' => $messageId,
            'conversation_id' => $conversationId,
            'message_seq' => $messageSeq,
            'change_seq' => 0,
            'message' => $message,
        ];

        return new RealtimeEvent(
            eventType: $eventType,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: $conversationType,
            messageId: $messageId,
            messageSeq: $messageSeq,
            changeSeq: 0,
            actorUserId: $actorUserId,
            originUserId: $originUserId,
            originClientId: $originClientId,
            targetUserId: null,
            recipientUserIds: $recipientUserIds,
            packetCommand: Command::PUSH,
            packetData: $packetData,
        );
    }

    /** @param array<string, mixed> $payload */
    private function mutation(
        array $payload,
        string $eventType,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        string $originUserId,
        string $originClientId,
    ): RealtimeEvent {
        $changeSeq = $this->integer($payload, 'change_seq', 1);
        $actorUserId = $this->string($payload, 'actor_user_id', self::MAX_USER_ID_BYTES);
        if ($actorUserId !== $originUserId) {
            throw new InvalidRealtimeEvent('mutation origin_user_id must equal actor_user_id');
        }
        if (!array_key_exists('target_user_id', $payload)) {
            throw new InvalidRealtimeEvent('target_user_id is required');
        }
        $targetUserId = $payload['target_user_id'];
        if ($targetUserId !== null) {
            if (!is_string($targetUserId)) {
                throw new InvalidRealtimeEvent('target_user_id must be null or string');
            }
            $targetUserId = $this->boundedString($targetUserId, 'target_user_id', self::MAX_USER_ID_BYTES);
        }
        $mutationPayload = $payload['payload'] ?? null;
        if (!is_array($mutationPayload) || array_is_list($mutationPayload)) {
            throw new InvalidRealtimeEvent('mutation payload must be an object');
        }

        $packetCommand = match ($eventType) {
            Constants::MQ_ROUTING_MESSAGE_RECALLED => $this->validateRecall($mutationPayload, $targetUserId),
            Constants::MQ_ROUTING_MESSAGE_EDITED => $this->validateEdit($mutationPayload, $targetUserId),
            Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH => $this->validateDeletedBoth($mutationPayload, $targetUserId),
            Constants::MQ_ROUTING_MESSAGE_DELETED_SELF => $this->validateDeletedSelf($mutationPayload, $targetUserId),
            default => throw new InvalidRealtimeEvent('unsupported mutation event'),
        };

        $packetData = array_merge($mutationPayload, [
            'event_type' => $eventType,
            'message_id' => $messageId,
            'conversation_id' => $conversationId,
            'message_seq' => $messageSeq,
            'change_seq' => $changeSeq,
            'actor_user_id' => $actorUserId,
            'target_user_id' => $targetUserId,
        ]);
        if ($eventType === Constants::MQ_ROUTING_MESSAGE_EDITED) {
            $packetData['message'] = $this->editedMessage(
                $mutationPayload,
                $organization,
                $conversationId,
                $conversationType,
                $messageId,
                $messageSeq,
                $actorUserId,
            );
        }

        return new RealtimeEvent(
            eventType: $eventType,
            organization: $organization,
            conversationId: $conversationId,
            conversationType: $conversationType,
            messageId: $messageId,
            messageSeq: $messageSeq,
            changeSeq: $changeSeq,
            actorUserId: $actorUserId,
            originUserId: $originUserId,
            originClientId: $originClientId,
            targetUserId: $targetUserId,
            recipientUserIds: [],
            packetCommand: $packetCommand,
            packetData: $packetData,
        );
    }

    /** @param array<string, mixed> $payload */
    private function validateRecall(array $payload, ?string $targetUserId): string
    {
        if ($targetUserId !== null || ($payload['status'] ?? null) !== 'recalled') {
            throw new InvalidRealtimeEvent('message.recalled schema is invalid');
        }

        return Command::RECALL;
    }

    /** @param array<string, mixed> $payload */
    private function validateEdit(array $payload, ?string $targetUserId): string
    {
        if ($targetUserId !== null || !is_array($payload['content'] ?? null)) {
            throw new InvalidRealtimeEvent('message.edited content or target is invalid');
        }
        $this->string($payload, 'edit_time', 32);
        $this->integer($payload, 'edit_count', 1);

        return Command::EDIT;
    }

    /**
     * @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private function editedMessage(
        array $payload,
        int $organization,
        string $conversationId,
        int $conversationType,
        string $messageId,
        int $messageSeq,
        string $actorUserId,
    ): array {
        $message = $payload['message'] ?? null;
        if (!is_array($message) || array_is_list($message)) {
            throw new InvalidRealtimeEvent('message.edited message must be an object');
        }
        if (
            $this->integer($message, 'organization', 1) !== $organization
            || $this->string($message, 'conversation_id', self::MAX_CONVERSATION_ID_BYTES) !== $conversationId
            || $this->integer($message, 'conversation_type', 1, 2) !== $conversationType
            || $this->string($message, 'message_id', self::MAX_MESSAGE_ID_BYTES) !== $messageId
            || $this->integer($message, 'message_seq', 1) !== $messageSeq
            || $this->string($message, 'sender_id', self::MAX_USER_ID_BYTES) !== $actorUserId
        ) {
            throw new InvalidRealtimeEvent('message.edited envelope and message identity differ');
        }
        $globalSeq = $this->string($message, 'global_seq', 20);
        if (preg_match('/^[1-9][0-9]*$/', $globalSeq) !== 1) {
            throw new InvalidRealtimeEvent('message.edited global_seq must be a positive decimal string');
        }
        $this->string($message, 'client_msg_id', 80);
        if ($this->integer($message, 'message_type', 1) !== 1) {
            throw new InvalidRealtimeEvent('message.edited message_type must be text');
        }
        if (
            !is_array($message['content'] ?? null)
            || $message['content'] !== $payload['content']
            || ($message['status'] ?? null) !== 'normal'
            || ($message['edit_time'] ?? null) !== $payload['edit_time']
            || ($message['edit_count'] ?? null) !== $payload['edit_count']
        ) {
            throw new InvalidRealtimeEvent('message.edited body does not match its mutation payload');
        }
        $this->string($message, 'create_time', 32);
        $this->string($message, 'update_time', 32);

        return $message;
    }

    /** @param array<string, mixed> $payload */
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

    /** @param array<string, mixed> $payload */
    private function validateDeletedSelf(array $payload, ?string $targetUserId): string
    {
        if ($targetUserId === null || ($payload['scope'] ?? null) !== 'self') {
            throw new InvalidRealtimeEvent('message.deleted_self schema is invalid');
        }

        return Command::DELETE;
    }

    /** @param array<string, mixed> $payload */
    private function integer(array $payload, string $key, int $min, int $max = PHP_INT_MAX): int
    {
        $value = $payload[$key] ?? null;
        if (!is_int($value) || $value < $min || $value > $max) {
            throw new InvalidRealtimeEvent($key . ' must be an integer in the allowed range');
        }

        return $value;
    }

    /** @param array<string, mixed> $payload */
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
        $value = trim($value);
        if ($value === '' || strlen($value) > $maxBytes || preg_match('/[\x00-\x1f\x7f]/', $value) === 1) {
            throw new InvalidRealtimeEvent($key . ' is empty, too long, or contains control bytes');
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $payload
     * @return list<string>
     */
    private function userIdList(array $payload, string $key): array
    {
        $value = $payload[$key] ?? null;
        if (!is_array($value) || !array_is_list($value)) {
            throw new InvalidRealtimeEvent($key . ' must be a list');
        }

        $result = [];
        foreach ($value as $userId) {
            if (!is_string($userId)) {
                throw new InvalidRealtimeEvent($key . ' must contain only strings');
            }
            $userId = $this->boundedString($userId, $key, self::MAX_USER_ID_BYTES);
            if (isset($result[$userId])) {
                throw new InvalidRealtimeEvent($key . ' must not contain duplicate user IDs');
            }
            $result[$userId] = $userId;
        }

        return array_values($result);
    }
}
