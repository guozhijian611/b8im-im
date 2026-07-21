<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

final class FriendRequestRealtimeEvent
{
    public const CREATED = 'friend_request.created';
    public const ACCEPTED = 'friend_request.accepted';
    public const REJECTED = 'friend_request.rejected';
    private const EVENTS = ['created' => 1, 'accepted' => 2, 'rejected' => 3];
    private const DATA_KEYS = [
        'actor_organization', 'actor_user_id', 'create_time', 'cross_org_access_snapshot_id',
        'event', 'from_organization', 'from_user_id', 'handle_time', 'request_id', 'status',
        'target_organization', 'target_user_id', 'to_organization', 'to_user_id',
    ];

    private function __construct(
        public readonly string $raw,
        public readonly string $eventId,
        public readonly string $event,
        public readonly int $requestId,
        public readonly int $status,
        public readonly int $fromOrganization,
        public readonly string $fromUserId,
        public readonly int $toOrganization,
        public readonly string $toUserId,
        public readonly int $targetOrganization,
        public readonly string $targetUserId,
        public readonly int $actorOrganization,
        public readonly string $actorUserId,
        public readonly ?string $crossOrgAccessSnapshotId,
        public readonly string $createTime,
        public readonly ?string $handleTime,
    ) {
    }

    public static function fromRaw(string $raw): self
    {
        $envelope = json_decode($raw, true, flags: JSON_THROW_ON_ERROR);
        if (!is_array($envelope) || array_is_list($envelope)) {
            throw new \InvalidArgumentException('friend request envelope must be an object');
        }
        self::exactKeys($envelope, ['data', 'event_id', 'organization', 'type']);
        if (($envelope['type'] ?? null) !== 'friend_request') {
            throw new \InvalidArgumentException('friend request envelope type is invalid');
        }
        $eventId = $envelope['event_id'] ?? null;
        $data = $envelope['data'] ?? null;
        if (
            !is_string($eventId)
            || preg_match('/^[0-9a-f]{64}$/D', $eventId) !== 1
            || !is_array($data)
            || array_is_list($data)
        ) {
            throw new \InvalidArgumentException('friend request envelope identity is invalid');
        }
        self::exactKeys($data, self::DATA_KEYS);

        $transition = $data['event'] ?? null;
        $requestId = $data['request_id'] ?? null;
        $status = $data['status'] ?? null;
        if (
            !is_string($transition)
            || !isset(self::EVENTS[$transition])
            || !is_int($requestId)
            || $requestId <= 0
            || $requestId > 9007199254740991
            || !is_int($status)
            || $status !== self::EVENTS[$transition]
        ) {
            throw new \InvalidArgumentException('friend request transition is invalid');
        }

        $organization = self::organization($envelope['organization'] ?? null);
        $fromOrganization = self::organization($data['from_organization'] ?? null);
        $toOrganization = self::organization($data['to_organization'] ?? null);
        $targetOrganization = self::organization($data['target_organization'] ?? null);
        $actorOrganization = self::organization($data['actor_organization'] ?? null);
        $fromUserId = self::userId($data['from_user_id'] ?? null);
        $toUserId = self::userId($data['to_user_id'] ?? null);
        $targetUserId = self::userId($data['target_user_id'] ?? null);
        $actorUserId = self::userId($data['actor_user_id'] ?? null);
        if ($fromOrganization === $toOrganization && hash_equals($fromUserId, $toUserId)) {
            throw new \InvalidArgumentException('friend request identities must be distinct');
        }
        $snapshotId = $data['cross_org_access_snapshot_id'] ?? null;
        if ($fromOrganization === $toOrganization) {
            if ($snapshotId !== null) {
                throw new \InvalidArgumentException('same-organization friend event has an access snapshot');
            }
        } elseif (!self::uint64($snapshotId)) {
            throw new \InvalidArgumentException('cross-organization friend event has no access snapshot');
        }

        $event = 'friend_request.' . $transition;
        if ($event === self::CREATED) {
            if (
                $targetOrganization !== $toOrganization
                || !hash_equals($targetUserId, $toUserId)
                || $actorOrganization !== $fromOrganization
                || !hash_equals($actorUserId, $fromUserId)
                || $data['handle_time'] !== null
            ) {
                throw new \InvalidArgumentException('created friend event target or actor is invalid');
            }
        } elseif (
            $targetOrganization !== $fromOrganization
            || !hash_equals($targetUserId, $fromUserId)
            || $actorOrganization !== $toOrganization
            || !hash_equals($actorUserId, $toUserId)
            || !self::dateTime($data['handle_time'] ?? null)
        ) {
            throw new \InvalidArgumentException('handled friend event target or actor is invalid');
        }
        if ($organization !== $targetOrganization || !self::dateTime($data['create_time'] ?? null)) {
            throw new \InvalidArgumentException('friend request envelope home or time is invalid');
        }

        $expectedId = hash('sha256', json_encode([
            'friend_request.v1', $requestId, $event,
            (string) $fromOrganization, $fromUserId,
            (string) $toOrganization, $toUserId,
            (string) $targetOrganization, $targetUserId, $snapshotId,
        ], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR));
        if (!hash_equals($expectedId, $eventId)) {
            throw new \InvalidArgumentException('friend request event_id differs from its canonical tuple');
        }

        return new self(
            $raw, $eventId, $event, $requestId, $status,
            $fromOrganization, $fromUserId, $toOrganization, $toUserId,
            $targetOrganization, $targetUserId, $actorOrganization, $actorUserId,
            $snapshotId, (string) $data['create_time'],
            $data['handle_time'] === null ? null : (string) $data['handle_time'],
        );
    }

    public function packetData(): array
    {
        return [
            'event' => substr($this->event, strlen('friend_request.')),
            'event_id' => $this->eventId,
            'request_id' => $this->requestId,
            'status' => $this->status,
            'from_organization' => (string) $this->fromOrganization,
            'from_user_id' => $this->fromUserId,
            'to_organization' => (string) $this->toOrganization,
            'to_user_id' => $this->toUserId,
            'target_organization' => (string) $this->targetOrganization,
            'target_user_id' => $this->targetUserId,
            'actor_organization' => (string) $this->actorOrganization,
            'actor_user_id' => $this->actorUserId,
            'cross_org_access_snapshot_id' => $this->crossOrgAccessSnapshotId,
            'create_time' => $this->createTime,
            'handle_time' => $this->handleTime,
        ];
    }

    private static function exactKeys(array $value, array $expected): void
    {
        $actual = array_keys($value);
        sort($actual);
        sort($expected);
        if ($actual !== $expected) {
            throw new \InvalidArgumentException('friend request event shape is invalid');
        }
    }

    private static function organization(mixed $value): int
    {
        if (!is_string($value) || preg_match('/^[1-9][0-9]{0,9}$/D', $value) !== 1) {
            throw new \InvalidArgumentException('friend request organization is not canonical');
        }
        $number = (int) $value;
        if ($number <= 0 || $number > 4294967295 || (string) $number !== $value) {
            throw new \InvalidArgumentException('friend request organization is outside uint32');
        }
        return $number;
    }

    private static function userId(mixed $value): string
    {
        if (
            !is_string($value) || $value === '' || strlen($value) > 64 || trim($value) !== $value
            || str_contains($value, "\0") || str_contains($value, '|')
        ) {
            throw new \InvalidArgumentException('friend request user identity is invalid');
        }
        return $value;
    }

    private static function dateTime(mixed $value): bool
    {
        if (!is_string($value) || preg_match('/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$/D', $value) !== 1) {
            return false;
        }
        if ((int) substr($value, 0, 4) < 1000) {
            return false;
        }
        $parsed = \DateTimeImmutable::createFromFormat('!Y-m-d H:i:s', $value);
        return $parsed !== false && $parsed->format('Y-m-d H:i:s') === $value;
    }

    private static function uint64(mixed $value): bool
    {
        if (!is_string($value) || preg_match('/^[1-9][0-9]{0,19}$/D', $value) !== 1) {
            return false;
        }
        return strlen($value) < 20
            || strcmp($value, '18446744073709551615') <= 0;
    }
}
