<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

final class RealtimeAuthEvent
{
    public const SESSION_REVOKED = 'auth.session_revoked';
    public const DEVICE_DISABLED = 'auth.device_disabled';
    public const ORGANIZATION_DISABLED = 'auth.organization_disabled';
    public const ORGANIZATION_ENABLED = 'auth.organization_enabled';

    private const TYPES = [
        self::SESSION_REVOKED,
        self::DEVICE_DISABLED,
        self::ORGANIZATION_DISABLED,
        self::ORGANIZATION_ENABLED,
    ];

    /**
     * @param list<string> $credentialSessionIds
     */
    private function __construct(
        public readonly string $type,
        public readonly int $organization,
        public readonly string $userId,
        public readonly string $deviceId,
        public readonly ?string $clientId,
        public readonly ?string $connectionSessionId,
        public readonly array $credentialSessionIds,
    ) {
    }

    /** @param array<string, mixed> $envelope */
    public static function fromEnvelope(array $envelope): self
    {
        $type = $envelope['type'] ?? null;
        if (!is_string($type) || !in_array($type, self::TYPES, true)) {
            throw new \InvalidArgumentException('unsupported realtime auth event type');
        }

        $organization = $envelope['organization'] ?? null;
        if (!is_int($organization) || $organization <= 0) {
            throw new \InvalidArgumentException('realtime auth organization must be a positive integer');
        }

        if (!array_key_exists('data', $envelope)) {
            throw new \InvalidArgumentException('realtime auth data is required');
        }
        $data = $envelope['data'];
        if (!is_array($data) || ($data !== [] && array_is_list($data))) {
            throw new \InvalidArgumentException('realtime auth data must be an object');
        }
        if (array_key_exists('organization', $data)) {
            throw new \InvalidArgumentException('realtime auth organization must have one authoritative location');
        }
        if (array_key_exists('occurred_at', $data)) {
            self::boundedString($data['occurred_at'], 'occurred_at', 32);
        }

        if (in_array($type, [self::ORGANIZATION_DISABLED, self::ORGANIZATION_ENABLED], true)) {
            foreach (['user_id', 'device_id', 'client_id', 'connection_session_id', 'credential_session_ids'] as $field) {
                if (array_key_exists($field, $data)) {
                    throw new \InvalidArgumentException('organization auth event must not contain connection targeting fields');
                }
            }

            return new self($type, $organization, '', '', null, null, []);
        }

        $userId = self::boundedString($data['user_id'] ?? null, 'user_id', 64);
        $deviceId = self::boundedString($data['device_id'] ?? null, 'device_id', 100);
        $clientId = self::nullableString($data, 'client_id', 128);
        $connectionSessionId = self::nullableString($data, 'connection_session_id', 32);
        if ($connectionSessionId !== null && preg_match('/^[a-f0-9]{32}$/', $connectionSessionId) !== 1) {
            throw new \InvalidArgumentException('connection_session_id must be a 128-bit lowercase hexadecimal value');
        }
        $credentialSessionIds = self::credentialSessionIds($data);
        if ($type === self::SESSION_REVOKED && count($credentialSessionIds) !== 1) {
            throw new \InvalidArgumentException('session_revoked must target exactly one credential session');
        }

        return new self(
            $type,
            $organization,
            $userId,
            $deviceId,
            $clientId,
            $connectionSessionId,
            $credentialSessionIds,
        );
    }

    public static function supports(mixed $type): bool
    {
        return is_string($type) && in_array($type, self::TYPES, true);
    }

    public function isOrganizationEvent(): bool
    {
        return in_array($this->type, [self::ORGANIZATION_DISABLED, self::ORGANIZATION_ENABLED], true);
    }

    /** @param array<string, mixed> $data */
    private static function nullableString(array $data, string $key, int $maxBytes): ?string
    {
        if (!array_key_exists($key, $data) || $data[$key] === null) {
            return null;
        }

        return self::boundedString($data[$key], $key, $maxBytes);
    }

    private static function boundedString(mixed $value, string $key, int $maxBytes): string
    {
        if (!is_string($value)) {
            throw new \InvalidArgumentException($key . ' must be a string');
        }
        $value = trim($value);
        if ($value === '' || strlen($value) > $maxBytes || preg_match('/[\x00-\x1f\x7f]/', $value) === 1) {
            throw new \InvalidArgumentException($key . ' is empty, too long, or contains control bytes');
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $data
     * @return list<string>
     */
    private static function credentialSessionIds(array $data): array
    {
        $values = $data['credential_session_ids'] ?? null;
        if (!is_array($values) || !array_is_list($values)) {
            throw new \InvalidArgumentException('credential_session_ids must be a list');
        }

        $result = [];
        foreach ($values as $value) {
            $value = self::boundedString($value, 'credential_session_id', 128);
            if (isset($result[$value])) {
                throw new \InvalidArgumentException('credential_session_ids must not contain duplicates');
            }
            $result[$value] = $value;
        }

        return array_values($result);
    }
}
