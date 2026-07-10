<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 鉴权上下文
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use InvalidArgumentException;

final class AuthContext
{
    public function __construct(
        public readonly int $organization,
        public readonly string $userId,
        public readonly string $deviceId,
        public readonly string $clientId,
        public readonly string $credentialSessionId,
        public readonly string $sessionId,
        public readonly string $clientFamily,
        public readonly string $os,
        public readonly string $issuer,
        public readonly string $audience,
        public readonly int $notBefore,
        public readonly int $expireAt,
        public readonly string $username = '',
    ) {
        if ($organization <= 0) {
            throw new InvalidArgumentException('organization must be a positive integer');
        }

        foreach ([
            'user_id' => $userId,
            'device_id' => $deviceId,
            'client_id' => $clientId,
            'credential_session_id' => $credentialSessionId,
            'client_family' => $clientFamily,
            'os' => $os,
            'issuer' => $issuer,
            'audience' => $audience,
        ] as $field => $value) {
            if (trim($value) === '') {
                throw new InvalidArgumentException($field . ' cannot be empty');
            }
        }

        if ($notBefore <= 0 || $expireAt <= $notBefore) {
            throw new InvalidArgumentException('token validity window is invalid');
        }
        if (!in_array($clientFamily, ['web', 'app', 'desktop'], true)) {
            throw new InvalidArgumentException('client_family is invalid');
        }
        if (!in_array($os, ['browser', 'android', 'ios', 'windows', 'macos', 'linux', 'other'], true)) {
            throw new InvalidArgumentException('os is invalid');
        }
        $validClientOs = match ($clientFamily) {
            'web' => $os === 'browser',
            'app' => in_array($os, ['android', 'ios', 'other'], true),
            'desktop' => in_array($os, ['windows', 'macos', 'linux', 'other'], true),
        };
        if (!$validClientOs) {
            throw new InvalidArgumentException('client_family and os are inconsistent');
        }
    }

    public function uid(): string
    {
        return self::uidFor($this->organization, $this->userId);
    }

    public function withSessionId(string $sessionId): self
    {
        if (!preg_match('/^[a-f0-9]{32}$/', $sessionId)) {
            throw new InvalidArgumentException('session_id must be a 128-bit lowercase hexadecimal value');
        }

        return new self(
            organization: $this->organization,
            userId: $this->userId,
            deviceId: $this->deviceId,
            clientId: $this->clientId,
            credentialSessionId: $this->credentialSessionId,
            sessionId: $sessionId,
            clientFamily: $this->clientFamily,
            os: $this->os,
            issuer: $this->issuer,
            audience: $this->audience,
            notBefore: $this->notBefore,
            expireAt: $this->expireAt,
            username: $this->username,
        );
    }

    /**
     * @return array<string, int|string>
     */
    public function toArray(): array
    {
        return [
            'organization' => $this->organization,
            'user_id' => $this->userId,
            'device_id' => $this->deviceId,
            'client_id' => $this->clientId,
            'credential_session_id' => $this->credentialSessionId,
            'session_id' => $this->sessionId,
            'client_family' => $this->clientFamily,
            'os' => $this->os,
            'issuer' => $this->issuer,
            'audience' => $this->audience,
            'not_before' => $this->notBefore,
            'expire_at' => $this->expireAt,
            'username' => $this->username,
        ];
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            organization: (int) ($data['organization'] ?? 0),
            userId: (string) ($data['user_id'] ?? ''),
            deviceId: (string) ($data['device_id'] ?? ''),
            clientId: (string) ($data['client_id'] ?? ''),
            credentialSessionId: (string) ($data['credential_session_id'] ?? ''),
            sessionId: (string) ($data['session_id'] ?? ''),
            clientFamily: (string) ($data['client_family'] ?? ''),
            os: (string) ($data['os'] ?? ''),
            issuer: (string) ($data['issuer'] ?? ''),
            audience: (string) ($data['audience'] ?? ''),
            notBefore: (int) ($data['not_before'] ?? 0),
            expireAt: (int) ($data['expire_at'] ?? 0),
            username: (string) ($data['username'] ?? ''),
        );
    }

    public static function uidFor(int $organization, string $userId): string
    {
        return $organization . ':' . $userId;
    }
}
