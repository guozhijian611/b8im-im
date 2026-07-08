<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 鉴权上下文
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

final class AuthContext
{
    public function __construct(
        public readonly int $organization,
        public readonly string $userId,
        public readonly string $deviceId,
        public readonly string $platform = '',
        public readonly string $username = '',
        public readonly int $expireAt = 0,
    ) {
    }

    public function uid(): string
    {
        return self::uidFor($this->organization, $this->userId);
    }

    public function toArray(): array
    {
        return [
            'organization' => $this->organization,
            'user_id' => $this->userId,
            'device_id' => $this->deviceId,
            'platform' => $this->platform,
            'username' => $this->username,
            'expire_at' => $this->expireAt,
        ];
    }

    public static function fromArray(array $data): self
    {
        return new self(
            organization: (int) ($data['organization'] ?? 0),
            userId: (string) ($data['user_id'] ?? ''),
            deviceId: (string) ($data['device_id'] ?? ''),
            platform: (string) ($data['platform'] ?? ''),
            username: (string) ($data['username'] ?? ''),
            expireAt: (int) ($data['expire_at'] ?? 0),
        );
    }

    public static function uidFor(int $organization, string $userId): string
    {
        return $organization . ':' . $userId;
    }
}
