<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM token 签发与校验
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;

final class ImToken
{
    public function __construct(private readonly Config $config)
    {
    }

    public function verify(string $token, int $packetOrganization, array $authData): AuthContext
    {
        if ($token === '') {
            throw new ImException('缺少 IM token', 'AUTH_TOKEN_EMPTY');
        }

        if ($this->config->allowInsecureToken && str_starts_with($token, 'dev.')) {
            return $this->verifyDevToken(substr($token, 4), $packetOrganization, $authData);
        }

        if ($this->config->imTokenSecret === '') {
            throw new ImException('服务端未配置 IM_TOKEN_SECRET', 'AUTH_SECRET_EMPTY');
        }

        $parts = explode('.', $token);
        if (count($parts) !== 3 || $parts[0] !== 'v1') {
            throw new ImException('IM token 格式错误', 'AUTH_TOKEN_FORMAT');
        }

        [$version, $payloadBase64, $signature] = $parts;
        $expected = self::base64UrlEncode(hash_hmac('sha256', $version . '.' . $payloadBase64, $this->config->imTokenSecret, true));
        if (!hash_equals($expected, $signature)) {
            throw new ImException('IM token 签名无效', 'AUTH_TOKEN_SIGNATURE');
        }

        $payload = json_decode(self::base64UrlDecode($payloadBase64), true);
        if (!is_array($payload)) {
            throw new ImException('IM token 载荷无效', 'AUTH_TOKEN_PAYLOAD');
        }

        return $this->contextFromPayload($payload, $packetOrganization, $authData);
    }

    public function generate(array $payload): string
    {
        if ($this->config->imTokenSecret === '') {
            throw new ImException('服务端未配置 IM_TOKEN_SECRET', 'AUTH_SECRET_EMPTY');
        }

        $payloadBase64 = self::base64UrlEncode(json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR));
        $signature = self::base64UrlEncode(hash_hmac('sha256', 'v1.' . $payloadBase64, $this->config->imTokenSecret, true));

        return 'v1.' . $payloadBase64 . '.' . $signature;
    }

    private function verifyDevToken(string $payloadBase64, int $packetOrganization, array $authData): AuthContext
    {
        $payload = json_decode(self::base64UrlDecode($payloadBase64), true);
        if (!is_array($payload)) {
            throw new ImException('开发 IM token 载荷无效', 'AUTH_TOKEN_PAYLOAD');
        }

        return $this->contextFromPayload($payload, $packetOrganization, $authData, checkExpire: false);
    }

    private function contextFromPayload(array $payload, int $packetOrganization, array $authData, bool $checkExpire = true): AuthContext
    {
        $organization = (int) ($payload['organization'] ?? 0);
        $userId = trim((string) ($payload['user_id'] ?? ''));
        $expireAt = (int) ($payload['exp'] ?? 0);

        if ($organization <= 0 || $userId === '') {
            throw new ImException('IM token 缺少 organization 或 user_id', 'AUTH_TOKEN_IDENTITY');
        }
        if ($packetOrganization > 0 && $packetOrganization !== $organization) {
            throw new ImException('IM token 机构与请求机构不一致', 'AUTH_ORGANIZATION_MISMATCH');
        }
        if ($checkExpire && $expireAt > 0 && $expireAt < time()) {
            throw new ImException('IM token 已过期', 'AUTH_TOKEN_EXPIRED');
        }

        $deviceId = trim((string) ($authData['device_id'] ?? $payload['device_id'] ?? ''));
        if ($deviceId === '') {
            $deviceId = 'device-' . substr(hash('sha256', $userId . microtime(true) . random_int(1, PHP_INT_MAX)), 0, 16);
        }

        return new AuthContext(
            organization: $organization,
            userId: $userId,
            deviceId: $deviceId,
            platform: (string) ($authData['platform'] ?? $payload['platform'] ?? ''),
            username: (string) ($payload['username'] ?? ''),
            expireAt: $expireAt,
        );
    }

    private static function base64UrlEncode(string $value): string
    {
        return rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
    }

    private static function base64UrlDecode(string $value): string
    {
        $padding = strlen($value) % 4;
        if ($padding > 0) {
            $value .= str_repeat('=', 4 - $padding);
        }

        $decoded = base64_decode(strtr($value, '-_', '+/'), true);
        if ($decoded === false) {
            throw new ImException('IM token base64 解码失败', 'AUTH_TOKEN_BASE64');
        }

        return $decoded;
    }
}
