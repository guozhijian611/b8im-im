<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM JWT 签发与校验
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use B8im\ImBusiness\Exception\ImException;
use JsonException;

final class ImToken
{
    public function __construct(private readonly TokenPolicy $policy)
    {
    }

    public function verify(string $token, string $gatewayClientId, ?int $now = null): AuthContext
    {
        if ($token === '') {
            throw new ImException('缺少 IM token', 'AUTH_TOKEN_EMPTY');
        }

        $parts = explode('.', $token);
        if (count($parts) !== 3) {
            throw new ImException('IM token 格式错误', 'AUTH_TOKEN_FORMAT');
        }

        [$headerBase64, $payloadBase64, $signature] = $parts;
        $expected = self::base64UrlEncode(hash_hmac(
            'sha256',
            $headerBase64 . '.' . $payloadBase64,
            $this->policy->secret,
            true,
        ));
        if (!hash_equals($expected, $signature)) {
            throw new ImException('IM token 签名无效', 'AUTH_TOKEN_SIGNATURE');
        }

        $header = $this->decodeObject($headerBase64, 'AUTH_TOKEN_HEADER');
        if (($header['alg'] ?? null) !== 'HS256' || ($header['typ'] ?? null) !== 'JWT') {
            throw new ImException('IM token 算法或类型无效', 'AUTH_TOKEN_HEADER');
        }

        $payload = $this->decodeObject($payloadBase64, 'AUTH_TOKEN_PAYLOAD');
        $issuer = $this->requiredString($payload, 'iss');
        if (!in_array($issuer, $this->policy->trustedIssuers, true)) {
            throw new ImException('IM token issuer 不受信任', 'AUTH_TOKEN_ISSUER');
        }
        $deploymentId = $this->requiredString($payload, 'deployment_id');
        if (!hash_equals($issuer, $deploymentId)) {
            throw new ImException('IM token deployment_id 与 issuer 不一致', 'AUTH_TOKEN_DEPLOYMENT');
        }

        $audiences = $this->audiences($payload['aud'] ?? null);
        if (!in_array($this->policy->audience, $audiences, true)) {
            throw new ImException('IM token audience 不匹配', 'AUTH_TOKEN_AUDIENCE');
        }

        $expireAt = $this->requiredInteger($payload, 'exp');
        $notBefore = $this->requiredInteger($payload, 'nbf');
        if ($expireAt <= 0 || $notBefore <= 0) {
            throw new ImException('IM token exp/nbf 必须是正整数', 'AUTH_TOKEN_TIME_RANGE');
        }
        if ($expireAt <= $notBefore) {
            throw new ImException('IM token 有效期无效', 'AUTH_TOKEN_TIME_RANGE');
        }

        $now ??= time();
        if ($notBefore > $now + $this->policy->clockSkewSeconds) {
            throw new ImException('IM token 尚未生效', 'AUTH_TOKEN_NOT_BEFORE');
        }
        if ($expireAt <= $now) {
            throw new ImException('IM token 已过期', 'AUTH_TOKEN_EXPIRED');
        }

        $organization = $this->requiredInteger($payload, 'organization');
        if ($organization <= 0) {
            throw new ImException('IM token organization 无效', 'AUTH_TOKEN_IDENTITY');
        }

        $clientId = $this->requiredString($payload, 'client_id');
        if ($gatewayClientId === '' || !hash_equals($clientId, $gatewayClientId)) {
            throw new ImException('IM token client_id 与当前连接不一致', 'AUTH_CLIENT_MISMATCH');
        }

        $clientFamily = $this->requiredString($payload, 'client_family');
        $os = $this->requiredString($payload, 'os');
        if (!in_array($clientFamily, ['web', 'app', 'desktop'], true)) {
            throw new ImException('IM token client_family 无效', 'AUTH_TOKEN_CLIENT_FAMILY');
        }
        if (!in_array($os, ['browser', 'android', 'ios', 'windows', 'macos', 'linux', 'other'], true)) {
            throw new ImException('IM token os 无效', 'AUTH_TOKEN_OS');
        }
        $validClientOs = match ($clientFamily) {
            'web' => $os === 'browser',
            'app' => in_array($os, ['android', 'ios', 'other'], true),
            'desktop' => in_array($os, ['windows', 'macos', 'linux', 'other'], true),
        };
        if (!$validClientOs) {
            throw new ImException('IM token client_family 与 os 不一致', 'AUTH_TOKEN_CLIENT_OS');
        }

        return new AuthContext(
            organization: $organization,
            userId: $this->requiredString($payload, 'user_id'),
            deviceId: $this->requiredString($payload, 'device_id'),
            clientId: $clientId,
            credentialSessionId: $this->requiredString($payload, 'session_id'),
            sessionId: '',
            clientFamily: $clientFamily,
            os: $os,
            issuer: $issuer,
            audience: $this->policy->audience,
            notBefore: $notBefore,
            expireAt: $expireAt,
            username: trim((string) ($payload['username'] ?? '')),
        );
    }

    /**
     * Test and issuer helper. Production issuance belongs to the authenticated HTTP control plane.
     *
     * @param array<string, mixed> $payload
     */
    public function generate(array $payload): string
    {
        $header = ['alg' => 'HS256', 'typ' => 'JWT'];
        $headerBase64 = self::base64UrlEncode(json_encode($header, JSON_THROW_ON_ERROR));
        $payloadBase64 = self::base64UrlEncode(json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR));
        $signature = self::base64UrlEncode(hash_hmac(
            'sha256',
            $headerBase64 . '.' . $payloadBase64,
            $this->policy->secret,
            true,
        ));

        return $headerBase64 . '.' . $payloadBase64 . '.' . $signature;
    }

    /**
     * @return array<string, mixed>
     */
    private function decodeObject(string $encoded, string $errorCode): array
    {
        try {
            $value = json_decode(self::base64UrlDecode($encoded), true, 32, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            throw new ImException('IM token JSON 载荷无效', $errorCode);
        }

        if (!is_array($value)) {
            throw new ImException('IM token JSON 载荷无效', $errorCode);
        }

        return $value;
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function requiredString(array $payload, string $claim): string
    {
        if (!isset($payload[$claim]) || !is_string($payload[$claim]) || trim($payload[$claim]) === '') {
            throw new ImException('IM token 缺少必要声明: ' . $claim, 'AUTH_TOKEN_CLAIM');
        }

        return trim($payload[$claim]);
    }

    /**
     * @param array<string, mixed> $payload
     */
    private function requiredInteger(array $payload, string $claim): int
    {
        if (!isset($payload[$claim]) || !is_int($payload[$claim])) {
            throw new ImException('IM token 缺少必要整数声明: ' . $claim, 'AUTH_TOKEN_CLAIM');
        }

        return $payload[$claim];
    }

    /**
     * @return list<string>
     */
    private function audiences(mixed $audience): array
    {
        if (is_string($audience) && trim($audience) !== '') {
            return [trim($audience)];
        }
        if (is_array($audience)) {
            $audiences = [];
            foreach ($audience as $value) {
                if (!is_string($value) || trim($value) === '') {
                    throw new ImException('IM token aud 声明无效', 'AUTH_TOKEN_AUDIENCE');
                }
                $audiences[] = trim($value);
            }
            if ($audiences !== []) {
                return array_values(array_unique($audiences));
            }
        }

        throw new ImException('IM token 缺少 aud 声明', 'AUTH_TOKEN_AUDIENCE');
    }

    private static function base64UrlEncode(string $value): string
    {
        return rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
    }

    private static function base64UrlDecode(string $value): string
    {
        if ($value === '' || preg_match('/^[A-Za-z0-9_-]+$/', $value) !== 1) {
            throw new ImException('IM token base64 编码无效', 'AUTH_TOKEN_BASE64');
        }

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
