<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | WebSocket 握手 Origin 信任策略
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImGateway\Security;

use InvalidArgumentException;

final class OriginPolicy
{
    /**
     * @param list<string> $trustedOrigins
     */
    public function __construct(private readonly array $trustedOrigins)
    {
    }

    public static function fromCsv(string $origins): self
    {
        $normalized = [];
        foreach (explode(',', $origins) as $origin) {
            $origin = trim($origin);
            if ($origin === '') {
                continue;
            }
            $normalized[] = self::normalize($origin);
        }

        return new self(array_values(array_unique($normalized)));
    }

    /**
     * 无 Origin 的原生 App/Desktop 可继续握手，但后续仍必须通过 token 鉴权。
     * 浏览器一旦携带 Origin，则必须精确命中受信任列表。
     */
    public function assertAllowed(?string $origin): void
    {
        if ($origin === null || trim($origin) === '') {
            return;
        }

        $normalized = self::normalize($origin);
        if (!in_array($normalized, $this->trustedOrigins, true)) {
            throw new InvalidArgumentException('untrusted WebSocket Origin');
        }
    }

    /**
     * @return list<string>
     */
    public function trustedOrigins(): array
    {
        return $this->trustedOrigins;
    }

    private static function normalize(string $origin): string
    {
        $parts = parse_url(trim($origin));
        if (
            !is_array($parts)
            || !isset($parts['scheme'], $parts['host'])
            || !in_array(strtolower((string) $parts['scheme']), ['http', 'https'], true)
            || isset($parts['user'])
            || isset($parts['pass'])
            || isset($parts['query'])
            || isset($parts['fragment'])
            || (isset($parts['path']) && $parts['path'] !== '' && $parts['path'] !== '/')
        ) {
            throw new InvalidArgumentException('invalid WebSocket Origin');
        }

        $scheme = strtolower((string) $parts['scheme']);
        $host = strtolower(rtrim((string) $parts['host'], '.'));
        if ($host === '') {
            throw new InvalidArgumentException('invalid WebSocket Origin host');
        }

        $port = isset($parts['port']) ? (int) $parts['port'] : null;
        if (($scheme === 'http' && $port === 80) || ($scheme === 'https' && $port === 443)) {
            $port = null;
        }

        return $scheme . '://' . $host . ($port === null ? '' : ':' . $port);
    }
}
