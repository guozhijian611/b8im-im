<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM token 信任策略
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use B8im\ImShared\Support\RuntimeEnvironment;
use InvalidArgumentException;

final class TokenPolicy
{
    public readonly string $secret;

    /**
     * @param list<string> $trustedIssuers
     */
    public function __construct(
        string $secret,
        public readonly array $trustedIssuers,
        public readonly string $audience,
        public readonly int $clockSkewSeconds = 30,
    ) {
        $this->secret = RuntimeEnvironment::requireSecret($secret, 'IM_TOKEN_SECRET');
        if ($trustedIssuers === [] || array_filter($trustedIssuers, static fn (string $issuer): bool => trim($issuer) === '') !== []) {
            throw new InvalidArgumentException('IM_TOKEN_TRUSTED_ISSUERS must contain non-empty deployment identifiers');
        }
        if (trim($audience) === '') {
            throw new InvalidArgumentException('IM_TOKEN_AUDIENCE cannot be empty');
        }
        if ($clockSkewSeconds < 0 || $clockSkewSeconds > 300) {
            throw new InvalidArgumentException('IM_TOKEN_CLOCK_SKEW_SECONDS must be between 0 and 300');
        }
    }
}
