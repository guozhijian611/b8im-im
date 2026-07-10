<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 连接级鉴权失效分类
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use B8im\ImShared\Protocol\Command;

final class ConnectionFailurePolicy
{
    private const TERMINAL_AUTH_CODES = [
        'AUTH_REQUIRED',
        'AUTH_CONTEXT_INVALID',
        'AUTH_SESSION_MISMATCH',
        'AUTH_SESSION_NOT_BOUND',
        'AUTH_TOKEN_EXPIRED',
        'AUTH_ORGANIZATION_INACTIVE',
        'AUTH_USER_INACTIVE',
        'AUTH_DEVICE_INACTIVE',
        'AUTH_SESSION_INACTIVE',
        'AUTH_SESSION_EXPIRED',
        'ACCOUNT_POLICY_BLOCKED',
    ];

    public static function shouldClose(string $command, string $errorCode): bool
    {
        return $command === Command::AUTH
            || in_array($errorCode, self::TERMINAL_AUTH_CODES, true);
    }
}
