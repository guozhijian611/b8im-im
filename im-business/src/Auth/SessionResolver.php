<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 连接鉴权上下文解析
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Runtime;
use GatewayWorker\Lib\Gateway;

/**
 * 连接鉴权上下文解析
 *
 * 核心 handler 与商业模块 handler 共用：从 client_id 还原 AuthContext。
 * 先读 Gateway session，缺失时回退到 Redis 连接映射。
 */
final class SessionResolver
{
    /**
     * 解析并要求连接已鉴权，未鉴权抛 ImException。
     */
    public static function mustResolve(string $clientId): AuthContext
    {
        $session = Gateway::getSession($clientId);
        if (!is_array($session) || empty($session['user_id'])) {
            $session = Runtime::connections()->get($clientId);
        }
        if (!is_array($session) || empty($session['user_id'])) {
            throw new ImException('连接未鉴权', 'AUTH_REQUIRED');
        }

        return AuthContext::fromArray($session);
    }
}
