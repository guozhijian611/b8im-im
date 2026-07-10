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
 * 先读 Gateway session，缺失时回源 Redis 连接映射。
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

        try {
            $context = AuthContext::fromArray($session);
        } catch (\InvalidArgumentException) {
            throw new ImException('连接鉴权上下文无效', 'AUTH_CONTEXT_INVALID');
        }

        if ($context->clientId !== $clientId || $context->sessionId === '') {
            throw new ImException('连接会话与 client_id 不一致', 'AUTH_SESSION_MISMATCH');
        }
        if (!Runtime::connections()->isBoundConnection($context)) {
            throw new ImException('当前连接不在有效会话索引中', 'AUTH_SESSION_NOT_BOUND');
        }
        if ($context->expireAt <= time()) {
            throw new ImException('IM token 已过期', 'AUTH_TOKEN_EXPIRED');
        }

        return $context;
    }
}
