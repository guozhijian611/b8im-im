<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM token 目标身份与会话状态校验
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Auth;

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;

final class AuthIdentityValidator
{
    public function __construct(private readonly ImRepository $repository)
    {
    }

    public function assertActive(AuthContext $context, ?int $now = null): void
    {
        $organization = $this->repository->fetchOne(
            'SELECT id FROM sm_system_organization
              WHERE id = ? AND status = 1 AND delete_time IS NULL
              LIMIT 1',
            [$context->organization],
        );
        if ($organization === null) {
            throw new ImException('目标 organization 不存在或已停用', 'AUTH_ORGANIZATION_INACTIVE');
        }

        $user = $this->repository->fetchOne(
            'SELECT id FROM im_user
              WHERE organization = ? AND user_id = ? AND status = 1 AND delete_time IS NULL
              LIMIT 1',
            [$context->organization, $context->userId],
        );
        if ($user === null) {
            throw new ImException('目标 IM 用户不存在或已停用', 'AUTH_USER_INACTIVE');
        }

        $device = $this->repository->fetchOne(
            'SELECT id FROM im_user_device
              WHERE organization = ?
                AND user_id = ?
                AND device_id = ?
                AND client_family = ?
                AND os = ?
                AND status = 1
                AND delete_time IS NULL
              LIMIT 1',
            [
                $context->organization,
                $context->userId,
                $context->deviceId,
                $context->clientFamily,
                $context->os,
            ],
        );
        if ($device === null) {
            throw new ImException('目标设备不存在、已停用或客户端信息不一致', 'AUTH_DEVICE_INACTIVE');
        }

        $session = $this->repository->fetchOne(
            'SELECT expire_at, web_access_jti FROM im_auth_session
              WHERE organization = ?
                AND user_id = ?
                AND device_id = ?
                AND client_id = ?
                AND session_id = ?
                AND status = 1
                AND revoked_at IS NULL
              LIMIT 1',
            [
                $context->organization,
                $context->userId,
                $context->deviceId,
                $context->clientId,
                $context->credentialSessionId,
            ],
        );
        if ($session === null) {
            throw new ImException('目标 IM 凭证会话不存在或已撤销', 'AUTH_SESSION_INACTIVE');
        }

        $sessionExpireAt = strtotime((string) ($session['expire_at'] ?? '')) ?: 0;
        $now ??= time();
        if ($sessionExpireAt <= $now || $context->expireAt > $sessionExpireAt) {
            throw new ImException('IM token 超出凭证会话有效期', 'AUTH_SESSION_EXPIRED');
        }

        $webAccessJti = $session['web_access_jti'] ?? null;
        if ($context->clientFamily === 'web') {
            if (!is_string($webAccessJti) || preg_match('/^[a-f0-9]{32}$/', $webAccessJti) !== 1) {
                throw new ImException('Web access 会话缺失或无效', 'AUTH_SESSION_INACTIVE');
            }
            $webAccess = $this->repository->fetchOne(
                'SELECT expire_at FROM im_web_access_session
                  WHERE organization = ?
                    AND jti = ?
                    AND im_user_id = ?
                    AND user_id = ?
                    AND device_id = ?
                    AND status = 1
                    AND revoked_at IS NULL
                  LIMIT 1',
                [
                    $context->organization,
                    $webAccessJti,
                    (int) $user['id'],
                    $context->userId,
                    $context->deviceId,
                ],
            );
            $webAccessExpireAt = $webAccess === null
                ? 0
                : (strtotime((string) ($webAccess['expire_at'] ?? '')) ?: 0);
            if ($webAccessExpireAt <= $now || $context->expireAt > $webAccessExpireAt) {
                throw new ImException('Web access 会话已撤销或过期', 'AUTH_SESSION_INACTIVE');
            }
        } elseif ($webAccessJti !== null && trim((string) $webAccessJti) !== '') {
            throw new ImException('IM 凭证与 Web access 会话绑定异常', 'AUTH_SESSION_INACTIVE');
        }
    }
}
