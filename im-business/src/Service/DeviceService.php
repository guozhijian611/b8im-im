<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 登录设备状态
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;

final class DeviceService
{
    public function __construct(private readonly ImRepository $repository)
    {
    }

    public function online(AuthContext $context, string $clientIp = ''): void
    {
        $now = date('Y-m-d H:i:s');
        $clientIp = filter_var($clientIp, FILTER_VALIDATE_IP) !== false ? $clientIp : '';
        $updated = $this->repository->execute(
            'UPDATE im_user_device
                SET client_id = ?,
                    session_id = ?,
                    current_ip = NULLIF(?, \'\'),
                    last_login_ip = COALESCE(NULLIF(?, \'\'), last_login_ip),
                    last_login_at = ?,
                    last_seen_at = ?,
                    current_online_state = 1,
                    update_time = ?
              WHERE organization = ?
                AND user_id = ?
                AND device_id = ?
                AND client_family = ?
                AND os = ?
                AND status = 1
                AND delete_time IS NULL',
            [
                $context->clientId,
                $context->sessionId,
                $clientIp,
                $clientIp,
                $now,
                $now,
                $now,
                $context->organization,
                $context->userId,
                $context->deviceId,
                $context->clientFamily,
                $context->os,
            ],
        );

        if ($updated !== 1) {
            throw new ImException('设备状态已变更，拒绝绑定连接', 'AUTH_DEVICE_STATE_CHANGED');
        }

        $this->repository->execute(
            'INSERT INTO im_user_login_audit
                (organization, user_id, device_id, client_id, client_family, os,
                 login_ip, login_at, login_result, audit_scope, current_online_state, create_time)
             VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, \'\'), ?, \'success\', \'login\', 1, ?)',
            [
                $context->organization,
                $context->userId,
                $context->deviceId,
                $context->clientId,
                $context->clientFamily,
                $context->os,
                $clientIp,
                $now,
                $now,
            ],
        );
    }

    /**
     * Compare-and-delete: 旧连接断开不得清理同设备已建立的新连接。
     *
     * @param array<string, mixed> $connection
     */
    public function offline(array $connection): void
    {
        if (
            empty($connection['organization'])
            || empty($connection['user_id'])
            || empty($connection['device_id'])
            || empty($connection['client_id'])
            || empty($connection['session_id'])
        ) {
            return;
        }

        $now = date('Y-m-d H:i:s');
        $this->repository->execute(
            'UPDATE im_user_device
                SET client_id = NULL,
                    session_id = NULL,
                    current_ip = NULL,
                    current_ip_geo = NULL,
                    current_online_state = 2,
                    last_seen_at = ?,
                    update_time = ?
              WHERE organization = ?
                AND user_id = ?
                AND device_id = ?
                AND client_id = ?
                AND session_id = ?
                AND status = 1
                AND delete_time IS NULL',
            [
                $now,
                $now,
                (int) $connection['organization'],
                (string) $connection['user_id'],
                (string) $connection['device_id'],
                (string) $connection['client_id'],
                (string) $connection['session_id'],
            ],
        );
        $this->repository->execute(
            'UPDATE im_user_login_audit
                SET logout_at = ?, current_online_state = 2
              WHERE organization = ?
                AND user_id = ?
                AND device_id = ?
                AND client_id = ?
                AND login_result = \'success\'
                AND logout_at IS NULL
              ORDER BY id DESC
              LIMIT 1',
            [
                $now,
                (int) $connection['organization'],
                (string) $connection['user_id'],
                (string) $connection['device_id'],
                (string) $connection['client_id'],
            ],
        );
    }
}
