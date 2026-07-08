<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | IM 登录设备状态
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Repository\ImRepository;

final class DeviceService
{
    public function __construct(private readonly ImRepository $repository)
    {
    }

    public function online(AuthContext $context, string $clientId): void
    {
        $now = date('Y-m-d H:i:s');
        $this->repository->execute(
            'INSERT INTO im_user_device
                (organization, user_id, device_id, platform, client_id, status, online_time, last_active_time, create_time, update_time)
             VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                platform = VALUES(platform),
                client_id = VALUES(client_id),
                status = 1,
                online_time = VALUES(online_time),
                last_active_time = VALUES(last_active_time),
                update_time = VALUES(update_time)',
            [
                $context->organization,
                $context->userId,
                $context->deviceId,
                $context->platform,
                $clientId,
                $now,
                $now,
                $now,
                $now,
            ],
        );
    }

    public function offline(array $connection): void
    {
        if (empty($connection['organization']) || empty($connection['user_id']) || empty($connection['device_id'])) {
            return;
        }

        $now = date('Y-m-d H:i:s');
        $this->repository->execute(
            'UPDATE im_user_device
                SET status = 2, client_id = NULL, offline_time = ?, last_active_time = ?, update_time = ?
              WHERE organization = ? AND user_id = ? AND device_id = ?',
            [
                $now,
                $now,
                $now,
                (int) $connection['organization'],
                (string) $connection['user_id'],
                (string) $connection['device_id'],
            ],
        );
    }
}
