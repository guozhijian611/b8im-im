<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Repository\ImRepository;

final class ImRepositoryTenantImPolicyStore implements TenantImPolicyStoreInterface
{
    public function __construct(private readonly ImRepository $repository)
    {
    }

    public function fetchPolicy(int $organization): ?array
    {
        return $this->repository->fetchOne(
            'SELECT organization,
                    allowed_client_families_json,
                    allow_multi_device_online,
                    max_online_devices,
                    same_device_login_policy,
                    cross_device_login_policy,
                    max_message_concurrency,
                    max_message_qps,
                    default_group_display_member_count,
                    message_recall_window_seconds,
                    message_edit_window_seconds,
                    recall_notice_enabled,
                    group_recall_notice_enabled,
                    status,
                    version
               FROM sm_tenant_im_policy
              WHERE organization = ?
              LIMIT 1',
            [$organization],
        );
    }

    public function isActiveUser(int $organization, string $userId): bool
    {
        return $this->repository->fetchOne(
            'SELECT id
               FROM im_user
              WHERE organization = ?
                AND user_id = ?
                AND status = 1
                AND delete_time IS NULL
              LIMIT 1',
            [$organization, $userId],
        ) !== null;
    }
}
