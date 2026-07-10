<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

interface TenantImPolicyStoreInterface
{
    /** @return array<string, mixed>|null */
    public function fetchPolicy(int $organization): ?array;

    public function isActiveUser(int $organization, string $userId): bool;
}
