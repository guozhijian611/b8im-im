<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 消息分片路由数据访问边界
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Repository;

interface MessageShardRepositoryInterface
{
    public function fetchOne(string $sql, array $params = []): ?array;

    public function fetchAll(string $sql, array $params = []): array;
}
