<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Repository;

interface GroupMemberAccessRepository
{
    public function transaction(callable $callback): mixed;

    public function fetchOne(string $sql, array $params = []): ?array;

    public function fetchAll(string $sql, array $params = []): array;
}
