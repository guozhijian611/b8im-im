<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

interface ModuleLicenseRepositoryInterface
{
    /** @return array<string, mixed>|null */
    public function fetchOne(string $sql, array $params = []): ?array;
}
