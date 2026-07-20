<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface GroupMemberAccessSessionInvalidator
{
    public function invalidateGroupAccessSnapshot(string $clientId, string $currentSnapshotId): void;
}
