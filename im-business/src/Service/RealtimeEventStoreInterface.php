<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

interface RealtimeEventStoreInterface
{
    public function claim(string $workerId): ?RealtimeEventClaim;

    public function ack(RealtimeEventClaim $claim): void;

    public function requeue(RealtimeEventClaim $claim): void;

    public function recoverExpired(int $limit = 100): int;

    /** @param list<string> $credentialSessionIds */
    public function invalidateCredentialSessions(int $organization, array $credentialSessionIds): void;

    public function setOrganizationInactive(int $organization, bool $inactive): void;
}
