<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

interface RealtimeGateway
{
    /** @return list<string> */
    public function clientIdsForOrganizationUser(int $organization, string $userId): array;

    public function sendToClient(string $clientId, string $packet): void;
}
