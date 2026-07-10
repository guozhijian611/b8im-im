<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

interface RealtimeEventGatewayInterface
{
    public function sendToUser(int $organization, string $userId, string $packet): void;

    /** @return array<string, array<string, mixed>> */
    public function sessionsForUser(int $organization, string $userId): array;

    /** @return array<string, array<string, mixed>> */
    public function allSessions(): array;

    public function close(string $clientId): void;
}
