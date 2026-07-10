<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

use B8im\ImBusiness\Auth\AuthContext;
use GatewayWorker\Lib\Gateway;

final class GatewayRealtimeEventGateway implements RealtimeEventGatewayInterface
{
    public function sendToUser(int $organization, string $userId, string $packet): void
    {
        Gateway::sendToUid(AuthContext::uidFor($organization, $userId), $packet);
    }

    public function sessionsForUser(int $organization, string $userId): array
    {
        $sessions = [];
        foreach (Gateway::getClientIdByUid(AuthContext::uidFor($organization, $userId)) as $clientId) {
            $clientId = (string) $clientId;
            $session = Gateway::getSession($clientId);
            if ($clientId !== '' && is_array($session)) {
                $sessions[$clientId] = $session;
            }
        }

        return $sessions;
    }

    public function allSessions(): array
    {
        $sessions = Gateway::getAllClientSessions();

        return is_array($sessions) ? $sessions : [];
    }

    public function close(string $clientId): void
    {
        Gateway::closeClient($clientId);
    }
}
