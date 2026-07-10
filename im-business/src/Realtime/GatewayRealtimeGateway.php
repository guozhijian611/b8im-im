<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Auth\AuthContext;
use GatewayWorker\Lib\Gateway;

final class GatewayRealtimeGateway implements RealtimeGateway
{
    public function clientIdsForOrganizationUser(int $organization, string $userId): array
    {
        $uid = AuthContext::uidFor($organization, $userId);

        return array_values(array_unique(array_map('strval', Gateway::getClientIdByUid($uid))));
    }

    public function sendToClient(string $clientId, string $packet): void
    {
        if (Gateway::sendToClient($clientId, $packet) !== true) {
            throw new \RuntimeException(sprintf(
                'Gateway realtime delivery failed for client_id=%s',
                $clientId,
            ));
        }
    }
}
