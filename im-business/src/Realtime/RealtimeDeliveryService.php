<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImShared\Support\Constants;

final class RealtimeDeliveryService implements RealtimeEventDeliverer
{
    public function __construct(
        private readonly RealtimeRecipientProvider $recipients,
        private readonly RealtimeGateway $gateway,
        private readonly RealtimeDeliveryCheckpoint $checkpoints,
    ) {
    }

    public function deliver(RealtimeEvent $event): void
    {
        $packet = $event->encodedPacket();
        foreach ($this->recipientUserIds($event) as $userId) {
            foreach ($this->gateway->clientIdsForOrganizationUser($event->organization, $userId) as $clientId) {
                if ($clientId === $event->originClientId || $this->checkpoints->wasDelivered($event, $clientId)) {
                    continue;
                }
                $this->gateway->sendToClient($clientId, $packet);
                $this->checkpoints->markDelivered($event, $clientId);
            }
        }
    }

    /** @return list<string> */
    private function recipientUserIds(RealtimeEvent $event): array
    {
        if ($event->eventType === Constants::MQ_ROUTING_MESSAGE_DELETED_SELF) {
            return $event->targetUserId === null ? [] : [$event->targetUserId];
        }

        $activeUserIds = $this->recipients->activeUserIds(
            $event->organization,
            $event->conversationId,
            $event->messageSeq,
        );
        if ($event->eventType !== Constants::MQ_ROUTING_MESSAGE_CREATED) {
            return $activeUserIds;
        }

        $allowed = array_fill_keys($event->recipientUserIds, true);
        if (($event->packetData['message']['sender_id'] ?? null) === $event->originUserId) {
            $allowed[$event->originUserId] = true;
        }

        return array_values(array_filter(
            $activeUserIds,
            static fn (string $userId): bool => isset($allowed[$userId]),
        ));
    }
}
