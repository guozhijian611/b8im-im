<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImShared\Support\Constants;
use B8im\ImBusiness\Telemetry\Telemetry;
use OpenTelemetry\API\Trace\SpanKind;

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
        foreach ($this->recipientUserIds($event) as $userId) {
            $homeOrganization = (int) ($event->recipientHomes[$userId] ?? $event->organization);
            if ($homeOrganization <= 0) {
                $homeOrganization = $event->organization;
            }
            foreach ($this->gateway->clientIdsForOrganizationUser($homeOrganization, $userId) as $clientId) {
                if ($clientId === $event->originClientId || $this->checkpoints->wasDelivered($event, $clientId)) {
                    continue;
                }
                $trace = Telemetry::start(
                    'im.gateway.push',
                    SpanKind::KIND_PRODUCER,
                    attributes: [
                        'operation' => 'im.gateway.push',
                        'b8im.organization' => $event->organization,
                        'b8im.message_id' => $event->messageId,
                        'b8im.event_id' => $event->eventId(),
                    ],
                );
                try {
                    $this->gateway->sendToClient(
                        $clientId,
                        $event->encodedPacket(Telemetry::currentTraceContext()),
                    );
                    $this->checkpoints->markDelivered($event, $clientId);
                } catch (\Throwable $throwable) {
                    Telemetry::recordError(
                        $trace->span,
                        $throwable,
                        'IM_GATEWAY_PUSH_FAILED',
                        'delivery',
                        'im.gateway.push',
                        [
                            'retry_count' => 0,
                            'b8im.message_id' => $event->messageId,
                            'b8im.event_id' => $event->eventId(),
                        ],
                    );
                    throw $throwable;
                } finally {
                    $trace->end();
                }
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
