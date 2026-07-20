<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImBusiness\Telemetry\Telemetry;
use B8im\ImShared\Support\Constants;
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
        $this->assertHomeIdentities($event, $event->recipientIdentities, 'event');
        if ($event->eventType === Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED) {
            // The revocation notification itself must remain deliverable after
            // the policy transition. Its canonical local target was validated
            // by the projector, so it intentionally bypasses the current gate.
            if (
                $event->targetOrganization !== $event->organization
                || $event->targetUserId === null
                || $event->recipientIdentities !== [[
                    'organization' => $event->targetOrganization,
                    'user_id' => $event->targetUserId,
                ]]
            ) {
                throw new \RuntimeException('access realtime target differs from its home projection');
            }
            $this->deliverToIdentities($event, $event->recipientIdentities);
            return;
        }

        $targeted = $event->targetOrganization !== null || $event->targetUserId !== null;
        if ($targeted && (
            $event->targetOrganization !== $event->organization
            || $event->targetUserId === null
            || $event->recipientIdentities !== [[
                'organization' => $event->targetOrganization,
                'user_id' => $event->targetUserId,
            ]]
        )) {
            throw new \RuntimeException('targeted realtime event identity differs from its home projection');
        }

        $this->recipients->withDeliverableIdentities(
            $event,
            function (array $activeIdentities) use ($event, $targeted): void {
                $this->assertHomeIdentities($event, $activeIdentities, 'active');
                $active = [];
                foreach ($activeIdentities as $identity) {
                    $active[$this->identityKey($identity)] = true;
                }
                $identities = array_values(array_filter(
                    $event->recipientIdentities,
                    fn (array $identity): bool => isset($active[$this->identityKey($identity)]),
                ));
                if ($targeted && count($identities) > 1) {
                    throw new \RuntimeException('targeted realtime event resolved multiple recipients');
                }

                // The callback executes while the database authorization
                // boundary is locked. Revocation therefore orders either
                // before this fanout (empty identities) or after all sends.
                $this->deliverToIdentities($event, $identities);
            },
        );
    }

    /** @param list<array{organization:int,user_id:string}> $identities */
    private function deliverToIdentities(RealtimeEvent $event, array $identities): void
    {
        foreach ($identities as $identity) {
            $homeOrganization = (int) $identity['organization'];
            $userId = (string) $identity['user_id'];
            if ($homeOrganization !== $event->organization) {
                throw new \RuntimeException('realtime event recipient is outside its home projection');
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

    /** @param array{organization:int,user_id:string} $identity */
    private function identityKey(array $identity): string
    {
        return json_encode(
            [(int) $identity['organization'], (string) $identity['user_id']],
            JSON_THROW_ON_ERROR,
        );
    }

    /** @param list<array{organization:int,user_id:string}> $identities */
    private function assertHomeIdentities(RealtimeEvent $event, array $identities, string $source): void
    {
        foreach ($identities as $identity) {
            if ((int) ($identity['organization'] ?? 0) !== $event->organization) {
                throw new \RuntimeException($source . ' realtime recipient is outside its home projection');
            }
        }
    }
}
