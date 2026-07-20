<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Telemetry\TraceContext;

final class RealtimeEvent
{
    /**
     * @param list<array{organization:int,user_id:string}> $recipientIdentities
     * @param array<string, mixed> $packetData
     */
    public function __construct(
        public readonly string $eventType,
        public readonly int $organization,
        public readonly string $conversationId,
        public readonly int $conversationType,
        public readonly string $messageId,
        public readonly int $messageSeq,
        public readonly int $changeSeq,
        public readonly int $actorOrganization,
        public readonly string $actorUserId,
        public readonly int $originOrganization,
        public readonly string $originUserId,
        public readonly string $originClientId,
        public readonly ?int $targetOrganization,
        public readonly ?string $targetUserId,
        public readonly ?string $crossOrgAccessSnapshotId,
        public readonly array $recipientIdentities,
        public readonly string $packetCommand,
        public readonly array $packetData,
        public readonly string $stableEventId,
    ) {
    }

    public function encodedPacket(?TraceContext $traceContext = null): string
    {
        return Packet::make(
            $this->packetCommand,
            [...$this->packetData, 'event_id' => $this->eventId()],
            $this->organization,
            null,
            $traceContext,
        )->encode();
    }

    public function eventId(): string
    {
        return $this->stableEventId;
    }
}
