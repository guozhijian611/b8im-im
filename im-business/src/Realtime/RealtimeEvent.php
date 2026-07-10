<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use B8im\ImShared\Protocol\Packet;

final class RealtimeEvent
{
    /**
     * @param list<string> $recipientUserIds
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
        public readonly string $actorUserId,
        public readonly string $originUserId,
        public readonly string $originClientId,
        public readonly ?string $targetUserId,
        public readonly array $recipientUserIds,
        public readonly string $packetCommand,
        public readonly array $packetData,
    ) {
    }

    public function encodedPacket(): string
    {
        return Packet::make(
            $this->packetCommand,
            [...$this->packetData, 'event_id' => $this->eventId()],
            $this->organization,
        )->encode();
    }

    public function eventId(): string
    {
        return hash('sha256', implode('|', [
            (string) $this->organization,
            $this->eventType,
            $this->messageId,
            (string) $this->changeSeq,
        ]));
    }
}
