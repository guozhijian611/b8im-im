<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Service;

final class RealtimeEventClaim
{
    public function __construct(
        public readonly string $claimToken,
        public readonly string $workerId,
        public readonly string $raw,
        public readonly ?string $eventId,
    ) {
        if (preg_match('/^[a-f0-9]{40}$/', $claimToken) !== 1) {
            throw new \InvalidArgumentException('Realtime event claim token is invalid.');
        }
        if ($workerId === '' || trim($workerId) !== $workerId || strlen($workerId) > 160) {
            throw new \InvalidArgumentException('Realtime event worker id is invalid.');
        }
        if ($eventId !== null && preg_match('/^[a-f0-9]{64}$/', $eventId) !== 1) {
            throw new \InvalidArgumentException('Realtime event id is invalid.');
        }
    }
}
