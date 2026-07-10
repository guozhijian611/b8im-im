<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

final class RealtimeDeliveryResult
{
    public const ACK = 'ack';
    public const REQUEUE = 'requeue';
    public const DEAD_LETTER = 'dead_letter';

    public function __construct(
        public readonly string $outcome,
        public readonly string $reason = '',
        public readonly int $attempt = 0,
    ) {
        if (!in_array($outcome, [self::ACK, self::REQUEUE, self::DEAD_LETTER], true)) {
            throw new \InvalidArgumentException('invalid realtime delivery outcome');
        }
    }
}
