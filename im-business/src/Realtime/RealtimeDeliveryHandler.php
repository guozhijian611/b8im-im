<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use Throwable;

final class RealtimeDeliveryHandler
{
    public function __construct(
        private readonly RealtimeEventProjector $projector,
        private readonly RealtimeEventDeliverer $deliverer,
        private readonly RealtimeRetryCounter $retryCounter,
        private readonly int $maxRetry,
    ) {
        if ($maxRetry < 1) {
            throw new \InvalidArgumentException('realtime max retry must be positive');
        }
    }

    public function handle(string $routingKey, string $body): RealtimeDeliveryResult
    {
        try {
            $event = $this->projector->project($routingKey, $body);
        } catch (InvalidRealtimeEvent $exception) {
            return new RealtimeDeliveryResult(
                RealtimeDeliveryResult::DEAD_LETTER,
                $exception->getMessage(),
            );
        }

        try {
            $this->deliverer->deliver($event);
            $this->retryCounter->clear($event);

            return new RealtimeDeliveryResult(RealtimeDeliveryResult::ACK);
        } catch (Throwable $throwable) {
            $attempt = $this->retryCounter->increment($event);
            if ($attempt <= $this->maxRetry) {
                return new RealtimeDeliveryResult(
                    RealtimeDeliveryResult::REQUEUE,
                    $throwable->getMessage(),
                    $attempt,
                );
            }

            return new RealtimeDeliveryResult(
                RealtimeDeliveryResult::DEAD_LETTER,
                $throwable->getMessage(),
                $attempt,
            );
        }
    }
}
