<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Realtime;

use Throwable;
use B8im\ImBusiness\Telemetry\Telemetry;

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
        $trace = Telemetry::start('im.realtime.delivery', attributes: [
            'operation' => 'im.realtime.delivery',
            'messaging.destination.name' => $routingKey,
        ]);
        try {
            $event = $this->projector->project($routingKey, $body);
            Telemetry::setAttributes($trace->span, [
                'b8im.organization' => $event->organization,
                'b8im.message_id' => $event->messageId,
                'b8im.event_id' => $event->eventId(),
                'b8im.conversation_id' => $event->conversationId,
            ]);
        } catch (InvalidRealtimeEvent $exception) {
            Telemetry::recordError(
                $trace->span,
                $exception,
                'IM_REALTIME_EVENT_INVALID',
                'validation',
                'im.realtime.delivery',
                ['retry_count' => 0],
            );
            $trace->end();
            return new RealtimeDeliveryResult(
                RealtimeDeliveryResult::DEAD_LETTER,
                'IM_REALTIME_EVENT_INVALID',
            );
        }

        try {
            $this->deliverer->deliver($event);
            $this->retryCounter->clear($event);

            $trace->end();
            return new RealtimeDeliveryResult(RealtimeDeliveryResult::ACK);
        } catch (Throwable $throwable) {
            try {
                $attempt = $this->retryCounter->increment($event);
            } catch (Throwable $counterFailure) {
                Telemetry::recordError(
                    $trace->span,
                    $counterFailure,
                    'IM_REALTIME_RETRY_STATE_FAILED',
                    'infrastructure',
                    'im.realtime.delivery',
                    [
                        'retry_count' => 0,
                        'b8im.message_id' => $event->messageId,
                        'b8im.event_id' => $event->eventId(),
                    ],
                );
                $trace->end();
                throw $counterFailure;
            }
            Telemetry::recordError(
                $trace->span,
                $throwable,
                $attempt <= $this->maxRetry ? 'IM_GATEWAY_PUSH_RETRY' : 'IM_GATEWAY_PUSH_EXHAUSTED',
                'delivery',
                'im.realtime.delivery',
                [
                    'retry_count' => $attempt,
                    'b8im.message_id' => $event->messageId,
                    'b8im.event_id' => $event->eventId(),
                    'b8im.organization' => $event->organization,
                ],
            );
            $trace->end();
            if ($attempt <= $this->maxRetry) {
                return new RealtimeDeliveryResult(
                    RealtimeDeliveryResult::REQUEUE,
                    'IM_GATEWAY_PUSH_RETRY',
                    $attempt,
                );
            }

            return new RealtimeDeliveryResult(
                RealtimeDeliveryResult::DEAD_LETTER,
                'IM_GATEWAY_PUSH_EXHAUSTED',
                $attempt,
            );
        }
    }
}
