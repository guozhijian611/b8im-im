<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Telemetry;

use OpenTelemetry\Context\ContextInterface;
use OpenTelemetry\SDK\Common\Future\CancellationInterface;
use OpenTelemetry\SDK\Trace\ReadableSpanInterface;
use OpenTelemetry\SDK\Trace\ReadWriteSpanInterface;
use OpenTelemetry\SDK\Trace\SpanProcessorInterface;

final class BoundedSpanProcessor implements SpanProcessorInterface
{
    private int $pending = 0;
    private bool $closed = false;

    public function __construct(
        private readonly SpanProcessorInterface $delegate,
        private readonly int $maxQueueSize,
    ) {
    }

    public function onStart(ReadWriteSpanInterface $span, ContextInterface $parentContext): void
    {
        if (!$this->closed) {
            $this->delegate->onStart($span, $parentContext);
        }
    }

    public function onEnd(ReadableSpanInterface $span): void
    {
        if ($this->closed || !$span->getContext()->isSampled()) {
            return;
        }
        if ($this->pending >= $this->maxQueueSize) {
            Telemetry::queueDropped();

            return;
        }
        $this->pending++;
        $this->delegate->onEnd($span);
    }

    public function forceFlush(?CancellationInterface $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }
        try {
            return $this->delegate->forceFlush($cancellation);
        } finally {
            $this->pending = 0;
        }
    }

    public function shutdown(?CancellationInterface $cancellation = null): bool
    {
        if ($this->closed) {
            return false;
        }
        $this->closed = true;
        try {
            return $this->delegate->shutdown($cancellation);
        } finally {
            $this->pending = 0;
        }
    }
}
