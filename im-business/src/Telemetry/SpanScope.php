<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Telemetry;

use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\Context\ScopeInterface;
use Throwable;

final class SpanScope
{
    private bool $ended = false;

    public function __construct(
        public readonly SpanInterface $span,
        private readonly ?ScopeInterface $scope,
    ) {
    }

    public function end(): void
    {
        if ($this->ended) {
            return;
        }
        $this->ended = true;
        try {
            $this->scope?->detach();
        } catch (Throwable $throwable) {
            Telemetry::instrumentationFailed($throwable);
        }
        try {
            $this->span->end();
        } catch (Throwable $throwable) {
            Telemetry::instrumentationFailed($throwable);
        }
    }
}
