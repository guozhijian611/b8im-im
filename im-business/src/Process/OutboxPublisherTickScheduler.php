<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Process;

use Closure;
use Throwable;

/**
 * Keeps the ordinary Rabbit outbox ahead of the best-effort realtime-control
 * attempt and prevents a failed Redis dependency from being retried every tick.
 */
final class OutboxPublisherTickScheduler
{
    private int $controlFailures = 0;
    private float $controlNextAttemptAt = 0.0;
    private readonly Closure $clock;

    public function __construct(
        ?callable $clock = null,
        private readonly float $baseBackoffSeconds = 1.0,
        private readonly float $maxBackoffSeconds = 30.0,
    ) {
        if (
            $baseBackoffSeconds <= 0.0
            || $maxBackoffSeconds < $baseBackoffSeconds
            || $maxBackoffSeconds > 300.0
        ) {
            throw new \InvalidArgumentException('control outbox circuit-breaker backoff is invalid');
        }
        $this->clock = $clock === null
            ? static fn (): float => hrtime(true) / 1_000_000_000
            : Closure::fromCallable($clock);
    }

    /**
     * @param callable():void $rabbitBatch
     * @param callable():bool $controlAttempt true for an empty/successful attempt, false for a handled failure
     * @return array{
     *   rabbit_error:?Throwable,
     *   control_error:?Throwable,
     *   control_attempted:bool,
     *   control_succeeded:?bool
     * }
     */
    public function run(callable $rabbitBatch, callable $controlAttempt): array
    {
        $rabbitError = null;
        try {
            // Rabbit always gets the first opportunity in every timer tick.
            $rabbitBatch();
        } catch (Throwable $error) {
            $rabbitError = $error;
        }

        $now = ($this->clock)();
        if ($now < $this->controlNextAttemptAt) {
            return [
                'rabbit_error' => $rabbitError,
                'control_error' => null,
                'control_attempted' => false,
                'control_succeeded' => null,
            ];
        }

        $controlError = null;
        try {
            $controlSucceeded = $controlAttempt();
        } catch (Throwable $error) {
            $controlSucceeded = false;
            $controlError = $error;
        }

        if ($controlSucceeded) {
            $this->controlFailures = 0;
            $this->controlNextAttemptAt = 0.0;
        } else {
            ++$this->controlFailures;
            $exponent = min($this->controlFailures - 1, 20);
            $delay = min(
                $this->maxBackoffSeconds,
                $this->baseBackoffSeconds * (2 ** $exponent),
            );
            $this->controlNextAttemptAt = ($this->clock)() + $delay;
        }

        return [
            'rabbit_error' => $rabbitError,
            'control_error' => $controlError,
            'control_attempted' => true,
            'control_succeeded' => $controlSucceeded,
        ];
    }
}
