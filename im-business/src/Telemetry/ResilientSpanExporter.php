<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Telemetry;

use OpenTelemetry\SDK\Common\Future\CancellationInterface;
use OpenTelemetry\SDK\Common\Future\FutureInterface;
use OpenTelemetry\SDK\Trace\SpanExporterInterface;
use Throwable;

final class ResilientSpanExporter implements SpanExporterInterface
{
    public function __construct(private readonly SpanExporterInterface $delegate)
    {
    }

    public function export(iterable $batch, ?CancellationInterface $cancellation = null): FutureInterface
    {
        return $this->delegate->export($batch, $cancellation)
            ->map(static function (bool $success): bool {
                if (!$success) {
                    Telemetry::exportFailed();
                }

                return $success;
            })
            ->catch(static function (Throwable $throwable): bool {
                Telemetry::exportFailed($throwable);

                return false;
            });
    }

    public function shutdown(?CancellationInterface $cancellation = null): bool
    {
        try {
            return $this->delegate->shutdown($cancellation);
        } catch (Throwable $throwable) {
            Telemetry::exportFailed($throwable);

            return false;
        }
    }

    public function forceFlush(?CancellationInterface $cancellation = null): bool
    {
        try {
            return $this->delegate->forceFlush($cancellation);
        } catch (Throwable $throwable) {
            Telemetry::exportFailed($throwable);

            return false;
        }
    }
}
