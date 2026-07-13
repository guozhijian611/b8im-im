<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | 长驻 IM 进程 OpenTelemetry 适配器
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Telemetry;

use B8im\ImBusiness\Config;
use B8im\ImShared\Telemetry\TraceContext;
use Fiber;
use OpenTelemetry\API\Common\Time\Clock;
use OpenTelemetry\API\Behavior\Internal\Logging;
use OpenTelemetry\API\Trace\NoopTracerProvider;
use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\API\Trace\TracerInterface;
use OpenTelemetry\Contrib\Otlp\OtlpHttpTransportFactory;
use OpenTelemetry\Contrib\Otlp\SpanExporter;
use OpenTelemetry\Context\Context;
use OpenTelemetry\Context\ScopeInterface;
use OpenTelemetry\SDK\Common\Attribute\Attributes;
use OpenTelemetry\SDK\Resource\ResourceInfo;
use OpenTelemetry\SDK\Trace\SpanProcessor\BatchSpanProcessor;
use OpenTelemetry\SDK\Trace\TracerProvider;
use OpenTelemetry\SDK\Trace\TracerProviderInterface;
use Throwable;
use WeakMap;
use Workerman\Timer;

final class Telemetry
{
    private static ?TracerProviderInterface $provider = null;
    private static ?TracerInterface $tracer = null;
    private static bool $enabled = false;
    private static string $serviceName = 'b8im-im-business';
    private static int $flushTimerId = 0;
    private static float $lastExportWarningAt = 0.0;
    private static ?ScopeInterface $mainRootScope = null;
    /** @var WeakMap<Fiber, ScopeInterface>|null */
    private static ?WeakMap $fiberRootScopes = null;

    public static function boot(Config $config, string $serviceName): void
    {
        self::ensureContextInitialized();
        if (self::$provider !== null || self::$tracer !== null) {
            return;
        }
        self::$serviceName = $serviceName;
        if (!$config->otelEnabled) {
            self::$provider = null;
            self::$tracer = (new NoopTracerProvider())->getTracer('b8im/im');

            return;
        }

        try {
            // SDK 默认会把完整 exporter 异常写到 stderr；这里改为自有限频、脱敏告警。
            Logging::disable();
            $transport = (new OtlpHttpTransportFactory())->create(
                $config->otelTracesEndpoint,
                'application/x-protobuf',
                [],
                null,
                $config->otelExporterTimeoutMs / 1000,
                0,
                0,
            );
            $processor = new BoundedSpanProcessor(
                new BatchSpanProcessor(
                    new ResilientSpanExporter(new SpanExporter($transport)),
                    Clock::getDefault(),
                    $config->otelBatchMaxQueueSize,
                    $config->otelBatchScheduleDelayMs,
                    $config->otelExporterTimeoutMs,
                    $config->otelBatchMaxExportSize,
                    false,
                ),
                $config->otelBatchMaxQueueSize,
            );
            self::$provider = TracerProvider::builder()
                ->addSpanProcessor($processor)
                ->setResource(ResourceInfo::create(Attributes::create([
                    'service.name' => $serviceName,
                    'service.namespace' => 'b8im',
                    'service.version' => $config->otelServiceVersion,
                    'deployment.environment.name' => $config->otelEnvironment,
                    'service.instance.id' => sprintf('%s:%d', (string) (gethostname() ?: 'unknown'), getmypid() ?: 0),
                ])))
                ->build();
            self::$tracer = self::$provider->getTracer('b8im/im', $config->otelServiceVersion);
            self::$enabled = true;
            try {
                self::$flushTimerId = Timer::add(
                    $config->otelBatchScheduleDelayMs / 1000,
                    static fn (): bool => self::flush(),
                );
            } catch (\RuntimeException) {
                // CLI 单测没有 Workerman event-loop，由测试显式 flush。
                self::$flushTimerId = 0;
            }
        } catch (Throwable $throwable) {
            self::$provider = null;
            self::$tracer = (new NoopTracerProvider())->getTracer('b8im/im');
            self::warn('initialization', $throwable);
        }
    }

    public static function start(
        string $name,
        int $kind = SpanKind::KIND_INTERNAL,
        ?TraceContext $parent = null,
        array $attributes = [],
    ): SpanScope {
        try {
            self::ensureContextInitialized();
            $tracer = self::$tracer ?? (new NoopTracerProvider())->getTracer('b8im/im');
            $builder = $tracer->spanBuilder($name)->setSpanKind($kind)->setAttributes(self::safeAttributes($attributes));
            if ($parent !== null) {
                $builder->setParent(TraceContextPropagator::getInstance()->extract($parent->toCarrier(), context: Context::getRoot()));
            }
            $span = $builder->startSpan();

            return new SpanScope($span, $span->activate());
        } catch (Throwable $throwable) {
            self::instrumentationFailed($throwable);
            $span = (new NoopTracerProvider())->getTracer('b8im/im')->spanBuilder($name)->startSpan();

            return new SpanScope($span, $span->activate());
        }
    }

    public static function currentTraceContext(): ?TraceContext
    {
        try {
            self::ensureContextInitialized();
            $carrier = [];
            TraceContextPropagator::getInstance()->inject($carrier);
            return TraceContext::fromCarrier($carrier['traceparent'] ?? null, $carrier['tracestate'] ?? null);
        } catch (Throwable) {
            return null;
        }
    }

    public static function run(
        string $name,
        callable $callback,
        int $kind = SpanKind::KIND_INTERNAL,
        array $attributes = [],
    ): mixed {
        $scope = self::start($name, $kind, null, $attributes);
        try {
            return $callback($scope->span);
        } catch (Throwable $throwable) {
            $errorCode = method_exists($throwable, 'errorCode')
                ? (string) $throwable->errorCode()
                : 'IM_OPERATION_FAILED';
            self::recordError(
                $scope->span,
                $throwable,
                $errorCode,
                method_exists($throwable, 'errorCode') ? 'business' : 'internal',
                $name,
                ['retry_count' => 0],
            );
            throw $throwable;
        } finally {
            $scope->end();
        }
    }

    public static function recordError(
        SpanInterface $span,
        Throwable $throwable,
        string $errorCode,
        string $errorType,
        string $operation,
        array $attributes = [],
    ): void {
        try {
            $event = self::safeAttributes([
                'error.code' => $errorCode,
                'error.type' => $errorType,
                'service' => self::$serviceName,
                'operation' => $operation,
                'exception.type' => $throwable::class,
                'retry_count' => (int) ($attributes['retry_count'] ?? 0),
                ...$attributes,
            ]);
            $span->setStatus(StatusCode::STATUS_ERROR, $errorCode);
            $span->setAttributes($event);
            $span->addEvent('exception', $event);
        } catch (Throwable $instrumentationFailure) {
            self::instrumentationFailed($instrumentationFailure);
        }
    }

    public static function setAttributes(SpanInterface $span, array $attributes): void
    {
        try {
            $span->setAttributes(self::safeAttributes($attributes));
        } catch (Throwable $throwable) {
            self::instrumentationFailed($throwable);
        }
    }

    public static function setCurrentAttributes(array $attributes): void
    {
        try {
            self::ensureContextInitialized();
            self::setAttributes(Span::getCurrent(), $attributes);
        } catch (Throwable $throwable) {
            self::instrumentationFailed($throwable);
        }
    }

    public static function flush(): bool
    {
        if (!self::$enabled || self::$provider === null) {
            return true;
        }
        try {
            self::ensureContextInitialized();
            $ok = self::$provider->forceFlush();
            if (!$ok) {
                self::warn('export', new \RuntimeException('OTLP forceFlush returned false'));
            }

            return $ok;
        } catch (Throwable $throwable) {
            self::warn('export', $throwable);

            return false;
        }
    }

    public static function shutdown(): void
    {
        self::ensureContextInitialized();
        if (self::$flushTimerId > 0) {
            Timer::del(self::$flushTimerId);
            self::$flushTimerId = 0;
        }
        try {
            self::$provider?->shutdown();
        } catch (Throwable $throwable) {
            self::warn('shutdown', $throwable);
        } finally {
            self::$provider = null;
            self::$tracer = null;
            self::$enabled = false;
            self::releaseContextRoots();
        }
    }

    public static function exportFailed(?Throwable $throwable = null): void
    {
        self::warn('export', $throwable ?? new \RuntimeException('OTLP exporter returned false'));
    }

    public static function queueDropped(): void
    {
        self::warn('queue_overflow', new \RuntimeException('OTel batch queue is full'));
    }

    public static function instrumentationFailed(Throwable $throwable): void
    {
        self::warn('instrumentation', $throwable);
    }

    public static function logContext(): string
    {
        $context = self::currentTraceContext();
        if ($context === null) {
            return 'trace_id=none span_id=none trace_flags=00';
        }

        return sprintf(
            'trace_id=%s span_id=%s trace_flags=%02x',
            $context->traceId(),
            $context->spanId(),
            $context->traceFlags(),
        );
    }

    private static function safeAttributes(array $attributes): array
    {
        $safe = [];
        foreach ($attributes as $key => $value) {
            if (!is_string($key) || $key === '' || self::isSensitiveAttributeKey($key)) {
                continue;
            }
            if (is_string($value)) {
                $value = mb_substr(preg_replace('/[\x00-\x1f\x7f]/u', '', $value) ?? '', 0, 256);
            }
            if (is_scalar($value) || $value === null || (is_array($value) && self::isScalarList($value))) {
                $safe[$key] = $value;
            }
        }

        return $safe;
    }

    private static function isSensitiveAttributeKey(string $key): bool
    {
        $key = strtolower($key);
        if (in_array($key, ['exception.message', 'error.message'], true)) {
            return true;
        }

        return preg_match(
            '/(^|[._-])(authorization|cookie|password|token|secret|content|body|payload|sql|query|url|file|stack(?:trace)?|email|phone)([._-]|$)/D',
            $key,
        ) === 1;
    }

    private static function isScalarList(array $value): bool
    {
        if (!array_is_list($value)) {
            return false;
        }
        foreach ($value as $item) {
            if (!is_scalar($item)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Workerman 5 runs every onWorkerStart callback in a fresh Fiber even when
     * the selected event loop is not Fiber based. OpenTelemetry deliberately
     * does not inherit context into arbitrary Fibers, so each execution context
     * needs an explicit root before the first current()/activate() access.
     */
    private static function ensureContextInitialized(): void
    {
        $fiber = Fiber::getCurrent();
        if ($fiber === null) {
            self::$mainRootScope ??= Context::storage()->attach(Context::getRoot());

            return;
        }

        self::$fiberRootScopes ??= new WeakMap();
        if (!isset(self::$fiberRootScopes[$fiber])) {
            self::$fiberRootScopes[$fiber] = Context::storage()->attach(Context::getRoot());
        }
    }

    private static function releaseContextRoots(): void
    {
        if (self::$fiberRootScopes !== null) {
            foreach (self::$fiberRootScopes as $scope) {
                try {
                    $scope->detach();
                } catch (Throwable $throwable) {
                    self::instrumentationFailed($throwable);
                }
            }
            self::$fiberRootScopes = null;
        }

        try {
            self::$mainRootScope?->detach();
        } catch (Throwable $throwable) {
            self::instrumentationFailed($throwable);
        }
        self::$mainRootScope = null;
    }

    private static function warn(string $operation, Throwable $throwable): void
    {
        $now = microtime(true);
        if ($now - self::$lastExportWarningAt < 60) {
            return;
        }
        self::$lastExportWarningAt = $now;
        echo sprintf(
            "%s IM telemetry %s failed; spans dropped without affecting business: %s\n",
            date('Y-m-d H:i:s'),
            $operation,
            mb_substr($throwable::class, 0, 200),
        );
    }
}
