<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Telemetry\Telemetry;

require dirname(__DIR__) . '/vendor/autoload.php';

$warnings = [];
set_error_handler(static function (int $severity, string $message) use (&$warnings): bool {
    if (str_contains($message, 'not initialized OpenTelemetry context in fiber')) {
        $warnings[] = $message;

        return true;
    }

    return false;
});

$probe = static function (string $serviceName, bool $exerciseSpan): void {
    $config = Config::fromEnv();
    Telemetry::boot($config, $serviceName);
    Telemetry::boot($config, $serviceName);

    if ($exerciseSpan) {
        $scope = Telemetry::start('im.test.worker_start');
        Telemetry::currentTraceContext();
        Telemetry::setCurrentAttributes(['operation' => 'im.test.worker_start']);
        $scope->end();
    }
    Telemetry::shutdown();
};

try {
    // Workerman 5 uses this exact shape for onWorkerStart on a non-coroutine loop.
    // Enabled SDK boot is the exact staging regression: it used to warn before
    // any application span was created.
    putenv('OTEL_TRACES_ENABLED=true');
    putenv('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:9/v1/traces');
    (new Fiber(static fn () => $probe('b8im-im-fiber-worker-1', false)))->start();

    // Also prove no-op/CLI and a later independent Fiber are initialized and
    // that shutdown followed by another idempotent boot is safe.
    putenv('OTEL_TRACES_ENABLED=false');
    $probe('b8im-im-cli', true);
    (new Fiber(static fn () => $probe('b8im-im-fiber-worker-2', true)))->start();
} finally {
    restore_error_handler();
}

if ($warnings !== []) {
    throw new RuntimeException('OTel Fiber context was accessed before its root was attached');
}

fwrite(STDOUT, "[PASS] Workerman worker Fiber and CLI contexts initialize once without OTel warnings\n");
