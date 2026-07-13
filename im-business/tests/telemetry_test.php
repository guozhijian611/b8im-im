<?php

declare(strict_types=1);

use B8im\ImBusiness\Queue\RabbitMqPublisher;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Telemetry\Telemetry;
use B8im\ImShared\Telemetry\TraceContext;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Protocol\Command;
use OpenTelemetry\API\Trace\SpanKind;

require dirname(__DIR__) . '/vendor/autoload.php';

$traceparent = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
$context = new TraceContext($traceparent, 'b8im=test');
$headers = RabbitMqPublisher::applicationHeaders([
    'organization' => 7,
    'event_type' => 'message.created',
    'content' => ['text' => 'must never become a header'],
], $context);

if ($headers !== [
    'organization' => 7,
    'event_type' => 'message.created',
    'traceparent' => $traceparent,
    'tracestate' => 'b8im=test',
]) {
    throw new RuntimeException('RabbitMQ W3C application headers are incomplete');
}
if (array_key_exists('content', $headers) || array_key_exists('baggage', $headers)) {
    throw new RuntimeException('sensitive or unsupported propagation header was emitted');
}

$safeAttributes = new ReflectionMethod(Telemetry::class, 'safeAttributes');
$safeAttributes->setAccessible(true);
$filtered = $safeAttributes->invoke(null, [
    'b8im.message_id' => 'allowed-message-id',
    'messaging.message.id' => 'allowed-broker-id',
    'error.code' => 'IM_TEST_ERROR',
    'exception.message' => 'forbidden',
    'error.message' => 'forbidden',
    'exception.stacktrace' => 'forbidden',
    'request.payload' => 'forbidden',
    'db.query' => 'forbidden',
    'attachment.file' => 'forbidden',
    'user.email' => 'forbidden',
    'user.phone' => 'forbidden',
]);
if ($filtered !== [
    'b8im.message_id' => 'allowed-message-id',
    'messaging.message.id' => 'allowed-broker-id',
    'error.code' => 'IM_TEST_ERROR',
]) {
    throw new RuntimeException('telemetry attribute redaction policy drifted');
}

putenv('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT');
unset($_ENV['OTEL_EXPORTER_OTLP_TRACES_ENDPOINT'], $_SERVER['OTEL_EXPORTER_OTLP_TRACES_ENDPOINT']);
if (Config::fromEnv()->otelTracesEndpoint !== 'http://otel-collector:4318/v1/traces') {
    throw new RuntimeException('default OTLP path must target the Collector, never Jaeger directly');
}

putenv('OTEL_TRACES_ENABLED=true');
putenv('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:9/v1/traces');
putenv('OTEL_EXPORTER_OTLP_TRACES_TIMEOUT=50');
putenv('OTEL_BSP_SCHEDULE_DELAY_MS=250');
putenv('OTEL_SERVICE_NAME=must-not-override-trusted-code');
putenv('OTEL_SERVICE_VERSION=../../invalid service version');
if (Config::fromEnv()->otelServiceVersion !== 'unknown') {
    throw new RuntimeException('unsafe service version was not rejected');
}
putenv('OTEL_SERVICE_VERSION=2026.07.14-trace');
$config = Config::fromEnv();
if ($config->otelServiceVersion !== '2026.07.14-trace') {
    throw new RuntimeException('bounded service version was not loaded');
}
Telemetry::boot($config, 'b8im-im-telemetry-test');
$serviceName = new ReflectionProperty(Telemetry::class, 'serviceName');
$serviceName->setAccessible(true);
if ($serviceName->getValue() !== 'b8im-im-telemetry-test') {
    throw new RuntimeException('untrusted OTEL_SERVICE_NAME overrode the process-owned service name');
}
$server = Telemetry::start('im.ws.send', SpanKind::KIND_SERVER, $context);
$serverContext = Telemetry::currentTraceContext();
if ($serverContext === null
    || $serverContext->traceId() !== $context->traceId()
    || $serverContext->spanId() === $context->spanId()) {
    throw new RuntimeException('inbound W3C parent did not create a new server span');
}
$producer = Telemetry::start('im.rabbitmq.publish', SpanKind::KIND_PRODUCER);
$producerContext = Telemetry::currentTraceContext();
if ($producerContext === null
    || $producerContext->traceId() !== $context->traceId()
    || $producerContext->spanId() === $serverContext->spanId()) {
    throw new RuntimeException('producer did not create a distinct child span');
}
$logContext = Telemetry::logContext();
if (!str_contains($logContext, 'trace_id=' . $producerContext->traceId())
    || !str_contains($logContext, 'span_id=' . $producerContext->spanId())
    || !str_contains($logContext, 'trace_flags=')) {
    throw new RuntimeException('active error log correlation fields are incomplete');
}
$ack = Packet::make(Command::SEND_ACK, ['ok' => true], 7, 'qa-client', $serverContext);
if ($ack->traceContext()?->traceId() !== $context->traceId()
    || $ack->traceContext()?->spanId() === $context->spanId()) {
    throw new RuntimeException('ACK did not retain the trace with a new server span id');
}
Telemetry::recordError(
    $producer->span,
    new RuntimeException('secret body must not be exported'),
    'IM_TEST_ERROR',
    'test',
    'im.rabbitmq.publish',
    ['retry_count' => 1, 'content' => 'forbidden'],
);
$producer->end();
$consumer = Telemetry::start('im.rabbitmq.consume', SpanKind::KIND_CONSUMER, $producerContext);
$consumerContext = Telemetry::currentTraceContext();
if ($consumerContext === null
    || $consumerContext->traceId() !== $context->traceId()
    || $consumerContext->spanId() === $producerContext->spanId()) {
    throw new RuntimeException('consumer did not extract the producer context into a new span');
}
$push = Telemetry::start('im.gateway.push', SpanKind::KIND_PRODUCER);
$pushContext = Telemetry::currentTraceContext();
$pushPacket = Packet::make(Command::PUSH, ['event_id' => str_repeat('a', 64)], 7, null, $pushContext);
if ($pushPacket->traceContext()?->traceId() !== $context->traceId()
    || $pushPacket->traceContext()?->spanId() === $consumerContext->spanId()) {
    throw new RuntimeException('PUSH did not retain the trace with a new gateway span id');
}
$push->end();
$consumer->end();
$errorScope = Telemetry::start('im.ws.send', SpanKind::KIND_SERVER, $context);
$errorContext = Telemetry::currentTraceContext();
Telemetry::recordError(
    $errorScope->span,
    new RuntimeException('cross organization'),
    'SEND_SINGLE_RECEIVER_INVALID',
    'business',
    'im.ws.send',
    ['retry_count' => 0],
);
$errorPacket = Packet::make(
    Command::ERROR,
    ['code' => 'SEND_SINGLE_RECEIVER_INVALID'],
    7,
    'qa-cross',
    $errorContext,
);
if ($errorPacket->traceContext()?->traceId() !== $context->traceId()
    || $errorPacket->traceContext()?->spanId() === $context->spanId()) {
    throw new RuntimeException('ERROR did not retain the trace with a new server span id');
}
$errorScope->end();
$server->end();
Telemetry::flush();
Telemetry::shutdown();

fwrite(STDOUT, "[PASS] W3C spans, safe errors and non-fatal OTLP export are canonical\n");
