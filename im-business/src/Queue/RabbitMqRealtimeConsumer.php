<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Queue;

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Realtime\RealtimeDeliveryHandler;
use B8im\ImBusiness\Realtime\RealtimeDeliveryResult;
use B8im\ImShared\Support\Constants;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;
use B8im\ImBusiness\Telemetry\Telemetry;
use B8im\ImShared\Telemetry\TraceContext;
use OpenTelemetry\API\Trace\SpanKind;

final class RabbitMqRealtimeConsumer
{
    private ?AMQPStreamConnection $connection = null;
    private ?AMQPChannel $channel = null;

    public function __construct(
        private readonly Config $config,
        private readonly RealtimeDeliveryHandler $handler,
    ) {
    }

    public function poll(): void
    {
        $this->connect();
        $this->connection?->checkHeartBeat();
        for ($i = 0; $i < $this->config->mqRealtimePrefetch; $i++) {
            $this->channel?->wait(null, true);
        }
    }

    public function close(): void
    {
        try {
            $this->channel?->close();
        } catch (Throwable) {
        }
        try {
            $this->connection?->close();
        } catch (Throwable) {
        }
        $this->channel = null;
        $this->connection = null;
    }

    private function connect(): void
    {
        if ($this->channel !== null && $this->channel->is_open()) {
            return;
        }

        $this->connection = new AMQPStreamConnection(
            $this->config->rabbitmqHost,
            $this->config->rabbitmqPort,
            $this->config->rabbitmqUser,
            $this->config->rabbitmqPassword,
            $this->config->rabbitmqVhost,
            false,
            'AMQPLAIN',
            null,
            'en_US',
            3.0,
            65.0,
            null,
            true,
            30,
        );
        $this->channel = $this->connection->channel();
        $this->declareTopology($this->channel);
        $this->channel->basic_qos(0, $this->config->mqRealtimePrefetch, false);
        $this->channel->basic_consume(
            Constants::MQ_MESSAGE_AFTER,
            'im-realtime-' . (getmypid() ?: 0) . '-' . bin2hex(random_bytes(4)),
            false,
            false,
            false,
            false,
            function (AMQPMessage $message): void {
                $this->consume($message);
            },
        );
    }

    private function consume(AMQPMessage $message): void
    {
        $headers = $message->has('application_headers')
            ? $message->get('application_headers')->getNativeData()
            : [];
        $parent = null;
        try {
            $parent = TraceContext::fromCarrier(
                isset($headers['traceparent']) ? (string) $headers['traceparent'] : null,
                isset($headers['tracestate']) ? (string) $headers['tracestate'] : null,
            );
        } catch (\InvalidArgumentException) {
        }
        $messageId = $message->has('message_id') ? (string) $message->get('message_id') : 'unknown';
        $trace = Telemetry::start(
            'im.rabbitmq.consume ' . $message->getRoutingKey(),
            SpanKind::KIND_CONSUMER,
            $parent,
            [
                'operation' => 'im.rabbitmq.consume',
                'messaging.system' => 'rabbitmq',
                'messaging.destination.name' => $message->getRoutingKey(),
                'messaging.message.id' => $messageId,
                'b8im.organization' => (int) ($headers['organization'] ?? 0),
            ],
        );
        try {
            $result = $this->handler->handle($message->getRoutingKey(), $message->getBody());
        } catch (Throwable $throwable) {
            Telemetry::recordError(
                $trace->span,
                $throwable,
                'IM_REALTIME_CONSUMER_FAILED',
                'messaging',
                'im.rabbitmq.consume',
                ['retry_count' => 0, 'messaging.message.id' => $messageId],
            );
            $trace->end();
            throw $throwable;
        }

        if ($result->outcome === RealtimeDeliveryResult::ACK) {
            try {
                $message->ack();
            } catch (Throwable $throwable) {
                Telemetry::recordError(
                    $trace->span,
                    $throwable,
                    'IM_RABBITMQ_ACK_FAILED',
                    'messaging',
                    'im.rabbitmq.consume',
                    ['retry_count' => 0, 'messaging.message.id' => $messageId],
                );
                throw $throwable;
            } finally {
                $trace->end();
            }
            return;
        }

        Telemetry::recordError(
            $trace->span,
            new \RuntimeException($result->outcome),
            $result->outcome === RealtimeDeliveryResult::REQUEUE
                ? 'IM_REALTIME_DELIVERY_RETRY'
                : 'IM_REALTIME_DELIVERY_DEAD_LETTER',
            'messaging',
            'im.rabbitmq.consume',
            ['retry_count' => $result->attempt, 'messaging.message.id' => $messageId],
        );

        $errorCode = $result->outcome === RealtimeDeliveryResult::REQUEUE
            ? 'IM_REALTIME_DELIVERY_RETRY'
            : 'IM_REALTIME_DELIVERY_DEAD_LETTER';
        $line = sprintf(
            '%s IM realtime delivery %s: error_code=%s message_id=%s routing_key=%s attempt=%d %s',
            date('Y-m-d H:i:s'),
            $result->outcome,
            $errorCode,
            $messageId,
            $message->getRoutingKey(),
            $result->attempt,
            Telemetry::logContext(),
        );
        echo $line . PHP_EOL;

        if ($result->outcome === RealtimeDeliveryResult::REQUEUE) {
            try {
                $message->nack(true);
            } finally {
                $trace->end();
            }
            return;
        }

        try {
            $message->reject(false);
        } finally {
            $trace->end();
        }
    }

    private function declareTopology(AMQPChannel $channel): void
    {
        $exchange = $this->config->rabbitmqExchange;
        $deadLetterExchange = $exchange . '.dlx';
        $channel->exchange_declare($exchange, 'topic', false, true, false);
        $channel->exchange_declare($deadLetterExchange, 'topic', false, true, false);
        $channel->queue_declare(
            Constants::MQ_MESSAGE_AFTER,
            false,
            true,
            false,
            false,
            false,
            new AMQPTable(['x-dead-letter-exchange' => $deadLetterExchange]),
        );
        foreach ([
            Constants::MQ_ROUTING_MESSAGE_CREATED,
            Constants::MQ_ROUTING_MESSAGE_RECALLED,
            Constants::MQ_ROUTING_MESSAGE_EDITED,
            Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH,
            Constants::MQ_ROUTING_MESSAGE_DELETED_SELF,
            Constants::MQ_ROUTING_MESSAGE_RECEIPT,
            Constants::MQ_ROUTING_CONVERSATION_READ,
            Constants::MQ_ROUTING_CONVERSATION_ACCESS_CHANGED,
            Constants::MQ_ROUTING_GROUP_MEMBER_ACCESS_CHANGED,
        ] as $routingKey) {
            $channel->queue_bind(Constants::MQ_MESSAGE_AFTER, $exchange, $routingKey);
        }

        $channel->queue_declare(Constants::MQ_MESSAGE_DLX, false, true, false, false);
        $channel->queue_bind(Constants::MQ_MESSAGE_DLX, $deadLetterExchange, '#');
    }
}
