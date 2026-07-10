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
        $result = $this->handler->handle($message->getRoutingKey(), $message->getBody());
        $messageId = $message->has('message_id') ? (string) $message->get('message_id') : 'unknown';

        if ($result->outcome === RealtimeDeliveryResult::ACK) {
            $message->ack();
            return;
        }

        $line = sprintf(
            '%s IM realtime delivery %s: message_id=%s routing_key=%s attempt=%d reason=%s',
            date('Y-m-d H:i:s'),
            $result->outcome,
            $messageId,
            $message->getRoutingKey(),
            $result->attempt,
            mb_substr($result->reason, 0, 500),
        );
        echo $line . PHP_EOL;

        if ($result->outcome === RealtimeDeliveryResult::REQUEUE) {
            $message->nack(true);
            return;
        }

        $message->reject(false);
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
        ] as $routingKey) {
            $channel->queue_bind(Constants::MQ_MESSAGE_AFTER, $exchange, $routingKey);
        }

        $channel->queue_declare(Constants::MQ_MESSAGE_DLX, false, true, false, false);
        $channel->queue_bind(Constants::MQ_MESSAGE_DLX, $deadLetterExchange, '#');
    }
}
