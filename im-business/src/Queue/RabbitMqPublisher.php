<?php
// +----------------------------------------------------------------------
// | b8im [ 即时通讯系统 ]
// +----------------------------------------------------------------------
// | RabbitMQ 发布器
// +----------------------------------------------------------------------
declare(strict_types=1);

namespace B8im\ImBusiness\Queue;

use B8im\ImBusiness\Config;
use B8im\ImShared\Support\Constants;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;
use B8im\ImShared\Telemetry\TraceContext;

final class RabbitMqPublisher
{
    private ?AMQPStreamConnection $connection = null;
    private ?AMQPChannel $channel = null;
    private bool $topologyDeclared = false;

    public function __construct(private readonly Config $config)
    {
    }

    public function publish(
        string $routingKey,
        array $payload,
        string $messageId,
        ?TraceContext $traceContext = null,
    ): void
    {
        try {
            $body = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR);
            $message = new AMQPMessage($body, [
                'content_type' => 'application/json',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
                'message_id' => $messageId,
                'timestamp' => time(),
                'application_headers' => new AMQPTable(self::applicationHeaders($payload, $traceContext)),
            ]);

            $this->channel()->basic_publish(
                $message,
                $this->config->rabbitmqExchange,
                $routingKey,
                true,
            );
            $this->channel()->wait_for_pending_acks_returns(5.0);
        } catch (Throwable $throwable) {
            $this->close();
            throw $throwable;
        }
    }

    /** @return array<string, int|string> */
    public static function applicationHeaders(array $payload, ?TraceContext $traceContext): array
    {
        return array_filter([
            'organization' => (int) ($payload['organization'] ?? 0),
            'event_type' => (string) ($payload['event_type'] ?? ''),
            'traceparent' => $traceContext?->traceparent,
            'tracestate' => $traceContext?->tracestate,
        ], static fn (mixed $value): bool => $value !== null && $value !== '');
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
        $this->topologyDeclared = false;
    }

    private function channel(): AMQPChannel
    {
        if ($this->channel !== null && $this->channel->is_open()) {
            return $this->channel;
        }

        $this->connection = new AMQPStreamConnection(
            $this->config->rabbitmqHost,
            $this->config->rabbitmqPort,
            $this->config->rabbitmqUser,
            $this->config->rabbitmqPassword,
            $this->config->rabbitmqVhost,
        );
        $this->channel = $this->connection->channel();
        $this->declareTopology($this->channel);
        $this->channel->set_nack_handler(static function (): never {
            throw new \RuntimeException('RabbitMQ negatively acknowledged an outbox event.');
        });
        $this->channel->set_return_listener(static function (
            int $replyCode,
            string $replyText,
            string $exchange,
            string $routingKey,
        ): never {
            throw new \RuntimeException(sprintf(
                'RabbitMQ returned an unroutable outbox event: %d %s %s/%s',
                $replyCode,
                $replyText,
                $exchange,
                $routingKey,
            ));
        });
        $this->channel->confirm_select();

        return $this->channel;
    }

    private function declareTopology(AMQPChannel $channel): void
    {
        if ($this->topologyDeclared) {
            return;
        }

        $exchange = $this->config->rabbitmqExchange;
        $deadLetterExchange = $exchange . '.dlx';

        $channel->exchange_declare($exchange, 'topic', false, true, false);
        $channel->exchange_declare($deadLetterExchange, 'topic', false, true, false);

        $queueOptions = new AMQPTable([
            'x-dead-letter-exchange' => $deadLetterExchange,
        ]);

        $this->declareQueue($channel, Constants::MQ_MESSAGE_AFTER, Constants::MQ_ROUTING_MESSAGE_CREATED, $queueOptions);
        $channel->queue_bind(Constants::MQ_MESSAGE_AFTER, $exchange, Constants::MQ_ROUTING_MESSAGE_RECALLED);
        $channel->queue_bind(Constants::MQ_MESSAGE_AFTER, $exchange, Constants::MQ_ROUTING_MESSAGE_EDITED);
        $channel->queue_bind(Constants::MQ_MESSAGE_AFTER, $exchange, Constants::MQ_ROUTING_MESSAGE_DELETED_BOTH);
        $channel->queue_bind(Constants::MQ_MESSAGE_AFTER, $exchange, Constants::MQ_ROUTING_MESSAGE_DELETED_SELF);
        $this->declareQueue($channel, Constants::MQ_GROUP_FANOUT, Constants::MQ_ROUTING_GROUP_FANOUT, $queueOptions);
        $this->declareQueue($channel, Constants::MQ_OFFLINE_PUSH, Constants::MQ_ROUTING_OFFLINE_PUSH, $queueOptions);
        $this->declareQueue($channel, Constants::MQ_MESSAGE_AUDIT, Constants::MQ_ROUTING_MESSAGE_AUDIT, $queueOptions);

        $channel->queue_declare(Constants::MQ_MESSAGE_DLX, false, true, false, false);
        $channel->queue_bind(Constants::MQ_MESSAGE_DLX, $deadLetterExchange, '#');

        $this->topologyDeclared = true;
    }

    private function declareQueue(AMQPChannel $channel, string $queue, string $routingKey, AMQPTable $options): void
    {
        $channel->queue_declare($queue, false, true, false, false, false, $options);
        $channel->queue_bind($queue, $this->config->rabbitmqExchange, $routingKey);
    }
}
