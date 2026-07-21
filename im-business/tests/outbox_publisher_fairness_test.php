<?php

declare(strict_types=1);

use B8im\ImBusiness\Process\OutboxPublisherTickScheduler;
use B8im\ImBusiness\Queue\RedisRealtimeControlPublisher;
use B8im\ImBusiness\Queue\RedisRealtimeControlSocket;

require dirname(__DIR__) . '/vendor/autoload.php';

$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    ++$assertions;
};

$errorNumber = 0;
$errorMessage = '';
$listener = stream_socket_server(
    'tcp://127.0.0.1:0',
    $errorNumber,
    $errorMessage,
    STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
);
if (!is_resource($listener)) {
    throw new RuntimeException('unable to create isolated Redis blackhole listener');
}
$address = (string) stream_socket_get_name($listener, false);
$separator = strrpos($address, ':');
if ($separator === false) {
    fclose($listener);
    throw new RuntimeException('isolated Redis blackhole listener address is invalid');
}
$host = substr($address, 0, $separator);
$port = (int) substr($address, $separator + 1);
if ($host !== '127.0.0.1' || $port < 1024 || $port > 65535) {
    fclose($listener);
    throw new RuntimeException('isolated Redis blackhole listener escaped loopback');
}

$child = pcntl_fork();
if ($child === 0) {
    $client = @stream_socket_accept($listener, 2.0);
    if (is_resource($client)) {
        // Accept the RESP bytes but deliberately never send a response.
        usleep(800_000);
        fclose($client);
    }
    fclose($listener);
    exit(0);
}
if ($child < 0) {
    fclose($listener);
    throw new RuntimeException('unable to fork isolated Redis blackhole listener');
}
fclose($listener);

try {
    $connects = 0;
    $publisher = new RedisRealtimeControlPublisher(
        connector: static function () use ($host, $port, &$connects): object {
            ++$connects;

            return RedisRealtimeControlSocket::connect($host, $port, '', 0, 0.12, 0.12);
        },
    );
    $scheduler = new OutboxPublisherTickScheduler(baseBackoffSeconds: 1.0, maxBackoffSeconds: 4.0);
    $rabbitTicks = 0;
    $controlAttempts = 0;
    $order = [];
    $startedAt = hrtime(true);
    $first = $scheduler->run(
        static function () use (&$rabbitTicks, &$order): void {
            ++$rabbitTicks;
            $order[] = 'rabbit';
        },
        static function () use ($publisher, &$controlAttempts, &$order): bool {
            ++$controlAttempts;
            $order[] = 'control';
            $publisher->publish('{"event_id":"blackhole"}');

            return true;
        },
    );
    $elapsedSeconds = (hrtime(true) - $startedAt) / 1_000_000_000;

    $assert($order === ['rabbit', 'control'], 'Rabbit was not served before the slow control attempt');
    $assert($rabbitTicks === 1, 'first timer tick did not serve Rabbit');
    $assert($controlAttempts === 1 && $connects === 1, 'first timer tick did not make exactly one control attempt');
    $assert($first['rabbit_error'] === null, 'healthy Rabbit work was reported as failed');
    $assert($first['control_error'] instanceof RuntimeException, 'Redis blackhole did not fail visibly');
    $assert(
        str_contains($first['control_error']->getMessage(), 'timed out'),
        'Redis blackhole did not hit the finite I/O deadline',
    );
    $assert($elapsedSeconds < 0.75, 'Redis blackhole exceeded the small per-tick control budget');

    $secondStartedAt = hrtime(true);
    $second = $scheduler->run(
        static function () use (&$rabbitTicks, &$order): void {
            ++$rabbitTicks;
            $order[] = 'rabbit';
        },
        static function () use (&$controlAttempts): bool {
            ++$controlAttempts;
            throw new RuntimeException('circuit breaker failed to skip Redis');
        },
    );
    $secondElapsedSeconds = (hrtime(true) - $secondStartedAt) / 1_000_000_000;

    $assert($rabbitTicks === 2, 'Rabbit was not served on the tick after a Redis timeout');
    $assert($controlAttempts === 1 && $connects === 1, 'open Redis circuit retried before backoff elapsed');
    $assert(!$second['control_attempted'] && $second['control_error'] === null, 'Redis circuit did not open');
    $assert($secondElapsedSeconds < 0.1, 'open Redis circuit delayed the next Rabbit tick');
} finally {
    pcntl_waitpid($child, $childStatus);
    if (!pcntl_wifexited($childStatus) || pcntl_wexitstatus($childStatus) !== 0) {
        throw new RuntimeException('isolated Redis blackhole listener failed');
    }
}

fwrite(STDOUT, sprintf("Outbox publisher fairness: %d assertions passed.\n", $assertions));
