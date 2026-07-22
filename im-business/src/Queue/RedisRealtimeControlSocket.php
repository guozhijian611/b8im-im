<?php

declare(strict_types=1);

namespace B8im\ImBusiness\Queue;

/**
 * Minimal RESP connection with explicit connect, write and read deadlines.
 * Only the commands required by the realtime-control publisher are exposed.
 */
final class RedisRealtimeControlSocket
{
    /** @var resource */
    private $socket;
    private string $readBuffer = '';

    /** @param resource $socket */
    private function __construct($socket, private readonly float $ioTimeoutSeconds)
    {
        $this->socket = $socket;
    }

    public static function connect(
        string $host,
        int $port,
        string $password,
        int $database,
        float $connectTimeoutSeconds = 0.2,
        float $ioTimeoutSeconds = 0.2,
    ): self {
        if (
            $host === ''
            || $port < 1
            || $port > 65535
            || $database < 0
            || $connectTimeoutSeconds <= 0.0
            || $connectTimeoutSeconds > 2.0
            || $ioTimeoutSeconds <= 0.0
            || $ioTimeoutSeconds > 2.0
        ) {
            throw new \InvalidArgumentException('control outbox Redis endpoint is invalid');
        }
        $endpointHost = str_contains($host, ':') && $host[0] !== '[' ? '[' . $host . ']' : $host;
        $errorNumber = 0;
        $errorMessage = '';
        $socket = @stream_socket_client(
            'tcp://' . $endpointHost . ':' . $port,
            $errorNumber,
            $errorMessage,
            $connectTimeoutSeconds,
            STREAM_CLIENT_CONNECT,
        );
        if (!is_resource($socket)) {
            throw new \RuntimeException('control outbox Redis connection failed');
        }
        if (!stream_set_blocking($socket, false)) {
            fclose($socket);
            throw new \RuntimeException('control outbox Redis socket configuration failed');
        }

        $connection = new self($socket, $ioTimeoutSeconds);
        try {
            if ($password !== '') {
                $connection->expectOk($connection->command(['AUTH', $password]), 'authentication');
            }
            if ($database > 0) {
                $connection->expectOk(
                    $connection->command(['SELECT', (string) $database]),
                    'database selection',
                );
            }
        } catch (\Throwable $error) {
            $connection->close();
            throw $error;
        }

        return $connection;
    }

    public function rPush(string $key, string $raw): int|false
    {
        $result = $this->command(['RPUSH', $key, $raw]);

        return is_int($result) && $result >= 1 ? $result : false;
    }

    public function __destruct()
    {
        $this->close();
    }

    private function close(): void
    {
        if (is_resource($this->socket)) {
            fclose($this->socket);
        }
    }

    /** @param list<string> $arguments */
    private function command(array $arguments): int|string
    {
        $request = '*' . count($arguments) . "\r\n";
        foreach ($arguments as $argument) {
            $request .= '$' . strlen($argument) . "\r\n" . $argument . "\r\n";
        }
        $this->writeAll($request, $this->deadline());

        $line = $this->readLine($this->deadline());
        $prefix = $line[0] ?? '';
        $value = substr($line, 1);
        if ($prefix === '+') {
            return $value;
        }
        if ($prefix === ':' && preg_match('/^-?[0-9]+$/D', $value) === 1) {
            return (int) $value;
        }
        if ($prefix === '-') {
            throw new \RuntimeException('control outbox Redis command failed');
        }
        throw new \RuntimeException('control outbox Redis response is invalid');
    }

    private function expectOk(int|string $response, string $operation): void
    {
        if (!is_string($response) || !hash_equals('OK', $response)) {
            throw new \RuntimeException('control outbox Redis ' . $operation . ' failed');
        }
    }

    private function writeAll(string $value, float $deadline): void
    {
        $offset = 0;
        $length = strlen($value);
        while ($offset < $length) {
            $this->waitForIo(false, $deadline);
            $written = @fwrite($this->socket, substr($value, $offset));
            if ($written === false) {
                throw new \RuntimeException('control outbox Redis write failed');
            }
            if ($written === 0) {
                if (feof($this->socket)) {
                    throw new \RuntimeException('control outbox Redis connection closed during write');
                }
                continue;
            }
            $offset += $written;
        }
    }

    private function readLine(float $deadline): string
    {
        while (($position = strpos($this->readBuffer, "\r\n")) === false) {
            $this->waitForIo(true, $deadline);
            $chunk = @fread($this->socket, 8192);
            if ($chunk === false) {
                throw new \RuntimeException('control outbox Redis read failed');
            }
            if ($chunk === '') {
                if (feof($this->socket)) {
                    throw new \RuntimeException('control outbox Redis connection closed during read');
                }
                continue;
            }
            $this->readBuffer .= $chunk;
            if (strlen($this->readBuffer) > 65536) {
                throw new \RuntimeException('control outbox Redis response is too large');
            }
        }

        $line = substr($this->readBuffer, 0, $position);
        $this->readBuffer = substr($this->readBuffer, $position + 2);

        return $line;
    }

    private function waitForIo(bool $read, float $deadline): void
    {
        $remaining = $deadline - hrtime(true) / 1_000_000_000;
        if ($remaining <= 0.0) {
            throw new \RuntimeException('control outbox Redis I/O timed out');
        }
        $seconds = (int) floor($remaining);
        $microseconds = (int) min(999999, max(0, floor(($remaining - $seconds) * 1_000_000)));
        $readSockets = $read ? [$this->socket] : [];
        $writeSockets = $read ? [] : [$this->socket];
        $exceptSockets = [$this->socket];
        $ready = @stream_select(
            $readSockets,
            $writeSockets,
            $exceptSockets,
            $seconds,
            $microseconds,
        );
        if ($ready === false) {
            throw new \RuntimeException('control outbox Redis socket wait failed');
        }
        if ($ready === 0 || $exceptSockets !== []) {
            throw new \RuntimeException('control outbox Redis I/O timed out');
        }
    }

    private function deadline(): float
    {
        return hrtime(true) / 1_000_000_000 + $this->ioTimeoutSeconds;
    }
}
