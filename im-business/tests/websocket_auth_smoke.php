<?php

declare(strict_types=1);

use B8im\ImBusiness\Auth\ImToken;
use B8im\ImBusiness\Auth\TokenPolicy;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Support\Constants;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

/** @return string */
function readExact($stream, int $length): string
{
    $buffer = '';
    while (strlen($buffer) < $length) {
        $chunk = fread($stream, $length - strlen($buffer));
        if ($chunk === false || $chunk === '') {
            throw new RuntimeException('WebSocket connection closed while reading a frame.');
        }
        $buffer .= $chunk;
    }

    return $buffer;
}

/** @return array{opcode: int, payload: string} */
function readFrame($stream): array
{
    $header = readExact($stream, 2);
    $first = ord($header[0]);
    $second = ord($header[1]);
    $length = $second & 0x7f;
    if ($length === 126) {
        $length = unpack('n', readExact($stream, 2))[1];
    } elseif ($length === 127) {
        $parts = unpack('Nhigh/Nlow', readExact($stream, 8));
        if ($parts['high'] !== 0) {
            throw new RuntimeException('Frame is too large for the smoke test.');
        }
        $length = $parts['low'];
    }

    $masked = ($second & 0x80) !== 0;
    $mask = $masked ? readExact($stream, 4) : '';
    $payload = readExact($stream, $length);
    if ($masked) {
        for ($index = 0; $index < $length; ++$index) {
            $payload[$index] = $payload[$index] ^ $mask[$index % 4];
        }
    }

    return ['opcode' => $first & 0x0f, 'payload' => $payload];
}

function writeFrame($stream, string $payload, int $opcode = 1): void
{
    $length = strlen($payload);
    $header = chr(0x80 | $opcode);
    if ($length < 126) {
        $header .= chr(0x80 | $length);
    } elseif ($length <= 65535) {
        $header .= chr(0x80 | 126) . pack('n', $length);
    } else {
        $header .= chr(0x80 | 127) . pack('NN', 0, $length);
    }

    $mask = random_bytes(4);
    $masked = $payload;
    for ($index = 0; $index < $length; ++$index) {
        $masked[$index] = $masked[$index] ^ $mask[$index % 4];
    }

    fwrite($stream, $header . $mask . $masked);
}

$host = getenv('IM_WS_SMOKE_HOST') ?: '127.0.0.1';
$port = (int) (getenv('IM_WS_SMOKE_PORT') ?: 18787);
$origin = getenv('IM_WS_SMOKE_ORIGIN') ?: 'http://127.0.0.1:18080';
$stream = stream_socket_client("tcp://{$host}:{$port}", $errorCode, $errorMessage, 3);
if (!is_resource($stream)) {
    throw new RuntimeException("Unable to connect to isolated Gateway: {$errorCode} {$errorMessage}");
}
stream_set_timeout($stream, 5);

$key = base64_encode(random_bytes(16));
$request = "GET / HTTP/1.1\r\n"
    . "Host: {$host}:{$port}\r\n"
    . "Upgrade: websocket\r\n"
    . "Connection: Upgrade\r\n"
    . "Sec-WebSocket-Key: {$key}\r\n"
    . "Sec-WebSocket-Version: 13\r\n"
    . "Origin: {$origin}\r\n\r\n";
fwrite($stream, $request);

$status = trim((string) fgets($stream));
if (!str_contains($status, '101')) {
    throw new RuntimeException('Trusted Origin handshake failed: ' . $status);
}
while (($line = fgets($stream)) !== false && trim($line) !== '') {
}

$challengeFrame = readFrame($stream);
$challenge = json_decode($challengeFrame['payload'], true, 32, JSON_THROW_ON_ERROR);
$clientId = trim((string) ($challenge['data']['client_id'] ?? ''));
if (($challenge['cmd'] ?? null) !== Command::AUTH || $clientId === '') {
    throw new RuntimeException('Gateway did not provide an AUTH client_id challenge.');
}

$config = Config::fromEnv();
$repository = ImRepository::connect($config);
$expectedDatabase = trim((string) ($_ENV['IM_EXPECT_DATABASE'] ?? $_SERVER['IM_EXPECT_DATABASE'] ?? getenv('IM_EXPECT_DATABASE')));
if ($expectedDatabase === '') {
    throw new RuntimeException('IM_EXPECT_DATABASE is required for the WebSocket integration smoke test.');
}
$selectedDatabase = (string) ($repository->fetchOne('SELECT DATABASE() AS database_name')['database_name'] ?? '');
if ($config->dbName !== $expectedDatabase || $selectedDatabase !== $expectedDatabase) {
    throw new RuntimeException(sprintf(
        'WebSocket integration database mismatch: config=%s selected=%s expected=%s',
        $config->dbName,
        $selectedDatabase,
        $expectedDatabase,
    ));
}
$suffix = bin2hex(random_bytes(6));
$userId = 'ws-user-' . $suffix;
$deviceId = 'ws-device-' . $suffix;
$credentialSessionId = 'ws-session-' . $suffix;
$webAccessJti = md5('ws-web-access-' . $suffix);
$now = time();
$nowSql = date('Y-m-d H:i:s', $now);
$expireSql = date('Y-m-d H:i:s', $now + 600);
$connectionRevoked = false;
$authRedis = null;

try {
    $repository->execute(
        'INSERT INTO im_user
            (organization, user_id, account, password_hash, nickname, status, create_time, update_time)
         VALUES (1, ?, ?, ?, ?, 1, ?, ?)',
        [$userId, $userId, password_hash('integration-only', PASSWORD_DEFAULT), $userId, $nowSql, $nowSql],
    );
    $repository->execute(
        'INSERT INTO im_user_device
            (organization, user_id, device_id, client_family, os, status, create_time, update_time)
         VALUES (1, ?, ?, "web", "browser", 1, ?, ?)',
        [$userId, $deviceId, $nowSql, $nowSql],
    );
    $repository->execute(
        'INSERT INTO im_web_access_session
            (organization, jti, im_user_id, user_id, device_id, status, expire_at, create_time, update_time)
         SELECT 1, ?, id, user_id, ?, 1, ?, ?, ? FROM im_user
          WHERE organization = 1 AND user_id = ? LIMIT 1',
        [$webAccessJti, $deviceId, $expireSql, $nowSql, $nowSql, $userId],
    );
    $repository->execute(
        'INSERT INTO im_auth_session
            (organization, user_id, device_id, client_id, session_id, web_access_jti, status, expire_at, create_time, update_time)
         VALUES (1, ?, ?, ?, ?, ?, 1, ?, ?, ?)',
        [$userId, $deviceId, $clientId, $credentialSessionId, $webAccessJti, $expireSql, $nowSql, $nowSql],
    );

    $token = (new ImToken(new TokenPolicy(
        secret: $config->imTokenSecret,
        trustedIssuers: $config->imTokenTrustedIssuers,
        audience: $config->imTokenAudience,
        clockSkewSeconds: $config->imTokenClockSkewSeconds,
    )))->generate([
        'iss' => 'b8im-local',
        'deployment_id' => 'b8im-local',
        'aud' => ['im'],
        'nbf' => $now - 1,
        'exp' => $now + 300,
        'organization' => 1,
        'user_id' => $userId,
        'device_id' => $deviceId,
        'client_id' => $clientId,
        'session_id' => $credentialSessionId,
        'client_family' => 'web',
        'os' => 'browser',
    ]);

    writeFrame($stream, (new Packet(
        cmd: Command::AUTH,
        data: ['token' => $token],
        organization: 999999,
    ))->encode());
    $authAck = json_decode(readFrame($stream)['payload'], true, 32, JSON_THROW_ON_ERROR);
    if (
        ($authAck['cmd'] ?? null) !== Command::AUTH_ACK
        || ($authAck['organization'] ?? null) !== 1
        || ($authAck['data']['client_id'] ?? null) !== $clientId
        || preg_match('/^[a-f0-9]{32}$/', (string) ($authAck['data']['session_id'] ?? '')) !== 1
    ) {
        throw new RuntimeException('AUTH_ACK did not bind the trusted organization/client/session.');
    }

    writeFrame($stream, (new Packet(Command::PING, [], 999999))->encode());
    $pong = json_decode(readFrame($stream)['payload'], true, 32, JSON_THROW_ON_ERROR);
    if (($pong['cmd'] ?? null) !== Command::PONG || ($pong['organization'] ?? null) !== 1) {
        throw new RuntimeException('PONG trusted the client packet organization.');
    }

    $repository->execute(
        'UPDATE im_auth_session SET status = 2, revoked_at = ?, update_time = ?
          WHERE organization = 1 AND session_id = ?',
        [$nowSql, $nowSql, $credentialSessionId],
    );
    $authRedis = new Redis();
    $authRedis->connect($config->redisHost, $config->redisPort, 2.0);
    if ($config->redisPassword !== '') {
        $authRedis->auth($config->redisPassword);
    }
    if ($config->redisDb > 0) {
        $authRedis->select($config->redisDb);
    }
    $authRedis->del(sprintf(Constants::REDIS_AUTH_ACTIVE, 1, $credentialSessionId));

    writeFrame($stream, (new Packet(Command::PING, [], 1))->encode());
    $revoked = json_decode(readFrame($stream)['payload'], true, 32, JSON_THROW_ON_ERROR);
    if (
        ($revoked['cmd'] ?? null) !== Command::ERROR
        || ($revoked['data']['code'] ?? null) !== 'AUTH_SESSION_INACTIVE'
    ) {
        throw new RuntimeException('revoked credential session remained active on the real WebSocket.');
    }
    $serverClosed = false;
    try {
        $closeFrame = readFrame($stream);
        $serverClosed = $closeFrame['opcode'] === 8;
    } catch (RuntimeException $exception) {
        $serverClosed = str_contains($exception->getMessage(), 'connection closed');
    }
    if (!$serverClosed) {
        throw new RuntimeException('revoked credential received an error but its stale WebSocket remained open.');
    }
    $connectionRevoked = true;

    fwrite(STDOUT, "[PASS] real WebSocket trusted Origin, JWT AUTH, server-bound organization/session and fail-plus-close revocation\n");
} finally {
    if (!$connectionRevoked) {
        writeFrame($stream, '', 8);
    }
    fclose($stream);
    $closed = false;
    for ($attempt = 0; $attempt < 20; $attempt++) {
        usleep(50000);
        $device = $repository->fetchOne(
            'SELECT current_online_state FROM im_user_device
              WHERE organization = 1 AND user_id = ? AND device_id = ? LIMIT 1',
            [$userId, $deviceId],
        );
        $audit = $repository->fetchOne(
            'SELECT logout_at, current_online_state FROM im_user_login_audit
              WHERE organization = 1 AND user_id = ? AND client_id = ?
              ORDER BY id DESC LIMIT 1',
            [$userId, $clientId],
        );
        if (
            (int) ($device['current_online_state'] ?? 0) === 2
            && !empty($audit['logout_at'])
            && (int) ($audit['current_online_state'] ?? 0) === 2
        ) {
            $closed = true;
            break;
        }
    }
    $closeAuditFailed = !$closed;
    $repository->execute('DELETE FROM im_auth_session WHERE organization = 1 AND session_id = ?', [$credentialSessionId]);
    $repository->execute('DELETE FROM im_web_access_session WHERE organization = 1 AND jti = ?', [$webAccessJti]);
    if ($authRedis instanceof Redis) {
        $authRedis->del(sprintf(Constants::REDIS_AUTH_ACTIVE, 1, $credentialSessionId));
    }
    $repository->execute('DELETE FROM im_user_login_audit WHERE organization = 1 AND user_id = ?', [$userId]);
    $repository->execute('DELETE FROM im_user_device WHERE organization = 1 AND user_id = ? AND device_id = ?', [$userId, $deviceId]);
    $repository->execute('DELETE FROM im_user WHERE organization = 1 AND user_id = ?', [$userId]);
    if ($closeAuditFailed) {
        throw new RuntimeException('WebSocket close did not persist device/logout audit state.');
    }
}
