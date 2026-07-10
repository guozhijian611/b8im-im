<?php

declare(strict_types=1);

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Auth\ConnectionFailurePolicy;
use B8im\ImBusiness\Auth\ImToken;
use B8im\ImBusiness\Auth\TokenPolicy;
use B8im\ImBusiness\CmdDispatcher;
use B8im\ImBusiness\Connection\ConnectionStore;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImShared\Protocol\CmdHandlerInterface;
use B8im\ImShared\Protocol\Command;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Support\Constants;

require dirname(__DIR__) . '/vendor/autoload.php';

$tests = [];

function test(string $name, callable $callback): void
{
    global $tests;
    $tests[$name] = $callback;
}

function assertTrue(bool $condition, string $message = 'assertion failed'): void
{
    if (!$condition) {
        throw new RuntimeException($message);
    }
}

function expectImCode(string $code, callable $callback): void
{
    try {
        $callback();
    } catch (ImException $exception) {
        assertTrue($exception->errorCode() === $code, "expected {$code}, got {$exception->errorCode()}");
        return;
    }

    throw new RuntimeException("expected ImException {$code}");
}

$now = 1783600000;
$policy = new TokenPolicy(
    secret: str_repeat('s', 32),
    trustedIssuers: ['deployment-main'],
    audience: 'im',
    clockSkewSeconds: 0,
);
$tokenService = new ImToken($policy);
$claims = [
    'iss' => 'deployment-main',
    'deployment_id' => 'deployment-main',
    'aud' => ['api', 'im'],
    'nbf' => $now - 10,
    'exp' => $now + 300,
    'organization' => 1,
    'user_id' => 'user-1',
    'device_id' => 'device-1',
    'client_id' => 'gateway-client-1',
    'session_id' => 'credential-session-1',
    'client_family' => 'web',
    'os' => 'browser',
];

test('JWT requires trusted issuer, IM audience, time and identity claims', static function () use ($tokenService, $claims, $now): void {
    $context = $tokenService->verify($tokenService->generate($claims), 'gateway-client-1', $now);
    assertTrue($context->organization === 1);
    assertTrue($context->credentialSessionId === 'credential-session-1');
    assertTrue($context->sessionId === '');

    foreach (['iss', 'deployment_id', 'exp', 'nbf', 'organization', 'user_id', 'device_id', 'client_id', 'session_id', 'client_family', 'os'] as $claim) {
        $invalid = $claims;
        unset($invalid[$claim]);
        expectImCode(
            'AUTH_TOKEN_CLAIM',
            static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now),
        );
    }
});

test('JWT rejects wrong issuer, audience, client, nbf, expiry and signature', static function () use ($tokenService, $claims, $now): void {
    $invalid = $claims;
    $invalid['iss'] = 'deployment-foreign';
    expectImCode('AUTH_TOKEN_ISSUER', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $invalid = $claims;
    $invalid['deployment_id'] = 'deployment-foreign';
    expectImCode('AUTH_TOKEN_DEPLOYMENT', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $invalid = $claims;
    $invalid['aud'] = ['api'];
    expectImCode('AUTH_TOKEN_AUDIENCE', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    expectImCode('AUTH_CLIENT_MISMATCH', static fn () => $tokenService->verify($tokenService->generate($claims), 'gateway-client-2', $now));

    $invalid = $claims;
    $invalid['nbf'] = $now + 1;
    expectImCode('AUTH_TOKEN_NOT_BEFORE', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $invalid = $claims;
    $invalid['exp'] = $now;
    expectImCode('AUTH_TOKEN_EXPIRED', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $invalid = $claims;
    $invalid['nbf'] = 0;
    expectImCode('AUTH_TOKEN_TIME_RANGE', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $invalid = $claims;
    $invalid['client_family'] = 'mobile';
    expectImCode('AUTH_TOKEN_CLIENT_FAMILY', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $invalid = $claims;
    $invalid['os'] = 'darwin';
    expectImCode('AUTH_TOKEN_OS', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $invalid = $claims;
    $invalid['os'] = 'macos';
    expectImCode('AUTH_TOKEN_CLIENT_OS', static fn () => $tokenService->verify($tokenService->generate($invalid), 'gateway-client-1', $now));

    $token = $tokenService->generate($claims);
    $token[strlen($token) - 1] = $token[strlen($token) - 1] === 'A' ? 'B' : 'A';
    expectImCode('AUTH_TOKEN_SIGNATURE', static fn () => $tokenService->verify($token, 'gateway-client-1', $now));
});

test('authentication generates a unique connection session', static function () use ($tokenService, $claims, $now): void {
    $context = $tokenService->verify($tokenService->generate($claims), 'gateway-client-1', $now);
    $first = $context->withSessionId(bin2hex(random_bytes(16)));
    $second = $context->withSessionId(bin2hex(random_bytes(16)));
    assertTrue($first->sessionId !== $second->sessionId);
    assertTrue(preg_match('/^[a-f0-9]{32}$/', $first->sessionId) === 1);
});

test('token policy rejects long placeholder secrets and accepts strong random material', static function (): void {
    foreach ([
        'please-change-me-to-at-least-32-bytes',
        'example-secret-that-is-long-enough-for-hmac',
        'replace-with-a-real-random-token-secret-now',
    ] as $placeholder) {
        try {
            new TokenPolicy($placeholder, ['deployment-main'], 'im');
            throw new RuntimeException('placeholder IM token secret was accepted');
        } catch (InvalidArgumentException) {
        }
    }

    $strong = base64_encode(random_bytes(48));
    $policy = new TokenPolicy($strong, ['deployment-main'], 'im');
    assertTrue(hash_equals($strong, $policy->secret), 'strong random IM token secret was changed or rejected');
});

test('terminal identity failures close authenticated connections without classifying business errors', static function (): void {
    foreach ([
        'AUTH_SESSION_INACTIVE',
        'AUTH_DEVICE_INACTIVE',
        'AUTH_ORGANIZATION_INACTIVE',
        'AUTH_SESSION_NOT_BOUND',
        'AUTH_TOKEN_EXPIRED',
        'ACCOUNT_POLICY_BLOCKED',
    ] as $terminalCode) {
        assertTrue(
            ConnectionFailurePolicy::shouldClose(Command::PING, $terminalCode),
            $terminalCode . ' did not terminate a stale authenticated connection',
        );
    }
    foreach (['MESSAGE_QPS_EXCEEDED', 'CONVERSATION_FORBIDDEN', 'MODULE_NOT_ENABLED', 'TENANT_POLICY_FORBIDDEN'] as $businessCode) {
        assertTrue(
            !ConnectionFailurePolicy::shouldClose(Command::SEND, $businessCode),
            $businessCode . ' incorrectly terminated a valid connection',
        );
    }
    assertTrue(ConnectionFailurePolicy::shouldClose(Command::AUTH, 'AUTH_TOKEN_SIGNATURE'));
});

test('module dispatch replaces untrusted packet organization', static function (): void {
    $guardOrganization = null;
    $handler = new class() implements CmdHandlerInterface {
        public ?int $seenOrganization = null;

        public function cmd(): string
        {
            return 'tenant_probe';
        }

        public function handle(string $clientId, Packet $packet): void
        {
            $this->seenOrganization = $packet->organization;
        }
    };

    $dispatcher = new CmdDispatcher();
    $dispatcher->register($handler, static function (int $organization) use (&$guardOrganization): void {
        $guardOrganization = $organization;
    });
    $dispatcher->dispatch('client-1', 7, new Packet('tenant_probe', [], 999));

    assertTrue($handler->seenOrganization === 7, 'handler received client organization');
    assertTrue($guardOrganization === 7, 'license guard received client organization');
});

test('same device connections coexist with exact session binding and cleanup', static function (): void {
    $redis = new Redis();
    $redis->connect('127.0.0.1', 6379, 2.0);

    $organization = random_int(900000000, 999999999);
    $userId = 'race-' . bin2hex(random_bytes(4));
    $deviceId = 'device-shared';
    $oldClient = 'old-' . bin2hex(random_bytes(6));
    $newClient = 'new-' . bin2hex(random_bytes(6));
    $store = new ConnectionStore($redis, 60);

    $makeContext = static fn (string $clientId, string $sessionId): AuthContext => new AuthContext(
        organization: $organization,
        userId: $userId,
        deviceId: $deviceId,
        clientId: $clientId,
        credentialSessionId: 'credential-' . $clientId,
        sessionId: $sessionId,
        clientFamily: 'web',
        os: 'browser',
        issuer: 'deployment-main',
        audience: 'im',
        notBefore: time() - 1,
        expireAt: time() + 60,
    );

    $oldContext = $makeContext($oldClient, str_repeat('a', 32));
    $newContext = $makeContext($newClient, str_repeat('b', 32));
    $store->bind($oldClient, $oldContext);
    $store->bind($newClient, $newContext);
    assertTrue($store->isBoundConnection($oldContext), 'first same-device connection was overwritten');
    assertTrue($store->isBoundConnection($newContext), 'second same-device connection was not bound');

    $devicesKey = sprintf(Constants::REDIS_DEVICES, $organization, $userId);
    $deviceConnections = $redis->hGetAll($devicesKey);
    assertTrue(count($deviceConnections) === 2, 'same-device connection index did not retain both sessions');
    assertTrue(isset($deviceConnections[$oldContext->sessionId], $deviceConnections[$newContext->sessionId]));

    $store->unbind($oldClient);
    assertTrue(!$store->isBoundConnection($oldContext), 'closed same-device connection remained bound');
    assertTrue($store->isBoundConnection($newContext), 'closing one session removed its same-device peer');
    $remaining = json_decode((string) $redis->hGet($devicesKey, $newContext->sessionId), true);
    assertTrue(is_array($remaining) && $remaining['client_id'] === $newClient, 'exact session cleanup removed the wrong connection');

    $store->unbind($newClient);
    $redis->del($devicesKey, sprintf(Constants::REDIS_ONLINE, $organization, $userId));
});

test('request source tree contains no runtime DDL or packet organization trust', static function (): void {
    $sourceRoot = dirname(__DIR__) . '/src';
    $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($sourceRoot));
    foreach ($iterator as $file) {
        if (!$file->isFile() || $file->getExtension() !== 'php') {
            continue;
        }
        $source = (string) file_get_contents($file->getPathname());
        assertTrue(
            preg_match('/\b(CREATE|ALTER|DROP)\s+TABLE\b/i', $source) !== 1,
            'runtime DDL found in ' . $file->getPathname(),
        );
        assertTrue(
            !str_contains($source, '$packet->organization'),
            'client packet organization trust found in ' . $file->getPathname(),
        );
    }
});

$failed = 0;
foreach ($tests as $name => $callback) {
    try {
        $callback();
        fwrite(STDOUT, "[PASS] {$name}\n");
    } catch (Throwable $throwable) {
        ++$failed;
        fwrite(STDERR, sprintf("[FAIL] %s\n       %s: %s\n", $name, $throwable::class, $throwable->getMessage()));
    }
}

fwrite(STDOUT, sprintf("\n%d tests, %d failed.\n", count($tests), $failed));
exit($failed === 0 ? 0 : 1);
