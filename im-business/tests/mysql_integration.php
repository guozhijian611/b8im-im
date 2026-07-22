<?php

declare(strict_types=1);

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Auth\AuthIdentityValidator;
use B8im\ImBusiness\Auth\ActiveSessionGuard;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Realtime\DatabaseRealtimeRecipientProvider;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Service\DeviceService;
use B8im\ImBusiness\Service\MessageService;
use B8im\ImBusiness\Service\ModuleLicenseChecker;
use B8im\ImBusiness\Service\OutboxService;
use B8im\ImBusiness\Service\TenantImPolicyService;
use B8im\ImShared\Protocol\MessageType;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Support\Constants;
use B8im\ImBusiness\Telemetry\Telemetry;
use B8im\ImShared\Telemetry\TraceContext;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

$testExporterFailure = getenv('IM_TEST_OTEL_FAILURE') === '1';
if ($testExporterFailure) {
    putenv('OTEL_TRACES_ENABLED=true');
    putenv('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:9/v1/traces');
    putenv('OTEL_EXPORTER_OTLP_TRACES_TIMEOUT=50');
}
$config = Config::fromEnv();
if ($testExporterFailure) {
    Telemetry::boot($config, 'b8im-im-mysql-integration');
}
$repository = ImRepository::connect($config);
$expectedDatabase = trim((string) ($_ENV['IM_EXPECT_DATABASE'] ?? $_SERVER['IM_EXPECT_DATABASE'] ?? getenv('IM_EXPECT_DATABASE')));
if ($expectedDatabase === '') {
    throw new RuntimeException('IM_EXPECT_DATABASE is required for destructive IM integration tests');
}
$selectedDatabase = (string) ($repository->fetchOne('SELECT DATABASE() AS database_name')['database_name'] ?? '');
if ($config->dbName !== $expectedDatabase || $selectedDatabase !== $expectedDatabase) {
    throw new RuntimeException(sprintf(
        'integration database mismatch: config=%s selected=%s expected=%s',
        $config->dbName,
        $selectedDatabase,
        $expectedDatabase,
    ));
}
$tenantImPolicies = TenantImPolicyService::connect($config, $repository);
$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    $tenantImPolicies,
);
$messages->preflight();
$identityValidator = new AuthIdentityValidator($repository);

$suffix = bin2hex(random_bytes(6));
$senderId = 'it-sender-' . $suffix;
$recipientId = 'it-recipient-' . $suffix;
$otherId = 'it-other-' . $suffix;
$deviceId = 'it-device-' . $suffix;
$clientId = 'it-client-' . $suffix;
$credentialSessionId = 'it-session-' . $suffix;
$webAccessJti = md5('it-web-access-' . $suffix);
$assetFileId = sha1('it-asset-' . $suffix);
$senderAvatarFileId = sha1('it-avatar-' . $suffix);
$clientMsgId1 = 'it-message-1-' . $suffix;
$clientMsgId2 = 'it-message-2-' . $suffix;
$groupClientMsgIds = [
    'it-group-message-1-' . $suffix,
    'it-group-message-2-' . $suffix,
    'it-group-message-3-' . $suffix,
];
$createdMessageIds = [];
$conversationId = null;
$groupConversationId = null;
$initialGlobalSeq = null;
$authRedis = null;
$activeSessions = null;
$moduleKey = 'it_module_' . $suffix;
$moduleCacheKey = sprintf(Constants::REDIS_MODULE_LICENSE, 1, $moduleKey);

$context = static fn (string $userId): AuthContext => new AuthContext(
    organization: 1,
    userId: $userId,
    deviceId: $deviceId,
    clientId: $clientId,
    credentialSessionId: $credentialSessionId,
    sessionId: str_repeat($userId === $senderId ? 'a' : 'b', 32),
    clientFamily: 'web',
    os: 'browser',
    issuer: 'b8im-local',
    audience: 'im',
    notBefore: time() - 5,
    expireAt: time() + 300,
);

try {
    $now = date('Y-m-d H:i:s');
    $sessionExpireAt = date('Y-m-d H:i:s', time() + 600);
    $requiredTables = [
        'im_runtime_config',
        'im_user_profile',
        'im_user_privacy_setting',
        'im_user_security_policy',
        'im_friend_relation',
        'im_friend_request',
        'im_user_device',
        'im_user_login_audit',
        'im_web_access_session',
        'im_upload_asset',
        'im_group_profile',
        'im_message_group',
        'im_conversation_membership_period',
        'im_message_change',
        'im_realtime_control_outbox',
    ];
    foreach ($requiredTables as $requiredTable) {
        $table = $repository->fetchOne(
            'SELECT 1 AS present FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1',
            [$requiredTable],
        );
        if ($table === null) {
            throw new RuntimeException('required IM schema table is missing: ' . $requiredTable);
        }
    }
    $runtimeBuckets = $repository->fetchOne(
        'SELECT config_value FROM im_runtime_config WHERE config_key = ? LIMIT 1',
        ['message_shard_buckets'],
    );
    if ((int) ($runtimeBuckets['config_value'] ?? 0) !== $config->messageShardBuckets) {
        throw new RuntimeException('immutable message shard bucket configuration is missing or mismatched');
    }

    $assertColumns = static function (string $table, array $required, array $forbidden = []) use ($repository): void {
        $rows = $repository->fetchAll(
            'SELECT COLUMN_NAME AS column_name FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?',
            [$table],
        );
        $columns = array_map(static fn (array $row): string => (string) $row['column_name'], $rows);
        foreach ($required as $column) {
            if (!in_array($column, $columns, true)) {
                throw new RuntimeException($table . ' is missing required column ' . $column);
            }
        }
        foreach ($forbidden as $column) {
            if (in_array($column, $columns, true)) {
                throw new RuntimeException($table . ' retained forbidden legacy column ' . $column);
            }
        }
    };
    $assertColumns('im_user', ['password_hash'], ['password', 'signature']);
    $assertColumns('im_auth_session', ['web_access_jti']);
    $assertColumns('im_user_device', [
        'current_ip',
        'current_ip_geo',
        'last_login_ip',
        'last_login_ip_geo',
        'current_online_state',
    ], ['platform']);
    $assertColumns('im_conversation_member', [
        'member_role',
        'inviter_user_id',
        'mute_status',
        'conversation_remark',
        'message_group_id',
        'access_version',
        'join_at',
    ], ['role', 'remark', 'join_time']);
    $assertColumns('im_message_outbox', [
        'payload_json',
        'next_retry_at',
        'locked_until',
        'worker_id',
        'claim_token',
        'published_at',
        'traceparent',
        'tracestate',
    ], ['payload', 'next_retry_time', 'locked_time', 'published_time']);
    $assertColumns('im_realtime_control_outbox', [
        'event_id',
        'aggregate_type',
        'aggregate_id',
        'event_type',
        'organization',
        'target_user_id',
        'payload_json',
        'traceparent',
        'tracestate',
        'status',
        'retry_count',
        'next_retry_at',
        'locked_until',
        'worker_id',
        'claim_token',
        'published_at',
        'last_error',
    ]);

    foreach ([$senderId, $recipientId, $otherId] as $userId) {
        $repository->execute(
            'INSERT INTO im_user
                (organization, user_id, account, password_hash, nickname, avatar, status, create_time, update_time)
             VALUES (1, ?, ?, ?, ?, ?, 1, ?, ?)',
            [
                $userId,
                $userId,
                password_hash('integration-only', PASSWORD_DEFAULT),
                $userId,
                $userId === $senderId ? $senderAvatarFileId : null,
                $now,
                $now,
            ],
        );
    }
    $storedUser = $repository->fetchOne(
        'SELECT password_hash FROM im_user WHERE organization = 1 AND user_id = ? LIMIT 1',
        [$senderId],
    );
    if (!password_verify('integration-only', (string) ($storedUser['password_hash'] ?? ''))) {
        throw new RuntimeException('password_hash field does not contain a verifiable password hash');
    }
    $repository->execute(
        'INSERT INTO im_user_profile
            (organization, user_id, signature, moments_cover_url, status, create_time, update_time)
         VALUES (1, ?, ?, ?, 1, ?, ?)',
        [$senderId, 'integration signature', 'https://example.invalid/cover.png', $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_user_privacy_setting
            (organization, user_id, allow_add_by_mobile, allow_add_by_short_no, allow_add_by_username, create_time, update_time)
         VALUES (1, ?, 1, 2, 1, ?, ?)',
        [$recipientId, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_user_security_policy
            (organization, user_id, login_ip_policy, login_ip_whitelist_json, status, create_time, update_time)
         VALUES (1, ?, "whitelist_only", ?, 1, ?, ?)',
        [$senderId, json_encode(['203.0.113.10/32'], JSON_THROW_ON_ERROR), $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_friend_request
            (organization, from_organization, to_organization, from_user_id, to_user_id,
             add_method, message, status, create_time, update_time)
         VALUES (1, 1, 1, ?, ?, "username", "integration request", 1, ?, ?)',
        [$senderId, $recipientId, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_friend_relation
            (organization, user_id, friend_user_id, friend_organization, add_method,
             added_at, remark_name, card_remark, status, create_time, update_time)
         VALUES (1, ?, ?, 1, "username", ?, "integration friend", "integration card", 1, ?, ?)',
        [$senderId, $recipientId, $now, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_friend_relation
            (organization, user_id, friend_user_id, friend_organization, add_method,
             added_at, remark_name, card_remark, status, create_time, update_time)
         VALUES (1, ?, ?, 1, "username", ?, "integration reciprocal", "integration card", 1, ?, ?)',
        [$recipientId, $senderId, $now, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_message_group
            (organization, user_id, name, sort, status, create_time, update_time)
         VALUES (1, ?, "integration group", 10, 1, ?, ?)',
        [$senderId, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_upload_asset
            (organization, file_id, user_id, kind, name, url, storage_path, size_byte,
             mime_type, extension, status, create_time, update_time)
         VALUES (1, ?, ?, "image", "integration.png", "https://assets.example.invalid/canonical.png",
                 "organizations/1/im/integration.png", 128, "image/png", "png", 1, ?, ?)',
        [$assetFileId, $senderId, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_user_device
            (organization, user_id, device_id, client_family, os, status, create_time, update_time)
         VALUES (1, ?, ?, "web", "browser", 1, ?, ?)',
        [$senderId, $deviceId, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_web_access_session
            (organization, jti, im_user_id, user_id, device_id, status, expire_at, create_time, update_time)
         SELECT 1, ?, id, user_id, ?, 1, ?, ?, ? FROM im_user
          WHERE organization = 1 AND user_id = ? LIMIT 1',
        [$webAccessJti, $deviceId, $sessionExpireAt, $now, $now, $senderId],
    );
    $repository->execute(
        'INSERT INTO im_auth_session
            (organization, user_id, device_id, client_id, session_id, web_access_jti, status, expire_at, create_time, update_time)
         VALUES (1, ?, ?, ?, ?, ?, 1, ?, ?, ?)',
        [$senderId, $deviceId, $clientId, $credentialSessionId, $webAccessJti, $sessionExpireAt, $now, $now],
    );

    $devices = new DeviceService($repository);
    $devices->online($context($senderId), '203.0.113.10');
    $deviceSnapshot = $repository->fetchOne(
        'SELECT current_ip, last_login_ip, current_online_state
           FROM im_user_device
          WHERE organization = 1 AND user_id = ? AND device_id = ? LIMIT 1',
        [$senderId, $deviceId],
    );
    if (
        ($deviceSnapshot['current_ip'] ?? '') !== '203.0.113.10'
        || ($deviceSnapshot['last_login_ip'] ?? '') !== '203.0.113.10'
        || (int) ($deviceSnapshot['current_online_state'] ?? 0) !== 1
    ) {
        throw new RuntimeException('device/IP online snapshot was not persisted');
    }
    $devices->offline($context($senderId)->toArray());
    $deviceSnapshot = $repository->fetchOne(
        'SELECT current_ip, last_login_ip, current_online_state
           FROM im_user_device
          WHERE organization = 1 AND user_id = ? AND device_id = ? LIMIT 1',
        [$senderId, $deviceId],
    );
    $audit = $repository->fetchOne(
        'SELECT login_ip, login_result, logout_at, current_online_state
           FROM im_user_login_audit
          WHERE organization = 1 AND user_id = ? AND client_id = ?
          ORDER BY id DESC LIMIT 1',
        [$senderId, $clientId],
    );
    if (
        ($deviceSnapshot['current_ip'] ?? null) !== null
        || ($deviceSnapshot['last_login_ip'] ?? '') !== '203.0.113.10'
        || (int) ($deviceSnapshot['current_online_state'] ?? 0) !== 2
        || ($audit['login_ip'] ?? '') !== '203.0.113.10'
        || ($audit['login_result'] ?? '') !== 'success'
        || empty($audit['logout_at'])
        || (int) ($audit['current_online_state'] ?? 0) !== 2
    ) {
        throw new RuntimeException('device/IP logout audit was not persisted');
    }

    $identityValidator->assertActive($context($senderId));
    $authRedis = new Redis();
    $authRedis->connect($config->redisHost, $config->redisPort, 2.0);
    if ($config->redisPassword !== '') {
        $authRedis->auth($config->redisPassword);
    }
    if ($config->redisDb > 0) {
        $authRedis->select($config->redisDb);
    }
    $activeSessions = new ActiveSessionGuard($identityValidator, $authRedis, 1);
    $activeSessions->assertActive($context($senderId));
    $organizationInactiveKey = sprintf(Constants::REDIS_AUTH_ORGANIZATION_INACTIVE, 1);
    $authRedis->set($organizationInactiveKey, '1');
    try {
        $activeSessions->assertActive($context($senderId));
        throw new RuntimeException('organization inactive marker did not override the positive AUTH cache');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'AUTH_ORGANIZATION_INACTIVE') {
            throw $exception;
        }
    }
    $authRedis->del($organizationInactiveKey);
    $activeSessions->assertActive($context($senderId));
    $repository->execute('UPDATE im_user SET status = 2 WHERE organization = 1 AND user_id = ?', [$senderId]);
    $activeSessions->assertActive($context($senderId));
    usleep(1_100_000);
    try {
        $activeSessions->assertActive($context($senderId));
        throw new RuntimeException('disabled user remained active after bounded revalidation TTL');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'AUTH_USER_INACTIVE') {
            throw $exception;
        }
    }
    $repository->execute('UPDATE im_user SET status = 1 WHERE organization = 1 AND user_id = ?', [$senderId]);
    $activeSessions->invalidate(1, $credentialSessionId);
    $activeSessions->assertActive($context($senderId));

    $repository->execute(
        'UPDATE im_web_access_session SET status = 2, revoked_at = ?, update_time = ?
          WHERE organization = 1 AND jti = ?',
        [$now, $now, $webAccessJti],
    );
    $activeSessions->invalidate(1, $credentialSessionId);
    try {
        $activeSessions->assertActive($context($senderId));
        throw new RuntimeException('revoked Web access session remained active in IM');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'AUTH_SESSION_INACTIVE') {
            throw $exception;
        }
    }
    $repository->execute(
        'UPDATE im_web_access_session SET status = 1, revoked_at = NULL, update_time = ?
          WHERE organization = 1 AND jti = ?',
        [$now, $webAccessJti],
    );
    $activeSessions->invalidate(1, $credentialSessionId);
    $activeSessions->assertActive($context($senderId));
    $repository->execute(
        'UPDATE im_auth_session SET status = 2, revoked_at = ? WHERE organization = 1 AND session_id = ?',
        [$now, $credentialSessionId],
    );
    $activeSessions->invalidate(1, $credentialSessionId);
    try {
        $activeSessions->assertActive($context($senderId));
        throw new RuntimeException('revoked credential session remained active after cache invalidation');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'AUTH_SESSION_INACTIVE') {
            throw $exception;
        }
    }
    $repository->execute(
        'UPDATE im_auth_session SET status = 1, revoked_at = NULL WHERE organization = 1 AND session_id = ?',
        [$credentialSessionId],
    );
    $activeSessions->invalidate(1, $credentialSessionId);
    $activeSessions->assertActive($context($senderId));

    $repository->execute(
        'INSERT INTO sm_module
            (module_key, name, category, module_type, version, available_version,
             min_system_version, platforms_json, depends_on_json, conflicts_with_json,
             capabilities_json, manifest_json, manifest_path, status, lock_version,
             create_time, update_time)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            $moduleKey,
            'IM integration module',
            'integration',
            'business',
            '1.2.3',
            '1.2.3',
            '1.0.0',
            json_encode(['im', 'web'], JSON_THROW_ON_ERROR),
            '[]',
            '[]',
            json_encode(['im' => ['message.send']], JSON_THROW_ON_ERROR),
            '{}',
            '/tmp/' . $moduleKey . '/module.json',
            'ENABLED',
            7,
            $now,
            $now,
        ],
    );
    $moduleExpireAt = date('Y-m-d H:i:s', time() + 30);
    $repository->execute(
        'INSERT INTO sm_tenant_module_license
            (organization, module_key, status, expire_at, version, authorized_at,
             enabled_at, create_time, update_time)
         VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)',
        [$moduleKey, 'ENABLED', $moduleExpireAt, 11, $now, $now, $now, $now],
    );

    $moduleLicenses = new ModuleLicenseChecker($authRedis, $repository, 60);
    $moduleLicenses->invalidate(1, $moduleKey);
    if (!$moduleLicenses->isLicensed(1, $moduleKey)) {
        throw new RuntimeException('valid string-status IM module license was rejected');
    }
    $moduleCacheJson = $authRedis->get($moduleCacheKey);
    $moduleCache = is_string($moduleCacheJson) ? json_decode($moduleCacheJson, true) : null;
    $moduleCacheTtl = $authRedis->ttl($moduleCacheKey);
    if (
        !is_array($moduleCache)
        || $moduleCache !== [
            'enabled' => true,
            'effective_until' => strtotime($moduleExpireAt),
            'version' => 11,
            'module_version' => '1.2.3',
            'module_lock_version' => 7,
            'platforms' => ['im', 'web'],
            'capabilities' => ['im' => ['message.send']],
        ]
        || $moduleCacheTtl < 1
        || $moduleCacheTtl > 30
    ) {
        throw new RuntimeException('shared module license cache key, shape or bounded TTL is invalid');
    }

    $repository->execute(
        'UPDATE sm_tenant_module_license SET status = ? WHERE organization = 1 AND module_key = ?',
        ['DISABLED', $moduleKey],
    );
    if ($moduleLicenses->isLicensed(1, $moduleKey)) {
        throw new RuntimeException('stale positive module cache extended a revoked MySQL license');
    }

    $repository->execute(
        'UPDATE sm_tenant_module_license SET status = ?, expire_at = ? WHERE organization = 1 AND module_key = ?',
        ['1', null, $moduleKey],
    );
    $moduleLicenses->invalidate(1, $moduleKey);
    if ($moduleLicenses->isLicensed(1, $moduleKey)) {
        throw new RuntimeException('legacy numeric module license status was accepted');
    }

    $repository->execute(
        'UPDATE sm_tenant_module_license SET status = ?, expire_at = ? WHERE organization = 1 AND module_key = ?',
        ['ENABLED', date('Y-m-d H:i:s', time() - 1), $moduleKey],
    );
    $moduleLicenses->invalidate(1, $moduleKey);
    if ($moduleLicenses->isLicensed(1, $moduleKey)) {
        throw new RuntimeException('expired module license was accepted');
    }

    $repository->execute(
        'UPDATE sm_tenant_module_license SET expire_at = NULL WHERE organization = 1 AND module_key = ?',
        [$moduleKey],
    );
    $repository->execute(
        'UPDATE sm_module SET platforms_json = ? WHERE module_key = ?',
        [json_encode(['web'], JSON_THROW_ON_ERROR), $moduleKey],
    );
    $moduleLicenses->invalidate(1, $moduleKey);
    if ($moduleLicenses->isLicensed(1, $moduleKey)) {
        throw new RuntimeException('module without IM platform support was accepted');
    }

    $repository->execute(
        'UPDATE sm_module SET status = ? WHERE module_key = ?',
        ['DISABLED', $moduleKey],
    );
    $authRedis->setex($moduleCacheKey, 30, json_encode([
        'enabled' => true,
        'effective_until' => null,
        'version' => 12,
        'module_version' => '1.2.4',
        'module_lock_version' => 8,
        'platforms' => ['im'],
        'capabilities' => ['im' => ['message.send']],
    ], JSON_THROW_ON_ERROR));
    if ($moduleLicenses->isLicensed(1, $moduleKey)) {
        throw new RuntimeException('stale Server-compatible positive cache bypassed disabled MySQL module');
    }
    $moduleLicenses->invalidate(1, $moduleKey);

    $repository->execute(
        'UPDATE im_user_device SET status = 2 WHERE organization = 1 AND user_id = ? AND device_id = ?',
        [$senderId, $deviceId],
    );
    try {
        $identityValidator->assertActive($context($senderId));
        throw new RuntimeException('inactive device was accepted');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'AUTH_DEVICE_INACTIVE') {
            throw $exception;
        }
    }
    $repository->execute(
        'UPDATE im_user_device SET status = 1 WHERE organization = 1 AND user_id = ? AND device_id = ?',
        [$senderId, $deviceId],
    );

    $sequence = $repository->fetchOne(
        'SELECT next_global_seq FROM im_organization_message_sequence WHERE organization = 1',
    );
    $initialGlobalSeq = (int) ($sequence['next_global_seq'] ?? 0);
    if ($initialGlobalSeq <= 0) {
        throw new RuntimeException('organization global sequence is not initialized');
    }

    $send = static fn (string $clientMsgId, string $text): array => $messages->send(
        $context($senderId),
        new Packet(
            cmd: 'send',
            data: [
                'to_user_id' => $recipientId,
                'to_organization' => 1,
                'conversation_type' => 1,
                'message_type' => MessageType::TEXT,
                'content' => ['text' => $text],
            ],
            organization: 999999,
            clientMsgId: $clientMsgId,
        ),
    );

    try {
        $messages->send(
            $context($senderId),
            new Packet('send', [
                'to_user_id' => $recipientId,
                'to_organization' => 1,
                'conversation_type' => 1,
                'message_type' => MessageType::SYSTEM,
                'content' => ['text' => 'forged system notice'],
            ], 1, 'it-system-forgery-' . $suffix),
        );
        throw new RuntimeException('ordinary client forged a system message');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'SEND_MESSAGE_TYPE_INVALID') {
            throw $exception;
        }
    }

    try {
        $messages->send(
            $context($senderId),
            new Packet('send', [
                'to_user_id' => $otherId,
                'to_organization' => 1,
                'conversation_type' => 1,
                'message_type' => MessageType::TEXT,
                'content' => ['text' => 'friend boundary probe'],
            ], 1, 'it-unfriended-' . $suffix),
        );
        throw new RuntimeException('single message to a non-friend was accepted');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'SEND_SINGLE_FRIEND_REQUIRED') {
            throw $exception;
        }
    }
    try {
        $messages->send(
            $context($senderId),
            new Packet('send', [
                'conversation_type' => 2,
                'member_ids' => [$senderId, 'missing-' . $suffix],
                'message_type' => MessageType::TEXT,
                'content' => ['text' => 'missing group member probe'],
            ], 1, 'it-missing-member-' . $suffix),
        );
        throw new RuntimeException('SEND created a group conversation implicitly');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'SEND_GROUP_CONVERSATION_REQUIRED') {
            throw $exception;
        }
    }
    try {
        $messages->send(
            $context($senderId),
            new Packet('send', [
                'conversation_type' => 2,
                'conversation_id' => 'it-forbidden-group-' . $suffix,
                'member_ids' => [$senderId, $recipientId],
                'message_type' => MessageType::TEXT,
                'content' => ['text' => 'implicit group create probe'],
            ], 1, 'it-forbidden-group-create-' . $suffix),
        );
        throw new RuntimeException('SEND accepted group creation fields');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'SEND_GROUP_CREATE_FORBIDDEN') {
            throw $exception;
        }
    }

    $first = $send($clientMsgId1, 'integration one');
    $createdMessageIds[] = $first['message']['message_id'];
    $conversationId = $first['message']['conversation_id'];
    if (
        $first['message']['organization'] !== 1
        || $first['message']['message_seq'] <= 0
        || !ctype_digit($first['message']['global_seq'])
        || ($first['message']['sender_user']['signature'] ?? '') !== 'integration signature'
        || ($first['message']['sender_user']['avatar_file_id'] ?? '') !== $senderAvatarFileId
        || ($first['message']['sender_user']['avatar_url'] ?? null) !== ''
        || array_key_exists('avatar', $first['message']['sender_user'])
    ) {
        throw new RuntimeException('SEND did not use authenticated organization or sequence fields');
    }

    $periodCount = $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate
           FROM im_conversation_membership_period
          WHERE organization = 1 AND conversation_id = ? AND user_id IN (?, ?)
            AND status = 1 AND visible_from_message_seq = 1 AND visible_until_message_seq IS NULL',
        [$conversationId, $senderId, $recipientId],
    );
    if ((int) ($periodCount['aggregate'] ?? 0) !== 2) {
        throw new RuntimeException('single conversation membership periods were not created');
    }
    $messageGroup = $repository->fetchOne(
        'SELECT id FROM im_message_group WHERE organization = 1 AND user_id = ? AND name = "integration group" LIMIT 1',
        [$senderId],
    );
    $repository->execute(
        'UPDATE im_conversation_member SET message_group_id = ?, conversation_remark = "integration remark"
          WHERE organization = 1 AND conversation_id = ? AND user_id = ?',
        [(int) ($messageGroup['id'] ?? 0), $conversationId, $senderId],
    );
    $groupedConversation = $repository->fetchOne(
        'SELECT cm.message_group_id, cm.conversation_remark, mg.name AS message_group_name
           FROM im_conversation_member cm
           INNER JOIN im_message_group mg
             ON mg.organization = cm.organization AND mg.user_id = cm.user_id AND mg.id = cm.message_group_id
          WHERE cm.organization = 1 AND cm.conversation_id = ? AND cm.user_id = ? LIMIT 1',
        [$conversationId, $senderId],
    );
    if (
        (int) ($groupedConversation['message_group_id'] ?? 0) <= 0
        || ($groupedConversation['conversation_remark'] ?? '') !== 'integration remark'
        || ($groupedConversation['message_group_name'] ?? '') !== 'integration group'
    ) {
        throw new RuntimeException('message group or conversation_remark persistence failed');
    }

    $duplicate = $send($clientMsgId1, 'integration duplicate');
    if (!$duplicate['duplicated'] || $duplicate['message']['message_id'] !== $first['message']['message_id']) {
        throw new RuntimeException('client_msg_id idempotency failed');
    }

    $second = $send($clientMsgId2, 'integration two');
    $createdMessageIds[] = $second['message']['message_id'];
    if (
        $second['message']['message_seq'] !== $first['message']['message_seq'] + 1
        || (int) $second['message']['global_seq'] !== (int) $first['message']['global_seq'] + 1
    ) {
        throw new RuntimeException('message/global sequence is not monotonic');
    }

    $repository->execute(
        'UPDATE im_conversation SET status = 2 WHERE organization = 1 AND conversation_id = ?',
        [$conversationId],
    );
    try {
        $send('it-disabled-conversation-' . $suffix, 'must not persist');
        throw new RuntimeException('disabled conversation accepted SEND');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'SEND_CONVERSATION_INACTIVE') {
            throw $exception;
        }
    }
    $repository->execute(
        'UPDATE im_conversation SET status = 1 WHERE organization = 1 AND conversation_id = ?',
        [$conversationId],
    );

    try {
        $messages->ack($context($recipientId), [
            'message_id' => $first['message']['message_id'],
            'status' => 'read',
        ]);
        throw new RuntimeException('ACK without request client_msg_id was accepted');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'ACK_CLIENT_MSG_ID_INVALID') {
            throw $exception;
        }
    }
    $ack = $messages->ack($context($recipientId), [
        'message_id' => $first['message']['message_id'],
        'status' => 'read',
        'client_msg_id' => 'it-ack-first-' . $suffix,
    ]);
    if (
        $ack['organization'] !== 1
        || $ack['conversation_id'] !== $conversationId
        || $ack['message_seq'] !== $first['message']['message_seq']
        || $ack['global_seq'] !== $first['message']['global_seq']
    ) {
        throw new RuntimeException('ACK sequence response is incomplete');
    }
    $partialReadState = $repository->fetchOne(
        'SELECT unread_count, last_read_message_id, last_read_seq
           FROM im_conversation_member
          WHERE organization = 1 AND conversation_id = ? AND user_id = ? LIMIT 1',
        [$conversationId, $recipientId],
    );
    if ((int) ($partialReadState['unread_count'] ?? -1) !== 1
        || ($partialReadState['last_read_message_id'] ?? '') !== $first['message']['message_id']
        || (int) ($partialReadState['last_read_seq'] ?? 0) !== $first['message']['message_seq']) {
        throw new RuntimeException('ACK of an older message miscounted unread state or regressed the read cursor');
    }
    $messages->ack($context($recipientId), [
        'message_id' => $second['message']['message_id'],
        'status' => 'read',
        'client_msg_id' => 'it-ack-second-' . $suffix,
    ]);
    $fullyReadState = $repository->fetchOne(
        'SELECT unread_count, last_read_message_id, last_read_seq
           FROM im_conversation_member
          WHERE organization = 1 AND conversation_id = ? AND user_id = ? LIMIT 1',
        [$conversationId, $recipientId],
    );
    if ((int) ($fullyReadState['unread_count'] ?? -1) !== 0
        || ($fullyReadState['last_read_message_id'] ?? '') !== $second['message']['message_id']
        || (int) ($fullyReadState['last_read_seq'] ?? 0) !== $second['message']['message_seq']) {
        throw new RuntimeException('ACK of the latest message did not clear unread state monotonically');
    }
    $lateDelivered = $messages->ack($context($recipientId), [
        'message_id' => $first['message']['message_id'],
        'status' => 'delivered',
        'client_msg_id' => 'it-ack-late-' . $suffix,
    ]);
    $nonRegressedState = $repository->fetchOne(
        'SELECT unread_count, last_read_message_id, last_read_seq
           FROM im_conversation_member
          WHERE organization = 1 AND conversation_id = ? AND user_id = ? LIMIT 1',
        [$conversationId, $recipientId],
    );
    if ($nonRegressedState !== $fullyReadState || ($lateDelivered['status'] ?? '') !== 'read') {
        throw new RuntimeException('old/lower ACK regressed the read cursor or receipt status');
    }
    try {
        $messages->ack($context($recipientId), [
            'message_id' => $first['message']['message_id'],
            'status' => 2,
            'client_msg_id' => 'it-ack-invalid-status-' . $suffix,
        ]);
        throw new RuntimeException('numeric ACK status was accepted');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'ACK_STATUS_INVALID') {
            throw $exception;
        }
    }

    $globalSync = $messages->sync($context($recipientId), ['after_global_seq' => '0', 'limit' => 20]);
    if (
        $globalSync['organization'] !== 1
        || $globalSync['scope'] !== 'global'
        || count($globalSync['messages']) !== 2
        || $globalSync['next_after_global_seq'] !== $second['message']['global_seq']
    ) {
        throw new RuntimeException('global SYNC cursor or organization is invalid');
    }
    try {
        $messages->sync($context($recipientId), ['after_global_seq' => 0, 'limit' => 20]);
        throw new RuntimeException('numeric after_global_seq was accepted');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'SYNC_GLOBAL_SEQ_INVALID') {
            throw $exception;
        }
    }

    $conversationSync = $messages->sync($context($recipientId), [
        'conversation_id' => $conversationId,
        'after_seq' => 0,
        'limit' => 20,
    ]);
    if (
        $conversationSync['scope'] !== 'conversation'
        || $conversationSync['conversation_id'] !== $conversationId
        || $conversationSync['next_after_seq'] !== $second['message']['message_seq']
        || $conversationSync['next_after_change_seq'] !== 0
        || $conversationSync['changes'] !== []
        || $conversationSync['messages_has_more']
        || $conversationSync['changes_has_more']
    ) {
        throw new RuntimeException('conversation SYNC cursor is invalid');
    }

    $edit = $messages->edit($context($senderId), [
        'message_id' => $first['message']['message_id'],
        'content' => ['text' => 'integration edited'],
        'client_msg_id' => 'it-edit-first-' . $suffix,
    ]);
    $recall = $messages->recall($context($senderId), [
        'message_id' => $second['message']['message_id'],
        'client_msg_id' => 'it-recall-second-' . $suffix,
    ]);
    if (is_array($recall['notice_message'] ?? null)) {
        $createdMessageIds[] = (string) $recall['notice_message']['message_id'];
    }
    $deleteSelf = $messages->delete($context($recipientId), [
        'message_id' => $first['message']['message_id'],
        'scope' => 'self',
        'client_msg_id' => 'it-delete-self-first-' . $suffix,
    ]);
    if ($edit['change_seq'] !== 1 || $recall['change_seq'] !== 2 || $deleteSelf['change_seq'] !== 3) {
        throw new RuntimeException('message change_seq allocation is not monotonic');
    }

    $senderChanges = $messages->sync($context($senderId), [
        'conversation_id' => $conversationId,
        'after_seq' => (int) ($recall['notice_message']['message_seq'] ?? $second['message']['message_seq']),
        'after_change_seq' => 0,
        'limit' => 20,
    ]);
    $senderChangeTypes = array_map(
        static fn (array $change): string => (string) $change['change_type'],
        $senderChanges['changes'],
    );
    if (
        $senderChangeTypes !== ['edit', 'recall']
        || $senderChanges['next_after_change_seq'] !== 3
        || $senderChanges['changes_has_more']
    ) {
        throw new RuntimeException('targeted delete_self filtering or change cursor advancement failed');
    }

    $emptyFilteredChangePage = $messages->sync($context($senderId), [
        'conversation_id' => $conversationId,
        'after_seq' => (int) ($recall['notice_message']['message_seq'] ?? $second['message']['message_seq']),
        'after_change_seq' => 2,
        'limit' => 1,
    ]);
    if (
        $emptyFilteredChangePage['changes'] !== []
        || $emptyFilteredChangePage['next_after_change_seq'] !== 3
        || $emptyFilteredChangePage['changes_has_more']
    ) {
        throw new RuntimeException('empty targeted change page did not advance scan cursor');
    }

    $recipientChanges = $messages->sync($context($recipientId), [
        'conversation_id' => $conversationId,
        'after_seq' => 0,
        'after_change_seq' => 0,
        'limit' => 20,
    ]);
    $recipientChangeTypes = array_map(
        static fn (array $change): string => (string) $change['change_type'],
        $recipientChanges['changes'],
    );
    $recalledMessages = array_values(array_filter(
        $recipientChanges['messages'],
        static fn (array $message): bool => (string) $message['message_id'] === (string) $second['message']['message_id'],
    ));
    if (
        $recipientChangeTypes !== ['edit', 'recall', 'delete_self']
        || count($recalledMessages) !== 1
        || $recalledMessages[0]['status'] !== 'recalled'
        || $recalledMessages[0]['content'] !== null
    ) {
        throw new RuntimeException('recipient change stream or recalled content redaction failed');
    }

    $changeState = $repository->fetchOne(
        'SELECT next_change_seq, last_change_seq FROM im_conversation
          WHERE organization = 1 AND conversation_id = ? LIMIT 1',
        [$conversationId],
    );
    $changeOutbox = $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate FROM im_message_outbox
          WHERE organization = 1 AND event_type IN ("message.edited", "message.recalled", "message.deleted_self")
            AND message_id IN (?, ?)',
        [$first['message']['message_id'], $second['message']['message_id']],
    );
    if (
        (int) ($changeState['next_change_seq'] ?? 0) !== 4
        || (int) ($changeState['last_change_seq'] ?? 0) !== 3
        || (int) ($changeOutbox['aggregate'] ?? 0) !== 3
    ) {
        throw new RuntimeException('message change state or reliable outbox events are incomplete');
    }

    $outboxControl = new OutboxService($repository, $config);
    $claimedOutbox = $outboxControl->claimPending(1, 'integration-worker-a');
    if (count($claimedOutbox) !== 1
        || ($claimedOutbox[0]['worker_id'] ?? '') !== 'integration-worker-a'
        || preg_match('/^[a-f0-9]{40}$/', (string) ($claimedOutbox[0]['claim_token'] ?? '')) !== 1
        || empty($claimedOutbox[0]['locked_until'])) {
        throw new RuntimeException('outbox claim did not bind a worker/token/lease');
    }
    $claimedId = (int) $claimedOutbox[0]['id'];
    $claimedToken = (string) $claimedOutbox[0]['claim_token'];
    try {
        $outboxControl->markPublished($claimedId, str_repeat('0', 40));
        throw new RuntimeException('stale outbox claim token was accepted');
    } catch (RuntimeException $exception) {
        if (!str_contains($exception->getMessage(), 'claim is no longer current')) {
            throw $exception;
        }
    }
    $outboxControl->markPublished($claimedId, $claimedToken);
    $publishedOutbox = $repository->fetchOne(
        'SELECT status, published_at, worker_id, claim_token, locked_until
           FROM im_message_outbox WHERE id = ? LIMIT 1',
        [$claimedId],
    );
    if ((int) ($publishedOutbox['status'] ?? 0) !== 3
        || empty($publishedOutbox['published_at'])
        || $publishedOutbox['worker_id'] !== null
        || $publishedOutbox['claim_token'] !== null
        || $publishedOutbox['locked_until'] !== null) {
        throw new RuntimeException('outbox publish result did not atomically release its claim');
    }

    $assetMessage = $messages->send(
        $context($senderId),
        new Packet('send', [
            'to_user_id' => $recipientId,
            'to_organization' => 1,
            'conversation_type' => 1,
            'message_type' => MessageType::IMAGE,
            'content' => [
                'file_id' => $assetFileId,
                'url' => 'https://attacker.invalid/forged.png',
            ],
        ], 1, 'it-asset-message-' . $suffix),
    );
    $createdMessageIds[] = $assetMessage['message']['message_id'];
    $assetContent = $assetMessage['message']['content'] ?? null;
    if (!is_array($assetContent)
        || array_key_exists('url', $assetContent)
        || ($assetContent['file_id'] ?? null) !== $assetFileId
        || ($assetContent['name'] ?? null) !== 'integration.png'
        || ($assetContent['size'] ?? null) !== 128
        || ($assetContent['mime_type'] ?? null) !== 'image/png'
        || ($assetContent['extension'] ?? null) !== 'png') {
        throw new RuntimeException('non-text message did not use URL-free canonical server asset metadata');
    }
    $assetOutbox = $repository->fetchOne(
        'SELECT payload_json, traceparent, tracestate FROM im_message_outbox
          WHERE organization = 1 AND event_type = ? AND message_id = ? AND change_seq = 0
          LIMIT 1',
        [Constants::MQ_ROUTING_MESSAGE_CREATED, (string) $assetMessage['message']['message_id']],
    );
    $assetOutboxPayload = json_decode((string) ($assetOutbox['payload_json'] ?? ''), true, flags: JSON_THROW_ON_ERROR);
    if ($testExporterFailure) {
        $storedTrace = TraceContext::fromCarrier(
            isset($assetOutbox['traceparent']) ? (string) $assetOutbox['traceparent'] : null,
            isset($assetOutbox['tracestate']) ? (string) $assetOutbox['tracestate'] : null,
        );
        if ($storedTrace === null) {
            throw new RuntimeException('failed OTLP exporter prevented durable outbox trace persistence');
        }
    }
    $assetOutboxContent = $assetOutboxPayload['message']['content'] ?? null;
    if (!is_array($assetOutboxContent)
        || array_key_exists('url', $assetOutboxContent)
        || ($assetOutboxContent['file_id'] ?? null) !== $assetFileId) {
        throw new RuntimeException('message outbox persisted a private asset URL');
    }
    try {
        $messages->send(
            $context($recipientId),
            new Packet('send', [
                'to_user_id' => $senderId,
                'to_organization' => 1,
                'conversation_type' => 1,
                'message_type' => MessageType::IMAGE,
                'content' => ['file_id' => $assetFileId],
            ], 1, 'it-cross-user-asset-' . $suffix),
        );
        throw new RuntimeException('another user reused an asset file_id');
    } catch (ImException $exception) {
        if ($exception->errorCode() !== 'SEND_ASSET_FORBIDDEN') {
            throw $exception;
        }
    }

    $groupConversationId = 'it-group-' . $suffix;
    $repository->execute(
        'INSERT INTO im_conversation
            (organization, conversation_id, conversation_type, title, owner_user_id,
             owner_organization, status, create_time, update_time)
         VALUES (1, ?, 2, ?, ?, 1, 1, ?, ?)',
        [$groupConversationId, 'integration group', $senderId, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_group_profile
            (organization, conversation_id, owner_user_id, group_kind, history_visibility,
             display_member_count, description, status, create_time, update_time)
         VALUES (1, ?, ?, "normal", "since_join", 0, ?, 1, ?, ?)',
        [$groupConversationId, $senderId, 'integration group profile', $now, $now],
    );
    foreach ([$senderId, $recipientId, $otherId] as $groupMemberId) {
        $isOwner = $groupMemberId === $senderId;
        $repository->execute(
            'INSERT INTO im_conversation_member
                (organization, conversation_id, user_id, member_organization, member_role,
                 inviter_user_id, inviter_organization, status, access_version,
                 join_at, create_time, update_time)
             VALUES (1, ?, ?, 1, ?, ?, ?, 1, 1, ?, ?, ?)',
            [
                $groupConversationId,
                $groupMemberId,
                $isOwner ? 'owner' : 'member',
                $isOwner ? null : $senderId,
                $isOwner ? 0 : 1,
                $now,
                $now,
                $now,
            ],
        );
        $repository->execute(
            'INSERT INTO im_conversation_membership_period
                (organization, conversation_id, user_id, member_organization, period_no,
                 visible_from_message_seq, visible_until_message_seq, join_at,
                 leave_at, status, create_time, update_time)
             VALUES (1, ?, ?, 1, 1, 1, NULL, ?, NULL, 1, ?, ?)',
            [$groupConversationId, $groupMemberId, $now, $now, $now],
        );
    }

    $sendGroup = static function (string $clientMsgId, string $text) use (
        $messages,
        $context,
        $senderId,
        $groupConversationId,
    ): array {
        $data = [
            'conversation_type' => 2,
            'conversation_id' => $groupConversationId,
            'message_type' => MessageType::TEXT,
            'content' => ['text' => $text],
        ];

        return $messages->send(
            $context($senderId),
            new Packet('send', $data, 999999, $clientMsgId),
        );
    };

    $groupFirst = $sendGroup($groupClientMsgIds[0], 'group visible one');
    $createdMessageIds[] = $groupFirst['message']['message_id'];
    if ($groupFirst['message']['conversation_id'] !== $groupConversationId) {
        throw new RuntimeException('group SEND did not preserve the control-plane conversation id');
    }
    $groupSchema = $repository->fetchOne(
        'SELECT gp.group_kind, gp.history_visibility, gp.description,
                owner.member_role AS owner_role,
                invited.member_role AS invited_role,
                invited.inviter_user_id
           FROM im_group_profile gp
           INNER JOIN im_conversation_member owner
             ON owner.organization = gp.organization
            AND owner.conversation_id = gp.conversation_id
            AND owner.user_id = ?
           INNER JOIN im_conversation_member invited
             ON invited.organization = gp.organization
            AND invited.conversation_id = gp.conversation_id
            AND invited.user_id = ?
          WHERE gp.organization = 1 AND gp.conversation_id = ? LIMIT 1',
        [$senderId, $recipientId, $groupConversationId],
    );
    if (
        ($groupSchema['group_kind'] ?? '') !== 'normal'
        || ($groupSchema['history_visibility'] ?? '') !== 'since_join'
        || ($groupSchema['description'] ?? '') !== 'integration group profile'
        || ($groupSchema['owner_role'] ?? '') !== 'owner'
        || ($groupSchema['invited_role'] ?? '') !== 'member'
        || ($groupSchema['inviter_user_id'] ?? '') !== $senderId
    ) {
        throw new RuntimeException('group profile or member_role/inviter semantics failed');
    }

    $repository->execute(
        'UPDATE im_conversation_membership_period
            SET visible_until_message_seq = ?, leave_at = ?, update_time = ?
          WHERE organization = 1 AND conversation_id = ? AND user_id = ?
            AND status = 1 AND visible_until_message_seq IS NULL',
        [$groupFirst['message']['message_seq'], $now, $now, $groupConversationId, $recipientId],
    );
    $repository->execute(
        'UPDATE im_conversation_member
            SET status = 2, access_version = access_version + 1, update_time = ?
          WHERE organization = 1 AND conversation_id = ? AND user_id = ?',
        [$now, $groupConversationId, $recipientId],
    );

    $groupSecond = $sendGroup($groupClientMsgIds[1], 'group hidden while absent');
    $createdMessageIds[] = $groupSecond['message']['message_id'];

    $repository->execute(
        'UPDATE im_conversation_member
            SET status = 1, access_version = access_version + 1, join_at = ?, update_time = ?
          WHERE organization = 1 AND conversation_id = ? AND user_id = ?',
        [$now, $now, $groupConversationId, $recipientId],
    );
    $repository->execute(
        'INSERT INTO im_conversation_membership_period
            (organization, conversation_id, user_id, member_organization, period_no,
             visible_from_message_seq, visible_until_message_seq, join_at,
             leave_at, status, create_time, update_time)
         VALUES (1, ?, ?, 1, 2, ?, NULL, ?, NULL, 1, ?, ?)',
        [$groupConversationId, $recipientId, $groupSecond['message']['message_seq'] + 1, $now, $now, $now],
    );

    $mutationRecipientIdentities = (new DatabaseRealtimeRecipientProvider($repository))->activeIdentities(
        1,
        $groupConversationId,
        (int) $groupSecond['message']['message_seq'],
    );
    $mutationRecipients = array_column($mutationRecipientIdentities, 'user_id');
    if (in_array($recipientId, $mutationRecipients, true)) {
        throw new RuntimeException('late/rejoined since_join member could receive a mutation for a hidden message');
    }
    if (!in_array($senderId, $mutationRecipients, true) || !in_array($otherId, $mutationRecipients, true)) {
        throw new RuntimeException('visible active members were removed from mutation realtime recipients');
    }

    $groupThird = $sendGroup($groupClientMsgIds[2], 'group visible after rejoin');
    $createdMessageIds[] = $groupThird['message']['message_id'];
    $groupSync = $messages->sync($context($recipientId), [
        'conversation_id' => $groupConversationId,
        'after_seq' => 0,
        'after_change_seq' => 0,
        'limit' => 20,
    ]);
    $groupVisibleMessageIds = array_map(
        static fn (array $message): string => (string) $message['message_id'],
        $groupSync['messages'],
    );
    if ($groupVisibleMessageIds !== [
        $groupFirst['message']['message_id'],
        $groupThird['message']['message_id'],
    ]) {
        throw new RuntimeException('membership period did not hide leave/rejoin gap messages');
    }

    $groupGlobalSync = $messages->sync($context($recipientId), ['after_global_seq' => '0', 'limit' => 50]);
    $groupGlobalIds = array_values(array_map(
        static fn (array $message): string => (string) $message['message_id'],
        array_filter(
            $groupGlobalSync['messages'],
            static fn (array $message): bool => (string) $message['conversation_id'] === $groupConversationId,
        ),
    ));
    if ($groupGlobalIds !== [
        $groupFirst['message']['message_id'],
        $groupThird['message']['message_id'],
    ]) {
        throw new RuntimeException('global sync did not enforce historical membership periods');
    }

    $crossTenantWrites = $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate FROM im_message_index
          WHERE organization = 999999 AND sender_id = ? AND client_msg_id IN (?, ?, ?, ?, ?)',
        [$senderId, $clientMsgId1, $clientMsgId2, ...$groupClientMsgIds],
    );
    if ((int) ($crossTenantWrites['aggregate'] ?? -1) !== 0) {
        throw new RuntimeException('client packet organization crossed tenant boundary');
    }

    fwrite(STDOUT, "[PASS] real MySQL schema, bounded AUTH revalidation, shared module-license cache, IP audit, tenant isolation, idempotency, membership periods, change stream, SEND/ACK/SYNC and monotonic sequences\n");
} finally {
    if ($activeSessions instanceof ActiveSessionGuard) {
        $activeSessions->invalidate(1, $credentialSessionId);
    }
    if ($authRedis instanceof Redis) {
        try {
            $authRedis->del($moduleCacheKey, sprintf(Constants::REDIS_AUTH_ORGANIZATION_INACTIVE, 1));
        } catch (Throwable) {
        }
    }
    $repository->execute('DELETE FROM sm_tenant_module_license WHERE organization = 1 AND module_key = ?', [$moduleKey]);
    $repository->execute('DELETE FROM sm_module WHERE module_key = ?', [$moduleKey]);
    if ($createdMessageIds !== []) {
        $placeholders = implode(',', array_fill(0, count($createdMessageIds), '?'));
        $indexes = $repository->fetchAll(
            'SELECT message_id, shard_table FROM im_message_index WHERE organization = 1 AND message_id IN (' . $placeholders . ')',
            $createdMessageIds,
        );
        foreach ($indexes as $index) {
            $table = (string) $index['shard_table'];
            if (preg_match('/^im_message_[0-9]{4}_[0-9]{6}$/', $table) === 1) {
                $repository->execute(
                    'DELETE FROM `' . $table . '` WHERE organization = 1 AND message_id = ?',
                    [(string) $index['message_id']],
                );
            }
        }
        $repository->execute('DELETE FROM im_message_change WHERE organization = 1 AND message_id IN (' . $placeholders . ')', $createdMessageIds);
        $repository->execute('DELETE FROM im_message_user_delete WHERE organization = 1 AND message_id IN (' . $placeholders . ')', $createdMessageIds);
        $repository->execute('DELETE FROM im_message_outbox WHERE organization = 1 AND message_id IN (' . $placeholders . ')', $createdMessageIds);
        $repository->execute('DELETE FROM im_message_receipt WHERE organization = 1 AND message_id IN (' . $placeholders . ')', $createdMessageIds);
        $repository->execute('DELETE FROM im_message_index WHERE organization = 1 AND message_id IN (' . $placeholders . ')', $createdMessageIds);
    }
    foreach (array_filter([$conversationId, $groupConversationId]) as $createdConversationId) {
        $repository->execute('DELETE FROM im_conversation_membership_period WHERE organization = 1 AND conversation_id = ?', [$createdConversationId]);
        $repository->execute('DELETE FROM im_conversation_member WHERE organization = 1 AND conversation_id = ?', [$createdConversationId]);
        $repository->execute('DELETE FROM im_group_profile WHERE organization = 1 AND conversation_id = ?', [$createdConversationId]);
        $repository->execute('DELETE FROM im_conversation WHERE organization = 1 AND conversation_id = ?', [$createdConversationId]);
    }
    if ($initialGlobalSeq !== null) {
        $repository->execute(
            'UPDATE im_organization_message_sequence SET next_global_seq = ?
              WHERE organization = 1 AND next_global_seq = ?',
            [$initialGlobalSeq, $initialGlobalSeq + count($createdMessageIds)],
        );
    }
    $repository->execute('DELETE FROM im_auth_session WHERE organization = 1 AND session_id = ?', [$credentialSessionId]);
    $repository->execute('DELETE FROM im_web_access_session WHERE organization = 1 AND jti = ?', [$webAccessJti]);
    $repository->execute('DELETE FROM im_upload_asset WHERE organization = 1 AND file_id = ?', [$assetFileId]);
    $repository->execute('DELETE FROM im_user_login_audit WHERE organization = 1 AND user_id IN (?, ?, ?)', [$senderId, $recipientId, $otherId]);
    $repository->execute('DELETE FROM im_user_device WHERE organization = 1 AND user_id = ? AND device_id = ?', [$senderId, $deviceId]);
    $repository->execute('DELETE FROM im_message_group WHERE organization = 1 AND user_id IN (?, ?, ?)', [$senderId, $recipientId, $otherId]);
    $repository->execute('DELETE FROM im_friend_request WHERE organization = 1 AND (from_user_id IN (?, ?, ?) OR to_user_id IN (?, ?, ?))', [$senderId, $recipientId, $otherId, $senderId, $recipientId, $otherId]);
    $repository->execute('DELETE FROM im_friend_relation WHERE organization = 1 AND (user_id IN (?, ?, ?) OR friend_user_id IN (?, ?, ?))', [$senderId, $recipientId, $otherId, $senderId, $recipientId, $otherId]);
    $repository->execute('DELETE FROM im_user_security_policy WHERE organization = 1 AND user_id IN (?, ?, ?)', [$senderId, $recipientId, $otherId]);
    $repository->execute('DELETE FROM im_user_privacy_setting WHERE organization = 1 AND user_id IN (?, ?, ?)', [$senderId, $recipientId, $otherId]);
    $repository->execute('DELETE FROM im_user_profile WHERE organization = 1 AND user_id IN (?, ?, ?)', [$senderId, $recipientId, $otherId]);
    $repository->execute('DELETE FROM im_user WHERE organization = 1 AND user_id IN (?, ?, ?)', [$senderId, $recipientId, $otherId]);
    if ($testExporterFailure) {
        Telemetry::flush();
        Telemetry::shutdown();
    }
}
