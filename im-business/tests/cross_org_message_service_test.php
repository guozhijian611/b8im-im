<?php

declare(strict_types=1);

/**
 * Drives real MessageService::send for cross-org single chat with platform switch on/off.
 *
 * Run:
 *   IM_EXPECT_DATABASE=nb8im php tests/cross_org_message_service_test.php
 */

use B8im\ImBusiness\Auth\AuthContext;
use B8im\ImBusiness\Config;
use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImBusiness\Realtime\DatabaseRealtimeRecipientProvider;
use B8im\ImBusiness\Realtime\RealtimeEventProjector;
use B8im\ImBusiness\Service\CrossOrganizationConversationAccess;
use B8im\ImBusiness\Service\CrossOrganizationSocialPolicy;
use B8im\ImBusiness\Service\ConversationSyncService;
use B8im\ImBusiness\Service\DatabaseFriendRequestRealtimeAuthorizer;
use B8im\ImBusiness\Service\MessageService;
use B8im\ImBusiness\Service\OutboxService;
use B8im\ImBusiness\Service\TenantImPolicyService;
use B8im\ImBusiness\Service\TypingService;
use B8im\ImShared\Protocol\Packet;
use B8im\ImShared\Support\Constants;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

$config = Config::fromEnv();
$expectedDatabase = trim((string) ($_ENV['IM_EXPECT_DATABASE'] ?? getenv('IM_EXPECT_DATABASE')));
if ($expectedDatabase === '') {
    fwrite(STDOUT, "SKIP cross-org MessageService integration: IM_EXPECT_DATABASE is required.\n");
    exit(0);
}
if (!str_starts_with($expectedDatabase, 'nb8im_cross_org_')) {
    throw new RuntimeException('cross-org integration requires an isolated nb8im_cross_org_* database');
}
$repository = ImRepository::connect($config);
$selectedDatabase = (string) ($repository->fetchOne('SELECT DATABASE() AS database_name')['database_name'] ?? '');
if ($config->dbName !== $expectedDatabase || $selectedDatabase !== $expectedDatabase) {
    throw new RuntimeException(sprintf(
        'database mismatch config=%s selected=%s expected=%s',
        $config->dbName,
        $selectedDatabase,
        $expectedDatabase,
    ));
}

$passed = 0;
$failed = 0;
$assert = static function (bool $ok, string $msg) use (&$passed, &$failed): void {
    if ($ok) {
        $passed++;
        echo "PASS {$msg}\n";
        return;
    }
    $failed++;
    echo "FAIL {$msg}\n";
};

$suffix = bin2hex(random_bytes(4));
$orgA = 2;
$orgB = 10;
$userA = 'xorg-same-' . $suffix;
$userB = $userA;
$sameOrgPeer = 'same-org-peer-' . $suffix;
$now = date('Y-m-d H:i:s');

$cleanup = static function () use (
    $repository,
    $orgA,
    $orgB,
    $userA,
    $userB,
    $sameOrgPeer,
): void {
    foreach ([$orgA, $orgB] as $org) {
        foreach ([
            'im_message_outbox',
            'im_message_change',
            'im_message_user_delete',
            'im_message_receipt',
            'im_message_index',
            'im_conversation_membership_period',
            'im_conversation_member',
            'im_conversation',
            'im_user_profile',
            'im_user_privacy_setting',
            'im_organization_message_sequence',
            'sm_tenant_im_policy',
            'sm_tenant_config',
        ] as $table) {
            try {
                $repository->execute("DELETE FROM {$table} WHERE organization = ?", [$org]);
            } catch (Throwable) {
            }
        }
    }
    $messageTables = $repository->fetchAll(
        "SELECT TABLE_NAME AS table_name FROM information_schema.TABLES
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME REGEXP '^im_message_[0-9]{4}_[0-9]{6}$'",
    );
    foreach ($messageTables as $messageTable) {
        $table = str_replace('`', '``', (string) $messageTable['table_name']);
        $repository->execute("DELETE FROM `{$table}` WHERE organization IN (?, ?)", [$orgA, $orgB]);
    }
    $repository->execute(
        'DELETE FROM im_cross_organization_conversation
          WHERE left_organization IN (?, ?) OR right_organization IN (?, ?)',
        [$orgA, $orgB, $orgA, $orgB],
    );
    try {
        $repository->execute(
            'DELETE FROM im_friend_request WHERE organization IN (?, ?) OR from_user_id IN (?, ?) OR to_user_id IN (?, ?)',
            [$orgA, $orgB, $userA, $userB, $userA, $userB],
        );
        $repository->execute(
            'DELETE FROM im_friend_relation WHERE organization IN (?, ?) OR user_id IN (?, ?) OR friend_user_id IN (?, ?)',
            [$orgA, $orgB, $userA, $userB, $userA, $userB],
        );
        $repository->execute(
            'DELETE FROM im_user WHERE organization IN (?, ?) AND user_id IN (?, ?, ?)',
            [$orgA, $orgB, $userA, $userB, $sameOrgPeer],
        );
        $repository->execute('DELETE FROM sm_system_organization WHERE id IN (?, ?)', [$orgA, $orgB]);
    } catch (Throwable $e) {
        echo "cleanup warn: {$e->getMessage()}\n";
    }
};

$cleanup();

// Seed orgs + users + switch config
$repository->execute(
    'INSERT INTO sm_system_organization (id, enterprise_code, deployment_id, config_version, title, organization_name, status, is_init, create_time, update_time, favicon, icp, public_security_record_no, public_security_record_url, copyright, android_download_url, ios_download_url, user_agreement_title, privacy_policy_title)
     VALUES
       (?, ?, "b8im-local-test", 1, "甲测试机构", "甲测试机构", 1, 2, ?, ?, "", "", "", "", "", "", "", "用户协议", "隐私政策"),
       (?, ?, "b8im-local-test", 1, "乙测试机构", "乙测试机构", 1, 2, ?, ?, "", "", "", "", "", "", "", "用户协议", "隐私政策")
     ON DUPLICATE KEY UPDATE status = 1, organization_name = VALUES(organization_name), title = VALUES(title), delete_time = NULL',
    [$orgA, 'xorg_a_' . $suffix, $now, $now, $orgB, 'xorg_b_' . $suffix, $now, $now],
);
$repository->execute(
    'INSERT INTO sm_tenant_im_policy
        (organization, allowed_client_families_json, allow_multi_device_online,
         max_online_devices, same_device_login_policy, cross_device_login_policy,
         max_message_concurrency, max_message_qps, default_group_display_member_count,
         message_recall_window_seconds, message_edit_window_seconds,
         recall_notice_enabled, group_recall_notice_enabled, status, version,
         create_time, update_time)
     VALUES
        (?, ?, 1, 5, "replace", "allow", 8, 20, 50, 120, 120, 1, 1, "ENABLED", 1, ?, ?),
        (?, ?, 1, 5, "replace", "allow", 8, 20, 50, 120, 120, 1, 1, "ENABLED", 1, ?, ?)',
    [
        $orgA, '["web","app","desktop"]', $now, $now,
        $orgB, '["web","app","desktop"]', $now, $now,
    ],
);
foreach ([
    [$orgA, $userA, 'alice'],
    [$orgB, $userB, 'bob'],
    [$orgA, $sameOrgPeer, 'charlie'],
] as [$org, $uid, $acc]) {
    $repository->execute(
        'INSERT INTO im_user (organization, user_id, account, password_hash, nickname, status, is_system, create_time, update_time)
         VALUES (?, ?, ?, "x", ?, 1, 2, ?, ?)',
        [$org, $uid, $acc . $suffix, $acc, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_user_privacy_setting (organization, user_id, allow_add_by_mobile, allow_add_by_short_no, allow_add_by_username, create_time, update_time)
         VALUES (?, ?, 1, 1, 1, ?, ?)',
        [$org, $uid, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_organization_message_sequence (organization, next_global_seq, create_time, update_time)
         VALUES (?, 1, ?, ?)
         ON DUPLICATE KEY UPDATE organization = VALUES(organization)',
        [$org, $now, $now],
    );
}

$sameOrganizationCanonicalRejected = false;
$sameOrganizationCanonicalId = 'single_same_org_constraint_' . $suffix;
try {
    $repository->execute(
        'INSERT INTO im_cross_organization_conversation
            (conversation_id, left_organization, left_user_id, right_organization, right_user_id,
             next_message_seq, status, create_time, update_time)
         VALUES (?, ?, ?, ?, ?, 1, 1, ?, ?)',
        [
            $sameOrganizationCanonicalId,
            $orgA,
            $userA,
            $orgA,
            'same-org-peer-' . $suffix,
            $now,
            $now,
        ],
    );
} catch (PDOException $exception) {
    $driverCode = (int) ($exception->errorInfo[1] ?? 0);
    if (
        !in_array($driverCode, [3819, 4025], true)
        && !str_contains($exception->getMessage(), 'chk_cross_org_distinct_organizations')
    ) {
        throw $exception;
    }
    $sameOrganizationCanonicalRejected = true;
}
$assert(
    $sameOrganizationCanonicalRejected,
    'canonical cross-organization table rejects a same-organization identity pair',
);
$sameOrganizationCanonical = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate
       FROM im_cross_organization_conversation
      WHERE conversation_id = ?',
    [$sameOrganizationCanonicalId],
);
$assert(
    (int) ($sameOrganizationCanonical['aggregate'] ?? 1) === 0,
    'rejected same-organization canonical row is not persisted',
);

$repository->execute(
    "INSERT INTO sm_system_config_group (name, code, type, remark, created_by, updated_by, create_time, update_time)
     SELECT '社交边界配置', 'social_config', 1, 'test', 1, 1, ?, ?
     WHERE NOT EXISTS (SELECT 1 FROM sm_system_config_group WHERE code = 'social_config' AND delete_time IS NULL)",
    [$now, $now],
);
$groupId = (int) ($repository->fetchOne(
    "SELECT id FROM sm_system_config_group WHERE code = 'social_config' AND delete_time IS NULL LIMIT 1",
)['id'] ?? 0);
$assert($groupId > 0, 'social_config group exists');
$exists = $repository->fetchOne(
    "SELECT id FROM sm_system_config WHERE group_id = ? AND `key` = 'cross_org_social_enabled' LIMIT 1",
    [$groupId],
);
if ($exists === null) {
    $repository->execute(
        "INSERT INTO sm_system_config (group_id, `key`, `value`, name, input_type, config_select_data, sort, remark, created_by, updated_by, create_time, update_time)
         VALUES (?, 'cross_org_social_enabled', '0', '允许跨租户好友与单聊', 'radio', ?, 100, 'test', 1, 1, ?, ?)",
        [$groupId, '[{"label":"关闭","value":"0"},{"label":"开启","value":"1"}]', $now, $now],
    );
}
$snapshotExists = $repository->fetchOne(
    "SELECT id FROM sm_system_config WHERE group_id = ? AND `key` = 'cross_org_access_snapshot_id' LIMIT 1",
    [$groupId],
);
if ($snapshotExists === null) {
    $repository->execute(
        "INSERT INTO sm_system_config (group_id, `key`, `value`, name, input_type, config_select_data, sort, remark, created_by, updated_by, create_time, update_time)
         VALUES (?, 'cross_org_access_snapshot_id', '1', '跨机构社交访问快照', 'input', '', 101, 'test', 1, 1, ?, ?)",
        [$groupId, $now, $now],
    );
}

$repository->execute(
    "INSERT INTO sm_system_config_group (name, code, type, remark, created_by, updated_by, create_time, update_time)
     SELECT '消息操作配置', 'message_config', 1, 'test', 1, 1, ?, ?
     WHERE NOT EXISTS (SELECT 1 FROM sm_system_config_group WHERE code = 'message_config' AND delete_time IS NULL)",
    [$now, $now],
);
$messageConfigGroupId = (int) ($repository->fetchOne(
    "SELECT id FROM sm_system_config_group WHERE code = 'message_config' AND delete_time IS NULL LIMIT 1",
)['id'] ?? 0);
$assert($messageConfigGroupId > 0, 'message_config group exists');
foreach ([
    [
        $orgA,
        [
            'message_screenshot_notice_single_enabled' => '2',
            'message_delete_both_enabled' => '1',
            'message_delete_single_enabled' => '1',
        ],
    ],
    [
        $orgB,
        [
            'message_screenshot_notice_single_enabled' => '1',
            'message_delete_both_enabled' => '2',
            'message_delete_single_enabled' => '2',
        ],
    ],
] as [$organization, $messageConfig]) {
    $repository->execute(
        'INSERT INTO sm_tenant_config
            (organization, group_id, `value`, create_time, update_time)
         VALUES (?, ?, ?, ?, ?)',
        [
            $organization,
            $messageConfigGroupId,
            json_encode($messageConfig, JSON_THROW_ON_ERROR),
            $now,
            $now,
        ],
    );
}

$setSwitch = static function (string $value, string $snapshotId) use ($repository, $groupId): void {
    $repository->execute(
        "UPDATE sm_system_config SET value = ? WHERE group_id = ? AND `key` = 'cross_org_social_enabled'",
        [$value, $groupId],
    );
    $repository->execute(
        "UPDATE sm_system_config SET value = ? WHERE group_id = ? AND `key` = 'cross_org_access_snapshot_id'",
        [$snapshotId, $groupId],
    );
};

$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);

$context = static function (int $org, string $userId) use ($suffix): AuthContext {
    return new AuthContext(
        organization: $org,
        userId: $userId,
        deviceId: 'dev-' . $suffix,
        clientId: 'client-' . $suffix . '-' . $userId,
        credentialSessionId: 'cred-' . $suffix,
        sessionId: md5('sess-' . $suffix . $userId),
        clientFamily: 'web',
        os: 'browser',
        issuer: 'test',
        audience: 'im',
        notBefore: time() - 10,
        expireAt: time() + 3600,
        username: $userId,
    );
};

$blockedUpdate = static function (string $sql, array $params) use ($config): bool {
    $child = <<<'PHP'
$pdo = new PDO($argv[1], $argv[2], $argv[3], [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => false,
]);
$pdo->exec('SET SESSION innodb_lock_wait_timeout = 1');
$sql = base64_decode($argv[4], true);
$params = json_decode(base64_decode($argv[5], true), true, flags: JSON_THROW_ON_ERROR);
try {
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    fwrite(STDOUT, 'UPDATED');
    exit(2);
} catch (PDOException $exception) {
    $driverCode = (int) ($exception->errorInfo[1] ?? 0);
    if ($driverCode === 1205) {
        fwrite(STDOUT, 'BLOCKED:' . $driverCode);
        exit(0);
    }
    fwrite(STDERR, $exception->getMessage());
    exit(3);
}
PHP;
    $dsn = sprintf(
        'mysql:host=%s;port=%d;dbname=%s;charset=%s',
        $config->dbHost,
        $config->dbPort,
        $config->dbName,
        $config->dbCharset,
    );
    $process = proc_open(
        [
            PHP_BINARY,
            '-r',
            $child,
            $dsn,
            $config->dbUser,
            $config->dbPassword,
            base64_encode($sql),
            base64_encode(json_encode($params, JSON_THROW_ON_ERROR)),
        ],
        [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ],
        $pipes,
    );
    if (!is_resource($process)) {
        throw new RuntimeException('unable to start lock contention probe');
    }
    fclose($pipes[0]);
    $stdout = stream_get_contents($pipes[1]);
    $stderr = stream_get_contents($pipes[2]);
    fclose($pipes[1]);
    fclose($pipes[2]);
    $exitCode = proc_close($process);
    if ($exitCode !== 0 && $stderr !== '') {
        throw new RuntimeException('lock contention probe failed: ' . $stderr);
    }

    return $exitCode === 0 && str_starts_with($stdout, 'BLOCKED:');
};

$policyLockFailureCode = static function (int $groupId) use ($config, $repository): int {
    $child = <<<'PHP'
$pdo = new PDO($argv[1], $argv[2], $argv[3], [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => false,
]);
$pdo->beginTransaction();
$statement = $pdo->prepare(
    'SELECT id FROM sm_system_config
      WHERE group_id = ? AND `key` = ?
      LIMIT 1 FOR UPDATE',
);
$statement->execute([(int) $argv[4], $argv[5]]);
fwrite(STDOUT, "LOCKED\n");
fflush(STDOUT);
fgets(STDIN);
$pdo->rollBack();
PHP;
    $dsn = sprintf(
        'mysql:host=%s;port=%d;dbname=%s;charset=%s',
        $config->dbHost,
        $config->dbPort,
        $config->dbName,
        $config->dbCharset,
    );
    $process = proc_open(
        [
            PHP_BINARY,
            '-r',
            $child,
            $dsn,
            $config->dbUser,
            $config->dbPassword,
            (string) $groupId,
            CrossOrganizationSocialPolicy::CONFIG_KEY,
        ],
        [
            0 => ['pipe', 'r'],
            1 => ['pipe', 'w'],
            2 => ['pipe', 'w'],
        ],
        $pipes,
    );
    if (!is_resource($process)) {
        throw new RuntimeException('unable to start policy lock failure probe');
    }
    $ready = fgets($pipes[1]);
    if ($ready !== "LOCKED\n") {
        fwrite($pipes[0], "RELEASE\n");
        fclose($pipes[0]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        proc_close($process);
        throw new RuntimeException('policy lock failure probe did not acquire its lock: ' . $stderr);
    }

    $driverCode = 0;
    try {
        $repository->execute('SET SESSION innodb_lock_wait_timeout = 1');
        (new CrossOrganizationSocialPolicy($repository))->lockStateForWrite();
    } catch (PDOException $exception) {
        $driverCode = (int) ($exception->errorInfo[1] ?? 0);
    } finally {
        $repository->execute('SET SESSION innodb_lock_wait_timeout = 50');
        fwrite($pipes[0], "RELEASE\n");
        fclose($pipes[0]);
        stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $exitCode = proc_close($process);
        if ($exitCode !== 0) {
            throw new RuntimeException('policy lock failure probe failed: ' . $stderr);
        }
    }

    return $driverCode;
};

$sendPacket = static function (int $toOrganization, string $toUserId, string $clientMsgId): Packet {
    return Packet::make('send', [
        'conversation_type' => 1,
        'to_user_id' => $toUserId,
        'to_organization' => $toOrganization,
        'message_type' => 1,
        'content' => ['text' => 'cross-org-service-test'],
        'client_msg_id' => $clientMsgId,
    ], 0, $clientMsgId);
};

// --- OFF: cross-org send rejected ---
$setSwitch('0', '1');
$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);
try {
    $messages->send($context($orgA, $userA), $sendPacket($orgB, $userB, 'off-' . $suffix));
    $assert(false, 'switch off must reject cross-org send');
} catch (ImException $e) {
    $assert($e->errorCode() === 'SEND_SINGLE_RECEIVER_INVALID' || $e->errorCode() === 'SEND_SINGLE_FRIEND_REQUIRED',
        'switch off reject code=' . $e->errorCode());
}

// Snapshot 0 is the fail-closed sentinel for missing/invalid companion state.
$setSwitch('1', '0');
$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);
try {
    $messages->send($context($orgA, $userA), $sendPacket($orgB, $userB, 'zero-snapshot-' . $suffix));
    $assert(false, 'snapshot zero must fail closed');
} catch (ImException $e) {
    $assert($e->errorCode() === 'SEND_SINGLE_RECEIVER_INVALID', 'snapshot zero fails closed');
}

// --- ON: without friendship still friend required ---
$setSwitch('1', '2');
$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);
try {
    $messages->send($context($orgA, $userA), $sendPacket($orgB, $userB, 'on-nofriend-' . $suffix));
    $assert(false, 'switch on without friendship must still reject');
} catch (ImException $e) {
    $assert(
        in_array($e->errorCode(), ['SEND_SINGLE_FRIEND_REQUIRED', 'SEND_SINGLE_RECEIVER_INVALID'], true),
        'no-friend reject code=' . $e->errorCode(),
    );
}

// Create bidirectional cross-org friendship (same shape as control plane)
$repository->execute(
    'INSERT INTO im_friend_relation
        (organization, user_id, friend_user_id, friend_organization, add_method, added_at, status, create_time, update_time)
     VALUES
        (?, ?, ?, ?, "username", ?, 1, ?, ?),
        (?, ?, ?, ?, "username", ?, 1, ?, ?)',
    [
        $orgA, $userA, $userB, $orgB, $now, $now, $now,
        $orgB, $userB, $userA, $orgA, $now, $now, $now,
    ],
);
$repository->execute(
    'INSERT INTO im_friend_relation
        (organization, user_id, friend_user_id, friend_organization, add_method, added_at, status, create_time, update_time)
     VALUES
        (?, ?, ?, ?, "username", ?, 1, ?, ?),
        (?, ?, ?, ?, "username", ?, 1, ?, ?)',
    [
        $orgA, $userA, $sameOrgPeer, $orgA, $now, $now, $now,
        $orgA, $sameOrgPeer, $userA, $orgA, $now, $now, $now,
    ],
);

$failedSendConversationId = CrossOrganizationSocialPolicy::singleConversationId(
    $orgA,
    $userA,
    $orgB,
    $userB,
);
$failedSendClientMsgId = 'peer-policy-disabled-send-' . $suffix;
$crossSendArtifacts = static function (
    string $conversationId,
    string $clientMsgId,
) use ($repository, $orgA, $orgB): array {
    $counts = [];
    foreach ([
        'canonical' => [
            'SELECT COUNT(*) AS aggregate
               FROM im_cross_organization_conversation
              WHERE conversation_id = ?',
            [$conversationId],
        ],
        'home_conversations' => [
            'SELECT COUNT(*) AS aggregate
               FROM im_conversation
              WHERE organization IN (?, ?) AND conversation_id = ?',
            [$orgA, $orgB, $conversationId],
        ],
        'home_members' => [
            'SELECT COUNT(*) AS aggregate
               FROM im_conversation_member
              WHERE organization IN (?, ?) AND conversation_id = ?',
            [$orgA, $orgB, $conversationId],
        ],
        'message_indexes' => [
            'SELECT COUNT(*) AS aggregate
               FROM im_message_index
              WHERE organization IN (?, ?)
                AND conversation_id = ?
                AND client_msg_id = ?',
            [$orgA, $orgB, $conversationId, $clientMsgId],
        ],
        'outbox' => [
            'SELECT COUNT(*) AS aggregate
               FROM im_message_outbox
              WHERE organization IN (?, ?) AND conversation_id = ?',
            [$orgA, $orgB, $conversationId],
        ],
    ] as $name => [$sql, $params]) {
        $counts[$name] = (int) ($repository->fetchOne($sql, $params)['aggregate'] ?? -1);
    }
    $counts['message_bodies'] = 0;
    $messageTables = $repository->fetchAll(
        "SELECT TABLE_NAME AS table_name
           FROM information_schema.TABLES
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME REGEXP '^im_message_[0-9]{4}_[0-9]{6}$'",
    );
    foreach ($messageTables as $messageTable) {
        $table = str_replace('`', '``', (string) $messageTable['table_name']);
        $counts['message_bodies'] += (int) ($repository->fetchOne(
            "SELECT COUNT(*) AS aggregate
               FROM `{$table}`
              WHERE organization IN (?, ?)
                AND conversation_id = ?
                AND client_msg_id = ?",
            [$orgA, $orgB, $conversationId, $clientMsgId],
        )['aggregate'] ?? 0);
    }

    return $counts;
};

// A disabled peer home is part of the SEND authorization boundary. It must
// reject before the first canonical/home/message/outbox write, while an
// unrelated same-organization SEND keeps its existing one-home semantics.
$repository->execute(
    'UPDATE sm_tenant_im_policy
        SET status = "DISABLED", version = version + 1
      WHERE organization = ?',
    [$orgB],
);
$sameOrgConversationId = '';
try {
    try {
        $messages->send(
            $context($orgA, $userA),
            $sendPacket($orgB, $userB, $failedSendClientMsgId),
        );
        $assert(false, 'peer-home disabled policy must reject cross-org SEND');
    } catch (ImException $exception) {
        $assert(
            $exception->errorCode() === 'TENANT_POLICY_FORBIDDEN',
            'cross-org SEND fails closed on either home policy',
        );
    }
    try {
        $sameOrgSend = $messages->send(
            $context($orgA, $userA),
            $sendPacket($orgA, $sameOrgPeer, 'same-org-peer-policy-' . $suffix),
        );
        $assert(
            ($sameOrgSend['duplicated'] ?? true) === false,
            'same-organization SEND ignores an unrelated peer-home policy',
        );
        $sameOrgConversationId = (string) ($sameOrgSend['message']['conversation_id'] ?? '');
    } catch (Throwable $exception) {
        $assert(false, 'same-organization SEND remains available: ' . $exception->getMessage());
    }
    $assert(
        array_sum($crossSendArtifacts(
            $failedSendConversationId,
            $failedSendClientMsgId,
        )) === 0,
        'peer-home rejected SEND rolls back canonical, home, message and outbox artifacts',
    );
} finally {
    $repository->execute(
        'UPDATE sm_tenant_im_policy
            SET status = "ENABLED", version = version + 1
          WHERE organization = ?',
        [$orgB],
    );
}

// --- ON + friends: send succeeds twice ---
$ack1 = $messages->send($context($orgA, $userA), $sendPacket($orgB, $userB, 'on-1-' . $suffix));
$assert(($ack1['duplicated'] ?? true) === false, 'first send not duplicated');
$assert(isset($ack1['message']['message_id']), 'first send has message_id');
$conversationId = (string) $ack1['message']['conversation_id'];
$assert(
    $conversationId === CrossOrganizationSocialPolicy::singleConversationId($orgA, $userA, $orgB, $userB),
    'cross-org conversation id uses both complete identities',
);

$ack2 = $messages->send($context($orgA, $userA), $sendPacket($orgB, $userB, 'on-2-' . $suffix));
$assert(($ack2['duplicated'] ?? true) === false, 'second send not duplicated');
$assert((string) $ack2['message']['conversation_id'] === $conversationId, 'same conversation');

$ack3 = $messages->send(
    $context($orgB, $userB),
    $sendPacket($orgA, $userA, 'on-reverse-' . $suffix),
);
$assert((string) $ack3['message']['conversation_id'] === $conversationId, 'reverse send uses same conversation');
$originSequences = array_map(
    static fn (array $row): int => (int) $row['message_seq'],
    $repository->fetchAll(
        'SELECT message_seq FROM im_message_index
          WHERE organization = ? AND conversation_id = ? ORDER BY message_seq',
        [$orgA, $conversationId],
    ),
);
$peerSequences = array_map(
    static fn (array $row): int => (int) $row['message_seq'],
    $repository->fetchAll(
        'SELECT message_seq FROM im_message_index
          WHERE organization = ? AND conversation_id = ? ORDER BY message_seq',
        [$orgB, $conversationId],
    ),
);
$assert($originSequences === [1, 2, 3], 'origin home has a continuous canonical message sequence');
$assert($peerSequences === [1, 2, 3], 'peer home has the same canonical message sequence');
$canonical = $repository->fetchOne(
    'SELECT next_message_seq FROM im_cross_organization_conversation WHERE conversation_id = ?',
    [$conversationId],
);
$assert((int) ($canonical['next_message_seq'] ?? 0) === 4, 'canonical sequence owner advanced exactly once per send');
foreach ([$orgA, $orgB] as $homeOrganization) {
    $outboxCount = $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate FROM im_message_outbox
          WHERE organization = ? AND conversation_id = ? AND event_type = ?',
        [$homeOrganization, $conversationId, 'message.created'],
    );
    $assert((int) ($outboxCount['aggregate'] ?? 0) === 3, "home {$homeOrganization} has one created event per message");
}

$editClientMsgId = 'edit-' . $suffix;
$editAck = $messages->edit($context($orgA, $userA), [
    'message_id' => (string) $ack2['message']['message_id'],
    'content' => ['text' => 'edited-cross-org-service-test'],
    'client_msg_id' => $editClientMsgId,
]);
$assert(
    ($editAck['client_msg_id'] ?? '') === $editClientMsgId
        && ($editAck['actor_organization'] ?? 0) === $orgA
        && ($editAck['actor_user_id'] ?? '') === $userA
        && ($editAck['conversation_id'] ?? '') === $conversationId
        && ($editAck['message_id'] ?? '') === (string) $ack2['message']['message_id']
        && ($editAck['content'] ?? null) === ['text' => 'edited-cross-org-service-test'],
    'edit ACK binds request, actor, conversation, message and normalized content',
);
$changeSync = $messages->sync($context($orgB, $userB), [
    'conversation_id' => $conversationId,
    'after_seq' => 3,
    'after_change_seq' => 0,
    'limit' => 50,
]);
$editChange = array_values(array_filter(
    $changeSync['changes'] ?? [],
    static fn (array $change): bool => ($change['change_type'] ?? '') === 'edit',
))[0] ?? null;
$assert(
    is_array($editChange)
        && ($editChange['actor_organization'] ?? 0) === $orgA
        && ($editChange['actor_user_id'] ?? '') === $userA,
    'SYNC change exposes the persisted composite actor identity',
);

$repository->execute(
    'UPDATE sm_tenant_im_policy SET status = "DISABLED", version = version + 1 WHERE organization = ?',
    [$orgB],
);
try {
    foreach ([
        'edit' => static fn () => $messages->edit($context($orgA, $userA), [
            'message_id' => (string) $ack1['message']['message_id'],
            'content' => ['text' => 'must-not-cross-disabled-peer-policy'],
            'client_msg_id' => 'edit-peer-policy-disabled-' . $suffix,
        ]),
        'recall' => static fn () => $messages->recall($context($orgA, $userA), [
            'message_id' => (string) $ack1['message']['message_id'],
            'client_msg_id' => 'recall-peer-policy-disabled-' . $suffix,
        ]),
    ] as $operation => $invoke) {
        try {
            $invoke();
            $assert(false, "peer-home disabled policy must reject cross-org {$operation}");
        } catch (ImException $exception) {
            $assert(
                $exception->errorCode() === 'TENANT_POLICY_FORBIDDEN',
                "cross-org {$operation} fails closed on either home policy",
            );
        }
    }
} finally {
    $repository->execute(
        'UPDATE sm_tenant_im_policy SET status = "ENABLED", version = version + 1 WHERE organization = ?',
        [$orgB],
    );
}

$missingAckBindingRejected = false;
try {
    $messages->ack($context($orgB, $userB), [
        'message_id' => (string) $ack1['message']['message_id'],
        'status' => 'read',
    ]);
} catch (ImException $exception) {
    $missingAckBindingRejected = $exception->errorCode() === 'ACK_CLIENT_MSG_ID_INVALID';
}
$assert($missingAckBindingRejected, 'ACK rejects a missing request client_msg_id');
$receiptClientMsgId = 'ack-read-' . $suffix;
$receipt = $messages->ack($context($orgB, $userB), [
    'message_id' => (string) $ack1['message']['message_id'],
    'status' => 'read',
    'client_msg_id' => $receiptClientMsgId,
]);
$assert(
    ($receipt['user_organization'] ?? 0) === $orgB
        && ($receipt['client_msg_id'] ?? '') === $receiptClientMsgId
        && ($receipt['request_client_msg_id'] ?? '') === $receiptClientMsgId,
    'ACK binds request id and composite reader identity',
);
foreach ([$orgA, $orgB] as $homeOrganization) {
    $storedReceipt = $repository->fetchOne(
        'SELECT MAX(status) AS status FROM im_message_receipt
          WHERE organization = ? AND message_id = ?
            AND user_organization = ? AND user_id = ?',
        [$homeOrganization, $ack1['message']['message_id'], $orgB, $userB],
    );
    $assert((int) ($storedReceipt['status'] ?? 0) === 3, "read receipt mirrored to home {$homeOrganization}");
    $member = $repository->fetchOne(
        'SELECT last_read_seq FROM im_conversation_member
          WHERE organization = ? AND conversation_id = ?
            AND member_organization = ? AND user_id = ?',
        [$homeOrganization, $conversationId, $orgB, $userB],
    );
    $assert((int) ($member['last_read_seq'] ?? 0) === 1, "read cursor mirrored to home {$homeOrganization}");
}

$conversationRead = (new ConversationSyncService(
    $repository,
    new OutboxService($repository, $config),
))->markRead(
    $context($orgA, $userA),
    'client-' . $suffix . '-' . $userA,
    $conversationId,
    (string) $ack3['message']['message_id'],
);
$assert(($conversationRead['user_organization'] ?? 0) === $orgA, 'conversation_read carries composite reader identity');
foreach ([$orgA, $orgB] as $homeOrganization) {
    $member = $repository->fetchOne(
        'SELECT last_read_seq FROM im_conversation_member
          WHERE organization = ? AND conversation_id = ?
            AND member_organization = ? AND user_id = ?',
        [$homeOrganization, $conversationId, $orgA, $userA],
    );
    $assert((int) ($member['last_read_seq'] ?? 0) === 3, "conversation_read cursor mirrored to home {$homeOrganization}");
    $readOutbox = $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate FROM im_message_outbox
          WHERE organization = ? AND conversation_id = ? AND event_type = ?',
        [$homeOrganization, $conversationId, 'conversation.read'],
    );
    $assert((int) ($readOutbox['aggregate'] ?? 0) === 1, "conversation_read outbox written to home {$homeOrganization}");
}

$screenshotClientMsgId = 'screenshot-' . $suffix;
$screenshot1 = $messages->screenshot($context($orgA, $userA), [
    'conversation_id' => $conversationId,
    'client_msg_id' => $screenshotClientMsgId,
]);
$screenshot2 = $messages->screenshot($context($orgA, $userA), [
    'conversation_id' => $conversationId,
    'client_msg_id' => $screenshotClientMsgId,
]);
$assert(
    (string) ($screenshot1['notice_message']['message_id'] ?? '')
        === (string) ($screenshot2['notice_message']['message_id'] ?? ''),
    'screenshot retries create exactly one system notice',
);
$assert(
    ($screenshot2['client_msg_id'] ?? '') === $screenshotClientMsgId
        && ($screenshot2['actor_organization'] ?? 0) === $orgA
        && ($screenshot2['actor_user_id'] ?? '') === $userA,
    'screenshot ACK binds the top-level request and actor identity',
);
$assert(
    ($screenshot2['enabled'] ?? false) === true,
    'screenshot notice is generated when either home requires it',
);
$screenshotIndexCount = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate FROM im_message_index
      WHERE organization = ? AND sender_organization = ? AND sender_id = ? AND client_msg_id = ?',
    [
        $orgA,
        $orgA,
        'system_notification',
        'screenshot_' . hash('sha256', $orgA . ':' . $userA . ':' . $screenshotClientMsgId),
    ],
);
$assert((int) ($screenshotIndexCount['aggregate'] ?? 0) === 1, 'screenshot idempotency row exists once in actor home');

try {
    $messages->delete($context($orgA, $userA), [
        'message_id' => (string) $ack2['message']['message_id'],
        'scope' => 'both',
        'client_msg_id' => 'delete-both-peer-disabled-' . $suffix,
    ]);
    $assert(false, 'peer-home delete_both disable must reject the mutation');
} catch (ImException $exception) {
    $assert(
        $exception->errorCode() === 'DELETE_BOTH_DISABLED',
        'delete_both fails closed when either home forbids it',
    );
}
$deleteSelf = $messages->delete($context($orgA, $userA), [
    'message_id' => (string) $ack3['message']['message_id'],
    'scope' => 'self',
    'client_msg_id' => 'delete-self-current-home-' . $suffix,
]);
$assert(($deleteSelf['scope'] ?? '') === 'self', 'delete_self uses only the actor-home policy');
$deleteSelfHomes = array_map(
    static fn (array $row): int => (int) $row['organization'],
    $repository->fetchAll(
        'SELECT organization
           FROM im_message_user_delete
          WHERE conversation_id = ? AND message_id = ?
            AND user_organization = ? AND user_id = ?
          ORDER BY organization',
        [$conversationId, $ack3['message']['message_id'], $orgA, $userA],
    ),
);
$assert($deleteSelfHomes === [$orgA], 'delete_self persists only in the actor identity home');

$crossOutboxRows = $repository->fetchAll(
    'SELECT event_type, routing_key, payload_json
       FROM im_message_outbox
      WHERE conversation_id = ?
      ORDER BY id',
    [$conversationId],
);
$crossOutboxEventTypes = [];
$crossOutboxProjector = new RealtimeEventProjector();
foreach ($crossOutboxRows as $crossOutboxRow) {
    $crossOutboxPayload = json_decode(
        (string) $crossOutboxRow['payload_json'],
        true,
        flags: JSON_THROW_ON_ERROR,
    );
    $crossOutboxEventTypes[(string) $crossOutboxRow['event_type']] = true;
    $assert(
        ($crossOutboxPayload['cross_org_access_snapshot_id'] ?? null) === '2',
        (string) $crossOutboxRow['event_type'] . ' durable event carries transaction snapshot 2',
    );
    $projectedCrossOutbox = $crossOutboxProjector->project(
        (string) $crossOutboxRow['routing_key'],
        json_encode($crossOutboxPayload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
    );
    $assert(
        $projectedCrossOutbox->crossOrgAccessSnapshotId === '2',
        (string) $crossOutboxRow['event_type'] . ' realtime projection preserves snapshot 2',
    );
}
foreach ([
    Constants::MQ_ROUTING_MESSAGE_CREATED,
    Constants::MQ_ROUTING_MESSAGE_EDITED,
    Constants::MQ_ROUTING_MESSAGE_RECEIPT,
    Constants::MQ_ROUTING_CONVERSATION_READ,
] as $expectedEventType) {
    $assert(isset($crossOutboxEventTypes[$expectedEventType]), "{$expectedEventType} durable channel is covered");
}

$delayedOutbox = $repository->fetchOne(
    'SELECT routing_key, payload_json
       FROM im_message_outbox
      WHERE organization = ?
        AND message_id = ?
        AND event_type = ?
      LIMIT 1',
    [$orgB, $ack1['message']['message_id'], Constants::MQ_ROUTING_MESSAGE_CREATED],
);
$assert($delayedOutbox !== null, 'peer-home delayed message event exists');
$delayedPayload = json_decode(
    (string) ($delayedOutbox['payload_json'] ?? '{}'),
    true,
    flags: JSON_THROW_ON_ERROR,
);
$delayedMessageEvent = (new RealtimeEventProjector())->project(
    (string) ($delayedOutbox['routing_key'] ?? ''),
    json_encode($delayedPayload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
);
$assert(
    $delayedMessageEvent->crossOrgAccessSnapshotId === '2',
    'peer-home delayed message projects snapshot 2',
);
$realtimeProvider = new DatabaseRealtimeRecipientProvider($repository);
$authorizedRealtimeIdentities = null;
$realtimeProvider->withDeliverableIdentities(
    $delayedMessageEvent,
    static function (array $identities) use (
        &$authorizedRealtimeIdentities,
        $assert,
        $blockedUpdate,
        $groupId,
    ): void {
        $authorizedRealtimeIdentities = $identities;
        $assert($blockedUpdate(
            'UPDATE sm_system_config SET `value` = `value`
              WHERE group_id = ? AND `key` = ?',
            [$groupId, CrossOrganizationSocialPolicy::CONFIG_KEY],
        ), 'revoke waits until authorized peer-home realtime fanout finishes');
    },
);
$assert(
    $authorizedRealtimeIdentities === [['organization' => $orgB, 'user_id' => $userB]],
    'current peer-home message event resolves only its local recipient',
);

$repository->execute(
    'INSERT INTO im_friend_request
        (organization, from_organization, to_organization, from_user_id, to_user_id,
         add_method, message, status, create_time, update_time)
     VALUES (?, ?, ?, ?, ?, "username", "delayed friend request", 1, ?, ?)',
    [$orgB, $orgA, $orgB, $userA, $userB, $now, $now],
);
$friendRequestId = $repository->lastInsertId();
$friendAuthorizer = new DatabaseFriendRequestRealtimeAuthorizer(
    $repository,
    new CrossOrganizationConversationAccess(
        $repository,
        new CrossOrganizationSocialPolicy($repository),
    ),
);
$friendIdentityOrder = (new ReflectionClass($friendAuthorizer))
    ->getMethod('orderedIdentities');
$friendIdentityOrder->setAccessible(true);
$orderedFriendIdentities = $friendIdentityOrder->invoke(
    null,
    $orgA,
    $userA,
    $orgB,
    $userB,
);
$assert(
    array_column($orderedFriendIdentities, 'organization') === [10, 2],
    'friend realtime user locks use canonical identity byte order for the 2/10 counterexample',
);
$currentFriendDelivered = false;
$friendAuthorizer->withCurrentRequest(
    $orgB,
    $friendRequestId,
    $orgA,
    $userA,
    $orgB,
    $userB,
    '2',
    static function () use (
        &$currentFriendDelivered,
        $assert,
        $blockedUpdate,
        $groupId,
        $friendRequestId,
        $orgA,
        $orgB,
        $userA,
        $userB,
    ): void {
        $currentFriendDelivered = true;
        $assert($blockedUpdate(
            'UPDATE sm_system_config SET `value` = `value`
              WHERE group_id = ? AND `key` = ?',
            [$groupId, CrossOrganizationSocialPolicy::CONFIG_KEY],
        ), 'friend realtime fanout holds the current policy boundary');
        $assert($blockedUpdate(
            'UPDATE im_friend_request SET status = status WHERE id = ?',
            [$friendRequestId],
        ), 'friend realtime fanout holds the authoritative pending request');
        foreach ([[$orgB, $userB], [$orgA, $userA]] as [$organization, $userId]) {
            $assert($blockedUpdate(
                'UPDATE im_user SET status = status
                  WHERE organization = ? AND user_id = ?',
                [$organization, $userId],
            ), "friend realtime fanout holds canonical user lock {$organization}:{$userId}");
        }
    },
);
$assert($currentFriendDelivered, 'current cross-org friend event is deliverable');

$badGroupId = 'group-foreign-' . $suffix;
$badGroupMessageId = str_pad('badgroup' . $suffix, 40, '0');
$badGroupClientMsgId = 'bad-group-' . $suffix;
$badGroupIndex = $repository->fetchOne(
    'SELECT shard_table FROM im_message_index WHERE organization = ? AND message_id = ? LIMIT 1',
    [$orgA, $ack1['message']['message_id']],
);
$badGroupTable = (string) ($badGroupIndex['shard_table'] ?? '');
$maxGlobal = $repository->fetchOne(
    'SELECT COALESCE(MAX(global_seq), 0) + 100 AS next_seq FROM im_message_index WHERE organization = ?',
    [$orgA],
);
$badGroupGlobalSeq = (int) ($maxGlobal['next_seq'] ?? 100);
$repository->execute(
    'INSERT INTO im_conversation
        (organization, conversation_id, conversation_type, title, owner_user_id, owner_organization,
         next_message_seq, status, create_time, update_time)
     VALUES (?, ?, 2, "invalid-cross-group", ?, ?, 2, 1, ?, ?)',
    [$orgA, $badGroupId, $userA, $orgA, $now, $now],
);
foreach ([[$orgA, $userA, 'owner'], [$orgB, $userB, 'member']] as [$memberOrg, $memberUser, $role]) {
    $repository->execute(
        'INSERT INTO im_conversation_member
            (organization, conversation_id, user_id, member_organization, member_role,
             inviter_user_id, inviter_organization, status, join_at, create_time, update_time)
         VALUES (?, ?, ?, ?, ?, NULL, 0, 1, ?, ?, ?)',
        [$orgA, $badGroupId, $memberUser, $memberOrg, $role, $now, $now, $now],
    );
    $repository->execute(
        'INSERT INTO im_conversation_membership_period
            (organization, conversation_id, user_id, member_organization, period_no,
             visible_from_message_seq, join_at, status, create_time, update_time)
         VALUES (?, ?, ?, ?, 1, 1, ?, 1, ?, ?)',
        [$orgA, $badGroupId, $memberUser, $memberOrg, $now, $now, $now],
    );
}
$repository->execute(
    'INSERT INTO `' . str_replace('`', '``', $badGroupTable) . '`
        (organization, conversation_id, conversation_type, message_id, message_seq, client_msg_id,
         sender_id, sender_organization, message_type, content, status, create_time, update_time)
     VALUES (?, ?, 2, ?, 1, ?, ?, ?, 1, ?, 1, ?, ?)',
    [$orgA, $badGroupId, $badGroupMessageId, $badGroupClientMsgId, $userA, $orgA, '{"text":"invalid"}', $now, $now],
);
$repository->execute(
    'INSERT INTO im_message_index
        (organization, global_seq, message_id, conversation_id, message_seq, sender_id,
         sender_organization, client_msg_id, storage_node, shard_table, create_time)
     VALUES (?, ?, ?, ?, 1, ?, ?, ?, "mysql-primary", ?, ?)',
    [$orgA, $badGroupGlobalSeq, $badGroupMessageId, $badGroupId, $userA, $orgA, $badGroupClientMsgId, $badGroupTable, $now],
);
$badGroupOperations = [
    'SEND' => static fn () => $messages->send($context($orgA, $userA), Packet::make('send', [
        'conversation_id' => $badGroupId,
        'conversation_type' => 2,
        'message_type' => 1,
        'content' => ['text' => 'reject'],
    ], 0, 'bad-group-send-' . $suffix)),
    'ACK' => static fn () => $messages->ack($context($orgA, $userA), [
        'message_id' => $badGroupMessageId,
        'status' => 'read',
        'client_msg_id' => 'bad-group-ack-' . $suffix,
    ]),
    'SYNC' => static fn () => $messages->sync($context($orgA, $userA), [
        'conversation_id' => $badGroupId,
        'after_global_seq' => '0',
    ]),
    'read' => static fn () => (new ConversationSyncService(
        $repository,
        new OutboxService($repository, $config),
    ))->markRead($context($orgA, $userA), 'client-group', $badGroupId, $badGroupMessageId),
    'typing' => static fn () => (new TypingService($repository))->relay(
        $context($orgA, $userA),
        'client-group',
        ['conversation_id' => $badGroupId],
    ),
    'realtime-recipient' => static fn () => (new DatabaseRealtimeRecipientProvider($repository))
        ->activeIdentities($orgA, $badGroupId, 1),
];
foreach ($badGroupOperations as $operation => $invoke) {
    try {
        $invoke();
        $assert(false, "foreign group member rejects {$operation}");
    } catch (ImException|RuntimeException $exception) {
        $assert(
            str_contains($exception->getMessage(), '外机构')
                || str_contains($exception->getMessage(), 'outside its home projection'),
            "foreign group member rejects {$operation} explicitly",
        );
    }
}
$repository->execute('DELETE FROM im_message_index WHERE organization = ? AND message_id = ?', [$orgA, $badGroupMessageId]);
$repository->execute(
    'DELETE FROM `' . str_replace('`', '``', $badGroupTable) . '` WHERE organization = ? AND message_id = ?',
    [$orgA, $badGroupMessageId],
);
$repository->execute('DELETE FROM im_conversation_membership_period WHERE organization = ? AND conversation_id = ?', [$orgA, $badGroupId]);
$repository->execute('DELETE FROM im_conversation_member WHERE organization = ? AND conversation_id = ?', [$orgA, $badGroupId]);
$repository->execute('DELETE FROM im_conversation WHERE organization = ? AND conversation_id = ?', [$orgA, $badGroupId]);

// Dual-home: recipient org mirrored message must project sender_user
$mirrorIndex = $repository->fetchOne(
    'SELECT message_id, organization, shard_table FROM im_message_index
      WHERE organization = ? AND conversation_id = ? AND message_id = ?
      LIMIT 1',
    [$orgB, $conversationId, $ack1['message']['message_id']],
);
$assert($mirrorIndex !== null, 'mirror index exists in recipient org');
$table = (string) $mirrorIndex['shard_table'];
$mirrorBody = $repository->fetchOne(
    'SELECT * FROM `' . str_replace('`', '``', $table) . '` WHERE organization = ? AND message_id = ? LIMIT 1',
    [$orgB, (string) $mirrorIndex['message_id']],
);
$assert($mirrorBody !== null, 'mirror body exists');
$mirrorBody['global_seq'] = '1';
$mirrorBody['_message_table'] = $table;

// Drive real formatMessage via reflection (shipped private path used by send/SYNC)
$ref = new ReflectionClass($messages);
$format = $ref->getMethod('formatMessage');
$format->setAccessible(true);
$formatted = $format->invoke($messages, $mirrorBody);
$assert(is_array($formatted['sender_user'] ?? null), 'cross-org mirror sender_user projected');
$assert(($formatted['sender_user']['user_id'] ?? '') === $userA, 'sender_user is userA');
$assert(($formatted['sender_user']['is_cross_organization'] ?? false) === true, 'sender marked cross-org for recipient home');
$assert(str_contains((string) ($formatted['sender_user']['display_name'] ?? ''), '·'), 'sender display_name has company');
$assert(
    $policyLockFailureCode($groupId) === 1205,
    'policy locking read propagates lock-wait failure without continuing',
);

$accessGuard = new CrossOrganizationConversationAccess(
    $repository,
    new CrossOrganizationSocialPolicy($repository),
);
$accessPreview = $accessGuard->assertAccessible($orgA, $conversationId);
$lockMutationPolicyBoundary = (new ReflectionClass($messages))
    ->getMethod('lockCrossHomeTenantPolicyBoundary');
$lockMutationPolicyBoundary->setAccessible(true);
$repository->transaction(function () use (
    $accessGuard,
    $accessPreview,
    $lockMutationPolicyBoundary,
    $messages,
    $blockedUpdate,
    $assert,
    $groupId,
    $orgA,
    $orgB,
    $userA,
    $userB,
    $conversationId,
): void {
    $lockMutationPolicyBoundary->invoke(
        $messages,
        $accessPreview['home_organizations'],
    );
    foreach ([$orgA, $orgB] as $homeOrganization) {
        $assert($blockedUpdate(
            'UPDATE sm_tenant_im_policy
                SET version = version
              WHERE organization = ?',
            [$homeOrganization],
        ), "durable mutation prefix holds tenant policy {$homeOrganization}");
    }
    $assert(!$blockedUpdate(
        'UPDATE im_cross_organization_conversation
            SET update_time = update_time
          WHERE conversation_id = ?',
        [$conversationId],
    ), 'durable mutation locks both tenant policies before its canonical row');
    $accessGuard->assertAccessible(
        $orgA,
        $conversationId,
        true,
        $accessPreview['home_organizations'],
        $accessPreview['participant_identities'],
    );
    $assert($blockedUpdate(
        'UPDATE sm_system_config SET `value` = `value`
          WHERE group_id = ? AND `key` = ?',
        [$groupId, CrossOrganizationSocialPolicy::CONFIG_KEY],
    ), 'managed config revoke waits behind the write boundary');
    $assert($blockedUpdate(
        'UPDATE sm_system_organization SET status = status WHERE id = ?',
        [$orgB],
    ), 'organization revoke waits behind sorted organization locks');
    foreach ([[$orgB, $userB], [$orgA, $userA]] as [$organization, $userId]) {
        $assert($blockedUpdate(
            'UPDATE im_user SET status = status
              WHERE organization = ? AND user_id = ?',
            [$organization, $userId],
        ), "write boundary holds canonical 2/10 user lock {$organization}:{$userId}");
    }
    $assert($blockedUpdate(
        'UPDATE im_cross_organization_conversation
            SET update_time = update_time
          WHERE conversation_id = ?',
        [$conversationId],
    ), 'canonical mutation waits behind the conversation write lock');
});

$lockTenantPolicies = (new ReflectionClass($messages))->getMethod('lockHomeTenantPolicies');
$lockTenantPolicies->setAccessible(true);
$repository->transaction(function () use (
    $lockTenantPolicies,
    $messages,
    $blockedUpdate,
    $assert,
    $orgA,
    $orgB,
): void {
    $lockTenantPolicies->invoke($messages, [$orgB, $orgA]);
    foreach ([$orgA, $orgB] as $homeOrganization) {
        $assert($blockedUpdate(
            'UPDATE sm_tenant_im_policy SET version = version WHERE organization = ?',
            [$homeOrganization],
        ), "cross-home mutation holds tenant policy {$homeOrganization}");
    }
});
$lockMessageConfigs = (new ReflectionClass($messages))->getMethod('lockHomeMessageOperationConfigs');
$lockMessageConfigs->setAccessible(true);
$repository->transaction(function () use (
    $lockMessageConfigs,
    $messages,
    $blockedUpdate,
    $assert,
    $messageConfigGroupId,
    $orgA,
    $orgB,
): void {
    $lockMessageConfigs->invoke($messages, [$orgB, $orgA]);
    foreach ([$orgA, $orgB] as $homeOrganization) {
        $assert($blockedUpdate(
            'UPDATE sm_tenant_config SET update_time = update_time
              WHERE organization = ? AND group_id = ?',
            [$homeOrganization, $messageConfigGroupId],
        ), "cross-home mutation holds message operation policy {$homeOrganization}");
    }
});

$sendPrerequisites = (new ReflectionClass($messages))
    ->getMethod('lockCrossOrganizationSendPrerequisites');
$sendPrerequisites->setAccessible(true);
$canonicalFriendDirections = (new ReflectionClass($messages))
    ->getMethod('canonicalFriendDirections');
$canonicalFriendDirections->setAccessible(true);
$orderedSendIdentities = (new ReflectionClass($messages))->getMethod('orderedIdentityPair');
$orderedSendIdentities->setAccessible(true);
$assert(
    array_column(
        $orderedSendIdentities->invoke($messages, $orgA, $userA, $orgB, $userB),
        'organization',
    ) === [10, 2],
    'MessageService user locks use canonical UTF-8 byte order for the 2/10 counterexample',
);
$orderedFriendDirections = $canonicalFriendDirections->invoke(
    $messages,
    $orgA,
    $userA,
    $orgB,
    $userB,
);
$assert(
    array_column($orderedFriendDirections, 'organization') === [10, 2]
        && array_column($orderedFriendDirections, 'friend_organization') === [2, 10],
    'SEND exact friend locks use canonical owner identity order for the 2/10 counterexample',
);
$repository->transaction(function () use (
    $sendPrerequisites,
    $messages,
    $blockedUpdate,
    $assert,
    $orgA,
    $orgB,
    $userA,
    $userB,
    $conversationId,
): void {
    $sendPrerequisites->invoke($messages, $orgA, $userA, $orgB, $userB);
    foreach ([$orgA, $orgB] as $homeOrganization) {
        $assert($blockedUpdate(
            'UPDATE sm_tenant_im_policy
                SET version = version
              WHERE organization = ?',
            [$homeOrganization],
        ), "SEND prefix holds tenant policy {$homeOrganization}");
    }
    $assert(!$blockedUpdate(
        'UPDATE im_cross_organization_conversation
            SET update_time = update_time
          WHERE conversation_id = ?',
        [$conversationId],
    ), 'SEND locks both tenant policies before its canonical row');
    $assert($blockedUpdate(
        'UPDATE im_user SET status = status
          WHERE organization = ? AND user_id = ?',
        [$orgB, $userB],
    ), 'target-user revoke waits behind SEND identity locks');
    foreach ([
        [$orgB, $userB, $orgA, $userA],
        [$orgA, $userA, $orgB, $userB],
    ] as [$ownerOrganization, $ownerUserId, $friendOrganization, $friendUserId]) {
        $assert($blockedUpdate(
            'UPDATE im_friend_relation SET status = status
              WHERE organization = ? AND user_id = ?
                AND friend_organization = ? AND friend_user_id = ?',
            [$ownerOrganization, $ownerUserId, $friendOrganization, $friendUserId],
        ), "friend revoke waits behind exact SEND relation lock {$ownerOrganization}:{$ownerUserId}");
    }
});
$repository->transaction(function () use (
    $sendPrerequisites,
    $messages,
    $blockedUpdate,
    $assert,
    $orgA,
    $orgB,
    $userA,
    $userB,
): void {
    $sendPrerequisites->invoke($messages, $orgB, $userB, $orgA, $userA);
    $assert($blockedUpdate(
        'UPDATE im_user SET status = status
          WHERE organization = ? AND user_id = ?',
        [$orgA, $userA],
    ), 'reverse SEND locks the same deterministic identity set');
    $assert($blockedUpdate(
        'UPDATE im_friend_relation SET status = status
          WHERE organization = ? AND user_id = ?
            AND friend_organization = ? AND friend_user_id = ?',
        [$orgB, $userB, $orgA, $userA],
    ), 'reverse SEND locks the same deterministic relation set');
});

// Missing the first canonical direction must not short-circuit the second
// exact locking read. Both the absent unique-key gap and the surviving row
// remain locked until the transaction ends.
$repository->execute(
    'DELETE FROM im_friend_relation
      WHERE organization = ? AND user_id = ?
        AND friend_organization = ? AND friend_user_id = ?',
    [$orgB, $userB, $orgA, $userA],
);
$repository->transaction(function () use (
    $sendPrerequisites,
    $messages,
    $blockedUpdate,
    $assert,
    $orgA,
    $orgB,
    $userA,
    $userB,
    $now,
): void {
    try {
        $sendPrerequisites->invoke($messages, $orgA, $userA, $orgB, $userB);
        $assert(false, 'missing canonical friend direction must reject SEND');
    } catch (ImException $exception) {
        $assert(
            $exception->errorCode() === 'SEND_SINGLE_FRIEND_REQUIRED',
            'missing canonical friend direction fails closed after both locking reads',
        );
    }
    $assert($blockedUpdate(
        'INSERT INTO im_friend_relation
            (organization, user_id, friend_user_id, friend_organization,
             add_method, added_at, status, create_time, update_time)
         VALUES (?, ?, ?, ?, "username", ?, 1, ?, ?)',
        [$orgB, $userB, $userA, $orgA, $now, $now, $now],
    ), 'missing first friend direction holds its exact unique-key gap');
    $assert($blockedUpdate(
        'UPDATE im_friend_relation SET status = status
          WHERE organization = ? AND user_id = ?
            AND friend_organization = ? AND friend_user_id = ?',
        [$orgA, $userA, $orgB, $userB],
    ), 'missing first friend direction still locks the second exact row');
});
$repository->execute(
    'INSERT INTO im_friend_relation
        (organization, user_id, friend_user_id, friend_organization,
         add_method, added_at, status, create_time, update_time)
     VALUES (?, ?, ?, ?, "username", ?, 1, ?, ?)',
    [$orgB, $userB, $userA, $orgA, $now, $now, $now],
);

// OFF again after friends: still allow same-org? cross-org should reject again
$setSwitch('0', '3');
$revokedRealtimeIdentities = null;
$realtimeProvider->withDeliverableIdentities(
    $delayedMessageEvent,
    static function (array $identities) use (&$revokedRealtimeIdentities): void {
        $revokedRealtimeIdentities = $identities;
    },
);
$assert($revokedRealtimeIdentities === [], 'revoke-before-delivery drops delayed message content');
$revokedFriendDelivered = false;
$friendAuthorizer->withCurrentRequest(
    $orgB,
    $friendRequestId,
    $orgA,
    $userA,
    $orgB,
    $userB,
    '2',
    static function () use (&$revokedFriendDelivered): void {
        $revokedFriendDelivered = true;
    },
);
$assert(!$revokedFriendDelivered, 'revoke-before-delivery drops delayed friend request');

$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);
try {
    $messages->send($context($orgA, $userA), $sendPacket($orgB, $userB, 'off-again-' . $suffix));
    $assert(false, 'switch off again must reject even with friendship');
} catch (ImException $e) {
    $assert(
        in_array($e->errorCode(), ['SEND_SINGLE_RECEIVER_INVALID', 'SEND_SINGLE_FRIEND_REQUIRED'], true),
        'off-again reject code=' . $e->errorCode(),
    );
}

$offOperations = [
    'ACK' => static fn () => $messages->ack($context($orgB, $userB), [
        'message_id' => (string) $ack2['message']['message_id'],
        'status' => 'delivered',
        'client_msg_id' => 'off-ack-' . $suffix,
    ]),
    'recall' => static fn () => $messages->recall($context($orgA, $userA), [
        'message_id' => (string) $ack2['message']['message_id'],
        'client_msg_id' => 'off-recall-' . $suffix,
    ]),
    'edit' => static fn () => $messages->edit($context($orgA, $userA), [
        'message_id' => (string) $ack2['message']['message_id'],
        'content' => ['text' => 'must-not-edit'],
        'client_msg_id' => 'off-edit-' . $suffix,
    ]),
    'delete_self' => static fn () => $messages->delete($context($orgA, $userA), [
        'message_id' => (string) $ack2['message']['message_id'],
        'scope' => 'self',
        'client_msg_id' => 'off-delete-self-' . $suffix,
    ]),
    'delete_both' => static fn () => $messages->delete($context($orgA, $userA), [
        'message_id' => (string) $ack2['message']['message_id'],
        'scope' => 'both',
        'client_msg_id' => 'off-delete-both-' . $suffix,
    ]),
    'screenshot' => static fn () => $messages->screenshot($context($orgA, $userA), [
        'conversation_id' => $conversationId,
        'client_msg_id' => 'off-screenshot-' . $suffix,
    ]),
    'conversation_sync' => static fn () => $messages->sync($context($orgA, $userA), [
        'conversation_id' => $conversationId,
        'after_global_seq' => '0',
    ]),
    'conversation_read' => static fn () => (new ConversationSyncService(
        $repository,
        new OutboxService($repository, $config),
    ))->markRead(
        $context($orgA, $userA),
        'client-' . $suffix . '-' . $userA,
        $conversationId,
        (string) $ack3['message']['message_id'],
    ),
    'typing' => static fn () => (new TypingService($repository))->relay(
        $context($orgA, $userA),
        'client-' . $suffix . '-' . $userA,
        ['conversation_id' => $conversationId],
    ),
];
foreach ($offOperations as $operation => $invoke) {
    try {
        $invoke();
        $assert(false, "switch off rejects {$operation}");
    } catch (ImException $exception) {
        $assert(
            $exception->errorCode() === 'CROSS_ORG_ACCESS_REVOKED',
            "switch off rejects {$operation} with access-revoked code",
        );
    }
}

$globalAfterRevoke = $messages->sync($context($orgA, $userA), ['after_global_seq' => '0', 'limit' => 50]);
$globalAfterRevokeConversationIds = array_column(
    $globalAfterRevoke['messages'] ?? [],
    'conversation_id',
);
$assert(
    !in_array($conversationId, $globalAfterRevokeConversationIds, true)
        && in_array($sameOrgConversationId, $globalAfterRevokeConversationIds, true),
    'global SYNC withdraws revoked cross-org messages without hiding same-org messages',
);
$assert(($globalAfterRevoke['cross_org_access_snapshot_id'] ?? '') === '3', 'SYNC echoes current access snapshot');

$setSwitch('1', '4');
$restoredOldRealtimeIdentities = null;
$realtimeProvider->withDeliverableIdentities(
    $delayedMessageEvent,
    static function (array $identities) use (&$restoredOldRealtimeIdentities): void {
        $restoredOldRealtimeIdentities = $identities;
    },
);
$assert(
    $restoredOldRealtimeIdentities === [],
    'old message epoch cannot cross a newer restored access snapshot',
);
$restoredOldFriendDelivered = false;
$friendAuthorizer->withCurrentRequest(
    $orgB,
    $friendRequestId,
    $orgA,
    $userA,
    $orgB,
    $userB,
    '2',
    static function () use (&$restoredOldFriendDelivered): void {
        $restoredOldFriendDelivered = true;
    },
);
$assert(!$restoredOldFriendDelivered, 'old friend epoch cannot cross a newer restored access snapshot');

$currentPayload = $delayedPayload;
$currentPayload['cross_org_access_snapshot_id'] = '4';
$currentMessageEvent = (new RealtimeEventProjector())->project(
    Constants::MQ_ROUTING_MESSAGE_CREATED,
    json_encode($currentPayload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
);
$restoredCurrentRealtimeIdentities = null;
$realtimeProvider->withDeliverableIdentities(
    $currentMessageEvent,
    static function (array $identities) use (&$restoredCurrentRealtimeIdentities): void {
        $restoredCurrentRealtimeIdentities = $identities;
    },
);
$assert(
    $restoredCurrentRealtimeIdentities === [['organization' => $orgB, 'user_id' => $userB]],
    'current restored epoch remains deliverable',
);
$restoredCurrentFriendDelivered = false;
$friendAuthorizer->withCurrentRequest(
    $orgB,
    $friendRequestId,
    $orgA,
    $userA,
    $orgB,
    $userB,
    '4',
    static function () use (&$restoredCurrentFriendDelivered): void {
        $restoredCurrentFriendDelivered = true;
    },
);
$assert($restoredCurrentFriendDelivered, 'current restored friend epoch remains deliverable');

$repository->execute('UPDATE sm_system_organization SET status = 0 WHERE id = ?', [$orgB]);
$inactiveRealtimeIdentities = null;
$realtimeProvider->withDeliverableIdentities(
    $currentMessageEvent,
    static function (array $identities) use (&$inactiveRealtimeIdentities): void {
        $inactiveRealtimeIdentities = $identities;
    },
);
$assert($inactiveRealtimeIdentities === [], 'inactive peer organization drops current message event');
$inactiveFriendDelivered = false;
$friendAuthorizer->withCurrentRequest(
    $orgB,
    $friendRequestId,
    $orgA,
    $userA,
    $orgB,
    $userB,
    '4',
    static function () use (&$inactiveFriendDelivered): void {
        $inactiveFriendDelivered = true;
    },
);
$assert(!$inactiveFriendDelivered, 'inactive peer organization drops current friend event');

try {
    $messages->sync($context($orgA, $userA), [
        'conversation_id' => $conversationId,
        'after_global_seq' => '0',
    ]);
    $assert(false, 'inactive peer organization rejects conversation SYNC');
} catch (ImException $exception) {
    $assert(
        $exception->errorCode() === 'CROSS_ORG_ACCESS_REVOKED',
        'inactive peer organization has access-revoked code',
    );
}
$globalAfterOrgDisable = $messages->sync(
    $context($orgA, $userA),
    ['after_global_seq' => '0', 'limit' => 50],
);
$globalAfterOrgDisableConversationIds = array_column(
    $globalAfterOrgDisable['messages'] ?? [],
    'conversation_id',
);
$assert(
    !in_array($conversationId, $globalAfterOrgDisableConversationIds, true)
        && in_array($sameOrgConversationId, $globalAfterOrgDisableConversationIds, true),
    'global SYNC withdraws cross-org messages when peer is inactive without hiding same-org messages',
);
$repository->execute('UPDATE sm_system_organization SET status = 1 WHERE id = ?', [$orgB]);
$repository->execute(
    'DELETE FROM im_cross_organization_conversation WHERE conversation_id = ?',
    [$conversationId],
);
try {
    $messages->ack($context($orgA, $userA), [
        'message_id' => (string) $ack2['message']['message_id'],
        'status' => 'delivered',
        'client_msg_id' => 'missing-canonical-' . $suffix,
    ]);
    $assert(false, 'missing canonical row must fail closed');
} catch (ImException $exception) {
    $assert($exception->errorCode() === 'CROSS_ORG_CANONICAL_INVALID', 'missing canonical row has fail-closed code');
}

// Destructive rollback contract: new composite single-chat data must be
// removed before the old unique keys/identity shape are restored, while group
// data and same-organization friendships remain representable.
$repository->execute(
    'INSERT INTO im_cross_organization_conversation
        (conversation_id, left_organization, left_user_id, right_organization, right_user_id,
         next_message_seq, status, create_time, update_time)
     VALUES (?, ?, ?, ?, ?, 10, 1, ?, ?)',
    [$conversationId, $orgA, $userA, $orgB, $userB, $now, $now],
);
$repository->execute(
    'INSERT INTO im_message_user_delete
        (organization, conversation_id, message_id, user_id, user_organization, delete_time, create_time)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE delete_time = VALUES(delete_time)',
    [$orgA, $conversationId, $ack2['message']['message_id'], $userA, $orgA, $now, $now],
);
$repository->execute(
    'INSERT INTO im_message_change
        (organization, conversation_id, change_seq, message_id, message_seq, change_type,
         actor_organization, actor_user_id, target_user_id, target_organization, payload_json, create_time)
     VALUES (?, ?, 99, ?, ?, "delete_self", ?, ?, ?, ?, ?, ?)',
    [
        $orgA,
        $conversationId,
        $ack2['message']['message_id'],
        $ack2['message']['message_seq'],
        $orgA,
        $userA,
        $userA,
        $orgA,
        '{"scope":"self"}',
        $now,
    ],
);
$repository->execute(
    'INSERT INTO im_friend_relation
        (organization, user_id, friend_user_id, friend_organization, add_method, added_at, status, create_time, update_time)
     VALUES (?, ?, ?, ?, "username", ?, 1, ?, ?)',
    [$orgA, $userA, $userB, $orgA, $now, $now, $now],
);
$groupConversationId = 'group-down-' . $suffix;
$repository->execute(
    'INSERT INTO im_conversation
        (organization, conversation_id, conversation_type, title, owner_user_id, owner_organization, status, create_time, update_time)
     VALUES (?, ?, 2, "rollback-survivor", ?, ?, 1, ?, ?)',
    [$orgA, $groupConversationId, $userA, $orgA, $now, $now],
);
$repository->execute(
    'INSERT INTO im_conversation_member
        (organization, conversation_id, user_id, member_organization, member_role,
         inviter_user_id, inviter_organization, status, join_at, create_time, update_time)
     VALUES (?, ?, ?, ?, "owner", NULL, 0, 1, ?, ?, ?)',
    [$orgA, $groupConversationId, $userA, $orgA, $now, $now, $now],
);
$repository->execute(
    'INSERT INTO im_conversation_membership_period
        (organization, conversation_id, user_id, member_organization, period_no,
         visible_from_message_seq, join_at, status, create_time, update_time)
     VALUES (?, ?, ?, ?, 1, 1, ?, 1, ?, ?)',
    [$orgA, $groupConversationId, $userA, $orgA, $now, $now, $now],
);
$groupReceiptMessageId = str_pad('group-receipt-' . $suffix, 40, '0');
foreach (['reader-a', 'reader-b'] as $reader) {
    $eventId = hash('sha256', $groupConversationId . '|message.receipt|' . $reader);
    $repository->execute(
        'INSERT INTO im_message_outbox
            (event_id, organization, event_type, routing_key, message_id, change_seq,
             conversation_id, conversation_type, payload_json, status, retry_count,
             next_retry_at, create_time, update_time)
         VALUES (?, ?, "message.receipt", "message.receipt", ?, 0, ?, 2, "{}", 1, 0, ?, ?, ?)',
        [$eventId, $orgA, $groupReceiptMessageId, $groupConversationId, $now, $now, $now],
    );
}

$php = escapeshellarg(PHP_BINARY);
$phinx = escapeshellarg(dirname(__DIR__) . '/vendor/bin/phinx');
$phinxConfig = escapeshellarg(dirname(__DIR__, 2) . '/phinx.php');
passthru("{$php} {$phinx} -c {$phinxConfig} rollback -t 20260717010000 --no-ansi", $rollbackCode);
$assert($rollbackCode === 0, 'migration down succeeds with dual-home data and old-key collision candidate');
$canonicalTable = $repository->fetchOne(
    "SELECT COUNT(*) AS aggregate FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'im_cross_organization_conversation'",
);
$assert((int) ($canonicalTable['aggregate'] ?? 1) === 0, 'down drops canonical table after destructive cleanup');
foreach ([
    'im_conversation',
    'im_conversation_member',
    'im_conversation_membership_period',
    'im_message_index',
    'im_message_receipt',
    'im_message_user_delete',
    'im_message_change',
    'im_message_outbox',
] as $tableName) {
    $row = $repository->fetchOne(
        "SELECT COUNT(*) AS aggregate FROM `{$tableName}` WHERE conversation_id = ?",
        [$conversationId],
    );
    $assert((int) ($row['aggregate'] ?? 1) === 0, "down purges single-chat {$tableName}");
}
$crossFriendsAfterDown = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate FROM im_friend_relation WHERE organization = ? AND friend_organization <> organization',
    [$orgA],
);
$localFriendAfterDown = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate FROM im_friend_relation
      WHERE organization = ? AND user_id = ? AND friend_user_id = ? AND friend_organization = organization',
    [$orgA, $userA, $userB],
);
$groupAfterDown = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate FROM im_conversation WHERE organization = ? AND conversation_id = ? AND conversation_type = 2',
    [$orgA, $groupConversationId],
);
$groupReceiptOutboxAfterDown = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate FROM im_message_outbox
      WHERE organization = ? AND conversation_id = ? AND event_type = "message.receipt"',
    [$orgA, $groupConversationId],
);
$assert((int) ($crossFriendsAfterDown['aggregate'] ?? 1) === 0, 'down purges cross-organization friendships before old unique key');
$assert((int) ($localFriendAfterDown['aggregate'] ?? 0) === 1, 'down preserves same-organization friendship');
$assert((int) ($groupAfterDown['aggregate'] ?? 0) === 1, 'down preserves group conversation data');
$assert(
    (int) ($groupReceiptOutboxAfterDown['aggregate'] ?? 1) === 0,
    'down purges new group receipt events before restoring the old outbox unique key',
);

passthru("{$php} {$phinx} -c {$phinxConfig} migrate -t 20260720010000 --no-ansi", $migrateCode);
$assert($migrateCode === 0, 'migration up succeeds again after destructive down');

$cleanup();
$setSwitch('0', '5');

echo "\n{$passed} passed, {$failed} failed\n";
exit($failed > 0 ? 1 : 0);
