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
use B8im\ImBusiness\Service\CrossOrganizationSocialPolicy;
use B8im\ImBusiness\Service\MessageService;
use B8im\ImBusiness\Service\OutboxService;
use B8im\ImBusiness\Service\TenantImPolicyService;
use B8im\ImShared\Protocol\Packet;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

$config = Config::fromEnv();
$expectedDatabase = trim((string) ($_ENV['IM_EXPECT_DATABASE'] ?? getenv('IM_EXPECT_DATABASE') ?: 'nb8im'));
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
$orgA = 91001;
$orgB = 91002;
$userA = 'xorg-a-' . $suffix;
$userB = 'xorg-b-' . $suffix;
$now = date('Y-m-d H:i:s');

$cleanup = static function () use ($repository, $orgA, $orgB, $userA, $userB): void {
    foreach ([$orgA, $orgB] as $org) {
        foreach ([
            'im_message_outbox',
            'im_message_receipt',
            'im_message_index',
            'im_conversation_membership_period',
            'im_conversation_member',
            'im_conversation',
            'im_user_profile',
            'im_user_privacy_setting',
            'im_organization_message_sequence',
        ] as $table) {
            try {
                $repository->execute("DELETE FROM {$table} WHERE organization = ?", [$org]);
            } catch (Throwable) {
            }
        }
    }
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
            'DELETE FROM im_user WHERE organization IN (?, ?) AND user_id IN (?, ?)',
            [$orgA, $orgB, $userA, $userB],
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
foreach ([[$orgA, $userA, 'alice'], [$orgB, $userB, 'bob']] as [$org, $uid, $acc]) {
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

$setSwitch = static function (string $value) use ($repository, $groupId): void {
    $repository->execute(
        "UPDATE sm_system_config SET value = ? WHERE group_id = ? AND `key` = 'cross_org_social_enabled'",
        [$value, $groupId],
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

$sendPacket = static function (string $toUserId, string $clientMsgId): Packet {
    return Packet::make('send', [
        'conversation_type' => 1,
        'to_user_id' => $toUserId,
        'message_type' => 1,
        'content' => ['text' => 'cross-org-service-test'],
        'client_msg_id' => $clientMsgId,
    ], 0, $clientMsgId);
};

// --- OFF: cross-org send rejected ---
$setSwitch('0');
$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);
try {
    $messages->send($context($orgA, $userA), $sendPacket($userB, 'off-' . $suffix));
    $assert(false, 'switch off must reject cross-org send');
} catch (ImException $e) {
    $assert($e->errorCode() === 'SEND_SINGLE_RECEIVER_INVALID' || $e->errorCode() === 'SEND_SINGLE_FRIEND_REQUIRED',
        'switch off reject code=' . $e->errorCode());
}

// --- ON: without friendship still friend required ---
$setSwitch('1');
$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);
try {
    $messages->send($context($orgA, $userA), $sendPacket($userB, 'on-nofriend-' . $suffix));
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

// --- ON + friends: send succeeds twice ---
$ack1 = $messages->send($context($orgA, $userA), $sendPacket($userB, 'on-1-' . $suffix));
$assert(($ack1['duplicated'] ?? true) === false, 'first send not duplicated');
$assert(isset($ack1['message']['message_id']), 'first send has message_id');
$conversationId = (string) $ack1['message']['conversation_id'];
$assert(str_starts_with($conversationId, 'single_x_'), 'cross-org conversation id prefix');

$ack2 = $messages->send($context($orgA, $userA), $sendPacket($userB, 'on-2-' . $suffix));
$assert(($ack2['duplicated'] ?? true) === false, 'second send not duplicated');
$assert((string) $ack2['message']['conversation_id'] === $conversationId, 'same conversation');

// Dual-home: recipient org mirrored message must project sender_user
$mirrorIndex = $repository->fetchOne(
    'SELECT message_id, organization, shard_table FROM im_message_index
      WHERE organization = ? AND conversation_id = ?
   ORDER BY global_seq DESC LIMIT 1',
    [$orgB, $conversationId],
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

// OFF again after friends: still allow same-org? cross-org should reject again
$setSwitch('0');
$messages = new MessageService(
    $repository,
    $config,
    new OutboxService($repository, $config),
    TenantImPolicyService::connect($config, $repository),
    new CrossOrganizationSocialPolicy($repository),
);
try {
    $messages->send($context($orgA, $userA), $sendPacket($userB, 'off-again-' . $suffix));
    $assert(false, 'switch off again must reject even with friendship');
} catch (ImException $e) {
    $assert(
        in_array($e->errorCode(), ['SEND_SINGLE_RECEIVER_INVALID', 'SEND_SINGLE_FRIEND_REQUIRED'], true),
        'off-again reject code=' . $e->errorCode(),
    );
}

$cleanup();
$setSwitch('0');

echo "\n{$passed} passed, {$failed} failed\n";
exit($failed > 0 ? 1 : 0);
