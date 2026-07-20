<?php

declare(strict_types=1);

/**
 * Unit tests for IM cross-org social policy + conversation id.
 * Run: php tests/cross_org_social_policy_test.php
 */

require_once dirname(__DIR__) . '/vendor/autoload.php';

use B8im\ImBusiness\Service\CrossOrganizationSocialPolicy;

$failed = 0;
$passed = 0;
$assert = static function (bool $cond, string $msg) use (&$failed, &$passed): void {
    if ($cond) {
        $passed++;
        echo "PASS {$msg}\n";
        return;
    }
    $failed++;
    echo "FAIL {$msg}\n";
};

$assert(CrossOrganizationSocialPolicy::truthy('1'), 'truthy on');
$assert(!CrossOrganizationSocialPolicy::truthy('0'), 'truthy off');

$id1 = CrossOrganizationSocialPolicy::singleConversationId(2, 'u2', 1, 'u1');
$id2 = CrossOrganizationSocialPolicy::singleConversationId(1, 'u1', 2, 'u2');
$assert($id1 === $id2, 'conversation id order-independent');
$assert($id1 === 'single_2118193dd11825a86050c3575d1f9aa52849d5e3', 'cross organization fixed vector');
$assert(strlen($id1) === strlen('single_') + 40, 'conversation id length');

$id3 = CrossOrganizationSocialPolicy::singleConversationId(1, 'u1', 2, 'u3');
$assert($id1 !== $id3, 'different peers different id');
$assert(
    CrossOrganizationSocialPolicy::singleConversationId(1, 'same', 2, 'same')
        === 'single_3d9ff05c919aa120bba0770a87bf422ba31e2e8b',
    'same user_id across organizations stays distinct',
);
$assert(
    CrossOrganizationSocialPolicy::singleConversationId(7, 'alice', 7, 'bob')
        === 'single_06077c21d48263b3d726c0c3df9daadb63e2a9b7',
    'same organization fixed vector',
);
$invalidIdentityRejected = false;
try {
    CrossOrganizationSocialPolicy::singleConversationId(0, 'alice', 7, 'bob');
} catch (InvalidArgumentException) {
    $invalidIdentityRejected = true;
}
$assert($invalidIdentityRejected, 'incomplete identity fails closed');

$assert(CrossOrganizationSocialPolicy::CONFIG_KEY === 'cross_org_social_enabled', 'config key');
$assert(
    CrossOrganizationSocialPolicy::SNAPSHOT_CONFIG_KEY === 'cross_org_access_snapshot_id',
    'access snapshot config key',
);
$assert(CrossOrganizationSocialPolicy::CONFIG_GROUP === 'social_config', 'config group');

// Optional database probe; default unit tests never mutate a developer database.
if (getenv('IM_CROSS_ORG_POLICY_DB_TEST') === '1') {
try {
    $pdo = new PDO('mysql:host=127.0.0.1;port=3306;dbname=nb8im;charset=utf8mb4', 'root', 'root', [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    ]);
    $pdo->exec("INSERT INTO sm_system_config_group (name, code, remark, create_time, update_time)
        SELECT '社交边界配置', 'social_config', 'test', NOW(), NOW()
        WHERE NOT EXISTS (SELECT 1 FROM sm_system_config_group WHERE code = 'social_config' AND delete_time IS NULL)");
    $groupId = (int) $pdo->query("SELECT id FROM sm_system_config_group WHERE code = 'social_config' AND delete_time IS NULL LIMIT 1")->fetchColumn();
    $exists = $pdo->query("SELECT id FROM sm_system_config WHERE group_id = {$groupId} AND `key` = 'cross_org_social_enabled' LIMIT 1")->fetchColumn();
    if (!$exists) {
        $pdo->exec("INSERT INTO sm_system_config (group_id, `key`, `value`, name, input_type, sort, create_time, update_time)
            VALUES ({$groupId}, 'cross_org_social_enabled', '0', '允许跨租户好友与单聊', 'switch', 100, NOW(), NOW())");
    }
    $snapshotExists = $pdo->query(
        "SELECT id FROM sm_system_config WHERE group_id = {$groupId} AND `key` = 'cross_org_access_snapshot_id' LIMIT 1",
    )->fetchColumn();
    if (!$snapshotExists) {
        $pdo->exec("INSERT INTO sm_system_config (group_id, `key`, `value`, name, input_type, sort, create_time, update_time)
            VALUES ({$groupId}, 'cross_org_access_snapshot_id', '1', '跨机构社交访问快照', 'input', 101, NOW(), NOW())");
    }

    $set = static function (string $value, string $snapshotId) use ($pdo, $groupId): void {
        $stmt = $pdo->prepare(
            "UPDATE sm_system_config SET value = CASE `key`
                 WHEN 'cross_org_social_enabled' THEN ?
                 WHEN 'cross_org_access_snapshot_id' THEN ?
               END
             WHERE group_id = ? AND `key` IN ('cross_org_social_enabled', 'cross_org_access_snapshot_id')",
        );
        $stmt->execute([$value, $snapshotId, $groupId]);
    };
    $policySql = static function (PDO $pdo): bool {
        $group = $pdo->query(
            "SELECT id FROM sm_system_config_group WHERE code = 'social_config' AND delete_time IS NULL LIMIT 1",
        )->fetch(PDO::FETCH_ASSOC);
        if (!$group) {
            return false;
        }
        $stmt = $pdo->prepare("SELECT `key`, `value` FROM sm_system_config
            WHERE group_id = ? AND `key` IN ('cross_org_social_enabled', 'cross_org_access_snapshot_id')
              AND delete_time IS NULL");
        $stmt->execute([(int) $group['id']]);
        $values = [];
        foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $values[(string) $row['key']] = (string) $row['value'];
        }

        return CrossOrganizationSocialPolicy::truthy($values['cross_org_social_enabled'] ?? '0')
            && preg_match('/^[1-9][0-9]*$/', $values['cross_org_access_snapshot_id'] ?? '') === 1;
    };

    $set('0', '1');
    $assert($policySql($pdo) === false, 'switch off rejects cross-org path');
    $set('1', '2');
    $assert($policySql($pdo) === true, 'switch on allows cross-org path');
    $set('1', '0');
    $assert($policySql($pdo) === false, 'snapshot zero fails closed');
    $set('0', '3');
    $assert($policySql($pdo) === false, 'switch off again');
} catch (Throwable $e) {
    echo "SKIP live switch SQL: {$e->getMessage()}\n";
}
}

echo "\n{$passed} passed, {$failed} failed\n";
exit($failed > 0 ? 1 : 0);
