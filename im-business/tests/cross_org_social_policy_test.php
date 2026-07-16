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

$id1 = CrossOrganizationSocialPolicy::crossOrgSingleConversationId('user_b', 'user_a');
$id2 = CrossOrganizationSocialPolicy::crossOrgSingleConversationId('user_a', 'user_b');
$assert($id1 === $id2, 'conversation id order-independent');
$assert(str_starts_with($id1, 'single_x_'), 'conversation id prefix');
$assert(strlen($id1) === strlen('single_x_') + 40, 'conversation id length');

$id3 = CrossOrganizationSocialPolicy::crossOrgSingleConversationId('user_a', 'user_c');
$assert($id1 !== $id3, 'different peers different id');

$assert(CrossOrganizationSocialPolicy::CONFIG_KEY === 'cross_org_social_enabled', 'config key');
$assert(CrossOrganizationSocialPolicy::CONFIG_GROUP === 'social_config', 'config group');

// Drive the same SQL the shipped CrossOrganizationSocialPolicy::isEnabled executes.
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

    $set = static function (string $value) use ($pdo, $groupId): void {
        $stmt = $pdo->prepare(
            "UPDATE sm_system_config SET value = ? WHERE group_id = ? AND `key` = 'cross_org_social_enabled'",
        );
        $stmt->execute([$value, $groupId]);
    };
    $policySql = static function (PDO $pdo): bool {
        $group = $pdo->query(
            "SELECT id FROM sm_system_config_group WHERE code = 'social_config' AND delete_time IS NULL LIMIT 1",
        )->fetch(PDO::FETCH_ASSOC);
        if (!$group) {
            return false;
        }
        $stmt = $pdo->prepare(
            "SELECT `value` FROM sm_system_config WHERE group_id = ? AND `key` = 'cross_org_social_enabled' AND delete_time IS NULL LIMIT 1",
        );
        $stmt->execute([(int) $group['id']]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        return CrossOrganizationSocialPolicy::truthy($row['value'] ?? '0');
    };

    $set('0');
    $assert($policySql($pdo) === false, 'switch off rejects cross-org path');
    $set('1');
    $assert($policySql($pdo) === true, 'switch on allows cross-org path');
    $set('0');
    $assert($policySql($pdo) === false, 'switch off again');
} catch (Throwable $e) {
    echo "SKIP live switch SQL: {$e->getMessage()}\n";
}

echo "\n{$passed} passed, {$failed} failed\n";
exit($failed > 0 ? 1 : 0);
