<?php

declare(strict_types=1);

use B8im\ImBusiness\Exception\ImException;
use B8im\ImBusiness\Repository\MessageShardRepositoryInterface;
use B8im\ImBusiness\Service\MessageShardRouter;

require dirname(__DIR__) . '/vendor/autoload.php';

final class MessageShardRouterTestRepository implements MessageShardRepositoryInterface
{
    /** @param array<string, true> $tables */
    public function __construct(
        public array $tables,
        public int $bucketCount = 1,
    ) {
    }

    public function fetchOne(string $sql, array $params = []): ?array
    {
        if (str_contains($sql, 'information_schema.TABLES')) {
            $table = (string) ($params[0] ?? '');

            return isset($this->tables[$table]) ? ['present' => 1] : null;
        }
        if (str_contains($sql, 'FROM im_runtime_config')) {
            return ['config_value' => (string) $this->bucketCount];
        }

        throw new RuntimeException('unexpected fetchOne query');
    }

    public function fetchAll(string $sql, array $params = []): array
    {
        throw new RuntimeException('unexpected fetchAll query');
    }
}

/** @return array<string, true> */
function completeRuntimeTables(): array
{
    $tables = [
        'im_runtime_config',
        'im_user',
        'im_user_profile',
        'im_user_privacy_setting',
        'im_user_security_policy',
        'im_friend_relation',
        'im_friend_request',
        'im_user_device',
        'im_user_login_audit',
        'im_web_access_session',
        'im_auth_session',
        'im_upload_asset',
        'im_conversation',
        'im_cross_organization_conversation',
        'im_group_profile',
        'im_conversation_member',
        'im_message_group',
        'im_conversation_membership_period',
        'im_organization_message_sequence',
        'im_message_index',
        'im_message_receipt',
        'im_message_user_delete',
        'im_message_change',
        'im_message_outbox',
        'sm_tenant_im_policy',
        'im_message',
        sprintf('im_message_0000_%s', date('Ym')),
        sprintf('im_message_0000_%s', date('Ym', strtotime('first day of next month'))),
    ];

    return array_fill_keys($tables, true);
}

$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    ++$assertions;
};

$repository = new MessageShardRouterTestRepository(completeRuntimeTables());
(new MessageShardRouter($repository, 1))->preflight();
$assert(true, '完整运行时表被错误拒绝');

foreach (['im_upload_asset', 'im_web_access_session', 'im_cross_organization_conversation', 'sm_tenant_im_policy'] as $missingTable) {
    $tables = completeRuntimeTables();
    unset($tables[$missingTable]);
    try {
        (new MessageShardRouter(new MessageShardRouterTestRepository($tables), 1))->preflight();
        throw new RuntimeException('缺失运行时表未阻止启动: ' . $missingTable);
    } catch (ImException $exception) {
        $assert($exception->errorCode() === 'IM_SCHEMA_NOT_READY', '缺表错误码不明确: ' . $missingTable);
        $assert(
            $exception->getMessage() === 'IM 运行时表未迁移: ' . $missingTable,
            '缺表错误未精确指向表名: ' . $missingTable,
        );
    }
}

fwrite(STDOUT, sprintf("Message shard startup schema preflight: %d assertions passed.\n", $assertions));
