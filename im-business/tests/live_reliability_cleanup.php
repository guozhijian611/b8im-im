<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Repository\ImRepository;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

$env = static fn (string $name): string => trim((string) (
    $_ENV[$name] ?? $_SERVER[$name] ?? getenv($name) ?: ''
));
$fail = static function (string $message): never {
    throw new RuntimeException($message);
};
if ($env('QA_ALLOW_CLEANUP') !== '1') {
    $fail('set QA_ALLOW_CLEANUP=1 to confirm exact-manifest QA cleanup');
}
$manifestPath = $env('QA_MANIFEST');
if ($manifestPath === '' || !is_file($manifestPath)) {
    $fail('QA_MANIFEST is required');
}
$manifest = json_decode((string) file_get_contents($manifestPath), true, flags: JSON_THROW_ON_ERROR);
$runId = (string) ($manifest['qa_run_id'] ?? '');
$organization = (int) ($manifest['organization'] ?? 0);
$messages = $manifest['messages'] ?? null;
$accounts = $manifest['accounts'] ?? null;
if (preg_match('/^[a-z0-9][a-z0-9-]{7,39}$/', $runId) !== 1
    || $organization <= 0
    || !is_array($messages)
    || $messages === []
    || !is_array($accounts)) {
    $fail('QA manifest identity is invalid');
}

$messageIds = [];
$conversationIds = [];
foreach ($messages as $message) {
    $messageId = (string) ($message['message_id'] ?? '');
    $clientMessageId = (string) ($message['client_msg_id'] ?? '');
    $conversationId = (string) ($message['conversation_id'] ?? '');
    if (preg_match('/^[0-9]{20}[a-f0-9]{8}$/', $messageId) !== 1
        || !str_starts_with($clientMessageId, 'qa-im-' . $runId . '-')
        || $conversationId === '') {
        $fail('cleanup manifest contains a non-QA message');
    }
    $messageIds[] = $messageId;
    $conversationIds[] = $conversationId;
}
$messageIds = array_values(array_unique($messageIds));
$conversationIds = array_values(array_unique($conversationIds));
$qaUsers = array_values(array_unique(array_map(
    static fn (array $account): string => (string) ($account['user_id'] ?? ''),
    array_filter($accounts, static fn (mixed $account): bool => is_array($account)
        && (int) ($account['organization'] ?? 0) === $organization),
)));
if ($qaUsers === [] || in_array('', $qaUsers, true)) {
    $fail('cleanup manifest has no valid same-organization QA users');
}

$config = Config::fromEnv();
$repository = ImRepository::connect($config);
$selectedDatabase = (string) ($repository->fetchOne('SELECT DATABASE() AS name')['name'] ?? '');
if ($selectedDatabase !== $config->dbName || $selectedDatabase === '') {
    $fail('cleanup database selection is inconsistent');
}
$expectedDatabase = $env('IM_EXPECT_DATABASE');
if ($expectedDatabase !== '' && $selectedDatabase !== $expectedDatabase) {
    $fail(sprintf('cleanup database mismatch: selected=%s expected=%s', $selectedDatabase, $expectedDatabase));
}

$messagePlaceholders = implode(',', array_fill(0, count($messageIds), '?'));
$indexRows = $repository->fetchAll(
    'SELECT message_id, client_msg_id, conversation_id, shard_table FROM im_message_index
      WHERE organization = ? AND message_id IN (' . $messagePlaceholders . ') FOR UPDATE',
    [$organization, ...$messageIds],
);
if (count($indexRows) !== count($messageIds)) {
    $fail('cleanup refuses an incomplete or already partially removed manifest');
}
foreach ($indexRows as $row) {
    if (!str_starts_with((string) $row['client_msg_id'], 'qa-im-' . $runId . '-')
        || preg_match('/^im_message_[0-9]{4}_[0-9]{6}$/', (string) $row['shard_table']) !== 1) {
        $fail('cleanup database rows do not match the QA marker');
    }
}
foreach ($conversationIds as $conversationId) {
    $otherMessages = $repository->fetchOne(
        'SELECT COUNT(*) AS aggregate FROM im_message_index
          WHERE organization = ? AND conversation_id = ? AND message_id NOT IN (' . $messagePlaceholders . ')',
        [$organization, $conversationId, ...$messageIds],
    );
    if ((int) ($otherMessages['aggregate'] ?? 0) !== 0) {
        $fail('cleanup requires dedicated reset QA conversations; non-manifest messages exist in ' . $conversationId);
    }
    $members = $repository->fetchAll(
        'SELECT user_id FROM im_conversation_member WHERE organization = ? AND conversation_id = ?',
        [$organization, $conversationId],
    );
    $memberIds = array_values(array_unique(array_column($members, 'user_id')));
    if ($memberIds === [] || array_diff($memberIds, $qaUsers) !== []) {
        $fail('cleanup refuses a conversation containing non-QA members: ' . $conversationId);
    }
}

$deleted = $repository->transaction(static function (ImRepository $repository) use (
    $organization,
    $messageIds,
    $messagePlaceholders,
    $indexRows,
    $conversationIds,
): array {
    $counts = [];
    foreach (['im_message_change', 'im_message_user_delete', 'im_message_receipt', 'im_message_outbox'] as $table) {
        $counts[$table] = $repository->execute(
            sprintf('DELETE FROM `%s` WHERE organization = ? AND message_id IN (%s)', $table, $messagePlaceholders),
            [$organization, ...$messageIds],
        );
    }
    foreach ($indexRows as $row) {
        $table = (string) $row['shard_table'];
        $counts[$table] = ($counts[$table] ?? 0) + $repository->execute(
            sprintf('DELETE FROM `%s` WHERE organization = ? AND message_id = ?', $table),
            [$organization, (string) $row['message_id']],
        );
    }
    $counts['im_message_index'] = $repository->execute(
        'DELETE FROM im_message_index WHERE organization = ? AND message_id IN (' . $messagePlaceholders . ')',
        [$organization, ...$messageIds],
    );

    foreach ($conversationIds as $conversationId) {
        $remaining = $repository->fetchOne(
            'SELECT COUNT(*) AS aggregate FROM im_message_index WHERE organization = ? AND conversation_id = ?',
            [$organization, $conversationId],
        );
        if ((int) ($remaining['aggregate'] ?? 0) !== 0) {
            throw new RuntimeException('QA conversation changed concurrently during cleanup: ' . $conversationId);
        }
        $repository->execute(
            'DELETE FROM im_conversation_membership_period WHERE organization = ? AND conversation_id = ?',
            [$organization, $conversationId],
        );
        $repository->execute(
            'DELETE FROM im_conversation_member WHERE organization = ? AND conversation_id = ?',
            [$organization, $conversationId],
        );
        $repository->execute(
            'DELETE FROM im_conversation WHERE organization = ? AND conversation_id = ?',
            [$organization, $conversationId],
        );
    }

    return $counts;
});

$remaining = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate FROM im_message_index WHERE organization = ? AND message_id IN (' . $messagePlaceholders . ')',
    [$organization, ...$messageIds],
);
if ((int) ($remaining['aggregate'] ?? -1) !== 0) {
    $fail('QA cleanup verification failed');
}

fwrite(STDOUT, json_encode([
    'ok' => true,
    'qa_run_id' => $runId,
    'database' => $selectedDatabase,
    'deleted' => $deleted,
    'accounts_preserved' => $qaUsers,
], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR) . PHP_EOL);
