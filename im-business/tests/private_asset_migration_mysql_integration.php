<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

$expectedDatabase = trim((string) (
    $_ENV['IM_EXPECT_DATABASE']
    ?? $_SERVER['IM_EXPECT_DATABASE']
    ?? getenv('IM_EXPECT_DATABASE')
));
if (preg_match('/^nb8im_private_asset_migration_[a-f0-9]{8,32}_test$/', $expectedDatabase) !== 1) {
    throw new RuntimeException(
        'private asset migration test requires an isolated nb8im_private_asset_migration_<random>_test database',
    );
}

$config = Config::fromEnv();
$pdo = new PDO(
    sprintf(
        'mysql:host=%s;port=%d;dbname=%s;charset=%s',
        $config->dbHost,
        $config->dbPort,
        $config->dbName,
        $config->dbCharset,
    ),
    $config->dbUser,
    $config->dbPassword,
    [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ],
);
$selectedDatabase = (string) $pdo->query('SELECT DATABASE()')->fetchColumn();
if ($config->dbName !== $expectedDatabase || $selectedDatabase !== $expectedDatabase) {
    throw new RuntimeException(sprintf(
        'private asset migration database mismatch: config=%s selected=%s expected=%s',
        $config->dbName,
        $selectedDatabase,
        $expectedDatabase,
    ));
}

$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    ++$assertions;
};
$hasMigration = static function (string $version) use ($pdo): bool {
    $statement = $pdo->prepare('SELECT 1 FROM im_phinxlog WHERE version = ? LIMIT 1');
    $statement->execute([$version]);

    return $statement->fetchColumn() !== false;
};

$assert($hasMigration('20260710030000'), 'base IM migration is not installed in the isolated database');
$assert(!$hasMigration('20260710050000'), 'private asset migration already ran before the test seed was inserted');

$messageTables = $pdo->query(
    "SELECT TABLE_NAME AS table_name FROM information_schema.TABLES "
    . "WHERE TABLE_SCHEMA = DATABASE() "
    . "AND (TABLE_NAME = 'im_message' OR TABLE_NAME REGEXP '^im_message_[0-9]{4}_[0-9]{6}$') "
    . 'ORDER BY TABLE_NAME ASC',
)->fetchAll();
$messageTables = array_values(array_map(
    static fn (array $row): string => (string) $row['table_name'],
    $messageTables,
));
$assert($messageTables !== [], 'isolated database has no message tables');

$oldUrls = [
    'https://private.example.invalid/url-only.png?signature=old',
    'https://private.example.invalid/forward-image.png?signature=old',
    'https://private.example.invalid/forward-file.zip?signature=old',
    'https://private.example.invalid/deep-preview.mp4?signature=old',
    'https://private.example.invalid/outbox.png?signature=old',
    'https://private.example.invalid/avatar.png?signature=old',
    'https://private.example.invalid/preview.png?signature=old',
];
$safeText = 'ordinary text keeps https://docs.example.invalid/help unchanged';
$validFileId = sha1('private-asset-migration-valid-file');
$content = [
    'url' => $oldUrls[0],
    'name' => 'url-only.png',
    'text' => $safeText,
    'sender_user' => [
        'avatar_url' => $oldUrls[5],
    ],
    'forward_bundle' => [
        'forward_mode' => 'merged',
        'forward_items' => [
            [
                'type' => 'image',
                'file_id' => $validFileId,
                'url' => $oldUrls[1],
            ],
            [
                'type' => 'file',
                'url' => $oldUrls[2],
                'metadata' => [
                    'preview' => [
                        'url' => $oldUrls[3],
                        'preview_url' => $oldUrls[6],
                    ],
                ],
            ],
        ],
    ],
];

$insertMessage = static function (string $table, int $index) use ($pdo, $content): void {
    if ($table !== 'im_message' && preg_match('/^im_message_[0-9]{4}_[0-9]{6}$/', $table) !== 1) {
        throw new RuntimeException('unexpected message table in migration test');
    }
    $statement = $pdo->prepare(sprintf(
        'INSERT INTO `%s`
            (organization, conversation_id, conversation_type, message_id, message_seq,
             client_msg_id, sender_id, message_type, content, status, create_time, update_time)
         VALUES (1, ?, 1, ?, ?, ?, ?, 2, ?, 1, ?, ?)',
        $table,
    ));
    $now = date('Y-m-d H:i:s');
    $statement->execute([
        'private-asset-migration-' . $index,
        sha1('private-asset-message-' . $table),
        $index + 1,
        'private-asset-client-' . $index,
        'private-asset-sender',
        json_encode($content, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
        $now,
        $now,
    ]);
};
foreach ($messageTables as $index => $table) {
    $insertMessage($table, $index);
}

$now = date('Y-m-d H:i:s');
$pdo->prepare(
    'INSERT INTO im_upload_asset
        (organization, file_id, user_id, kind, name, url, storage_path, size_byte,
         mime_type, extension, status, create_time, update_time)
     VALUES (1, ?, ?, "image", ?, ?, ?, 128, "image/png", "png", 1, ?, ?)',
)->execute([
    $validFileId,
    'private-asset-sender',
    'url-only.png',
    $oldUrls[0],
    'organizations/1/im/url-only.png',
    $now,
    $now,
]);

$validAvatarFileId = sha1('private-asset-valid-avatar');
$pdo->prepare(
    'INSERT INTO im_user
        (organization, user_id, account, password_hash, nickname, avatar, status, create_time, update_time)
     VALUES
        (1, "private-avatar-invalid", "private-avatar-invalid", "not-used", "invalid", ?, 1, ?, ?),
        (1, "private-avatar-valid", "private-avatar-valid", "not-used", "valid", ?, 1, ?, ?)',
)->execute([$oldUrls[0], $now, $now, $validAvatarFileId, $now, $now]);
$pdo->prepare(
    'INSERT INTO im_conversation
        (organization, conversation_id, conversation_type, title, avatar, status, create_time, update_time)
     VALUES
        (1, "private-avatar-conversation-invalid", 2, "invalid", ?, 1, ?, ?),
        (1, "private-avatar-conversation-valid", 2, "valid", ?, 1, ?, ?)',
)->execute([$oldUrls[1], $now, $now, $validAvatarFileId, $now, $now]);

$outboxPayload = [
    'event_type' => 'message.created',
    'organization' => 1,
    'message_id' => sha1('private-asset-outbox-message'),
    'message' => [
        'content' => [
            'url' => $oldUrls[4],
            'text' => $safeText,
            'sender_user' => [
                'avatar_url' => $oldUrls[5],
            ],
            'preview_url' => $oldUrls[6],
            'forward_items' => $content['forward_bundle']['forward_items'],
        ],
    ],
];
$pdo->prepare(
    'INSERT INTO im_message_outbox
        (organization, event_type, routing_key, message_id, change_seq, conversation_id,
         conversation_type, payload_json, status, retry_count, next_retry_at, create_time, update_time)
     VALUES (1, "message.created", "message.created", ?, 0, "private-asset-outbox", 1, ?, 4, 1, ?, ?, ?)',
)->execute([
    $outboxPayload['message_id'],
    json_encode($outboxPayload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR),
    $now,
    $now,
    $now,
]);

$invalidOutboxMessageId = sha1('private-asset-invalid-outbox-message');
$pdo->prepare(
    'INSERT INTO im_message_outbox
        (organization, event_type, routing_key, message_id, change_seq, conversation_id,
         conversation_type, payload_json, status, retry_count, next_retry_at, create_time, update_time)
     VALUES (1, "message.created", "message.created", ?, 0, "private-asset-invalid-outbox", 1, ?, 4, 1, ?, ?, ?)',
)->execute([
    $invalidOutboxMessageId,
    '{"message":{"content":{"url":"https://private.example.invalid/broken"}',
    $now,
    $now,
    $now,
]);

$command = sprintf(
    '%s %s -c %s migrate -e development -t 20260710050000',
    escapeshellarg(PHP_BINARY),
    escapeshellarg(dirname(__DIR__) . '/vendor/bin/phinx'),
    escapeshellarg(dirname(__DIR__, 2) . '/phinx.php'),
);
$runMigration = static function () use ($command): array {
    $output = [];
    exec($command . ' 2>&1', $output, $exitCode);

    return [$exitCode, implode("\n", $output)];
};
[$failedExitCode, $failedOutput] = $runMigration();
$assert($failedExitCode !== 0, 'invalid outbox JSON did not fail the private asset migration');
$assert(
    str_contains($failedOutput, 'table=im_message_outbox')
    && str_contains($failedOutput, 'column=payload_json'),
    'invalid JSON migration failure did not identify its table and column',
);
$assert(str_contains($failedOutput, 'id='), 'invalid JSON migration failure did not identify its row id');
$assert(!$hasMigration('20260710050000'), 'failed private asset migration was recorded as successful');
$pdo->prepare('DELETE FROM im_message_outbox WHERE message_id = ?')->execute([$invalidOutboxMessageId]);

[$exitCode, $migrationOutput] = $runMigration();
if ($exitCode !== 0) {
    throw new RuntimeException("private asset normalization migration failed\n" . $migrationOutput);
}
fwrite(STDOUT, $migrationOutput . "\n");
$assert($hasMigration('20260710050000'), 'private asset migration version was not recorded');

$assertUrlFieldsCleared = static function (mixed $value, string $path = '$') use (&$assertUrlFieldsCleared, $assert): void {
    if (!is_array($value)) {
        return;
    }
    foreach ($value as $key => $item) {
        $childPath = $path . '.' . (string) $key;
        $normalizedKey = is_string($key) ? strtolower($key) : '';
        if ($normalizedKey === 'url' || str_ends_with($normalizedKey, '_url')) {
            $assert($item === '', 'URL field was not cleared at ' . $childPath);
        }
        $assertUrlFieldsCleared($item, $childPath);
    }
};
$assertOldUrlsAbsent = static function (string $json, string $scope) use ($assert, $oldUrls): void {
    foreach ($oldUrls as $oldUrl) {
        $assert(!str_contains($json, $oldUrl), $scope . ' still contains old private URL');
    }
};

foreach ($messageTables as $table) {
    $json = (string) $pdo->query(sprintf('SELECT content FROM `%s` LIMIT 1', $table))->fetchColumn();
    $decoded = json_decode($json, true, flags: JSON_THROW_ON_ERROR);
    $assertUrlFieldsCleared($decoded, '$.' . $table);
    $assertOldUrlsAbsent($json, $table);
    $assert(
        ($decoded['forward_bundle']['forward_items'][0]['file_id'] ?? null) === $validFileId,
        $table . ' lost the nested file_id while clearing URLs',
    );
    $assert(($decoded['text'] ?? null) === $safeText, $table . ' changed an ordinary text URL');
}

$outboxJson = (string) $pdo->query(
    'SELECT payload_json FROM im_message_outbox WHERE conversation_id = "private-asset-outbox" LIMIT 1',
)->fetchColumn();
$outboxDecoded = json_decode($outboxJson, true, flags: JSON_THROW_ON_ERROR);
$assertUrlFieldsCleared($outboxDecoded, '$.outbox');
$assertOldUrlsAbsent($outboxJson, 'outbox payload');
$assert(
    ($outboxDecoded['message']['content']['forward_items'][0]['file_id'] ?? null) === $validFileId,
    'outbox payload lost the nested file_id while clearing URLs',
);
$assert(
    ($outboxDecoded['message']['content']['text'] ?? null) === $safeText,
    'outbox payload changed an ordinary text URL',
);

$assert(
    $pdo->query('SELECT url FROM im_upload_asset WHERE file_id = ' . $pdo->quote($validFileId))->fetchColumn() === '',
    'im_upload_asset.url was not cleared',
);
$userAvatars = $pdo->query(
    'SELECT user_id, avatar FROM im_user WHERE user_id LIKE "private-avatar-%" ORDER BY user_id',
)->fetchAll(PDO::FETCH_KEY_PAIR);
$assert(
    array_key_exists('private-avatar-invalid', $userAvatars)
    && $userAvatars['private-avatar-invalid'] === null,
    'invalid user avatar reference was not cleared',
);
$assert(($userAvatars['private-avatar-valid'] ?? null) === $validAvatarFileId, 'valid user avatar file_id was changed');
$conversationAvatars = $pdo->query(
    'SELECT conversation_id, avatar FROM im_conversation
      WHERE conversation_id LIKE "private-avatar-conversation-%" ORDER BY conversation_id',
)->fetchAll(PDO::FETCH_KEY_PAIR);
$assert(
    array_key_exists('private-avatar-conversation-invalid', $conversationAvatars)
    && $conversationAvatars['private-avatar-conversation-invalid'] === null,
    'invalid conversation avatar reference was not cleared',
);
$assert(
    ($conversationAvatars['private-avatar-conversation-valid'] ?? null) === $validAvatarFileId,
    'valid conversation avatar file_id was changed',
);

fwrite(STDOUT, sprintf(
    "Private asset migration integration: %d assertions passed across %d message tables.\n",
    $assertions,
    count($messageTables),
));
