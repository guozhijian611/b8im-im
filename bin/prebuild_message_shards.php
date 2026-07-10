<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;

$businessRoot = dirname(__DIR__) . '/im-business';
require $businessRoot . '/vendor/autoload.php';

if (is_file($businessRoot . '/.env')) {
    Dotenv\Dotenv::createImmutable($businessRoot)->safeLoad();
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
    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
);

$lockName = 'b8im:im:prebuild-message-shards';
$statement = $pdo->prepare('SELECT GET_LOCK(?, 30)');
$statement->execute([$lockName]);
if ((int) $statement->fetchColumn() !== 1) {
    throw new RuntimeException('Unable to acquire the IM shard prebuild lock.');
}

try {
    $statement = $pdo->prepare(
        'SELECT config_value FROM im_runtime_config WHERE config_key = ? LIMIT 1',
    );
    $statement->execute(['message_shard_buckets']);
    $migratedBucketCount = $statement->fetchColumn();
    if ($migratedBucketCount === false || (int) $migratedBucketCount !== $config->messageShardBuckets) {
        throw new RuntimeException(
            'IM_MESSAGE_SHARD_BUCKETS differs from the immutable migrated runtime configuration.',
        );
    }

    $baseTable = $pdo->query(
        "SELECT 1 FROM information_schema.TABLES "
        . "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'im_message' LIMIT 1",
    )->fetchColumn();
    if ($baseTable === false) {
        throw new RuntimeException('im_message template table is missing; run the IM migration first.');
    }

    $months = [date('Ym'), date('Ym', strtotime('first day of next month'))];
    $created = 0;
    foreach ($months as $month) {
        for ($bucket = 0; $bucket < $config->messageShardBuckets; $bucket++) {
            $table = sprintf('im_message_%04d_%s', $bucket, $month);
            $pdo->exec(sprintf('CREATE TABLE IF NOT EXISTS `%s` LIKE `im_message`', $table));
            ++$created;
        }
    }

    fwrite(STDOUT, sprintf("Verified %d message shard tables.\n", $created));
} finally {
    $statement = $pdo->prepare('SELECT RELEASE_LOCK(?)');
    $statement->execute([$lockName]);
}
