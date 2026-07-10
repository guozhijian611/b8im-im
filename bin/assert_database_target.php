<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Support\RuntimeEnvironment;

require dirname(__DIR__) . '/im-business/vendor/autoload.php';

$businessRoot = dirname(__DIR__) . '/im-business';
if (is_file($businessRoot . '/.env')) {
    Dotenv\Dotenv::createImmutable($businessRoot)->safeLoad();
}

$expected = RuntimeEnvironment::value('IM_EXPECT_DATABASE', '') ?? '';
if ($expected === '') {
    throw new RuntimeException('IM_EXPECT_DATABASE must be set for database target verification');
}

$config = Config::fromEnv();
$businessDatabase = (string) (ImRepository::connect($config)->fetchOne('SELECT DATABASE() AS database_name')['database_name'] ?? '');
if ($config->dbName !== $expected || $businessDatabase !== $expected) {
    throw new RuntimeException(sprintf(
        'Business PDO database mismatch: config=%s selected=%s expected=%s',
        $config->dbName,
        $businessDatabase,
        $expected,
    ));
}

/** @var array<string, mixed> $phinxConfig */
$phinxConfig = require dirname(__DIR__) . '/phinx.php';
$environmentName = (string) ($phinxConfig['environments']['default_environment'] ?? 'development');
$environment = $phinxConfig['environments'][$environmentName] ?? null;
if (!is_array($environment)) {
    throw new RuntimeException('Phinx default environment configuration is missing');
}

$dsn = sprintf(
    'mysql:host=%s;port=%d;dbname=%s;charset=%s',
    (string) $environment['host'],
    (int) $environment['port'],
    (string) $environment['name'],
    (string) ($environment['charset'] ?? 'utf8mb4'),
);
$phinxPdo = new PDO($dsn, (string) $environment['user'], (string) $environment['pass'], [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);
$phinxDatabase = (string) $phinxPdo->query('SELECT DATABASE()')->fetchColumn();
if ((string) $environment['name'] !== $expected || $phinxDatabase !== $expected) {
    throw new RuntimeException(sprintf(
        'Phinx PDO database mismatch: config=%s selected=%s expected=%s',
        (string) $environment['name'],
        $phinxDatabase,
        $expected,
    ));
}

fwrite(STDOUT, sprintf("[PASS] Business PDO SELECT DATABASE() = %s\n", $businessDatabase));
fwrite(STDOUT, sprintf("[PASS] Phinx PDO SELECT DATABASE() = %s\n", $phinxDatabase));
