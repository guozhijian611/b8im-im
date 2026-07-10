<?php

declare(strict_types=1);

use B8im\ImShared\Support\RuntimeEnvironment;

$businessRoot = __DIR__ . '/im-business';
$businessAutoload = $businessRoot . '/vendor/autoload.php';
if (!is_file($businessAutoload)) {
    throw new RuntimeException(
        'IM migration dependencies are missing; run composer install in im-business before loading phinx.php.',
    );
}
if (!class_exists(RuntimeEnvironment::class, false)) {
    require_once $businessAutoload;
}
if (!class_exists(RuntimeEnvironment::class)) {
    throw new RuntimeException('IM shared RuntimeEnvironment is unavailable after loading im-business dependencies.');
}
if (is_file($businessRoot . '/.env') && class_exists(Dotenv\Dotenv::class)) {
    Dotenv\Dotenv::createImmutable($businessRoot)->safeLoad();
}

$env = static function (string $key, string $default): string {
    return RuntimeEnvironment::value($key, $default) ?? $default;
};

RuntimeEnvironment::configureTimezone($env('IM_TIMEZONE', RuntimeEnvironment::DEFAULT_TIMEZONE));

return [
    'paths' => [
        'migrations' => __DIR__ . '/database/migrations',
    ],
    'environments' => [
        'default_migration_table' => 'im_phinxlog',
        'default_environment' => 'development',
        'development' => [
            'adapter' => 'mysql',
            'host' => $env('DB_HOST', '127.0.0.1'),
            'port' => (int) $env('DB_PORT', '3306'),
            'name' => $env('DB_NAME', 'nb8im'),
            'user' => $env('DB_USER', 'root'),
            'pass' => $env('DB_PASSWORD', ''),
            'charset' => $env('DB_CHARSET', 'utf8mb4'),
        ],
    ],
];
