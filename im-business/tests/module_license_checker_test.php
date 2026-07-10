<?php

declare(strict_types=1);

use B8im\ImBusiness\Service\ModuleLicenseChecker;
use B8im\ImBusiness\Service\ModuleLicenseRepositoryInterface;
use B8im\ImShared\Support\Constants;

require dirname(__DIR__) . '/vendor/autoload.php';

final class ModuleLicenseTestRedis
{
    /** @var array<string, string> */
    public array $values = [];

    /** @var list<string> */
    public array $deleted = [];

    public function get(string $key): string|false
    {
        return $this->values[$key] ?? false;
    }

    public function del(string $key): int
    {
        $this->deleted[] = $key;
        $present = isset($this->values[$key]);
        unset($this->values[$key]);

        return $present ? 1 : 0;
    }

    /** @param list<string> $arguments */
    public function eval(string $script, array $arguments, int $numberOfKeys): int
    {
        if ($numberOfKeys !== 1) {
            throw new RuntimeException('unexpected Redis key count');
        }
        [$key, $encoded] = $arguments;
        $this->values[$key] = $encoded;

        return 1;
    }
}

final class ModuleLicenseTestRepository implements ModuleLicenseRepositoryInterface
{
    /** @var array<string, mixed>|null */
    public ?array $row = null;

    public bool $fail = false;

    public int $reads = 0;

    public function fetchOne(string $sql, array $params = []): ?array
    {
        ++$this->reads;
        if ($this->fail) {
            throw new RuntimeException('database unavailable');
        }

        return $this->row;
    }
}

/** @return array<string, mixed> */
function moduleLicenseTestSnapshot(bool $enabled): array
{
    return [
        'enabled' => $enabled,
        'effective_until' => null,
        'version' => 3,
        'module_version' => '1.0.0',
        'module_lock_version' => 4,
        'platforms' => ['im'],
        'capabilities' => ['im' => ['message.send']],
    ];
}

/** @return array<string, mixed> */
function moduleLicenseTestRow(string $moduleStatus, string $licenseStatus): array
{
    return [
        'module_status' => $moduleStatus,
        'license_status' => $licenseStatus,
        'expire_at' => null,
        'license_version' => 5,
        'module_version' => '1.1.0',
        'module_lock_version' => 6,
        'platforms_json' => '["im"]',
        'capabilities_json' => '{"im":["message.send"]}',
    ];
}

$assertions = 0;
$assert = static function (bool $condition, string $message) use (&$assertions): void {
    if (!$condition) {
        throw new RuntimeException($message);
    }
    ++$assertions;
};

$redis = new ModuleLicenseTestRedis();
$repository = new ModuleLicenseTestRepository();
$checker = new ModuleLicenseChecker($redis, $repository, 60);
$key = sprintf(Constants::REDIS_MODULE_LICENSE, 7, 'customer_service');

$redis->values[$key] = json_encode(moduleLicenseTestSnapshot(true), JSON_THROW_ON_ERROR);
$repository->row = moduleLicenseTestRow('ENABLED', 'DISABLED');
$assert(!$checker->isLicensed(7, 'customer_service'), 'stale positive cache extended a revoked license');
$assert($repository->reads === 1, 'positive cache did not revalidate against MySQL');
$rewritten = json_decode($redis->values[$key] ?? '', true);
$assert(is_array($rewritten) && $rewritten['enabled'] === false, 'revoked MySQL snapshot was not written back');

$redis->values[$key] = json_encode(moduleLicenseTestSnapshot(true), JSON_THROW_ON_ERROR);
$repository->fail = true;
$assert(!$checker->isLicensed(7, 'customer_service'), 'database failure allowed a stale positive cache');
$assert(!isset($redis->values[$key]), 'unverifiable positive cache was not removed after database failure');

$repository->fail = false;
$repository->reads = 0;
$redis->values[$key] = json_encode(moduleLicenseTestSnapshot(false), JSON_THROW_ON_ERROR);
$assert(!$checker->isLicensed(7, 'customer_service'), 'cached deny was not enforced');
$assert($repository->reads === 0, 'cached deny unnecessarily queried MySQL');

$repository->row = moduleLicenseTestRow('ENABLED', 'ENABLED');
unset($redis->values[$key]);
$assert($checker->isLicensed(7, 'customer_service'), 'fresh enabled MySQL license was rejected');
$assert(isset($redis->values[$key]), 'fresh MySQL snapshot was not cached');

fwrite(STDOUT, sprintf("Module license fail-closed cache: %d assertions passed.\n", $assertions));
