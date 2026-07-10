<?php

declare(strict_types=1);

$imRoot = dirname(__DIR__, 2);
if (class_exists(B8im\ImShared\Support\RuntimeEnvironment::class, false)) {
    throw new RuntimeException('standalone bootstrap test unexpectedly started with IM autoload state');
}

$originalCwd = getcwd();
if (!chdir(sys_get_temp_dir())) {
    throw new RuntimeException('failed to switch standalone bootstrap test cwd');
}

try {
    $config = require $imRoot . '/phinx.php';
} finally {
    if (is_string($originalCwd)) {
        chdir($originalCwd);
    }
}

if (!class_exists(B8im\ImShared\Support\RuntimeEnvironment::class, false)) {
    throw new RuntimeException('phinx.php did not load the IM shared runtime dependency');
}
if (($config['paths']['migrations'] ?? null) !== $imRoot . '/database/migrations') {
    throw new RuntimeException('phinx.php migration path depends on the caller cwd');
}
if (($config['environments']['development']['adapter'] ?? null) !== 'mysql') {
    throw new RuntimeException('phinx.php did not return the canonical migration configuration');
}

fwrite(STDOUT, "Standalone IM Phinx config bootstrap passed.\n");
