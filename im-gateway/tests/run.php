<?php

declare(strict_types=1);

use B8im\ImGateway\Security\OriginPolicy;

require dirname(__DIR__) . '/vendor/autoload.php';

$policy = OriginPolicy::fromCsv('https://chat.example.com, http://localhost:5173,https://chat.example.com:443');
$policy->assertAllowed('https://CHAT.example.com');
$policy->assertAllowed('http://localhost:5173');
$policy->assertAllowed(null); // native App/Desktop; token auth remains mandatory

$rejected = 0;
foreach (['https://evil.example.com', 'null', 'https://chat.example.com/path', 'javascript://chat.example.com'] as $origin) {
    try {
        $policy->assertAllowed($origin);
    } catch (InvalidArgumentException) {
        ++$rejected;
    }
}

if ($rejected !== 4) {
    fwrite(STDERR, "[FAIL] Origin policy accepted an untrusted browser origin.\n");
    exit(1);
}

fwrite(STDOUT, "[PASS] trusted browser origins are exact and native no-Origin clients remain token-gated\n");
