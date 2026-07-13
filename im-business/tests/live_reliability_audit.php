<?php

declare(strict_types=1);

use B8im\ImBusiness\Config;
use B8im\ImBusiness\Repository\ImRepository;
use B8im\ImShared\Support\Constants;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use B8im\ImShared\Telemetry\TraceContext;

require dirname(__DIR__) . '/vendor/autoload.php';

if (is_file(dirname(__DIR__) . '/.env')) {
    Dotenv\Dotenv::createImmutable(dirname(__DIR__))->safeLoad();
}

$env = static fn (string $name, string $default = ''): string => trim((string) (
    $_ENV[$name] ?? $_SERVER[$name] ?? getenv($name) ?: $default
));
$fail = static function (string $message): never {
    throw new RuntimeException($message);
};

$manifestPath = $env('QA_MANIFEST');
if ($manifestPath === '' || !is_file($manifestPath)) {
    $fail('QA_MANIFEST must point to the JSON emitted by live_phase1_websocket.mjs');
}
$manifest = json_decode((string) file_get_contents($manifestPath), true, flags: JSON_THROW_ON_ERROR);
$runId = (string) ($manifest['qa_run_id'] ?? '');
$organization = (int) ($manifest['organization'] ?? 0);
$messages = $manifest['messages'] ?? null;
if (preg_match('/^[a-z0-9][a-z0-9-]{7,39}$/', $runId) !== 1 || $organization <= 0 || !is_array($messages) || $messages === []) {
    $fail('QA manifest identity or message list is invalid');
}
foreach ($messages as $message) {
    if (!is_array($message)
        || !str_starts_with((string) ($message['client_msg_id'] ?? ''), 'qa-im-' . $runId . '-')
        || preg_match('/^[0-9]{20}[a-f0-9]{8}$/', (string) ($message['message_id'] ?? '')) !== 1) {
        $fail('QA manifest contains a message outside its run marker');
    }
}

$config = Config::fromEnv();
$repository = ImRepository::connect($config);
$selectedDatabase = (string) ($repository->fetchOne('SELECT DATABASE() AS name')['name'] ?? '');
if ($selectedDatabase !== $config->dbName || $selectedDatabase === '') {
    $fail('IM audit database selection is inconsistent');
}

$messageIds = array_values(array_unique(array_map(
    static fn (array $message): string => (string) $message['message_id'],
    $messages,
)));
$placeholders = implode(',', array_fill(0, count($messageIds), '?'));
$indexRows = $repository->fetchAll(
    'SELECT organization, global_seq, message_id, conversation_id, message_seq, sender_id, client_msg_id, shard_table
       FROM im_message_index WHERE organization = ? AND message_id IN (' . $placeholders . ')',
    [$organization, ...$messageIds],
);
if (count($indexRows) !== count($messageIds)) {
    $fail(sprintf('message index mismatch: expected=%d actual=%d', count($messageIds), count($indexRows)));
}

$byMessageId = [];
foreach ($indexRows as $row) {
    $messageId = (string) $row['message_id'];
    if (isset($byMessageId[$messageId])) {
        $fail('duplicate message index row: ' . $messageId);
    }
    if (preg_match('/^im_message_[0-9]{4}_[0-9]{6}$/', (string) $row['shard_table']) !== 1) {
        $fail('unsafe or invalid shard table recorded for ' . $messageId);
    }
    $stored = $repository->fetchOne(
        sprintf(
            'SELECT organization, message_id, conversation_id, message_seq, sender_id, client_msg_id FROM `%s` WHERE organization = ? AND message_id = ? LIMIT 1',
            $row['shard_table'],
        ),
        [$organization, $messageId],
    );
    if ($stored === null
        || (string) $stored['client_msg_id'] !== (string) $row['client_msg_id']
        || (string) $stored['conversation_id'] !== (string) $row['conversation_id']
        || (int) $stored['message_seq'] !== (int) $row['message_seq']) {
        $fail('message shard/index inconsistency: ' . $messageId);
    }
    $byMessageId[$messageId] = $row;
}

$clientIds = array_column($indexRows, 'client_msg_id');
if (count(array_unique($clientIds)) !== count($clientIds)) {
    $fail('client_msg_id idempotency index contains duplicates');
}
$offlineRows = array_values(array_filter(
    $messages,
    static fn (array $message): bool => str_starts_with((string) ($message['scenario'] ?? ''), 'offline_recovery_'),
));
usort($offlineRows, static fn (array $a, array $b): int => (int) $a['message_seq'] <=> (int) $b['message_seq']);
if (count($offlineRows) !== 2
    || (int) $offlineRows[1]['message_seq'] !== (int) $offlineRows[0]['message_seq'] + 1
    || (int) $offlineRows[1]['global_seq'] <= (int) $offlineRows[0]['global_seq']) {
    $fail('offline recovery sequence contract is incomplete');
}

$deadline = microtime(true) + max(1, (int) $env('QA_OUTBOX_WAIT_SECONDS', '20'));
do {
    $outboxRows = $repository->fetchAll(
        'SELECT message_id, status, retry_count, worker_id, claim_token, published_at, last_error, traceparent, tracestate
           FROM im_message_outbox WHERE organization = ? AND event_type = ? AND change_seq = 0
            AND message_id IN (' . $placeholders . ')',
        [$organization, Constants::MQ_ROUTING_MESSAGE_CREATED, ...$messageIds],
    );
    $published = count($outboxRows) === count($messageIds)
        && array_reduce($outboxRows, static fn (bool $ok, array $row): bool => $ok
            && (int) $row['status'] === 3
            && $row['published_at'] !== null
            && $row['worker_id'] === null
            && $row['claim_token'] === null, true);
    if ($published) {
        break;
    }
    usleep(250_000);
} while (microtime(true) < $deadline);
if (!$published) {
    $fail('outbox did not reach published/unclaimed state before the deadline');
}
foreach ($outboxRows as $row) {
    if ((int) $row['retry_count'] > (int) $env('QA_OUTBOX_MAX_RETRY_COUNT', '0') || trim((string) $row['last_error']) !== '') {
        $fail('outbox retry/error detected for ' . $row['message_id']);
    }
    try {
        $trace = TraceContext::fromCarrier(
            isset($row['traceparent']) ? (string) $row['traceparent'] : null,
            isset($row['tracestate']) ? (string) $row['tracestate'] : null,
        );
    } catch (InvalidArgumentException $exception) {
        $fail('outbox trace context is invalid for ' . $row['message_id']);
    }
    if ($trace === null) {
        $fail('outbox trace context is missing for ' . $row['message_id']);
    }
    $manifestMessage = array_values(array_filter(
        $messages,
        static fn (array $message): bool => (string) $message['message_id'] === (string) $row['message_id'],
    ))[0] ?? null;
    if (is_array($manifestMessage)
        && isset($manifestMessage['trace_id'])
        && $trace->traceId() !== (string) $manifestMessage['trace_id']) {
        $fail('outbox trace_id differs from the originating SEND for ' . $row['message_id']);
    }
}

$onlineOffset = array_search('online_delivery', array_column($messages, 'scenario'), true);
if ($onlineOffset === false) {
    $fail('online delivery scenario is missing from the QA manifest');
}
$onlineId = (string) ($messages[$onlineOffset]['message_id'] ?? '');
$receipt = $repository->fetchOne(
    'SELECT COUNT(*) AS rows_count, MAX(status) AS max_status FROM im_message_receipt WHERE organization = ? AND message_id = ? AND status = 2',
    [$organization, $onlineId],
);
if ((int) ($receipt['rows_count'] ?? 0) !== 1 || (int) ($receipt['max_status'] ?? 0) !== 2) {
    $fail('duplicate delivered ACK was not persisted idempotently');
}

$forbidden = $repository->fetchOne(
    'SELECT COUNT(*) AS aggregate FROM im_message_index WHERE organization <> ? AND client_msg_id LIKE ?',
    [$organization, 'qa-im-' . $runId . '-%'],
);
if ((int) ($forbidden['aggregate'] ?? 0) !== 0) {
    $fail('QA message crossed organization boundary');
}

$requiredIndexes = [
    'uni_organization_global_seq',
    'uni_organization_message',
    'uni_organization_client_msg',
    'uni_organization_conversation_seq',
    'idx_organization_conversation_global',
];
$indexDefinitions = $repository->fetchAll(
    'SELECT DISTINCT INDEX_NAME AS name FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?',
    ['im_message_index'],
);
$actualIndexes = array_column($indexDefinitions, 'name');
foreach ($requiredIndexes as $requiredIndex) {
    if (!in_array($requiredIndex, $actualIndexes, true)) {
        $fail('required message index is missing: ' . $requiredIndex);
    }
}

$rabbit = null;
if ($env('QA_AUDIT_RABBITMQ', '1') === '1') {
    $connection = new AMQPStreamConnection(
        $config->rabbitmqHost,
        $config->rabbitmqPort,
        $config->rabbitmqUser,
        $config->rabbitmqPassword,
        $config->rabbitmqVhost,
    );
    try {
        $channel = $connection->channel();
        $rabbitDeadline = microtime(true) + max(1, (int) $env('QA_RABBIT_WAIT_SECONDS', '20'));
        do {
            [, $ready, $consumers] = $channel->queue_declare(Constants::MQ_MESSAGE_AFTER, true);
            [, $deadLetters] = $channel->queue_declare(Constants::MQ_MESSAGE_DLX, true);
            $rabbit = ['ready' => $ready, 'consumers' => $consumers, 'dead_letters' => $deadLetters];
            $rabbitHealthy = $ready <= (int) $env('QA_RABBIT_MAX_READY', '0')
                && $deadLetters <= (int) $env('QA_RABBIT_MAX_DLX', '0')
                && $consumers >= (int) $env('QA_RABBIT_MIN_CONSUMERS', '1');
            if ($rabbitHealthy) {
                break;
            }
            usleep(250_000);
        } while (microtime(true) < $rabbitDeadline);
        if (!$rabbitHealthy) {
            $fail('RabbitMQ backlog/consumer threshold failed: ' . json_encode($rabbit));
        }
        $channel->close();
    } finally {
        $connection->close();
    }
}

$logFiles = array_values(array_filter(array_map('trim', explode(',', $env('QA_LOG_FILES')))));
$logErrors = [];
foreach ($logFiles as $logFile) {
    if (!is_file($logFile)) {
        $fail('QA log file does not exist: ' . $logFile);
    }
    $contents = (string) file_get_contents($logFile, false, null, max(0, filesize($logFile) - 2_000_000));
    foreach (preg_split('/\R/', $contents) ?: [] as $line) {
        if (preg_match('/(?:Fatal error|Unhandled|outbox publish failed|realtime delivery error|MySQL server has gone away)/i', $line) === 1) {
            $logErrors[] = basename($logFile) . ': ' . trim($line);
        }
    }
}
if ($logErrors !== []) {
    $fail('IM error log signatures found: ' . implode(' | ', array_slice($logErrors, 0, 10)));
}

fwrite(STDOUT, json_encode([
    'ok' => true,
    'qa_run_id' => $runId,
    'database' => $selectedDatabase,
    'messages' => count($messageIds),
    'outbox_published' => count($outboxRows),
    'receipt_idempotent' => true,
    'indexes' => $requiredIndexes,
    'rabbitmq' => $rabbit,
    'logs_scanned' => $logFiles,
], JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR) . PHP_EOL);
