<?php

declare(strict_types=1);

use B8im\ImShared\Protocol\Dto\CanonicalDecimal;
use B8im\ImShared\Protocol\Dto\SearchProjectionEvent;
use Phinx\Migration\AbstractMigration;

final class AddSearchProjectionEventSequence extends AbstractMigration
{
    private const MAINTENANCE_LOCK = 'b8im:im:search-projection-event-sequence';
    private const UNIQUE_INDEX = 'uni_outbox_organization_source_event_seq';

    public function up(): void
    {
        $connection = $this->requirePdoConnection();
        $this->withMaintenanceLock($connection, function () use ($connection): void {
            $this->assertBaseline($connection);
            $this->assertSequenceOwnersExist($connection);

            try {
                $this->execute(<<<'SQL'
ALTER TABLE `im_organization_message_sequence`
  ADD COLUMN `last_search_event_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 0
    COMMENT '最后已分配的搜索投影事实序号' AFTER `next_global_seq`
SQL);
                $this->execute(<<<'SQL'
ALTER TABLE `im_message_outbox`
  ADD COLUMN `source_event_seq` bigint(20) UNSIGNED NULL DEFAULT NULL
    COMMENT '四类搜索投影事实的机构连续序号' AFTER `change_seq`
SQL);

                $this->backfillHistory($connection);
                $this->execute(sprintf(
                    'ALTER TABLE `im_message_outbox` ADD UNIQUE KEY `%s` (`organization`, `source_event_seq`)',
                    self::UNIQUE_INDEX,
                ));
                $this->assertTarget($connection);
            } catch (Throwable $exception) {
                $this->cleanupFailedUp($connection);
                throw $exception;
            }
        });
    }

    public function down(): void
    {
        $connection = $this->requirePdoConnection();
        $this->withMaintenanceLock($connection, function () use ($connection): void {
            $this->assertTarget($connection);
            $this->assertCanonicalRows($connection);
            $this->assertContinuousSequences($connection);

            $this->removeContractFields($connection);
            $this->execute(sprintf(
                'ALTER TABLE `im_message_outbox` DROP INDEX `%s`, DROP COLUMN `source_event_seq`',
                self::UNIQUE_INDEX,
            ));
            $this->execute(<<<'SQL'
ALTER TABLE `im_organization_message_sequence`
  DROP COLUMN `last_search_event_seq`
SQL);
            $this->assertBaseline($connection);
        });
    }

    private function requirePdoConnection(): PDO
    {
        $connection = $this->getAdapter()->getConnection();
        if (!$connection instanceof PDO) {
            throw new RuntimeException('search projection sequence migration requires a PDO adapter');
        }

        return $connection;
    }

    /** @param callable():void $operation */
    private function withMaintenanceLock(PDO $connection, callable $operation): void
    {
        $statement = $connection->prepare('SELECT GET_LOCK(?, 30)');
        $statement->execute([self::MAINTENANCE_LOCK]);
        if ((int) $statement->fetchColumn() !== 1) {
            throw new RuntimeException('unable to acquire search projection maintenance lock');
        }

        $failure = null;
        try {
            $operation();
        } catch (Throwable $exception) {
            $failure = $exception;
        }

        try {
            $statement = $connection->prepare('SELECT RELEASE_LOCK(?)');
            $statement->execute([self::MAINTENANCE_LOCK]);
            if ((int) $statement->fetchColumn() !== 1) {
                throw new RuntimeException('unable to release search projection maintenance lock');
            }
        } catch (Throwable $releaseFailure) {
            if ($failure !== null) {
                throw new RuntimeException(
                    $failure->getMessage() . '; additionally failed to release maintenance lock',
                    previous: $failure,
                );
            }
            throw $releaseFailure;
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    private function assertBaseline(PDO $connection): void
    {
        if (
            $this->columnExists($connection, 'im_organization_message_sequence', 'last_search_event_seq')
            || $this->columnExists($connection, 'im_message_outbox', 'source_event_seq')
            || $this->indexExists($connection, 'im_message_outbox', self::UNIQUE_INDEX)
        ) {
            throw new RuntimeException('search projection migration baseline contains partial target artifacts');
        }
    }

    private function assertTarget(PDO $connection): void
    {
        if (
            !$this->columnExists($connection, 'im_organization_message_sequence', 'last_search_event_seq')
            || !$this->columnExists($connection, 'im_message_outbox', 'source_event_seq')
            || !$this->indexExists($connection, 'im_message_outbox', self::UNIQUE_INDEX)
        ) {
            throw new RuntimeException('search projection migration target artifacts are incomplete');
        }

        $sequenceShape = $this->columnShape(
            $connection,
            'im_organization_message_sequence',
            'last_search_event_seq',
        );
        $outboxShape = $this->columnShape($connection, 'im_message_outbox', 'source_event_seq');
        if ($sequenceShape !== [
            'column_type' => 'bigint unsigned',
            'is_nullable' => 'NO',
            'column_default' => '0',
        ]) {
            throw new RuntimeException('last_search_event_seq column shape drifted');
        }
        if ($outboxShape !== [
            'column_type' => 'bigint unsigned',
            'is_nullable' => 'YES',
            'column_default' => null,
        ]) {
            throw new RuntimeException('source_event_seq column shape drifted');
        }
        if ($this->indexShape($connection, 'im_message_outbox', self::UNIQUE_INDEX) !== [
            ['non_unique' => 0, 'seq_in_index' => 1, 'column_name' => 'organization'],
            ['non_unique' => 0, 'seq_in_index' => 2, 'column_name' => 'source_event_seq'],
        ]) {
            throw new RuntimeException('conditional source_event_seq unique index shape drifted');
        }

        $this->assertCanonicalRows($connection);
    }

    private function assertSequenceOwnersExist(PDO $connection): void
    {
        $row = $connection->query(
            'SELECT o.organization
               FROM im_message_outbox o
               LEFT JOIN im_organization_message_sequence s
                 ON s.organization = o.organization
              WHERE o.event_type IN (' . $this->eventTypeSql() . ')
                AND s.organization IS NULL
              ORDER BY o.organization
              LIMIT 1',
        )?->fetch(PDO::FETCH_ASSOC);
        if (is_array($row)) {
            throw new RuntimeException(
                'search projection history has no organization sequence owner: '
                . (string) ($row['organization'] ?? ''),
            );
        }
    }

    private function backfillHistory(PDO $connection): void
    {
        $connection->beginTransaction();
        try {
            $statement = $connection->query(
                'SELECT id, event_id, organization, event_type, message_id, payload_json
                   FROM im_message_outbox
                  WHERE event_type IN (' . $this->eventTypeSql() . ')
                  ORDER BY organization ASC, id ASC
                  FOR UPDATE',
            );
            if ($statement === false) {
                throw new RuntimeException('failed to lock search projection history');
            }

            $updateRow = $connection->prepare(
                'UPDATE im_message_outbox
                    SET source_event_seq = ?, payload_json = ?
                  WHERE id = ? AND source_event_seq IS NULL',
            );
            $updateSequence = $connection->prepare(
                'UPDATE im_organization_message_sequence
                    SET last_search_event_seq = ?, update_time = ?
                  WHERE organization = ? AND last_search_event_seq = 0',
            );
            $sequenceByOrganization = [];
            foreach ($statement->fetchAll(PDO::FETCH_ASSOC) as $row) {
                $organization = (int) ($row['organization'] ?? 0);
                $sourceEventSeq = CanonicalDecimal::increment(
                    $sequenceByOrganization[$organization] ?? '0',
                    'source_event_seq',
                );
                $sequenceByOrganization[$organization] = $sourceEventSeq;
                $payload = $this->decodePayload((string) ($row['payload_json'] ?? ''));
                $identity = new SearchProjectionEvent(
                    (string) ($row['event_id'] ?? ''),
                    $organization,
                    (string) ($row['event_type'] ?? ''),
                    $sourceEventSeq,
                    (string) ($row['message_id'] ?? ''),
                );
                $payload = array_replace($payload, $identity->toArray());
                $updateRow->execute([
                    $sourceEventSeq,
                    json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                    (int) $row['id'],
                ]);
                if ($updateRow->rowCount() !== 1) {
                    throw new RuntimeException('search projection history row changed during backfill');
                }
            }

            $now = date('Y-m-d H:i:s');
            foreach ($sequenceByOrganization as $organization => $sourceEventSeq) {
                $updateSequence->execute([$sourceEventSeq, $now, $organization]);
                if ($updateSequence->rowCount() !== 1) {
                    throw new RuntimeException(
                        'search projection sequence owner changed during backfill: ' . $organization,
                    );
                }
            }
            $connection->commit();
        } catch (Throwable $exception) {
            if ($connection->inTransaction()) {
                $connection->rollBack();
            }
            throw $exception;
        }
    }

    private function assertCanonicalRows(PDO $connection): void
    {
        $rows = $connection->query(
            'SELECT event_id, organization, event_type, message_id,
                    CAST(source_event_seq AS CHAR) AS source_event_seq, payload_json
               FROM im_message_outbox
              WHERE event_type IN (' . $this->eventTypeSql() . ')
              ORDER BY organization ASC, id ASC',
        );
        if ($rows === false) {
            throw new RuntimeException('failed to inspect search projection outbox rows');
        }

        foreach ($rows->fetchAll(PDO::FETCH_ASSOC) as $row) {
            if (($row['source_event_seq'] ?? null) === null) {
                throw new RuntimeException('search projection outbox row has no source_event_seq');
            }
            $identity = new SearchProjectionEvent(
                (string) ($row['event_id'] ?? ''),
                (int) ($row['organization'] ?? 0),
                (string) ($row['event_type'] ?? ''),
                (string) $row['source_event_seq'],
                (string) ($row['message_id'] ?? ''),
            );
            $payload = $this->decodePayload((string) ($row['payload_json'] ?? ''));
            foreach ($identity->toArray() as $field => $value) {
                if (!array_key_exists($field, $payload) || $payload[$field] !== $value) {
                    throw new RuntimeException(
                        'search projection payload identity differs from outbox columns: ' . $field,
                    );
                }
            }
        }

        $nonSearch = $connection->query(
            'SELECT 1 FROM im_message_outbox
              WHERE event_type NOT IN (' . $this->eventTypeSql() . ')
                AND source_event_seq IS NOT NULL
              LIMIT 1',
        )?->fetchColumn();
        if ($nonSearch !== false) {
            throw new RuntimeException('non-search outbox event has a source_event_seq');
        }

    }

    private function assertContinuousSequences(PDO $connection): void
    {
        $rows = $connection->query(
            'SELECT s.organization,
                    CAST(s.last_search_event_seq AS CHAR) AS last_search_event_seq,
                    COUNT(o.id) AS event_count,
                    CAST(COALESCE(MIN(o.source_event_seq), 0) AS CHAR) AS min_seq,
                    CAST(COALESCE(MAX(o.source_event_seq), 0) AS CHAR) AS max_seq
               FROM im_organization_message_sequence s
               LEFT JOIN im_message_outbox o
                 ON o.organization = s.organization
                AND o.event_type IN (' . $this->eventTypeSql() . ')
              GROUP BY s.organization, s.last_search_event_seq
              ORDER BY s.organization',
        );
        if ($rows === false) {
            throw new RuntimeException('failed to inspect search projection continuity');
        }
        foreach ($rows->fetchAll(PDO::FETCH_ASSOC) as $row) {
            $count = (string) ($row['event_count'] ?? '0');
            $minimum = (string) ($row['min_seq'] ?? '0');
            $maximum = (string) ($row['max_seq'] ?? '0');
            $last = (string) ($row['last_search_event_seq'] ?? '0');
            if ($maximum !== $last || ($count === '0' ? $minimum !== '0' : $minimum !== '1')) {
                throw new RuntimeException(
                    'search projection sequence is not continuous for organization '
                    . (string) ($row['organization'] ?? ''),
                );
            }
            if ($maximum !== $count) {
                throw new RuntimeException(
                    'search projection sequence contains a committed gap for organization '
                    . (string) ($row['organization'] ?? ''),
                );
            }
        }
    }

    private function removeContractFields(PDO $connection): void
    {
        $this->execute(
            'UPDATE im_message_outbox
                SET payload_json = JSON_REMOVE(
                    payload_json,
                    \'$.event_contract\',
                    \'$.source_event_seq\'
                )
              WHERE event_type IN (' . $this->eventTypeSql() . ')
                AND JSON_VALID(payload_json) = 1',
        );
    }

    private function cleanupFailedUp(PDO $connection): void
    {
        if ($connection->inTransaction()) {
            $connection->rollBack();
        }
        if ($this->columnExists($connection, 'im_message_outbox', 'source_event_seq')) {
            $this->removeContractFields($connection);
            if ($this->indexExists($connection, 'im_message_outbox', self::UNIQUE_INDEX)) {
                $this->execute(sprintf(
                    'ALTER TABLE `im_message_outbox` DROP INDEX `%s`',
                    self::UNIQUE_INDEX,
                ));
            }
            $this->execute('ALTER TABLE `im_message_outbox` DROP COLUMN `source_event_seq`');
        }
        if ($this->columnExists(
            $connection,
            'im_organization_message_sequence',
            'last_search_event_seq',
        )) {
            $this->execute(
                'ALTER TABLE `im_organization_message_sequence` DROP COLUMN `last_search_event_seq`',
            );
        }
    }

    /** @return array<string,mixed> */
    private function decodePayload(string $json): array
    {
        try {
            $payload = json_decode($json, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException $exception) {
            throw new RuntimeException('search projection outbox payload is invalid JSON', previous: $exception);
        }
        if (!is_array($payload) || array_is_list($payload)) {
            throw new RuntimeException('search projection outbox payload must be a JSON object');
        }

        return $payload;
    }

    private function eventTypeSql(): string
    {
        return implode(', ', array_map(
            static fn (string $eventType): string => "'" . $eventType . "'",
            SearchProjectionEvent::EVENT_TYPES,
        ));
    }

    private function columnExists(PDO $connection, string $table, string $column): bool
    {
        $statement = $connection->prepare(
            'SELECT 1 FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1',
        );
        $statement->execute([$table, $column]);

        return $statement->fetchColumn() !== false;
    }

    private function indexExists(PDO $connection, string $table, string $index): bool
    {
        $statement = $connection->prepare(
            'SELECT 1 FROM information_schema.STATISTICS
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1',
        );
        $statement->execute([$table, $index]);

        return $statement->fetchColumn() !== false;
    }

    /** @return list<array{non_unique:int,seq_in_index:int,column_name:string}> */
    private function indexShape(PDO $connection, string $table, string $index): array
    {
        $statement = $connection->prepare(
            'SELECT NON_UNIQUE AS non_unique, SEQ_IN_INDEX AS seq_in_index,
                    COLUMN_NAME AS column_name
               FROM information_schema.STATISTICS
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?
              ORDER BY SEQ_IN_INDEX',
        );
        $statement->execute([$table, $index]);

        return array_map(
            static fn (array $row): array => [
                'non_unique' => (int) ($row['non_unique'] ?? 1),
                'seq_in_index' => (int) ($row['seq_in_index'] ?? 0),
                'column_name' => (string) ($row['column_name'] ?? ''),
            ],
            $statement->fetchAll(PDO::FETCH_ASSOC),
        );
    }

    /** @return array{column_type:string,is_nullable:string,column_default:?string}|null */
    private function columnShape(PDO $connection, string $table, string $column): ?array
    {
        $statement = $connection->prepare(
            'SELECT COLUMN_TYPE AS column_type, IS_NULLABLE AS is_nullable,
                    COLUMN_DEFAULT AS column_default
               FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1',
        );
        $statement->execute([$table, $column]);
        $row = $statement->fetch(PDO::FETCH_ASSOC);
        if (!is_array($row)) {
            return null;
        }

        return [
            'column_type' => strtolower((string) ($row['column_type'] ?? '')),
            'is_nullable' => (string) ($row['is_nullable'] ?? ''),
            'column_default' => $row['column_default'] === null
                ? null
                : (string) $row['column_default'],
        ];
    }
}
