<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

final class NormalizePrivateAssetReferences extends AbstractMigration
{
    public function up(): void
    {
        if ($this->hasTable('im_upload_asset')) {
            $this->execute("UPDATE `im_upload_asset` SET `url` = '' WHERE `url` IS NOT NULL AND `url` <> ''");
        }
        if ($this->hasTable('im_user')) {
            $this->execute(
                "UPDATE `im_user` SET `avatar` = NULL "
                . "WHERE `avatar` IS NOT NULL AND `avatar` <> '' "
                . "AND `avatar` NOT REGEXP '^[a-f0-9]{40}$'",
            );
        }
        if ($this->hasTable('im_conversation')) {
            $this->execute(
                "UPDATE `im_conversation` SET `avatar` = NULL "
                . "WHERE `avatar` IS NOT NULL AND `avatar` <> '' "
                . "AND `avatar` NOT REGEXP '^[a-f0-9]{40}$'",
            );
        }

        foreach ($this->messageTables() as $table) {
            $this->clearNestedUrls($table, 'content');
        }
        if ($this->hasTable('im_message_outbox')) {
            $this->clearNestedUrls('im_message_outbox', 'payload_json');
        }
    }

    public function down(): void
    {
        // 私有对象 URL 和旧外链头像属于已撤销的持久化凭据，不能在回滚时恢复。
    }

    /** @return list<string> */
    private function messageTables(): array
    {
        $rows = $this->fetchAll(
            "SELECT TABLE_NAME AS table_name FROM information_schema.TABLES "
            . "WHERE TABLE_SCHEMA = DATABASE() "
            . "AND (TABLE_NAME = 'im_message' OR TABLE_NAME REGEXP '^im_message_[0-9]{4}_[0-9]{6}$')",
        );
        $tables = [];
        foreach ($rows as $row) {
            $table = (string) ($row['table_name'] ?? '');
            if ($table === 'im_message' || preg_match('/^im_message_[0-9]{4}_[0-9]{6}$/', $table) === 1) {
                $tables[] = $table;
            }
        }

        return $tables;
    }

    private function clearNestedUrls(string $table, string $column): void
    {
        if (
            preg_match('/^[a-z0-9_]+$/', $table) !== 1
            || !in_array($column, ['content', 'payload_json'], true)
        ) {
            throw new RuntimeException('invalid private asset migration table or column');
        }

        $connection = $this->getAdapter()->getConnection();
        $select = $connection->prepare(sprintf(
            "SELECT `id`, `%s` AS `document` FROM `%s` "
            . "WHERE `id` > :last_id AND `%s` IS NOT NULL AND `%s` <> '' "
            . "ORDER BY `id` ASC LIMIT 500",
            $column,
            $table,
            $column,
            $column,
        ));
        $update = $connection->prepare(sprintf(
            "UPDATE `%s` SET `%s` = :document WHERE `id` = :id",
            $table,
            $column,
        ));

        $lastId = 0;
        while (true) {
            $select->bindValue(':last_id', $lastId, PDO::PARAM_INT);
            $select->execute();
            $rows = $select->fetchAll(PDO::FETCH_ASSOC);
            if ($rows === []) {
                break;
            }

            foreach ($rows as $row) {
                $id = (int) ($row['id'] ?? 0);
                if ($id <= $lastId) {
                    throw new RuntimeException('private asset migration encountered an invalid row id');
                }
                $lastId = $id;
                try {
                    $document = $this->withoutNestedUrls((string) ($row['document'] ?? ''));
                } catch (JsonException $exception) {
                    throw new RuntimeException(sprintf(
                        'private asset migration found invalid JSON: table=%s column=%s id=%d',
                        $table,
                        $column,
                        $id,
                    ), 0, $exception);
                }
                if ($document === null) {
                    continue;
                }

                $update->bindValue(':document', $document, PDO::PARAM_STR);
                $update->bindValue(':id', $id, PDO::PARAM_INT);
                $update->execute();
            }
        }
    }

    private function withoutNestedUrls(string $document): ?string
    {
        $value = json_decode($document, true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($value)) {
            return null;
        }

        $changed = false;
        $value = $this->clearUrlFields($value, $changed);
        if (!$changed) {
            return null;
        }

        return json_encode(
            $value,
            JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR,
        );
    }

    /**
     * @param array<array-key, mixed> $value
     * @return array<array-key, mixed>
     */
    private function clearUrlFields(array $value, bool &$changed): array
    {
        foreach ($value as $key => $item) {
            $normalizedKey = is_string($key) ? strtolower($key) : '';
            if ($normalizedKey === 'url' || str_ends_with($normalizedKey, '_url')) {
                if ($item !== '') {
                    $value[$key] = '';
                    $changed = true;
                }
                continue;
            }
            if (is_array($item)) {
                $value[$key] = $this->clearUrlFields($item, $changed);
            }
        }

        return $value;
    }
}
