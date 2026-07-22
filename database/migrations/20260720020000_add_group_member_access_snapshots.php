<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

/** Adds the user epoch and immutable audit read by IM access snapshots. */
final class AddGroupMemberAccessSnapshots extends AbstractMigration
{
    private const GROUP_ACCESS_EVENT = 'group.member_access_changed';

    public function up(): void
    {
        $connection = $this->requirePdoConnection();
        $this->assertRequiredBaseSchema($connection);
        $schemaState = $this->schemaState($connection);
        if ($schemaState === 'complete') {
            return;
        }
        if ($this->hasGroupAccessOutboxEvents($connection)) {
            throw new \RuntimeException(
                'group access migration refused recovery while group.member_access_changed outbox rows exist',
            );
        }
        $this->assertGroupBaselineIsCanonical($connection);
        if ($schemaState === 'partial') {
            $this->cleanupSchemaArtifacts($connection);
        }
        $this->execute(<<<'SQL'
CREATE TABLE im_user_group_access_state (
  organization int(11) UNSIGNED NOT NULL COMMENT '用户所属机构',
  user_id varchar(64) NOT NULL COMMENT '用户ID',
  access_snapshot_id bigint(20) UNSIGNED NOT NULL DEFAULT 1 COMMENT '用户级群访问快照epoch',
  create_time datetime NOT NULL COMMENT '创建时间',
  update_time datetime NOT NULL COMMENT '最后一次真实访问变更时间',
  PRIMARY KEY (organization, user_id) USING BTREE,
  CONSTRAINT chk_im_user_group_access_state_positive CHECK (access_snapshot_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='IM用户群成员访问快照epoch' ROW_FORMAT=DYNAMIC
SQL);
        $this->execute(<<<'SQL'
INSERT INTO im_user_group_access_state
  (organization, user_id, access_snapshot_id, create_time, update_time)
SELECT organization, user_id, 1,
       COALESCE(create_time, CURRENT_TIMESTAMP),
       COALESCE(update_time, create_time, CURRENT_TIMESTAMP)
  FROM im_user
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE im_conversation_member
  ADD COLUMN access_state varchar(16) NOT NULL DEFAULT 'active'
    COMMENT 'active,history_only,revoked' AFTER access_version
SQL);
        $this->execute(<<<'SQL'
UPDATE im_conversation_member cm
INNER JOIN im_conversation conversation
   ON conversation.organization = cm.organization
  AND conversation.conversation_id = cm.conversation_id
  AND conversation.conversation_type = 2
   SET cm.access_state = CASE
     WHEN cm.status = 1 AND cm.delete_time IS NULL AND EXISTS (
       SELECT 1 FROM im_conversation_membership_period open_period
        WHERE open_period.organization = cm.organization
          AND open_period.conversation_id = cm.conversation_id
          AND open_period.member_organization = cm.member_organization
          AND open_period.user_id = cm.user_id
          AND open_period.status = 1
          AND open_period.visible_until_message_seq IS NULL
     ) THEN 'active'
     WHEN EXISTS (
       SELECT 1 FROM im_conversation_membership_period visible_period
        WHERE visible_period.organization = cm.organization
          AND visible_period.conversation_id = cm.conversation_id
          AND visible_period.member_organization = cm.member_organization
          AND visible_period.user_id = cm.user_id
          AND visible_period.status = 1
     ) THEN 'history_only'
     ELSE 'revoked'
   END
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE im_conversation_member
  ADD CONSTRAINT chk_im_conversation_member_access_state
    CHECK (access_state IN ('active', 'history_only', 'revoked'))
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE im_group_member_access_audit (
  id bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '审计主键',
  event_id char(64) NOT NULL COMMENT '稳定事件幂等ID',
  organization int(11) UNSIGNED NOT NULL COMMENT '群所属机构/home',
  conversation_id varchar(64) NOT NULL COMMENT '群会话ID',
  member_organization int(11) UNSIGNED NOT NULL COMMENT '目标成员所属机构',
  user_id varchar(64) NOT NULL COMMENT '目标成员ID',
  access_snapshot_id bigint(20) UNSIGNED NOT NULL COMMENT '用户级访问快照epoch',
  access_version bigint(20) UNSIGNED NOT NULL COMMENT '该成员会话访问版本',
  access_state varchar(16) NOT NULL COMMENT 'active,history_only,revoked',
  last_message_seq bigint(20) UNSIGNED NOT NULL COMMENT '变更时会话最后消息序号',
  last_change_seq bigint(20) UNSIGNED NOT NULL COMMENT '变更时会话最后消息变更序号',
  periods_json longtext NOT NULL COMMENT '变更后的有效历史可见区间',
  reason varchar(64) NOT NULL COMMENT '业务原因或migration_backfill',
  actor_organization int(11) UNSIGNED NOT NULL COMMENT '操作人机构',
  actor_user_id varchar(64) NOT NULL COMMENT '操作人ID，系统为system',
  create_time datetime NOT NULL COMMENT '不可变审计发生时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY uni_group_member_access_event (event_id) USING BTREE,
  UNIQUE KEY uni_group_member_access_version
    (organization, conversation_id, member_organization, user_id, access_version) USING BTREE,
  KEY idx_group_member_access_snapshot
    (member_organization, user_id, access_snapshot_id, id) USING BTREE,
  CONSTRAINT chk_group_member_access_snapshot_positive CHECK (access_snapshot_id > 0),
  CONSTRAINT chk_group_member_access_version_positive CHECK (access_version > 0),
  CONSTRAINT chk_group_member_access_state
    CHECK (access_state IN ('active', 'history_only', 'revoked')),
  CONSTRAINT chk_group_member_access_periods_json CHECK (JSON_VALID(periods_json)),
  CONSTRAINT chk_group_member_access_actor
    CHECK (actor_organization > 0 AND actor_user_id <> '')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='IM群成员访问不可变审计' ROW_FORMAT=DYNAMIC
SQL);

        $this->backfillImmutableAudit($connection);

        $this->execute(<<<'SQL'
ALTER TABLE im_conversation_member
  ADD KEY idx_group_access_snapshot_page
    (member_organization, user_id, conversation_id, access_version, access_state, status)
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE im_conversation_membership_period
  ADD KEY idx_group_access_period_page
    (member_organization, user_id, conversation_id, status, period_no)
SQL);
    }

    public function down(): void
    {
        $connection = $this->requirePdoConnection();
        $this->deleteGroupAccessOutboxEvents($connection);
        $this->cleanupSchemaArtifacts($connection);
    }

    private function assertGroupBaselineIsCanonical(\PDO $connection): void
    {
        $invalidUser = $this->firstRow($connection, <<<'SQL'
SELECT user_state_backing.id
  FROM im_user user_state_backing
 WHERE user_state_backing.organization = 0
    OR user_state_backing.user_id = ''
    OR BINARY user_state_backing.user_id <> BINARY TRIM(user_state_backing.user_id)
    OR LOCATE(CHAR(0), user_state_backing.user_id) > 0
    OR LOCATE('|', user_state_backing.user_id) > 0
 LIMIT 1
SQL);
        if (is_array($invalidUser)) {
            throw new \RuntimeException(
                'group access migration refused an invalid user identity before state backfill',
            );
        }
        $invalidMember = $this->firstRow($connection, <<<'SQL'
SELECT member.id
  FROM im_conversation_member member
  INNER JOIN im_conversation conversation
    ON conversation.organization = member.organization
   AND conversation.conversation_id = member.conversation_id
   AND conversation.conversation_type = 2
  LEFT JOIN im_user user_backing
    ON user_backing.organization = member.member_organization
   AND user_backing.user_id = member.user_id
 WHERE member.organization = 0
    OR member.member_organization = 0
    OR member.member_organization <> member.organization
    OR member.conversation_id = ''
    OR BINARY member.conversation_id <> BINARY TRIM(member.conversation_id)
    OR LOCATE(CHAR(0), member.conversation_id) > 0
    OR LOCATE('|', member.conversation_id) > 0
    OR member.user_id = ''
    OR BINARY member.user_id <> BINARY TRIM(member.user_id)
    OR LOCATE(CHAR(0), member.user_id) > 0
    OR LOCATE('|', member.user_id) > 0
    OR member.access_version = 0
    OR user_backing.id IS NULL
 LIMIT 1
SQL);
        if (is_array($invalidMember)) {
            throw new \RuntimeException(
                'group access migration refused a group member with invalid identity, access_version, or user backing',
            );
        }
        $invalidPeriodIdentity = $this->firstRow($connection, <<<'SQL'
SELECT period.id
  FROM im_conversation_membership_period period
  INNER JOIN im_conversation conversation
    ON conversation.organization = period.organization
   AND conversation.conversation_id = period.conversation_id
   AND conversation.conversation_type = 2
  LEFT JOIN im_conversation_member member_backing
    ON member_backing.organization = period.organization
   AND member_backing.conversation_id = period.conversation_id
   AND member_backing.member_organization = period.member_organization
   AND member_backing.user_id = period.user_id
 WHERE period.organization = 0
     OR period.member_organization = 0
     OR period.member_organization <> period.organization
     OR period.conversation_id = ''
     OR BINARY period.conversation_id <> BINARY TRIM(period.conversation_id)
     OR LOCATE(CHAR(0), period.conversation_id) > 0
     OR LOCATE('|', period.conversation_id) > 0
     OR period.user_id = ''
     OR BINARY period.user_id <> BINARY TRIM(period.user_id)
     OR LOCATE(CHAR(0), period.user_id) > 0
     OR LOCATE('|', period.user_id) > 0
     OR member_backing.id IS NULL
 LIMIT 1
SQL);
        if (is_array($invalidPeriodIdentity)) {
            throw new \RuntimeException(
                'group access migration refused a group period without its canonical member identity',
            );
        }
        $invalidScalar = $this->firstRow($connection, <<<'SQL'
SELECT period.id
  FROM im_conversation_membership_period period
  INNER JOIN im_conversation conversation
    ON conversation.organization = period.organization
   AND conversation.conversation_id = period.conversation_id
   AND conversation.conversation_type = 2
 WHERE period.period_no = 0
    OR period.visible_from_message_seq = 0
    OR (period.visible_until_message_seq IS NOT NULL
        AND period.visible_until_message_seq < period.visible_from_message_seq)
 LIMIT 1
SQL);
        if (is_array($invalidScalar)) {
            throw new \RuntimeException('group access migration refused an invalid membership period scalar');
        }
        $overlap = $this->firstRow($connection, <<<'SQL'
SELECT left_period.id
  FROM im_conversation_membership_period left_period
  INNER JOIN im_conversation conversation
    ON conversation.organization = left_period.organization
   AND conversation.conversation_id = left_period.conversation_id
   AND conversation.conversation_type = 2
  INNER JOIN im_conversation_membership_period right_period
    ON right_period.organization = left_period.organization
   AND right_period.conversation_id = left_period.conversation_id
   AND right_period.member_organization = left_period.member_organization
   AND right_period.user_id = left_period.user_id
   AND right_period.status = 1
   AND right_period.id > left_period.id
 WHERE left_period.status = 1
   AND left_period.visible_from_message_seq <= COALESCE(right_period.visible_until_message_seq, 18446744073709551615)
   AND right_period.visible_from_message_seq <= COALESCE(left_period.visible_until_message_seq, 18446744073709551615)
 LIMIT 1
SQL);
        if (is_array($overlap)) {
            throw new \RuntimeException('group access migration refused overlapping membership periods');
        }
        $reverseOrder = $this->firstRow($connection, <<<'SQL'
SELECT earlier.id
  FROM im_conversation_membership_period earlier
  INNER JOIN im_conversation conversation
    ON conversation.organization = earlier.organization
   AND conversation.conversation_id = earlier.conversation_id
   AND conversation.conversation_type = 2
  INNER JOIN im_conversation_membership_period later
    ON later.organization = earlier.organization
   AND later.conversation_id = earlier.conversation_id
   AND later.member_organization = earlier.member_organization
   AND later.user_id = earlier.user_id
   AND later.status = 1
   AND later.period_no > earlier.period_no
 WHERE earlier.status = 1
   AND later.visible_from_message_seq <= COALESCE(earlier.visible_until_message_seq, 18446744073709551615)
 LIMIT 1
SQL);
        if (is_array($reverseOrder)) {
            throw new \RuntimeException('group access migration refused reverse-ordered membership periods');
        }
        $multipleOpen = $this->firstRow($connection, <<<'SQL'
SELECT period.organization, period.conversation_id, period.member_organization, period.user_id
  FROM im_conversation_membership_period period
  INNER JOIN im_conversation conversation
    ON conversation.organization = period.organization
   AND conversation.conversation_id = period.conversation_id
   AND conversation.conversation_type = 2
 WHERE period.status = 1 AND period.visible_until_message_seq IS NULL
GROUP BY period.organization, period.conversation_id, period.member_organization, period.user_id
HAVING COUNT(*) > 1
 LIMIT 1
SQL);
        if (is_array($multipleOpen)) {
            throw new \RuntimeException('group access migration refused multiple open membership periods');
        }
        $stateMismatch = $this->firstRow($connection, <<<'SQL'
SELECT cm.id
  FROM im_conversation_member cm
  INNER JOIN im_conversation c
    ON c.organization = cm.organization
   AND c.conversation_id = cm.conversation_id
   AND c.conversation_type = 2
 WHERE (
   cm.status = 1 AND cm.delete_time IS NULL AND NOT EXISTS (
     SELECT 1 FROM im_conversation_membership_period period
      WHERE period.organization = cm.organization
        AND period.conversation_id = cm.conversation_id
        AND period.member_organization = cm.member_organization
        AND period.user_id = cm.user_id
        AND period.status = 1
        AND period.visible_until_message_seq IS NULL
   )
 ) OR (
   (cm.status <> 1 OR cm.delete_time IS NOT NULL) AND EXISTS (
     SELECT 1 FROM im_conversation_membership_period period
      WHERE period.organization = cm.organization
        AND period.conversation_id = cm.conversation_id
        AND period.member_organization = cm.member_organization
        AND period.user_id = cm.user_id
        AND period.status = 1
        AND period.visible_until_message_seq IS NULL
   )
 )
 LIMIT 1
SQL);
        if (is_array($stateMismatch)) {
            throw new \RuntimeException('group access migration refused member/open-period state mismatch');
        }
    }

    private function backfillImmutableAudit(\PDO $connection): void
    {
        $periodRows = $this->fetchAll(<<<'SQL'
SELECT period.organization, period.conversation_id, period.member_organization,
       period.user_id, period.period_no, period.visible_from_message_seq,
       period.visible_until_message_seq
  FROM im_conversation_membership_period period
  INNER JOIN im_conversation conversation
    ON conversation.organization = period.organization
   AND conversation.conversation_id = period.conversation_id
   AND conversation.conversation_type = 2
 WHERE period.status = 1
 ORDER BY period.organization, period.member_organization, period.user_id,
          period.conversation_id COLLATE utf8mb4_bin, period.period_no
SQL);
        $periodsByMember = [];
        foreach ($periodRows as $period) {
            $key = json_encode([
                (int) $period['organization'],
                (string) $period['conversation_id'],
                (int) $period['member_organization'],
                (string) $period['user_id'],
            ], JSON_THROW_ON_ERROR);
            $periodsByMember[$key][] = [
                'period_no' => (string) $period['period_no'],
                'from_seq' => (string) $period['visible_from_message_seq'],
                'to_seq' => $period['visible_until_message_seq'] === null
                    ? null
                    : (string) $period['visible_until_message_seq'],
            ];
        }
        $members = $this->fetchAll(<<<'SQL'
SELECT member.organization, member.conversation_id, member.member_organization,
       member.user_id, member.access_version, member.access_state,
       conversation.last_message_seq, conversation.last_change_seq,
       COALESCE(member.update_time, member.create_time, CURRENT_TIMESTAMP) AS audit_time
  FROM im_conversation_member member
  INNER JOIN im_conversation conversation
    ON conversation.organization = member.organization
   AND conversation.conversation_id = member.conversation_id
   AND conversation.conversation_type = 2
 ORDER BY member.organization, member.member_organization, member.user_id,
          member.conversation_id COLLATE utf8mb4_bin
SQL);
        $statement = $connection->prepare(<<<'SQL'
INSERT INTO im_group_member_access_audit
  (event_id, organization, conversation_id, member_organization, user_id,
   access_snapshot_id, access_version, access_state, last_message_seq, last_change_seq,
   periods_json, reason, actor_organization, actor_user_id, create_time)
VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 'migration_backfill', ?, 'system', ?)
SQL);
        foreach ($members as $member) {
            $organization = (int) $member['organization'];
            $conversationId = (string) $member['conversation_id'];
            $memberOrganization = (int) $member['member_organization'];
            $userId = (string) $member['user_id'];
            $accessVersion = (string) $member['access_version'];
            $key = json_encode(
                [$organization, $conversationId, $memberOrganization, $userId],
                JSON_THROW_ON_ERROR,
            );
            $statement->execute([
                hash('sha256', implode('|', [
                    'group-access-baseline',
                    $organization,
                    $conversationId,
                    $memberOrganization,
                    $userId,
                    $accessVersion,
                    '1',
                ])),
                $organization,
                $conversationId,
                $memberOrganization,
                $userId,
                $accessVersion,
                (string) $member['access_state'],
                (string) $member['last_message_seq'],
                (string) $member['last_change_seq'],
                json_encode($periodsByMember[$key] ?? [], JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                $organization,
                (string) $member['audit_time'],
            ]);
        }
    }

    private function requirePdoConnection(): \PDO
    {
        $connection = $this->getAdapter()->getConnection();
        if (!$connection instanceof \PDO) {
            throw new \RuntimeException('group access migration requires a PDO migration adapter before DDL');
        }

        return $connection;
    }

    private function assertRequiredBaseSchema(\PDO $connection): void
    {
        foreach ([
            'im_user',
            'im_conversation',
            'im_conversation_member',
            'im_conversation_membership_period',
            'im_message_outbox',
        ] as $table) {
            if (!$this->tableExists($connection, $table)) {
                throw new \RuntimeException('group access migration requires base table ' . $table);
            }
        }
        foreach ([
            ['im_user', 'organization'],
            ['im_user', 'user_id'],
            ['im_conversation', 'conversation_type'],
            ['im_conversation', 'last_message_seq'],
            ['im_conversation', 'last_change_seq'],
            ['im_conversation_member', 'member_organization'],
            ['im_conversation_member', 'access_version'],
            ['im_conversation_membership_period', 'member_organization'],
            ['im_message_outbox', 'event_type'],
            ['im_message_outbox', 'routing_key'],
        ] as [$table, $column]) {
            if (!$this->columnExists($connection, $table, $column)) {
                throw new \RuntimeException(
                    sprintf('group access migration requires base column %s.%s', $table, $column),
                );
            }
        }
    }

    private function schemaState(\PDO $connection): string
    {
        $artifacts = [
            $this->tableExists($connection, 'im_user_group_access_state'),
            $this->constraintExists(
                $connection,
                'im_user_group_access_state',
                'chk_im_user_group_access_state_positive',
            ),
            $this->columnExists($connection, 'im_conversation_member', 'access_state'),
            $this->constraintExists(
                $connection,
                'im_conversation_member',
                'chk_im_conversation_member_access_state',
            ),
            $this->tableExists($connection, 'im_group_member_access_audit'),
            $this->indexExists($connection, 'im_group_member_access_audit', 'uni_group_member_access_event'),
            $this->indexExists($connection, 'im_group_member_access_audit', 'uni_group_member_access_version'),
            $this->indexExists($connection, 'im_group_member_access_audit', 'idx_group_member_access_snapshot'),
            $this->constraintExists(
                $connection,
                'im_group_member_access_audit',
                'chk_group_member_access_snapshot_positive',
            ),
            $this->constraintExists(
                $connection,
                'im_group_member_access_audit',
                'chk_group_member_access_version_positive',
            ),
            $this->constraintExists(
                $connection,
                'im_group_member_access_audit',
                'chk_group_member_access_state',
            ),
            $this->constraintExists(
                $connection,
                'im_group_member_access_audit',
                'chk_group_member_access_periods_json',
            ),
            $this->constraintExists(
                $connection,
                'im_group_member_access_audit',
                'chk_group_member_access_actor',
            ),
            $this->indexExists($connection, 'im_conversation_member', 'idx_group_access_snapshot_page'),
            $this->indexExists(
                $connection,
                'im_conversation_membership_period',
                'idx_group_access_period_page',
            ),
        ];
        $present = count(array_filter($artifacts));
        if ($present === 0) {
            return 'pristine';
        }

        return $present === count($artifacts) ? 'complete' : 'partial';
    }

    private function hasGroupAccessOutboxEvents(\PDO $connection): bool
    {
        if (
            !$this->tableExists($connection, 'im_message_outbox')
            || !$this->columnExists($connection, 'im_message_outbox', 'event_type')
        ) {
            return false;
        }
        $conditions = ['event_type = ?'];
        $params = [self::GROUP_ACCESS_EVENT];
        if ($this->columnExists($connection, 'im_message_outbox', 'routing_key')) {
            $conditions[] = 'routing_key = ?';
            $params[] = self::GROUP_ACCESS_EVENT;
        }
        $statement = $connection->prepare(
            'SELECT 1 FROM im_message_outbox WHERE ' . implode(' OR ', $conditions) . ' LIMIT 1',
        );
        $statement->execute($params);

        return $statement->fetchColumn() !== false;
    }

    private function deleteGroupAccessOutboxEvents(\PDO $connection): void
    {
        if (
            !$this->tableExists($connection, 'im_message_outbox')
            || !$this->columnExists($connection, 'im_message_outbox', 'event_type')
        ) {
            return;
        }
        $conditions = ['event_type = ?'];
        $params = [self::GROUP_ACCESS_EVENT];
        if ($this->columnExists($connection, 'im_message_outbox', 'routing_key')) {
            $conditions[] = 'routing_key = ?';
            $params[] = self::GROUP_ACCESS_EVENT;
        }
        $statement = $connection->prepare(
            'DELETE FROM im_message_outbox WHERE ' . implode(' OR ', $conditions),
        );
        $statement->execute($params);
    }

    private function cleanupSchemaArtifacts(\PDO $connection): void
    {
        if (
            $this->indexExists(
                $connection,
                'im_conversation_membership_period',
                'idx_group_access_period_page',
            )
        ) {
            $this->execute(
                'ALTER TABLE im_conversation_membership_period DROP INDEX idx_group_access_period_page',
            );
        }
        if ($this->indexExists($connection, 'im_conversation_member', 'idx_group_access_snapshot_page')) {
            $this->execute(
                'ALTER TABLE im_conversation_member DROP INDEX idx_group_access_snapshot_page',
            );
        }
        if ($this->tableExists($connection, 'im_group_member_access_audit')) {
            $this->execute('DROP TABLE im_group_member_access_audit');
        }
        if ($this->columnExists($connection, 'im_conversation_member', 'access_state')) {
            if (
                $this->constraintExists(
                    $connection,
                    'im_conversation_member',
                    'chk_im_conversation_member_access_state',
                )
            ) {
                $this->execute(
                    'ALTER TABLE im_conversation_member '
                    . 'DROP CHECK chk_im_conversation_member_access_state',
                );
            }
            $this->execute('ALTER TABLE im_conversation_member DROP COLUMN access_state');
        }
        if ($this->tableExists($connection, 'im_user_group_access_state')) {
            $this->execute('DROP TABLE im_user_group_access_state');
        }
    }

    /** @return array<string,mixed>|null */
    private function firstRow(\PDO $connection, string $sql): ?array
    {
        $statement = $connection->query($sql);
        if (!$statement instanceof \PDOStatement) {
            throw new \RuntimeException('group access migration preflight query failed');
        }
        $row = $statement->fetch(\PDO::FETCH_ASSOC);

        return is_array($row) ? $row : null;
    }

    private function tableExists(\PDO $connection, string $table): bool
    {
        $statement = $connection->prepare(
            'SELECT 1 FROM information_schema.TABLES '
            . 'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1',
        );
        $statement->execute([$table]);

        return $statement->fetchColumn() !== false;
    }

    private function columnExists(\PDO $connection, string $table, string $column): bool
    {
        $statement = $connection->prepare(
            'SELECT 1 FROM information_schema.COLUMNS '
            . 'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1',
        );
        $statement->execute([$table, $column]);

        return $statement->fetchColumn() !== false;
    }

    private function indexExists(\PDO $connection, string $table, string $index): bool
    {
        $statement = $connection->prepare(
            'SELECT 1 FROM information_schema.STATISTICS '
            . 'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1',
        );
        $statement->execute([$table, $index]);

        return $statement->fetchColumn() !== false;
    }

    private function constraintExists(\PDO $connection, string $table, string $constraint): bool
    {
        $statement = $connection->prepare(
            'SELECT 1 FROM information_schema.TABLE_CONSTRAINTS '
            . 'WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_NAME = ? LIMIT 1',
        );
        $statement->execute([$table, $constraint]);

        return $statement->fetchColumn() !== false;
    }
}
