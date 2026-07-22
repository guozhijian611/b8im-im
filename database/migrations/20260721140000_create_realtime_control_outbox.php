<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

final class CreateRealtimeControlOutbox extends AbstractMigration
{
    private const TABLE = 'im_realtime_control_outbox';

    public function up(): void
    {
        $pdo = $this->pdo();
        if ($this->exists($pdo)) {
            $this->assertTarget($pdo);
            return;
        }
        $this->execute(<<<'SQL'
CREATE TABLE `im_realtime_control_outbox` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `event_id` char(64) NOT NULL COMMENT '稳定事件幂等ID(lowercase sha256)',
  `aggregate_type` varchar(32) NOT NULL COMMENT '聚合类型',
  `aggregate_id` bigint(20) UNSIGNED NOT NULL COMMENT '聚合主键',
  `event_type` varchar(64) NOT NULL COMMENT '控制事件类型',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '目标用户home机构',
  `target_user_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT '目标用户ID',
  `payload_json` longtext NOT NULL COMMENT '严格事件envelope JSON',
  `traceparent` char(55) NULL DEFAULT NULL COMMENT 'W3C Trace Context version 00',
  `tracestate` varchar(512) NULL DEFAULT NULL COMMENT 'W3C tracestate',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1待发布,2发布中,3已发布,4重试,5死信',
  `retry_count` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '累计失败次数',
  `next_retry_at` datetime NULL DEFAULT NULL COMMENT '下次重试时间',
  `locked_until` datetime NULL DEFAULT NULL COMMENT 'claim租约到期时间',
  `worker_id` varchar(64) NULL DEFAULT NULL COMMENT '当前claim worker',
  `claim_token` char(40) NULL DEFAULT NULL COMMENT '当前claim随机令牌',
  `published_at` datetime NULL DEFAULT NULL COMMENT '发布时间',
  `last_error` varchar(500) NULL DEFAULT NULL COMMENT '最后错误',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_realtime_control_event` (`event_id`) USING BTREE,
  UNIQUE KEY `uni_realtime_control_transition`
    (`aggregate_type`,`aggregate_id`,`event_type`,`organization`,`target_user_id`) USING BTREE,
  KEY `idx_realtime_control_claim` (`status`,`next_retry_at`,`locked_until`,`id`) USING BTREE,
  CONSTRAINT `chk_realtime_control_event_id` CHECK (`event_id` REGEXP '^[0-9a-f]{64}$'),
  CONSTRAINT `chk_realtime_control_aggregate` CHECK (`aggregate_type` = 'friend_request' AND `aggregate_id` > 0),
  CONSTRAINT `chk_realtime_control_event_type`
    CHECK (`event_type` IN ('friend_request.created','friend_request.accepted','friend_request.rejected')),
  CONSTRAINT `chk_realtime_control_target`
    CHECK (`organization` > 0 AND `target_user_id` <> ''
      AND BINARY `target_user_id` = BINARY TRIM(`target_user_id`)
      AND LOCATE(CHAR(0),`target_user_id`) = 0 AND LOCATE('|',`target_user_id`) = 0),
  CONSTRAINT `chk_realtime_control_payload_json` CHECK (JSON_VALID(`payload_json`)),
  CONSTRAINT `chk_realtime_control_status` CHECK (`status` IN (1,2,3,4,5)),
  CONSTRAINT `chk_realtime_control_retry_count` CHECK (`retry_count` <= 10),
  CONSTRAINT `chk_realtime_control_claim_state`
    CHECK ((`status`=2 AND `locked_until` IS NOT NULL AND `worker_id` IS NOT NULL AND `claim_token` IS NOT NULL)
      OR (`status`<>2 AND `locked_until` IS NULL AND `worker_id` IS NULL AND `claim_token` IS NULL)),
  CONSTRAINT `chk_realtime_control_retry_state`
    CHECK ((`status`=4 AND `next_retry_at` IS NOT NULL) OR (`status`<>4 AND `next_retry_at` IS NULL)),
  CONSTRAINT `chk_realtime_control_publish_state`
    CHECK ((`status`=3 AND `published_at` IS NOT NULL) OR (`status`<>3 AND `published_at` IS NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='IM实时控制事件Durable Outbox' ROW_FORMAT=DYNAMIC
SQL);
        try {
            $this->assertTarget($pdo);
        } catch (Throwable $error) {
            $this->execute('DROP TABLE IF EXISTS `' . self::TABLE . '`');
            throw $error;
        }
    }

    public function down(): void
    {
        $pdo = $this->pdo();
        if (!$this->exists($pdo)) {
            return;
        }
        $this->assertTarget($pdo);
        $this->execute('DROP TABLE `' . self::TABLE . '`');
    }

    private function pdo(): PDO
    {
        $pdo = $this->getAdapter()->getConnection();
        if (!$pdo instanceof PDO) {
            throw new RuntimeException('realtime control outbox migration requires PDO');
        }
        return $pdo;
    }

    private function exists(PDO $pdo): bool
    {
        $query = $pdo->prepare(
            'SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?',
        );
        $query->execute([self::TABLE]);
        return $query->fetchColumn() !== false;
    }

    private function assertTarget(PDO $pdo): void
    {
        $table = $pdo->prepare(
            'SELECT ENGINE,TABLE_COLLATION,TABLE_COMMENT,ROW_FORMAT FROM information_schema.TABLES '
            . 'WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?',
        );
        $table->execute([self::TABLE]);
        $tableShape = $table->fetch(PDO::FETCH_ASSOC);
        if (!is_array($tableShape) || [
            strtolower((string) ($tableShape['ENGINE'] ?? '')),
            (string) ($tableShape['TABLE_COLLATION'] ?? ''),
            (string) ($tableShape['TABLE_COMMENT'] ?? ''),
            strtolower((string) ($tableShape['ROW_FORMAT'] ?? '')),
        ] !== ['innodb', 'utf8mb4_bin', 'IM实时控制事件Durable Outbox', 'dynamic']) {
            throw new RuntimeException('realtime control outbox table shape drifted');
        }

        $columns = $pdo->prepare(
            'SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,EXTRA,CHARACTER_SET_NAME,COLLATION_NAME '
            . 'FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION',
        );
        $columns->execute([self::TABLE]);
        $actual = [];
        foreach ($columns->fetchAll(PDO::FETCH_ASSOC) as $column) {
            $actual[(string) $column['COLUMN_NAME']] = [
                strtolower((string) $column['COLUMN_TYPE']), (string) $column['IS_NULLABLE'],
                $column['COLUMN_DEFAULT'], strtolower((string) $column['EXTRA']),
                $column['CHARACTER_SET_NAME'], $column['COLLATION_NAME'],
            ];
        }
        if ($actual !== $this->expectedColumns()) {
            throw new RuntimeException('realtime control outbox column shape drifted');
        }

        $indexes = $pdo->prepare(
            'SELECT INDEX_NAME,NON_UNIQUE,SEQ_IN_INDEX,COLUMN_NAME,COLLATION,SUB_PART,PACKED,NULLABLE,'
            . 'INDEX_TYPE,COMMENT,INDEX_COMMENT,IS_VISIBLE,EXPRESSION FROM information_schema.STATISTICS '
            . 'WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX',
        );
        $indexes->execute([self::TABLE]);
        $actual = [];
        foreach ($indexes->fetchAll(PDO::FETCH_ASSOC) as $index) {
            $actual[(string) $index['INDEX_NAME']][] = [
                (int) $index['NON_UNIQUE'],
                (int) $index['SEQ_IN_INDEX'],
                $index['COLUMN_NAME'] === null ? null : (string) $index['COLUMN_NAME'],
                $index['COLLATION'] === null ? null : (string) $index['COLLATION'],
                $index['SUB_PART'] === null ? null : (int) $index['SUB_PART'],
                $index['PACKED'] === null ? null : (string) $index['PACKED'],
                (string) $index['NULLABLE'],
                strtolower((string) $index['INDEX_TYPE']),
                (string) $index['COMMENT'],
                (string) $index['INDEX_COMMENT'],
                (string) $index['IS_VISIBLE'],
                $index['EXPRESSION'] === null ? null : (string) $index['EXPRESSION'],
            ];
        }
        ksort($actual);
        $expected = $this->expectedIndexes();
        ksort($expected);
        if ($actual !== $expected) {
            throw new RuntimeException('realtime control outbox index shape drifted');
        }

        $checks = $pdo->prepare(
            'SELECT tc.CONSTRAINT_NAME,cc.CHECK_CLAUSE,tc.ENFORCED '
            . 'FROM information_schema.TABLE_CONSTRAINTS tc '
            . 'INNER JOIN information_schema.CHECK_CONSTRAINTS cc '
            . 'ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME '
            . "WHERE tc.CONSTRAINT_SCHEMA=DATABASE() AND tc.TABLE_NAME=? AND tc.CONSTRAINT_TYPE='CHECK' "
            . 'ORDER BY tc.CONSTRAINT_NAME',
        );
        $checks->execute([self::TABLE]);
        $actualChecks = [];
        foreach ($checks->fetchAll(PDO::FETCH_ASSOC) as $check) {
            $actualChecks[(string) $check['CONSTRAINT_NAME']] = [
                hash('sha256', (string) $check['CHECK_CLAUSE']),
                (string) $check['ENFORCED'],
            ];
        }
        if ($actualChecks !== [
            'chk_realtime_control_aggregate'=>['4dfd9ecf55561a7bc20bd8b12b5e7f97ee003f39cbab614ca1d0942c922fa9e1','YES'],
            'chk_realtime_control_claim_state'=>['9367cc693068617a50cdda240959f33bd0a65367de0ea22465aa46d16b04d04a','YES'],
            'chk_realtime_control_event_id'=>['ed35c4261d4f6f99c126306467efcab0cedf3aa8651be74b418412e375753c0e','YES'],
            'chk_realtime_control_event_type'=>['a3757bc9e0f1ee241db8093e4c678a471fccf602d16471828144b8f6719307cd','YES'],
            'chk_realtime_control_payload_json'=>['6cb5f6e924903937bfbf7d1babec06a276dfa7113254caf211c67eb34bce1bee','YES'],
            'chk_realtime_control_publish_state'=>['18edd7ff79e5241f8511de2bd0fa77b7f28a306de630f5aff55a6686a9835c87','YES'],
            'chk_realtime_control_retry_count'=>['d46f1526903e3d59e6324e31d377552afcf84652c52c22a66fda71ffa3a3960a','YES'],
            'chk_realtime_control_retry_state'=>['4a2ef9efd0a4b44077066eb0f8a5557b7affd87b3e8da14a14ba040f8b1009cb','YES'],
            'chk_realtime_control_status'=>['4f3dddf2e7c508cf991d43671460c4993c42d34cc753976b820a70cb918c85a1','YES'],
            'chk_realtime_control_target'=>['616b557947deec14e9ac35463042fa7501d2c4690cf511b112f9a326de811a48','YES'],
        ]) {
            throw new RuntimeException('realtime control outbox check shape drifted');
        }
    }

    private function expectedColumns(): array
    {
        $b = ['utf8mb4', 'utf8mb4_bin'];
        return [
            'id'=>['bigint unsigned','NO',null,'auto_increment',null,null],
            'event_id'=>['char(64)','NO',null,'',...$b],
            'aggregate_type'=>['varchar(32)','NO',null,'',...$b],
            'aggregate_id'=>['bigint unsigned','NO',null,'',null,null],
            'event_type'=>['varchar(64)','NO',null,'',...$b],
            'organization'=>['int unsigned','NO',null,'',null,null],
            'target_user_id'=>['varchar(64)','NO',null,'',...$b],
            'payload_json'=>['longtext','NO',null,'',...$b],
            'traceparent'=>['char(55)','YES',null,'',...$b],
            'tracestate'=>['varchar(512)','YES',null,'',...$b],
            'status'=>['tinyint unsigned','NO','1','',null,null],
            'retry_count'=>['int unsigned','NO','0','',null,null],
            'next_retry_at'=>['datetime','YES',null,'',null,null],
            'locked_until'=>['datetime','YES',null,'',null,null],
            'worker_id'=>['varchar(64)','YES',null,'',...$b],
            'claim_token'=>['char(40)','YES',null,'',...$b],
            'published_at'=>['datetime','YES',null,'',null,null],
            'last_error'=>['varchar(500)','YES',null,'',...$b],
            'create_time'=>['datetime','NO',null,'',null,null],
            'update_time'=>['datetime','NO',null,'',null,null],
        ];
    }

    private function expectedIndexes(): array
    {
        $column = static fn (
            int $nonUnique,
            int $sequence,
            string $name,
            string $nullable = '',
        ): array => [
            $nonUnique, $sequence, $name, 'A', null, null, $nullable,
            'btree', '', '', 'YES', null,
        ];

        return [
            'PRIMARY'=>[$column(0,1,'id')],
            'idx_realtime_control_claim'=>[
                $column(1,1,'status'),$column(1,2,'next_retry_at','YES'),
                $column(1,3,'locked_until','YES'),$column(1,4,'id'),
            ],
            'uni_realtime_control_event'=>[$column(0,1,'event_id')],
            'uni_realtime_control_transition'=>[
                $column(0,1,'aggregate_type'),$column(0,2,'aggregate_id'),
                $column(0,3,'event_type'),$column(0,4,'organization'),
                $column(0,5,'target_user_id'),
            ],
        ];
    }
}
