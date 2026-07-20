<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

/**
 * Makes every IM user reference a global (organization, user_id) identity and
 * introduces the canonical sequence owner for cross-organization single chat.
 *
 * The development branch intentionally removes every existing single-chat
 * projection. Both the old same-org and incomplete cross-org IDs used a
 * different identity rule, so preserving either would create ambiguous history.
 */
final class CloseCrossOrganizationImIdentity extends AbstractMigration
{
    public function up(): void
    {
        $messageTables = $this->messageTables();
        foreach ($messageTables as $table) {
            $this->execute(sprintf(
                'DELETE FROM `%s` WHERE conversation_type = 1',
                $table,
            ));
        }
        $this->execute(<<<'SQL'
DELETE FROM `im_message_outbox`
 WHERE conversation_type = 1
    OR event_type IN ('message.receipt', 'conversation.read', 'conversation.access_changed')
SQL);
        foreach ([
            'im_message_change',
            'im_message_user_delete',
            'im_message_receipt',
            'im_message_index',
            'im_conversation_membership_period',
            'im_conversation_member',
        ] as $table) {
            $this->execute(sprintf(
                'DELETE target FROM `%1$s` target '
                . 'INNER JOIN `im_conversation` c '
                . 'ON c.organization = target.organization '
                . 'AND c.conversation_id = target.conversation_id '
                . 'WHERE c.conversation_type = 1',
                $table,
            ));
        }
        $this->execute('DELETE FROM `im_conversation` WHERE conversation_type = 1');

        $this->execute(<<<'SQL'
CREATE TABLE `im_cross_organization_conversation` (
  `conversation_id` varchar(64) NOT NULL COMMENT '按双方完整身份生成的单聊会话ID',
  `left_organization` int(11) UNSIGNED NOT NULL COMMENT '字节序较小身份的机构',
  `left_user_id` varchar(64) NOT NULL COMMENT '字节序较小身份的用户ID',
  `right_organization` int(11) UNSIGNED NOT NULL COMMENT '字节序较大身份的机构',
  `right_user_id` varchar(64) NOT NULL COMMENT '字节序较大身份的用户ID',
  `next_message_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 1 COMMENT '双方home共享的下一消息序号',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1正常,2停用',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`conversation_id`) USING BTREE,
	  UNIQUE KEY `uni_cross_org_identity_pair`
	    (`left_organization`, `left_user_id`, `right_organization`, `right_user_id`) USING BTREE,
	  CONSTRAINT `chk_cross_org_left_organization_positive` CHECK (`left_organization` > 0),
	  CONSTRAINT `chk_cross_org_right_organization_positive` CHECK (`right_organization` > 0),
	  CONSTRAINT `chk_cross_org_distinct_organizations`
	    CHECK (`left_organization` <> `right_organization`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='跨机构单聊身份与权威消息序号' ROW_FORMAT=DYNAMIC
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation`
  ADD COLUMN `owner_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'owner_user_id所属机构；无owner为0' AFTER `owner_user_id`
SQL);
        $this->execute(<<<'SQL'
UPDATE `im_conversation`
   SET `owner_organization` = CASE WHEN COALESCE(`owner_user_id`, '') = '' THEN 0 ELSE `organization` END
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation`
  ADD CONSTRAINT `chk_im_conversation_owner_identity_complete`
    CHECK (
      (`owner_organization` = 0 AND COALESCE(`owner_user_id`, '') = '')
      OR
      (`owner_organization` > 0 AND COALESCE(`owner_user_id`, '') <> '')
    )
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation_member`
  ADD COLUMN `member_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'user_id所属机构' AFTER `user_id`,
  ADD COLUMN `inviter_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'inviter_user_id所属机构；无邀请人为0' AFTER `inviter_user_id`,
  DROP INDEX `uni_organization_member`,
  ADD UNIQUE KEY `uni_home_conversation_member_identity`
    (`organization`, `conversation_id`, `member_organization`, `user_id`),
  ADD KEY `idx_member_identity`
    (`member_organization`, `user_id`, `status`, `conversation_id`)
SQL);
        $this->execute(<<<'SQL'
UPDATE `im_conversation_member`
   SET `member_organization` = `organization`,
       `inviter_organization` = CASE WHEN COALESCE(`inviter_user_id`, '') = '' THEN 0 ELSE `organization` END
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation_member`
  MODIFY COLUMN `member_organization` int(11) UNSIGNED NOT NULL COMMENT 'user_id所属机构',
  ADD CONSTRAINT `chk_im_member_organization_positive` CHECK (`member_organization` > 0),
  ADD CONSTRAINT `chk_im_member_inviter_identity_complete`
    CHECK (
      (`inviter_organization` = 0 AND COALESCE(`inviter_user_id`, '') = '')
      OR
      (`inviter_organization` > 0 AND COALESCE(`inviter_user_id`, '') <> '')
    )
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation_membership_period`
  ADD COLUMN `member_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'user_id所属机构' AFTER `user_id`,
  DROP INDEX `uni_organization_membership_period`,
  ADD UNIQUE KEY `uni_home_membership_period_identity`
    (`organization`, `conversation_id`, `member_organization`, `user_id`, `period_no`),
  ADD KEY `idx_membership_identity`
    (`member_organization`, `user_id`, `conversation_id`, `status`)
SQL);
        $this->execute(
            'UPDATE `im_conversation_membership_period` SET `member_organization` = `organization`',
        );
        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation_membership_period`
  MODIFY COLUMN `member_organization` int(11) UNSIGNED NOT NULL COMMENT 'user_id所属机构',
  ADD CONSTRAINT `chk_im_membership_organization_positive` CHECK (`member_organization` > 0)
SQL);

        foreach ($messageTables as $table) {
            $this->execute(sprintf(
                'ALTER TABLE `%s` ADD COLUMN `sender_organization` int(11) UNSIGNED NOT NULL DEFAULT 0 '
                . "COMMENT 'sender_id所属机构' AFTER `sender_id`",
                $table,
            ));
            $this->execute(sprintf(
                'UPDATE `%s` SET `sender_organization` = `organization`',
                $table,
            ));
            $this->execute(sprintf(
                'ALTER TABLE `%1$s` MODIFY COLUMN `sender_organization` int(11) UNSIGNED NOT NULL '
                . "COMMENT 'sender_id所属机构', "
                . 'ADD CONSTRAINT `chk_%2$s_sender_organization_positive` CHECK (`sender_organization` > 0)',
                $table,
                $table,
            ));
        }

        $this->execute(<<<'SQL'
ALTER TABLE `im_message_index`
  ADD COLUMN `sender_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'sender_id所属机构' AFTER `sender_id`,
  DROP INDEX `uni_organization_client_msg`,
  ADD UNIQUE KEY `uni_home_sender_client_msg`
    (`organization`, `sender_organization`, `sender_id`, `client_msg_id`)
SQL);
        $this->execute(
            'UPDATE `im_message_index` SET `sender_organization` = `organization`',
        );
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_index`
  MODIFY COLUMN `sender_organization` int(11) UNSIGNED NOT NULL COMMENT 'sender_id所属机构',
  ADD CONSTRAINT `chk_im_message_index_sender_organization_positive`
    CHECK (`sender_organization` > 0)
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_message_receipt`
  ADD COLUMN `user_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'user_id所属机构' AFTER `user_id`,
  DROP INDEX `uni_organization_receipt_status`,
  ADD UNIQUE KEY `uni_home_receipt_identity_status`
    (`organization`, `message_id`, `user_organization`, `user_id`, `status`),
  ADD KEY `idx_receipt_identity_status`
    (`user_organization`, `user_id`, `status`)
SQL);
        $this->execute(
            'UPDATE `im_message_receipt` SET `user_organization` = `organization`',
        );
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_receipt`
  MODIFY COLUMN `user_organization` int(11) UNSIGNED NOT NULL COMMENT 'user_id所属机构',
  ADD CONSTRAINT `chk_im_message_receipt_user_organization_positive`
    CHECK (`user_organization` > 0)
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_message_user_delete`
  ADD COLUMN `user_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'user_id所属机构' AFTER `user_id`,
  DROP INDEX `uni_organization_user_message`,
  ADD UNIQUE KEY `uni_home_user_identity_message`
    (`organization`, `message_id`, `user_organization`, `user_id`)
SQL);
        $this->execute(
            'UPDATE `im_message_user_delete` SET `user_organization` = `organization`',
        );
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_user_delete`
  MODIFY COLUMN `user_organization` int(11) UNSIGNED NOT NULL COMMENT 'user_id所属机构',
  ADD CONSTRAINT `chk_im_message_delete_user_organization_positive`
    CHECK (`user_organization` > 0)
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_message_change`
  ADD COLUMN `actor_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT '执行变更用户所属机构' AFTER `change_type`,
  ADD COLUMN `actor_user_id` varchar(64) NOT NULL DEFAULT ''
    COMMENT '执行变更用户ID' AFTER `actor_organization`,
  ADD COLUMN `target_organization` int(11) UNSIGNED NULL DEFAULT NULL
    COMMENT 'target_user_id所属机构；广播变更为NULL' AFTER `target_user_id`
SQL);
        $this->execute(<<<'SQL'
UPDATE `im_message_change` mc
LEFT JOIN `im_message_index` mi
  ON mi.organization = mc.organization
 AND mi.message_id = mc.message_id
   SET mc.actor_organization = mc.organization,
       mc.actor_user_id = CASE
           WHEN mc.change_type = 'delete_self' THEN COALESCE(mc.target_user_id, '')
           ELSE COALESCE(mi.sender_id, '')
       END,
       mc.target_organization = CASE
           WHEN mc.target_user_id IS NULL THEN NULL
           ELSE mc.organization
       END
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_change`
  MODIFY COLUMN `actor_organization` int(11) UNSIGNED NOT NULL COMMENT '执行变更用户所属机构',
  MODIFY COLUMN `actor_user_id` varchar(64) NOT NULL COMMENT '执行变更用户ID',
  ADD CONSTRAINT `chk_im_change_actor_identity_complete`
    CHECK (`actor_organization` > 0 AND `actor_user_id` <> ''),
  ADD CONSTRAINT `chk_im_change_target_identity_complete`
    CHECK (
      (`target_organization` IS NULL AND `target_user_id` IS NULL)
      OR
      (`target_organization` > 0 AND COALESCE(`target_user_id`, '') <> '')
    )
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_friend_relation`
  DROP INDEX `uni_organization_friend_relation`,
  ADD UNIQUE KEY `uni_owner_friend_identity`
    (`organization`, `user_id`, `friend_organization`, `friend_user_id`),
  MODIFY COLUMN `friend_organization` int(11) UNSIGNED NOT NULL
    COMMENT '好友所属机构',
  ADD CONSTRAINT `chk_im_friend_organization_positive` CHECK (`friend_organization` > 0)
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_message_outbox`
  ADD COLUMN `event_id` char(64) NULL DEFAULT NULL
    COMMENT '稳定事件幂等ID' AFTER `id`
SQL);
        $this->execute(<<<'SQL'
UPDATE `im_message_outbox`
   SET `event_id` = SHA2(CONCAT_WS('|', organization, event_type, message_id, change_seq, id), 256)
 WHERE `event_id` IS NULL
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_outbox`
  MODIFY COLUMN `event_id` char(64) NOT NULL COMMENT '稳定事件幂等ID',
  DROP INDEX `uni_organization_event_message_change`,
  ADD UNIQUE KEY `uni_outbox_event_id` (`event_id`)
SQL);
    }

    public function down(): void
    {
        // The old schema cannot represent dual-home/composite identities. A
        // rollback is therefore intentionally destructive for all new single
        // chat state and cross-organization friendships; dropping identity
        // columns first would either violate old unique keys or leave records
        // with a false same-organization identity.
        foreach ($this->messageTables() as $table) {
            $this->execute(sprintf(
                'DELETE FROM `%s` WHERE conversation_type = 1',
                $table,
            ));
        }
        $this->execute(<<<'SQL'
DELETE FROM `im_message_outbox`
 WHERE conversation_type = 1
    OR event_type IN ('message.receipt', 'conversation.read', 'conversation.access_changed')
SQL);
        foreach ([
            'im_message_change',
            'im_message_user_delete',
            'im_message_receipt',
            'im_message_index',
            'im_conversation_membership_period',
            'im_conversation_member',
        ] as $table) {
            $this->execute(sprintf(
                'DELETE target FROM `%1$s` target '
                . 'INNER JOIN `im_conversation` c '
                . 'ON c.organization = target.organization '
                . 'AND c.conversation_id = target.conversation_id '
                . 'WHERE c.conversation_type = 1',
                $table,
            ));
        }
        $this->execute('DELETE FROM `im_conversation` WHERE conversation_type = 1');
        $this->execute('DELETE FROM `im_cross_organization_conversation`');
        $this->execute('DELETE FROM `im_friend_relation` WHERE `friend_organization` <> `organization`');

        $this->execute(<<<'SQL'
ALTER TABLE `im_message_outbox`
  DROP INDEX `uni_outbox_event_id`,
  DROP COLUMN `event_id`,
  ADD UNIQUE KEY `uni_organization_event_message_change`
    (`organization`, `event_type`, `message_id`, `change_seq`)
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_friend_relation`
  DROP CHECK `chk_im_friend_organization_positive`,
  DROP INDEX `uni_owner_friend_identity`,
  MODIFY COLUMN `friend_organization` int(11) UNSIGNED NOT NULL DEFAULT 0
    COMMENT '好友所属机构；同租户时等于 organization',
  ADD UNIQUE KEY `uni_organization_friend_relation`
    (`organization`, `user_id`, `friend_user_id`)
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_change`
  DROP CHECK `chk_im_change_target_identity_complete`,
  DROP CHECK `chk_im_change_actor_identity_complete`,
  DROP COLUMN `target_organization`,
  DROP COLUMN `actor_user_id`,
  DROP COLUMN `actor_organization`
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_user_delete`
  DROP CHECK `chk_im_message_delete_user_organization_positive`,
  DROP INDEX `uni_home_user_identity_message`,
  DROP COLUMN `user_organization`,
  ADD UNIQUE KEY `uni_organization_user_message`
    (`organization`, `message_id`, `user_id`)
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_receipt`
  DROP CHECK `chk_im_message_receipt_user_organization_positive`,
  DROP INDEX `uni_home_receipt_identity_status`,
  DROP INDEX `idx_receipt_identity_status`,
  DROP COLUMN `user_organization`,
  ADD UNIQUE KEY `uni_organization_receipt_status`
    (`organization`, `message_id`, `user_id`, `status`)
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_message_index`
  DROP CHECK `chk_im_message_index_sender_organization_positive`,
  DROP INDEX `uni_home_sender_client_msg`,
  DROP COLUMN `sender_organization`,
  ADD UNIQUE KEY `uni_organization_client_msg`
    (`organization`, `sender_id`, `client_msg_id`)
SQL);
        foreach ($this->messageTables() as $table) {
            $this->execute(sprintf(
                'ALTER TABLE `%1$s` '
                . 'DROP CHECK `chk_%1$s_sender_organization_positive`, '
                . 'DROP COLUMN `sender_organization`',
                $table,
            ));
        }
        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation_membership_period`
  DROP CHECK `chk_im_membership_organization_positive`,
  DROP INDEX `uni_home_membership_period_identity`,
  DROP INDEX `idx_membership_identity`,
  DROP COLUMN `member_organization`,
  ADD UNIQUE KEY `uni_organization_membership_period`
    (`organization`, `conversation_id`, `user_id`, `period_no`)
SQL);
        $this->execute(<<<'SQL'
ALTER TABLE `im_conversation_member`
  DROP CHECK `chk_im_member_inviter_identity_complete`,
  DROP CHECK `chk_im_member_organization_positive`,
  DROP INDEX `uni_home_conversation_member_identity`,
  DROP INDEX `idx_member_identity`,
  DROP COLUMN `inviter_organization`,
  DROP COLUMN `member_organization`,
  ADD UNIQUE KEY `uni_organization_member`
    (`organization`, `conversation_id`, `user_id`)
SQL);
        $this->execute(
            'ALTER TABLE `im_conversation` '
            . 'DROP CHECK `chk_im_conversation_owner_identity_complete`, '
            . 'DROP COLUMN `owner_organization`',
        );
        $this->execute('DROP TABLE IF EXISTS `im_cross_organization_conversation`');
    }

    /** @return list<string> */
    private function messageTables(): array
    {
        $rows = $this->fetchAll(
            "SELECT TABLE_NAME AS table_name
               FROM information_schema.TABLES
              WHERE TABLE_SCHEMA = DATABASE()
                AND (
                    TABLE_NAME = 'im_message'
                    OR TABLE_NAME REGEXP '^im_message_[0-9]{4}_[0-9]{6}$'
                )
              ORDER BY TABLE_NAME",
        );

        $tables = [];
        foreach ($rows as $row) {
            $table = (string) ($row['table_name'] ?? '');
            if ($table === 'im_message' || preg_match('/^im_message_\d{4}_\d{6}$/', $table) === 1) {
                $tables[] = $table;
            }
        }

        return $tables;
    }
}
