<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

/**
 * Cross-organization friend relation/request columns.
 * friend_organization = peer home org (equals organization for same-tenant friends).
 */
final class AddCrossOrganizationFriendColumns extends AbstractMigration
{
    public function up(): void
    {
        if ($this->table('im_friend_relation')->hasColumn('friend_organization') === false) {
            $this->execute(<<<'SQL'
ALTER TABLE `im_friend_relation`
  ADD COLUMN `friend_organization` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '好友所属机构；同租户时等于 organization' AFTER `friend_user_id`,
  ADD KEY `idx_friend_organization_user` (`friend_organization`, `friend_user_id`)
SQL);
            $this->execute(<<<'SQL'
UPDATE `im_friend_relation`
   SET `friend_organization` = `organization`
 WHERE `friend_organization` = 0
SQL);
        }

        if ($this->table('im_friend_request')->hasColumn('from_organization') === false) {
            $this->execute(<<<'SQL'
ALTER TABLE `im_friend_request`
  ADD COLUMN `from_organization` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '申请人所属机构' AFTER `organization`,
  ADD COLUMN `to_organization` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '接收人所属机构' AFTER `from_organization`,
  ADD KEY `idx_from_organization_user` (`from_organization`, `from_user_id`),
  ADD KEY `idx_to_organization_user` (`to_organization`, `to_user_id`)
SQL);
            $this->execute(<<<'SQL'
UPDATE `im_friend_request`
   SET `from_organization` = `organization`,
       `to_organization` = `organization`
 WHERE `from_organization` = 0 OR `to_organization` = 0
SQL);
        }
    }

    public function down(): void
    {
        if ($this->table('im_friend_relation')->hasColumn('friend_organization')) {
            $this->execute('ALTER TABLE `im_friend_relation` DROP COLUMN `friend_organization`');
        }
        if ($this->table('im_friend_request')->hasColumn('from_organization')) {
            $this->execute('ALTER TABLE `im_friend_request` DROP COLUMN `from_organization`, DROP COLUMN `to_organization`');
        }
    }
}
