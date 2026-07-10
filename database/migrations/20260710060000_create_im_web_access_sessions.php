<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

final class CreateImWebAccessSessions extends AbstractMigration
{
    public function up(): void
    {
        $this->execute(<<<'SQL'
CREATE TABLE `im_web_access_session` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `jti` char(32) NOT NULL COMMENT 'Web access token 唯一标识',
  `im_user_id` bigint(20) UNSIGNED NOT NULL COMMENT 'im_user 主键',
  `user_id` varchar(64) NOT NULL COMMENT 'IM用户ID',
  `device_id` varchar(100) NOT NULL COMMENT 'Web登录设备ID',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2撤销',
  `expire_at` datetime NOT NULL COMMENT '失效时间',
  `revoked_at` datetime NULL DEFAULT NULL COMMENT '撤销时间',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_jti` (`organization`, `jti`) USING BTREE,
  KEY `idx_identity_device_status` (`organization`, `user_id`, `device_id`, `status`, `expire_at`) USING BTREE,
  KEY `idx_user_pk_status` (`organization`, `im_user_id`, `status`, `expire_at`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='Web access token 服务端活性会话' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
ALTER TABLE `im_auth_session`
  ADD COLUMN `web_access_jti` char(32) NULL DEFAULT NULL COMMENT '签发该IM凭证的Web access token标识' AFTER `session_id`,
  ADD KEY `idx_web_access_status` (`organization`, `web_access_jti`, `status`, `expire_at`) USING BTREE;
SQL);
    }

    public function down(): void
    {
        $this->execute(<<<'SQL'
ALTER TABLE `im_auth_session`
  DROP INDEX `idx_web_access_status`,
  DROP COLUMN `web_access_jti`;
SQL);
        $this->execute('DROP TABLE IF EXISTS `im_web_access_session`');
    }
}
