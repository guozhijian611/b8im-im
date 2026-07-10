<?php

declare(strict_types=1);

use Phinx\Migration\AbstractMigration;

final class CreateImRuntimeV1 extends AbstractMigration
{
    public function up(): void
    {
        $shardBuckets = $this->shardBuckets();
        $this->execute(<<<'SQL'
CREATE TABLE `im_runtime_config` (
  `config_key` varchar(64) NOT NULL COMMENT '不可变运行时配置键',
  `config_value` varchar(255) NOT NULL COMMENT '配置值',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  PRIMARY KEY (`config_key`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM部署级不可变运行时配置' ROW_FORMAT=DYNAMIC;
SQL);
        $now = date('Y-m-d H:i:s');
        $this->execute(sprintf(
            'INSERT INTO im_runtime_config (config_key, config_value, create_time, update_time) VALUES (%s, %s, %s, %s)',
            $this->quoteValue('message_shard_buckets'),
            $this->quoteValue((string) $shardBuckets),
            $this->quoteValue($now),
            $this->quoteValue($now),
        ));

        $this->execute(<<<'SQL'
CREATE TABLE `im_user` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT 'IM用户ID',
  `im_short_no` varchar(32) NULL DEFAULT NULL COMMENT 'IM短号',
  `account` varchar(64) NOT NULL COMMENT '账号',
  `password_hash` varchar(255) NOT NULL COMMENT 'Web登录密码哈希',
  `nickname` varchar(64) NOT NULL COMMENT '昵称',
  `avatar` varchar(255) NULL DEFAULT NULL COMMENT '头像附件file_id',
  `mobile` varchar(32) NULL DEFAULT NULL COMMENT '手机号',
  `email` varchar(120) NULL DEFAULT NULL COMMENT '邮箱',
  `gender` tinyint(3) UNSIGNED NOT NULL DEFAULT 0 COMMENT '性别',
  `is_system` tinyint(3) UNSIGNED NOT NULL DEFAULT 2 COMMENT '1系统用户,2普通用户',
  `system_code` varchar(64) NULL DEFAULT NULL COMMENT '系统用户代码',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1正常,2停用,3封禁',
  `remark` varchar(255) NULL DEFAULT NULL COMMENT '备注',
  `login_time` datetime NULL DEFAULT NULL COMMENT '最后登录时间',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_user` (`organization`, `user_id`) USING BTREE,
  UNIQUE KEY `uni_organization_account` (`organization`, `account`) USING BTREE,
  UNIQUE KEY `uni_platform_im_short_no` (`im_short_no`) USING BTREE,
  KEY `idx_organization_mobile` (`organization`, `mobile`) USING BTREE,
  KEY `idx_organization_status` (`organization`, `status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_user_profile` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `signature` varchar(255) NULL DEFAULT NULL COMMENT '个性签名',
  `moments_cover_url` varchar(500) NULL DEFAULT NULL COMMENT '朋友圈封面',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2停用',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_user_profile` (`organization`, `user_id`) USING BTREE,
  KEY `idx_organization_status` (`organization`, `status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户扩展资料' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_user_privacy_setting` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `allow_add_by_mobile` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1允许,2禁止',
  `allow_add_by_short_no` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1允许,2禁止',
  `allow_add_by_username` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1允许,2禁止',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_user_privacy` (`organization`, `user_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户社交隐私设置' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_user_security_policy` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `login_ip_policy` varchar(24) NOT NULL DEFAULT 'disabled' COMMENT 'disabled,allow_all,whitelist_only',
  `login_ip_whitelist_json` longtext NULL COMMENT 'CIDR或单IP白名单JSON',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2停用',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_user_security` (`organization`, `user_id`) USING BTREE,
  KEY `idx_organization_policy_status` (`organization`, `login_ip_policy`, `status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户登录IP安全策略' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_friend_relation` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '关系持有人用户ID',
  `friend_user_id` varchar(64) NOT NULL COMMENT '好友用户ID',
  `add_method` varchar(32) NOT NULL COMMENT 'mobile,short_no,username,qr,group,admin,auto',
  `added_at` datetime NOT NULL COMMENT '好友关系建立时间',
  `remark_name` varchar(64) NULL DEFAULT NULL COMMENT '联系人昵称备注',
  `card_remark` varchar(255) NULL DEFAULT NULL COMMENT '联系人卡片补充备注',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1正常,2拉黑',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_friend_relation` (`organization`, `user_id`, `friend_user_id`) USING BTREE,
  KEY `idx_organization_user_status` (`organization`, `user_id`, `status`) USING BTREE,
  KEY `idx_organization_friend` (`organization`, `friend_user_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM同机构好友关系' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_friend_request` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `from_user_id` varchar(64) NOT NULL COMMENT '申请人用户ID',
  `to_user_id` varchar(64) NOT NULL COMMENT '接收人用户ID',
  `add_method` varchar(32) NOT NULL COMMENT 'mobile,short_no,username,qr,group,admin,auto',
  `message` varchar(120) NULL DEFAULT NULL COMMENT '申请验证消息',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1待处理,2已通过,3已拒绝,4已取消',
  `handle_time` datetime NULL DEFAULT NULL COMMENT '处理时间',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_organization_to_status` (`organization`, `to_user_id`, `status`, `create_time`) USING BTREE,
  KEY `idx_organization_from_status` (`organization`, `from_user_id`, `status`, `create_time`) USING BTREE,
  KEY `idx_organization_pair_status` (`organization`, `from_user_id`, `to_user_id`, `status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM同机构好友申请' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_user_device` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `device_id` varchar(100) NOT NULL COMMENT '设备实例ID',
  `client_family` varchar(32) NOT NULL COMMENT 'web,app,desktop',
  `os` varchar(32) NOT NULL COMMENT 'browser,android,ios,windows,macos,linux,other',
  `client_id` varchar(120) NULL DEFAULT NULL COMMENT '当前Gateway连接ID',
  `session_id` char(32) NULL DEFAULT NULL COMMENT '当前连接会话ID',
  `device_name` varchar(120) NULL DEFAULT NULL COMMENT '设备名称',
  `device_model` varchar(120) NULL DEFAULT NULL COMMENT '设备型号',
  `os_version` varchar(64) NULL DEFAULT NULL COMMENT '系统版本',
  `app_version` varchar(64) NULL DEFAULT NULL COMMENT '客户端版本',
  `current_ip` varchar(45) NULL DEFAULT NULL COMMENT '当前连接IP',
  `current_ip_geo` varchar(255) NULL DEFAULT NULL COMMENT '当前IP归属地',
  `last_login_ip` varchar(45) NULL DEFAULT NULL COMMENT '最后登录IP',
  `last_login_ip_geo` varchar(255) NULL DEFAULT NULL COMMENT '最后登录IP归属地',
  `last_login_at` datetime NULL DEFAULT NULL COMMENT '最后登录时间',
  `last_seen_at` datetime NULL DEFAULT NULL COMMENT '最后活跃时间',
  `current_online_state` tinyint(3) UNSIGNED NOT NULL DEFAULT 2 COMMENT '1在线,2离线',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1正常,2停用',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_user_device` (`organization`, `user_id`, `device_id`) USING BTREE,
  KEY `idx_organization_client` (`organization`, `client_id`) USING BTREE,
  KEY `idx_organization_status` (`organization`, `status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户设备表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_user_login_audit` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `device_id` varchar(100) NULL DEFAULT NULL COMMENT '设备ID',
  `client_id` varchar(120) NULL DEFAULT NULL COMMENT 'Gateway连接ID',
  `client_family` varchar(32) NULL DEFAULT NULL COMMENT 'web,app,desktop',
  `os` varchar(32) NULL DEFAULT NULL COMMENT 'browser,android,ios,windows,macos,linux,other',
  `device_name` varchar(120) NULL DEFAULT NULL COMMENT '设备名称',
  `device_model` varchar(120) NULL DEFAULT NULL COMMENT '设备型号',
  `os_version` varchar(64) NULL DEFAULT NULL COMMENT '系统版本',
  `app_version` varchar(64) NULL DEFAULT NULL COMMENT '客户端版本',
  `login_ip` varchar(45) NULL DEFAULT NULL COMMENT '登录IP',
  `login_ip_geo` varchar(255) NULL DEFAULT NULL COMMENT '登录IP归属地',
  `login_at` datetime NOT NULL COMMENT '登录或尝试登录时间',
  `logout_at` datetime NULL DEFAULT NULL COMMENT '退出时间',
  `login_result` varchar(32) NOT NULL COMMENT 'success,failed,kicked,logout,inactive',
  `audit_scope` varchar(32) NOT NULL DEFAULT 'login' COMMENT 'login,refresh,disconnect,policy',
  `current_online_state` tinyint(3) UNSIGNED NOT NULL DEFAULT 2 COMMENT '事件发生后的1在线,2离线',
  `failure_code` varchar(64) NULL DEFAULT NULL COMMENT '失败错误码',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_organization_user_login_at` (`organization`, `user_id`, `login_at`) USING BTREE,
  KEY `idx_organization_login_ip` (`organization`, `login_ip`) USING BTREE,
  KEY `idx_organization_device_login_at` (`organization`, `device_id`, `login_at`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户登录设备与IP永久审计' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_auth_session` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `device_id` varchar(100) NOT NULL COMMENT '设备ID',
  `client_id` varchar(120) NOT NULL COMMENT '预绑定Gateway连接ID',
  `session_id` varchar(128) NOT NULL COMMENT '控制面签发的凭证会话ID',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2撤销',
  `expire_at` datetime NOT NULL COMMENT '失效时间',
  `revoked_at` datetime NULL DEFAULT NULL COMMENT '撤销时间',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_session` (`organization`, `session_id`) USING BTREE,
  UNIQUE KEY `uni_organization_client` (`organization`, `client_id`) USING BTREE,
  KEY `idx_identity_status` (`organization`, `user_id`, `device_id`, `status`, `expire_at`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM凭证会话表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_upload_asset` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `file_id` char(40) NOT NULL COMMENT 'Server 签发的附件ID',
  `user_id` varchar(64) NOT NULL COMMENT '上传用户ID',
  `kind` varchar(16) NOT NULL COMMENT 'image,file,voice,video',
  `name` varchar(255) NOT NULL COMMENT '原始文件名',
  `url` varchar(1024) NOT NULL COMMENT '已验证访问地址',
  `storage_path` varchar(512) NOT NULL COMMENT '对象存储路径',
  `size_byte` bigint(20) UNSIGNED NOT NULL COMMENT '字节数',
  `mime_type` varchar(255) NOT NULL DEFAULT '' COMMENT 'MIME',
  `extension` varchar(32) NOT NULL DEFAULT '' COMMENT '扩展名',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2禁用',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_file` (`organization`, `file_id`) USING BTREE,
  KEY `idx_organization_user_status` (`organization`, `user_id`, `status`, `id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM 可信上传附件' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_conversation` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `conversation_type` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1单聊,2群聊',
  `title` varchar(100) NULL DEFAULT NULL COMMENT '会话标题',
  `avatar` varchar(255) NULL DEFAULT NULL COMMENT '会话头像附件file_id',
  `owner_user_id` varchar(64) NULL DEFAULT NULL COMMENT '群主/创建人',
  `next_message_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 1 COMMENT '下一会话消息序号',
  `last_message_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT '最后消息序号',
  `next_change_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 1 COMMENT '下一变更序号',
  `last_change_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT '最后变更序号',
  `last_message_id` varchar(40) NULL DEFAULT NULL COMMENT '最后消息ID',
  `last_message_time` datetime NULL DEFAULT NULL COMMENT '最后消息时间',
  `last_message_summary` varchar(255) NULL DEFAULT NULL COMMENT '最后消息摘要',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1正常,2停用',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_conversation` (`organization`, `conversation_id`) USING BTREE,
  KEY `idx_organization_last_message` (`organization`, `last_message_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM会话表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_group_profile` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '群会话ID',
  `owner_user_id` varchar(64) NOT NULL COMMENT '群主用户ID',
  `group_kind` varchar(16) NOT NULL DEFAULT 'normal' COMMENT 'normal,super',
  `history_visibility` varchar(16) NOT NULL DEFAULT 'since_join' COMMENT 'since_join,all',
  `display_member_count` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '0使用真实人数或租户默认值',
  `description` varchar(500) NULL DEFAULT NULL COMMENT '群说明',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2停用,3解散',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_group_profile` (`organization`, `conversation_id`) USING BTREE,
  KEY `idx_organization_owner_status` (`organization`, `owner_user_id`, `status`) USING BTREE,
  KEY `idx_organization_kind_status` (`organization`, `group_kind`, `status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM群资料与群规模模型' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_conversation_member` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `user_id` varchar(64) NOT NULL COMMENT '成员ID',
  `member_role` varchar(16) NOT NULL DEFAULT 'member' COMMENT 'owner,admin,member',
  `inviter_user_id` varchar(64) NULL DEFAULT NULL COMMENT '邀请人用户ID',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2已退出,3已移出',
  `mute_status` tinyint(3) UNSIGNED NOT NULL DEFAULT 0 COMMENT '0正常,1禁言',
  `mute_until` datetime NULL DEFAULT NULL COMMENT '禁言截止',
  `last_read_message_id` varchar(40) NULL DEFAULT NULL COMMENT '最后已读消息',
  `last_read_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT '最后已读序号',
  `unread_count` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '未读数',
  `is_pinned` tinyint(3) UNSIGNED NOT NULL DEFAULT 2 COMMENT '1置顶,2否',
  `is_muted` tinyint(3) UNSIGNED NOT NULL DEFAULT 2 COMMENT '1免打扰,2否',
  `conversation_remark` varchar(100) NULL DEFAULT NULL COMMENT '仅当前用户可见的会话备注',
  `message_group_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前用户的会话自定义分组ID',
  `access_version` bigint(20) UNSIGNED NOT NULL DEFAULT 1 COMMENT '会话访问权限版本',
  `join_at` datetime NULL DEFAULT NULL COMMENT '加入时间',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_member` (`organization`, `conversation_id`, `user_id`) USING BTREE,
  KEY `idx_organization_user` (`organization`, `user_id`, `status`, `conversation_id`) USING BTREE,
  KEY `idx_organization_user_group` (`organization`, `user_id`, `message_group_id`, `update_time`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM会话成员表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_message_group` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `name` varchar(40) NOT NULL COMMENT '分组名称',
  `sort` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '排序',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1正常,2禁用',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_user_group_name` (`organization`, `user_id`, `name`) USING BTREE,
  KEY `idx_organization_user_status_sort` (`organization`, `user_id`, `status`, `sort`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户会话自定义分组' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_conversation_membership_period` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `user_id` varchar(64) NOT NULL COMMENT '成员ID',
  `period_no` int(11) UNSIGNED NOT NULL COMMENT '成员在该会话的第几段可见周期',
  `visible_from_message_seq` bigint(20) UNSIGNED NOT NULL COMMENT '可见起始消息序号，包含',
  `visible_until_message_seq` bigint(20) UNSIGNED NULL DEFAULT NULL COMMENT '可见截止消息序号，包含；NULL表示当前仍在会话中',
  `join_at` datetime NOT NULL COMMENT '加入时间',
  `leave_at` datetime NULL DEFAULT NULL COMMENT '退出时间',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1有效,2已撤销',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_membership_period` (`organization`, `conversation_id`, `user_id`, `period_no`) USING BTREE,
  KEY `idx_organization_user_conversation_status` (`organization`, `user_id`, `conversation_id`, `status`) USING BTREE,
  KEY `idx_organization_conversation_visibility` (`organization`, `conversation_id`, `user_id`, `visible_from_message_seq`, `visible_until_message_seq`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM会话成员历史可见周期' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_message` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '分片内主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `conversation_type` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1单聊,2群聊',
  `message_id` varchar(40) NOT NULL COMMENT '服务端消息ID',
  `message_seq` bigint(20) UNSIGNED NOT NULL COMMENT '会话内序号',
  `client_msg_id` varchar(80) NOT NULL COMMENT '客户端幂等ID',
  `sender_id` varchar(64) NOT NULL COMMENT '发送人',
  `message_type` tinyint(3) UNSIGNED NOT NULL COMMENT '消息类型',
  `content` longtext NULL COMMENT '消息JSON',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1正常,2已撤回,3双向删除',
  `edit_time` datetime NULL DEFAULT NULL COMMENT '最后编辑时间',
  `edit_count` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '编辑次数',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  `delete_time` datetime NULL DEFAULT NULL COMMENT '软删除时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_message` (`organization`, `message_id`) USING BTREE,
  UNIQUE KEY `uni_organization_conversation_seq` (`organization`, `conversation_id`, `message_seq`) USING BTREE,
  KEY `idx_organization_conversation_seq` (`organization`, `conversation_id`, `message_seq`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM消息分片模板表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_organization_message_sequence` (
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `next_global_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 1 COMMENT '下一机构全局序号',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`organization`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM机构全局消息序号' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_message_index` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '索引主键，不是同步游标',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `global_seq` bigint(20) UNSIGNED NOT NULL COMMENT '机构全局序号',
  `message_id` varchar(40) NOT NULL COMMENT '消息ID',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `message_seq` bigint(20) UNSIGNED NOT NULL COMMENT '会话序号',
  `sender_id` varchar(64) NOT NULL COMMENT '发送人',
  `client_msg_id` varchar(80) NOT NULL COMMENT '客户端幂等ID',
  `storage_node` varchar(64) NOT NULL COMMENT '存储节点',
  `shard_table` varchar(64) NOT NULL COMMENT '消息分片表',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_global_seq` (`organization`, `global_seq`) USING BTREE,
  UNIQUE KEY `uni_organization_message` (`organization`, `message_id`) USING BTREE,
  UNIQUE KEY `uni_organization_client_msg` (`organization`, `sender_id`, `client_msg_id`) USING BTREE,
  UNIQUE KEY `uni_organization_conversation_seq` (`organization`, `conversation_id`, `message_seq`) USING BTREE,
  KEY `idx_organization_conversation_global` (`organization`, `conversation_id`, `global_seq`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM消息全局索引表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_message_receipt` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `message_id` varchar(40) NOT NULL COMMENT '消息ID',
  `user_id` varchar(64) NOT NULL COMMENT '用户ID',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1已发送,2已送达,3已读',
  `delivered_time` datetime NULL DEFAULT NULL COMMENT '送达时间',
  `read_time` datetime NULL DEFAULT NULL COMMENT '已读时间',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_receipt_status` (`organization`, `message_id`, `user_id`, `status`) USING BTREE,
  KEY `idx_organization_user_status` (`organization`, `user_id`, `status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM消息回执表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_message_user_delete` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `message_id` varchar(40) NOT NULL COMMENT '消息ID',
  `user_id` varchar(64) NOT NULL COMMENT '删除用户ID',
  `delete_time` datetime NOT NULL COMMENT '删除时间',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_user_message` (`organization`, `message_id`, `user_id`) USING BTREE,
  KEY `idx_organization_conversation_user` (`organization`, `conversation_id`, `user_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM用户单向删除表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_message_change` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `change_seq` bigint(20) UNSIGNED NOT NULL COMMENT '会话内变更序号',
  `message_id` varchar(40) NOT NULL COMMENT '目标消息ID',
  `message_seq` bigint(20) UNSIGNED NOT NULL COMMENT '目标消息序号',
  `change_type` varchar(32) NOT NULL COMMENT 'recall,edit,delete_both,delete_self',
  `target_user_id` varchar(64) NULL DEFAULT NULL COMMENT 'delete_self的目标用户，广播变更为空',
  `payload_json` longtext NOT NULL COMMENT '不含原始敏感内容的变更载荷JSON',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_conversation_change` (`organization`, `conversation_id`, `change_seq`) USING BTREE,
  KEY `idx_organization_conversation_target_change` (`organization`, `conversation_id`, `target_user_id`, `change_seq`) USING BTREE,
  KEY `idx_organization_message` (`organization`, `message_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM会话消息变更流' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(<<<'SQL'
CREATE TABLE `im_message_outbox` (
  `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  `organization` int(11) UNSIGNED NOT NULL COMMENT '机构编号',
  `event_type` varchar(50) NOT NULL COMMENT '事件类型',
  `routing_key` varchar(100) NOT NULL COMMENT 'MQ路由键',
  `message_id` varchar(40) NOT NULL COMMENT '消息ID',
  `change_seq` bigint(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT '消息创建为0，消息变更为对应change_seq',
  `conversation_id` varchar(64) NOT NULL COMMENT '会话ID',
  `conversation_type` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '会话类型',
  `payload_json` longtext NOT NULL COMMENT '事件JSON',
  `status` tinyint(3) UNSIGNED NOT NULL DEFAULT 1 COMMENT '1待发布,2发布中,3已发布,4失败',
  `retry_count` int(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '重试次数',
  `next_retry_at` datetime NULL DEFAULT NULL COMMENT '下次重试时间',
  `locked_until` datetime NULL DEFAULT NULL COMMENT 'claim租约到期时间',
  `worker_id` varchar(64) NULL DEFAULT NULL COMMENT '当前claim worker',
  `claim_token` char(40) NULL DEFAULT NULL COMMENT '当前claim随机令牌',
  `published_at` datetime NULL DEFAULT NULL COMMENT '发布时间',
  `last_error` varchar(500) NULL DEFAULT NULL COMMENT '最后错误',
  `create_time` datetime NULL DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uni_organization_event_message_change` (`organization`, `event_type`, `message_id`, `change_seq`) USING BTREE,
  KEY `idx_pending` (`status`, `next_retry_at`, `locked_until`, `id`) USING BTREE,
  KEY `idx_organization_message` (`organization`, `message_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='IM消息Outbox表' ROW_FORMAT=DYNAMIC;
SQL);

        $this->execute(sprintf(
            "INSERT INTO im_organization_message_sequence (organization, next_global_seq, create_time, update_time) "
            . "SELECT id, 1, %s, %s FROM sm_system_organization WHERE status = 1 AND delete_time IS NULL",
            $this->quoteValue($now),
            $this->quoteValue($now),
        ));

        foreach ($this->writeMonths() as $month) {
            for ($bucket = 0; $bucket < $shardBuckets; $bucket++) {
                $table = sprintf('im_message_%04d_%s', $bucket, $month);
                $this->execute(sprintf('CREATE TABLE `%s` LIKE `im_message`', $table));
            }
        }
    }

    public function down(): void
    {
        $rows = $this->fetchAll(
            "SELECT TABLE_NAME AS table_name FROM information_schema.TABLES "
            . "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME REGEXP '^im_message_[0-9]{4}_[0-9]{6}$'",
        );
        foreach ($rows as $row) {
            $table = (string) ($row['table_name'] ?? '');
            if (preg_match('/^im_message_[0-9]{4}_[0-9]{6}$/', $table) === 1) {
                $this->execute(sprintf('DROP TABLE `%s`', $table));
            }
        }

        foreach ([
            'im_message_outbox',
            'im_message_change',
            'im_message_user_delete',
            'im_message_receipt',
            'im_message_index',
            'im_organization_message_sequence',
            'im_message',
            'im_conversation_membership_period',
            'im_message_group',
            'im_conversation_member',
            'im_group_profile',
            'im_conversation',
            'im_upload_asset',
            'im_auth_session',
            'im_user_login_audit',
            'im_user_device',
            'im_friend_request',
            'im_friend_relation',
            'im_user_security_policy',
            'im_user_privacy_setting',
            'im_user_profile',
            'im_user',
            'im_runtime_config',
        ] as $table) {
            $this->execute(sprintf('DROP TABLE IF EXISTS `%s`', $table));
        }
    }

    /**
     * @return list<string>
     */
    private function writeMonths(): array
    {
        return [
            date('Ym'),
            date('Ym', strtotime('first day of next month')),
        ];
    }

    private function quoteValue(string $value): string
    {
        return $this->getAdapter()->getConnection()->quote($value);
    }

    private function shardBuckets(): int
    {
        $value = $_ENV['IM_MESSAGE_SHARD_BUCKETS']
            ?? $_SERVER['IM_MESSAGE_SHARD_BUCKETS']
            ?? getenv('IM_MESSAGE_SHARD_BUCKETS');

        return min(1024, max(1, (int) ($value === false || $value === null ? 64 : $value)));
    }
}
