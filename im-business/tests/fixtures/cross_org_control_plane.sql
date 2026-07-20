CREATE TABLE `sm_system_organization` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `enterprise_code` varchar(64) NOT NULL,
  `deployment_id` varchar(64) NOT NULL,
  `config_version` bigint unsigned NOT NULL DEFAULT 1,
  `title` varchar(255) DEFAULT NULL,
  `favicon` varchar(512) NOT NULL DEFAULT '',
  `icp` varchar(128) NOT NULL DEFAULT '',
  `public_security_record_no` varchar(128) NOT NULL DEFAULT '',
  `public_security_record_url` varchar(512) NOT NULL DEFAULT '',
  `copyright` varchar(255) NOT NULL DEFAULT '',
  `android_download_url` varchar(512) NOT NULL DEFAULT '',
  `ios_download_url` varchar(512) NOT NULL DEFAULT '',
  `user_agreement_title` varchar(128) NOT NULL DEFAULT '用户协议',
  `privacy_policy_title` varchar(128) NOT NULL DEFAULT '隐私政策',
  `organization_name` varchar(255) DEFAULT NULL,
  `status` smallint DEFAULT 1,
  `is_init` tinyint DEFAULT 2,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `delete_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_sm_org_enterprise_code` (`enterprise_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `sm_tenant_im_policy` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `organization` int unsigned NOT NULL,
  `allowed_client_families_json` json NOT NULL,
  `allow_multi_device_online` tinyint unsigned NOT NULL DEFAULT 1,
  `max_online_devices` int unsigned NOT NULL DEFAULT 5,
  `same_device_login_policy` varchar(16) NOT NULL DEFAULT 'replace',
  `cross_device_login_policy` varchar(16) NOT NULL DEFAULT 'allow',
  `max_message_concurrency` int unsigned NOT NULL DEFAULT 8,
  `max_message_qps` int unsigned NOT NULL DEFAULT 20,
  `default_group_display_member_count` int unsigned NOT NULL DEFAULT 50,
  `message_recall_window_seconds` int unsigned NOT NULL DEFAULT 120,
  `message_edit_window_seconds` int unsigned NOT NULL DEFAULT 120,
  `recall_notice_enabled` tinyint unsigned NOT NULL DEFAULT 1,
  `group_recall_notice_enabled` tinyint unsigned NOT NULL DEFAULT 1,
  `status` varchar(16) NOT NULL DEFAULT 'ENABLED',
  `version` bigint unsigned NOT NULL DEFAULT 1,
  `create_time` datetime NOT NULL,
  `update_time` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_sm_tenant_im_policy_organization` (`organization`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `sm_system_config_group` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(50) DEFAULT NULL,
  `code` varchar(100) DEFAULT NULL,
  `type` tinyint DEFAULT 1,
  `remark` varchar(255) DEFAULT NULL,
  `created_by` int DEFAULT NULL,
  `updated_by` int DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `delete_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_sm_system_config_group_code` (`code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `sm_system_config` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `group_id` int DEFAULT NULL,
  `key` varchar(32) NOT NULL,
  `value` text DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `input_type` varchar(32) DEFAULT NULL,
  `config_select_data` varchar(500) DEFAULT NULL,
  `sort` smallint unsigned DEFAULT 0,
  `remark` varchar(255) DEFAULT NULL,
  `created_by` int DEFAULT NULL,
  `updated_by` int DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `delete_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`, `key`),
  KEY `idx_sm_system_config_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

CREATE TABLE `sm_tenant_config` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `organization` int DEFAULT NULL,
  `group_id` int DEFAULT NULL,
  `value` text DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `delete_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_sm_tenant_config_group` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
