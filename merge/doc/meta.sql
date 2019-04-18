
CREATE database IF NOT EXISTS `binlog_backup`;

USE `binlog_backup`;

DROP TABLE IF EXISTS `position`;

-- binlog 备份表的位置
CREATE TABLE `position` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `domain` VARCHAR(64) NOT NULL COMMENT '域名',
  `ip` VARCHAR(16) NOT NULL COMMENT 'ip',
  `binlog_file` VARCHAR(64) DEFAULT NULL COMMENT 'binlog file',
  `binlog_pos` bigint(20) DEFAULT NULL COMMENT 'binlog pos',
  `gtid_sets` VARCHAR(254) NOT NULL COMMENT 'gtid_sets',
  `last_time` bigint(20) NOT NULL COMMENT '最后备份的时间戳',
  `back_file` VARCHAR(64) DEFAULT NULL COMMENT '备份文件名',
  `status`   enum('start', 'failure', 'success') COMMENT '任务启动，任务成功，任务失败',
  `create_time` timestamp NOT NULL DEFAULT '2016-12-31 16:00:00' COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `domain_idx` (`domain`, `last_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




