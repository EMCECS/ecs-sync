CREATE DATABASE IF NOT EXISTS ecs_sync
  DEFAULT CHARSET = utf8mb4
  DEFAULT COLLATE = utf8mb4_bin;
CREATE USER 'ecssync'@'%'
  IDENTIFIED BY 'ecs-sync-db';
GRANT ALL ON ecs_sync.* TO 'ecssync'@'%';
FLUSH PRIVILEGES;
