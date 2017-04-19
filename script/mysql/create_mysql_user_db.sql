CREATE DATABASE IF NOT EXISTS ecs_sync;
CREATE USER 'ecssync'@'%'
  IDENTIFIED BY 'ecs-sync-db';
GRANT ALL ON ecs_sync.* TO 'ecssync'@'%';
GRANT FILE ON *.* TO 'ecssync'@'%';
FLUSH PRIVILEGES;
