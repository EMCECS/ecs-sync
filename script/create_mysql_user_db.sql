create database if not exists vipr_sync;
create user 'viprsync'@'%' identified by 'vipr-sync-db';
grant all on vipr_sync.* to 'viprsync'@'%';
flush privileges;
