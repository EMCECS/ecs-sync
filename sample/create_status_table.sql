create table sync_status
(
  source_id varchar(512) primary key not null,
  target_id varchar(512),
  synced_at timestamp not null,
  status varchar(32) not null,
  message varchar(1024)
);