create table sync_status
(
  source_id varchar(512) primary key not null,
  target_id varchar(512),
  started_at timestamp,
  completed_at timestamp,
  verified_at timestamp,
  status varchar(32) not null,
  message varchar(1024)
);