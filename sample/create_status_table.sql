create table sync_status
(
  source_id varchar(512) primary key not null,
  target_id varchar(512),
  started_at timestamp null,
  completed_at timestamp null,
  verified_at timestamp null,
  status varchar(32) not null,
  message varchar(1024)
);
