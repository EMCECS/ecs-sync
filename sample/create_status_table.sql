create table sync_status
(
  source_id varchar(750) primary key not null,
  target_id varchar(1500),
  started_at timestamp null,
  completed_at timestamp null,
  verified_at timestamp null,
  status varchar(32) not null,
  message varchar(2048)
);
