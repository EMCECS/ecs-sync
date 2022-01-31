-- for MySQL only -- requires dbEnhancedDetailsEnabled=true

-- status counts
select status, count(*) from objects group by status;

-- add retention index to speed up queries
alter table objects add index retention_idx (source_retention_end_time, target_retention_end_time);

-- count of objects with active retention times within 2 hours of each other
select count(*) as active_retention_times_within_2_hours
from (select source_id,
             source_retention_end_time,
             target_retention_end_time,
             TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) as difference_in_seconds
      from objects
      where status in ('Transferred', 'Verified')
        and source_retention_end_time > now()
        and TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) <= 7200) as ret_diff;

-- count of objects with expired retention in the source and target
select count(*) as expired_retention_in_source_and_target
from objects
where status in ('Transferred', 'Verified')
  and source_retention_end_time < now()
  and target_retention_end_time < now();

-- count of objects with no retention in the source and target
select count(*) as no_retention_in_source_and_target
from objects
where source_retention_end_time is null
  and target_retention_end_time is null
  and status in ('Transferred', 'Verified');

-- these objects retention times are more than 2 hours off
select source_id,
       source_retention_end_time,
       target_retention_end_time,
       status,
       UNIX_TIMESTAMP(source_retention_end_time) as source_raw_time,
       UNIX_TIMESTAMP(target_retention_end_time) as target_raw_time,
       TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) as ret_diff
from (select source_id,
             source_retention_end_time,
             target_retention_end_time,
             status
      from objects
      where status in ('Transferred', 'Verified')
        and source_retention_end_time > now()
        and TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) > 7200) as objects_off;

-- error details
select source_id, left(first_error_message, 200) from objects where status = 'Error';