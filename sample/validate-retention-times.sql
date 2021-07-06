-- for MySQL only -- requires dbEnhancedDetailsEnabled=true

-- status counts
select status, count(*)
from objects
group by status;

-- count of objects with active retention times within 1 minute of each other
select count(*) as end_time_within_1_minute
from (select source_id,
             source_retention_end_time,
             target_retention_end_time,
             TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) as difference_in_seconds
      from objects
      where status in ('Transferred', 'Verified')
        and source_retention_end_time > now()
        and TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) <= 60) as ret_diff;

-- count of objects with active retention times within 5 seconds of each other
select count(*) as end_time_within_5_secs
from (select source_id,
             source_retention_end_time,
             target_retention_end_time,
             TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) as difference_in_seconds
      from objects
      where status in ('Transferred', 'Verified')
        and source_retention_end_time > now()
        and TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) <= 5) as ret_diff;

-- count of objects with expired retention in the source and target
select count(*) as expired_in_source_and_target
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

-- these objects retention times are more than 1 minute off
select source_id,
       source_retention_end_time,
       target_retention_end_time,
       status,
       UNIX_TIMESTAMP(source_retention_end_time) as source_raw_time,
       UNIX_TIMESTAMP(target_retention_end_time) as target_raw_time
from (select source_id,
             source_retention_end_time,
             target_retention_end_time,
             status
      from objects
      where status in ('Transferred', 'Verified')
        and source_retention_end_time > now()
        and TIMESTAMPDIFF(SECOND, source_retention_end_time, target_retention_end_time) > 60) as ret_diff;

-- error details
select source_id, left(first_error_message, 200)
from objects
where status = 'Error';