-- for MySQL only -- requires dbEnhancedDetailsEnabled=true

-- status counts
select status, count(*) from objects group by status;

-- add MD5 index to speed up queries
alter table objects add index md5_idx (source_md5, target_md5);

-- count of objects with matching MD5s
select count(*) as matching_md5_source_and_target from objects
    where status in ('Transferred','Verified') and source_md5 = target_md5;

-- objects with non-matching MD5s
select source_id, mtime, source_md5, target_id, target_mtime, target_md5 from objects
    where status in ('Transferred','Verified') and source_md5 <> target_md5;

-- error details
select source_id, left(first_error_message, 200) from objects where status = 'Error';