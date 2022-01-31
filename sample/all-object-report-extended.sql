SELECT
    'Source ID', 'Target ID', 'Directory', 'Size', 'Source Last Modified', 'Source MD5',
    'Source Retention End-time', 'Target Last Modified', 'Target MD5',
    'Target Retention End-time', 'Status', 'Transfer Start', 'Transfer Complete',
    'Verify Start', 'Verify Complete', 'Retry Count', 'Error Message',
    'First Error Message', 'Source Deleted'
UNION ALL
SELECT
    source_id, target_id, is_directory, size, mtime, source_md5,
    source_retention_end_time, target_mtime, target_md5,
    target_retention_end_time, status, transfer_start, transfer_complete,
    verify_start, verify_complete, retry_count, REPLACE( IFNULL(error_message, ''), '"' , '""' ) AS error_message,
    REPLACE( IFNULL(error_message, ''), '"' , '""' ), is_source_deleted
FROM objects
INTO OUTFILE 'all-object-report.csv'
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    ESCAPED BY ''
    LINES TERMINATED BY '\r\n';

# NOTE: on CentOS 7 with MariaDB, the output file should be located in /var/lib/mysql/ecs_sync