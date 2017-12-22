SELECT CONCAT(
    'Source ID,',
    'Target ID,',
    'Directory,',
    'Size,',
    'Source mtime,',
    'Status,',
    'Transfer Start,',
    'Transfer Complete,',
    'Verify Start,',
    'Verify Complete,',
    'Retry Count,',
    'Error Message,',
    'Source Deleted,'
)
UNION ALL
SELECT CONCAT(
    '"', source_id, '",',
    '"', COALESCE(target_id, ''), '",',
    '"', IF(is_directory = 0, 'false', 'true'), '",',
    '"', COALESCE(size, ''), '",',
    '"', COALESCE(mtime, ''), '",',
    '"', COALESCE(status, ''), '",',
    '"', COALESCE(transfer_start, ''), '",',
    '"', COALESCE(transfer_complete, ''), '",',
    '"', COALESCE(verify_start, ''), '",',
    '"', COALESCE(verify_complete, ''), '",',
    '"', COALESCE(retry_count, ''), '",',
    '"', COALESCE(error_message, ''), '",',
    '"', COALESCE(is_source_deleted, ''), '"'
)
FROM objects
INTO OUTFILE 'objects-report.csv';
