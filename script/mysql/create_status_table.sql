CREATE TABLE IF NOT EXISTS objects (
    source_id VARCHAR(750) PRIMARY KEY NOT NULL,
    target_id VARCHAR(1500),
    is_directory INT NOT NULL,
    size BIGINT,
    mtime DATETIME,
    status VARCHAR(32) NOT NULL,
    transfer_start DATETIME NULL,
    transfer_complete DATETIME NULL,
    verify_start DATETIME NULL,
    verify_complete DATETIME NULL,
    retry_count INT,
    error_message VARCHAR(2048),
    is_source_deleted INT NULL,
    INDEX status_idx (status)
)
  ENGINE = InnoDB
  ROW_FORMAT = COMPRESSED;