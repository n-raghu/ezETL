CREATE TABLE IF NOT EXISTS framework.tracker_archiveset(
    pid BIGINT,
    tbl_id INT GENERATED ALWAYS AS IDENTITY,
    original_copy TEXT,
    archive_copy TEXT,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    finish_time TIMESTAMP WITHOUT TIME ZONE,
    purge_status BOOLEAN,
    purge_date DATE,
    db_stamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
)