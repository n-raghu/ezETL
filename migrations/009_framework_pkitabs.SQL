CREATE TABLE framework.tracker_pkicollections(
    pid BIGINT,
    tbl_id INT GENERATED ALWAYS AS IDENTITY,
    ingest_success BOOLEAN,
    instancecode TEXT,
    zip_set TEXT,
    collection_name TEXT,
    src_app TEXT,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    db_stamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
)