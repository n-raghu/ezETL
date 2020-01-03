CREATE TABLE framework.tracker_collections(
    pid BIGINT,
    tbl_id INT GENERATED ALWAYS AS IDENTITY,
    ingest_success BOOLEAN,
    instancecode TEXT,
    zip_set TEXT,
    mother_collection TEXT,
    instance_collection TEXT,
    src_app TEXT,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    rower BIGINT,
    db_stamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
)