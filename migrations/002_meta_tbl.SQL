CREATE TABLE IF NOT EXISTS framework.collectionmaps(
    active BOOLEAN,
    tbl_id INT GENERATED ALWAYS AS IDENTITY,
    src_app TEXT,
    collection_name TEXT,
    fld_type TEXT,
    fld_name TEXT
);