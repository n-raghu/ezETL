CREATE TABLE IF NOT EXISTS framework.mother_tbl_schema(
    active BOOLEAN,
    tbl_id INT GENERATED ALWAYS AS IDENTITY,
    app TEXT,
    collection_name TEXT,
    fld_type TEXT,
    fld_name TEXT
);