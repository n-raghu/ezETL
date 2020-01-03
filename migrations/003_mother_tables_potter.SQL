INSERT INTO framework.collectionmaps(collection_name, fld_name, fld_type, src_app)
SELECT 'tbl_salesforce_request_master', 'instancecode', 'TEXT', 'potter'
UNION ALL
SELECT 'tbl_salesforce_request_master', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'potter'
UNION ALL
SELECT 'tbl_salesforce_request_master', 'rower', 'BIGINT', 'potter'
UNION ALL
SELECT 'tbl_potter_client_master', 'instancecode', 'TEXT', 'potter'
UNION ALL
SELECT 'tbl_potter_client_master', 'row_timestamp', 'TIMESTAMP WITHOUT TIME ZONE', 'potter'
UNION ALL
SELECT 'tbl_potter_client_master', 'rower', 'BIGINT', 'potter';