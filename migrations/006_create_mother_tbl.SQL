CREATE OR REPLACE FUNCTION framework.create_mother_tbl(app_code TEXT, db_schema TEXT, tbl_name TEXT) RETURNS TEXT
AS
$$
    DECLARE
        qry TEXT='CREATE TABLE IF NOT EXISTS ';
        cols_with_type TEXT;
    BEGIN
        SELECT string_agg(result,',') as q INTO cols_with_type FROM(
        SELECT array_to_string(ARRAY[fld_name ,fld_type], ' ')  AS result
        FROM framework.collectionmaps
        WHERE active=true AND collection_name=tbl_name AND src_app=app_code
        )A;
        qry = CONCAT(qry, db_schema, '.', tbl_name, '(', cols_with_type, ')');
        EXECUTE qry;
        RETURN qry;
    END;
$$
LANGUAGE plpgsql VOLATILE
COST 100;