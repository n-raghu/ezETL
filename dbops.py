import sys
from datetime import datetime as dtm

from psycopg2.extras import RealDictCursor

from dimlib import timetracer

@timetracer
def get_active_tables(cnx):
    sql_query = f'''
                    SELECT
                        n.nspname as schema,
                        c.relname as name
                    FROM
                        pg_catalog.pg_class c
                    LEFT JOIN
                        pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relkind IN ('r','p','')
                            AND n.nspname <> 'pg_catalog'
                            AND n.nspname <> 'information_schema'
                            AND n.nspname !~ '^pg_toast'
                            AND c.relpartbound IS NULL
                            AND NOT EXISTS (
                                SELECT
                                    1
                                FROM
                                    pg_catalog.pg_inherits i
                                WHERE
                                    i.inhrelid = c.oid
                            );
                '''
    cnx.rollback()
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_query)
        mother_tables = dbcur.fetchall()
    return {
        _[1] for _ in mother_tables
    }


def get_mother_tbl_columns(cnx, mother_tbl):
    sql_query = f'''SELECT
                        column_name
                    FROM
                        information_schema.columns
                    WHERE
                        table_name='{mother_tbl}' '''
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_query)
        base_columns = dbcur.fetchall()
    return {
        _[0] for _ in base_columns
    }


def create_ins_tbl(
    cnx,
    mother_tbl,
    ins_tbl,
    ins_tbl_map,
):
    tbl_columns = ''
    mother_tbl_list = get_mother_tbl_columns(cnx, mother_tbl)
    ins_only_columns = set(ins_tbl_map) - set(mother_tbl_list)
    for fld in ins_only_columns:
        tbl_columns += f'{fld} {ins_tbl_map[fld]},'
    tbl_columns = tbl_columns[:-1]
    tbl_statement = f''' CREATE TABLE IF NOT EXISTS {ins_tbl}(
        {tbl_columns}
        ) INHERITS ({mother_tbl});'''
    with cnx.cursor() as dbcur:
        dbcur.execute(tbl_statement)
        cnx.commit()
    return True
