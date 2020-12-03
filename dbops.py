import sys
from datetime import datetime as dtm

from psycopg2.extras import RealDictCursor

from dimlib import sql_query_cleanser


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
    db_schema,
    ins_tbl_map,
):
    tbl_columns = ''
    mother_tbl_list = get_mother_tbl_columns(cnx, mother_tbl)
    ins_only_columns = set(ins_tbl_map) - set(mother_tbl_list)
    for fld in ins_only_columns:
        tbl_columns += f'{fld} {ins_tbl_map[fld]},'
    tbl_columns = tbl_columns[:-1]
    tbl_statement = f''' CREATE TABLE IF NOT EXISTS {db_schema}.{ins_tbl}(
        {tbl_columns}
        ) INHERITS ({db_schema}.{mother_tbl});'''
    with cnx.cursor() as dbcur:
        dbcur.execute(tbl_statement)
        cnx.commit()
    return True
