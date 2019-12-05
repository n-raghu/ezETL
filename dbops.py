from dimlib import os, sys
from dimlib import pgconnector
from dimtraces import error_trace, dimlogger


def create_mother_tables(pguri, itr_obj_tbl_cfg):
    job_status = True
    cnx = pgconnector(pguri)
    sql_statements = []
    sql_query_prefix = 'SELECT framework.create_mother_tbl'
    if isinstance(itr_obj_tbl_cfg, list):
        for tbl_cfg in itr_obj_tbl_cfg:
            app_name = tbl_cfg['app_name']
            schema_name = tbl_cfg['schema_name']
            tbl_name = tbl_cfg['tbl_name']
            sql_query = f"('{app_name}', '{schema_name}', '{tbl_name}')"
            sql_statements.append(sql_query_prefix+sql_query)
    elif isinstance(itr_obj_tbl_cfg, dict):
        tbl_cfg = itr_obj_tbl_cfg
        app_name = tbl_cfg['app_name']
        schema_name = tbl_cfg['schema_name']
        tbl_name = tbl_cfg['tbl_name']
        sql_query = f'({app_name}, {schema_name}, {tbl_name})'
        sql_statements.append(sql_query_prefix+sql_query)
    else:
        sys.exit('Invalid arguments provided.')
    with cnx.cursor() as pgcur:
        for stmt in sql_statements:
            pgcur.execute(stmt)
        cnx.commit()
    cnx.close()
    return job_status


def get_mother_tbl_columns(pgx, mother_tbl):
    sql_query = f'''SELECT fld_name
    FROM framework.mother_tbl_schema
    WHERE active=true AND collection_name='{mother_tbl}' '''
    with pgx.cursor() as pgcur:
        pgcur.execute(sql_query)
        base_columns = pgcur.fetchall()
    return {
        _[0] for _ in base_columns
    }


def create_ins_tbl(
    pgx,
    mother_tbl,
    ins_tbl,
    db_schema,
    ins_tbl_map,
):
    tbl_columns = ''
    mother_tbl_structure = {}
    mother_tbl_list = get_mother_tbl_columns(pgx, mother_tbl)
    ins_only_columns = set(ins_tbl_map) - set(mother_tbl_list)
    for fld in ins_only_columns:
        tbl_columns += f'{fld} {ins_tbl_map[fld]},'
    if tbl_columns.endswith(','):
        tbl_columns = tbl_columns[:-1]
    tbl_statement = f''' CREATE TABLE IF NOT EXISTS {db_schema}.{ins_tbl}(
        {tbl_columns}
        ) INHERITS ({db_schema}.{mother_tbl});'''
    try:
        with pgx.cursor() as pgcur:
            pgcur.execute(tbl_statement)
            pgx.commit()
        return True
    except Exception as err:
        return err
