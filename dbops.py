from datetime import datetime as dtm

from psycopg2.extras import RealDictCursor

from dimlib import os, sys
from dimlib import pgconnector, sql_query_cleanser
from dimtraces import timetracer, bugtracer


@timetracer
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
            _pki = 'true' if tbl_cfg['pki'] else 'false'
            _dat = 'true' if tbl_cfg['dat'] else 'false'
            sql_query = f"('{app_name}', '{schema_name}', '{tbl_name}', '{_dat}', '{_pki}');"
            sql_statements.append(sql_query_prefix+sql_query)
    elif isinstance(itr_obj_tbl_cfg, dict):
        tbl_cfg = itr_obj_tbl_cfg
        app_name = tbl_cfg['app_name']
        schema_name = tbl_cfg['schema_name']
        tbl_name = tbl_cfg['tbl_name']
        _pki = 'true' if tbl_cfg['pki'] else 'false'
        _dat = 'true' if tbl_cfg['dat'] else 'false'
        sql_query = f"('{app_name}', '{schema_name}', '{tbl_name}', '{_dat}', '{_pki}');"
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
    FROM framework.collectionmaps
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
        print(f'CREATE-INS-TBL-ERR|{err}')
        print(tbl_statement)
        return False


def collectiontracker(
    dburi,
    pid,
    storageset,
):
    cnx = pgconnector(dburi)
    insert_qry = '''INSERT INTO framework.tracker_collections(
        pid,
        ingest_success,
        instancecode,
        zip_set,
        mother_collection,
        instance_collection,
        src_app)'''
    select_qry = ' '
    for _set in storageset:
        zipset = str(_set['dataset']).split('.')
        zipset = f'{zipset[0]}.zip.gpg'
        select_qry += f'''SELECT {pid},
        false,
        '{_set['icode']}',
        '{zipset}',
        '{_set['mother_tbl']}',
        '{_set['ins_tbl']}',
        '{_set['app_name']}' UNION ALL '''
    select_qry = select_qry.strip()
    if select_qry.endswith('UNION ALL'):
        select_qry = select_qry[:-9]
    sql_qry = insert_qry + select_qry
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_qry)
    cnx.commit()
    return True


def record_job_success(
    cnx,
    pid,
    ins_tbl,
    start_time,
    end_time,
    rowversion
):
    try:
        if rowversion:
            sql_qry = f'''UPDATE framework.tracker_collections
            SET ingest_success=true,start_time='{start_time}',
            finish_time='{end_time}', rower={rowversion}
            WHERE pid={pid} AND instance_collection='{ins_tbl}';'''
        else:
            sql_qry = f'''UPDATE framework.tracker_collections
            SET ingest_success=true,start_time='{start_time}',
            finish_time='{end_time}'
            WHERE pid={pid} AND instance_collection='{ins_tbl}';'''
    except Exception as err:
        rowversion = False
        record_error(
            cnx,
            pid,
            dtask='zip_consumer',
            err_src='record_job_success',
            err_txt=f'getRowVersion|{err}'
        )
    try:
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_query_cleanser(sql_qry))
        cnx.commit()
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask='zip_consumer',
            err_src='record_job_success',
            err_txt=f'queryExecError|{err}'
        )
        print(f'Recording Error:{err}')


def get_active_tables(dburi):
    cnx = pgconnector(dburi)
    sql_qry = '''SELECT DISTINCT
                    collection_name AS collection,
                    ingest_dat AS dat,
                    ingest_pki AS pki
                FROM
                    framework.collections
                WHERE
                    active;
            '''
    with cnx.cursor(cursor_factory=RealDictCursor) as dbcur:
        dbcur.execute(sql_qry)
        sql_dat = dbcur.fetchall()
    cnx.close()
    return [dict(row) for row in sql_dat]


def record_error(cnx, pid, dtask, err_src, err_txt):
    _txt = err_txt.replace("'", "''")
    sql_qry = f'''INSERT INTO framework.errorlogs(pid, dim_task, err_src, err_txt, err_time)
        SELECT {pid}, '{dtask}', '{err_src}', '{_txt}', '{dtm.utcnow()}'::timestamp; '''
    try:
        cnx.rollback()
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_qry)
        _ = cnx.commit()
        return None
    except Exception as err:
        print(f'RecordError|{err}')
        print(sql_qry)
        return None
