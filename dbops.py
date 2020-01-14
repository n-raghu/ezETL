from dimlib import os, sys
from dimlib import pgconnector
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
        print(f'CREATE-INS-TBL-ERR: {err}')
        return err


def collectiontracker(
    dburi,
    pid,
    storageset,
):
    try:
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
    except Exception as err:
        sys.exit(err)
        return False
    finally:
        cnx.close()


def record_job_success(
    cnx,
    pid,
    ins_tbl,
    start_time,
    end_time,
):
    try:
        sql_qry = f'''UPDATE framework.tracker_collections
        SET ingest_success=true,start_time='{start_time}',end_time='{end_time}'
        WHERE pid={pid} AND instance_collection='{ins_tbl}';'''
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_qry)
        cnx.commit()
    except Exception as err:
        print(f'Recording Error:{err}')
