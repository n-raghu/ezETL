import sys
from time import time as ttime
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from psycopg2.extras import execute_values as pg_execute_values

from dbops import record_error
from dimlib import pgconnector, sql_query_cleanser, refresh_config

cache_schema = 'cached'
purge_schema = 'purged'
stage_schema = 'staging'


def cmp_n_pop_purge(
    pid,
    tbl_name,
    dburx,
    instance_list=False,
    stage_schema=stage_schema,
    purge_schema=purge_schema,
    cache_schema=cache_schema
):
    task = False
    start_epoch = int(ttime())

    try:
        cnx = pgconnector(dburx)
        tbl_parts = tbl_name.split('_')
        ins_code = tbl_parts.pop(0)
        if instance_list:
            if ins_code not in instance_list:
                return {
                    'task_status': 'NotRequired',
                    'tbl_name': tbl_name,
                }
        purge_tbl = '_'.join(tbl_parts)
        mother_tbl = purge_tbl[:-4]

        staged_count = -1
        cached_count = -1
        purged_count = -1
        post_purge_count = -1

    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask='filter_pki',
            err_src='cmp_n_pop_purge|identify_purge_tbl',
            err_txt=str(err)
        )
        return {
            'tbl_name': tbl_name,
            'task_status': task,
            'time_elapsed': int(ttime()) - start_epoch
        }

    try:
        tbl_comma_sep_column_str = get_tbl_column_str(mother_tbl)
        with cnx.cursor() as dbcur:
            dbcur.execute(tbl_comma_sep_column_str)
            sql_dat = dbcur.fetchall()
        tbl_comma_sep_columns = [r[0] for r in sql_dat][0]

        staged_count, cached_count, purged_count = get_records_count(
            cnx,
            tbl_name,
            purge_tbl,
        )

        ins_sql = f'''
                INSERT INTO {purge_schema}.{purge_tbl}({tbl_comma_sep_columns})
                SELECT
                    {tbl_comma_sep_columns}
                FROM
                    {cache_schema}.{tbl_name}
                EXCEPT
                SELECT
                    {tbl_comma_sep_columns}
                FROM
                    {stage_schema}.{tbl_name}; '''
        alter_sql = f'''
                        DROP TABLE IF EXISTS {cache_schema}.{tbl_name};
                        ALTER TABLE {stage_schema}.{tbl_name}
                            SET SCHEMA {cache_schema} ;'''

        # print(sql_query_cleanser(ins_sql))
        # print(sql_query_cleanser(alter_sql))
        cnx.rollback()
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_query_cleanser(ins_sql))
            dbcur.execute(sql_query_cleanser(alter_sql))
        cnx.commit()
        post_purge_count = get_records_count(cnx, False, purge_tbl)
        task = True
    except Exception as err:
        task = False
        record_error(
            cnx,
            pid,
            'filter_pki',
            err_src='cmp_n_pop_purge',
            err_txt=str(err)
        )
    finally:
        cnx.close()
    return {
        'tbl_name': tbl_name,
        'task_status': task,
        'time_elapsed': int(ttime()) - start_epoch,
        'prefilter_stage_count': staged_count,
        'prefilter_cache_count': cached_count,
        'prefilter_purge_count': purged_count,
        'postfilter_purge_count': post_purge_count
    }


def get_tbl_column_str(tbl_name):
    return f'''
            SELECT
                string_agg(fld_name,',') AS comma_sep_cols
            FROM
                framework.collectionmaps
            WHERE
                active=true
            AND
                collection_name='{tbl_name}'
            AND
                fld_name NOT IN ('rower', 'row_timestamp');
        '''


def get_successful_pki_list(dbcnx, cycle_id):
    qry = f'''
            SELECT
                instance_collection
            FROM
                framework.tracker_collections
            WHERE
                ingest_success
            AND
                pid = {cycle_id}
            AND
                instance_collection LIKE '%_pki'
            AND
                mother_collection NOT IN ('tbl_user_master_deleted_pki');
        '''
    try:
        dbcnx.rollback()
        with dbcnx.cursor() as dbcur:
            dbcur.execute(sql_query_cleanser(qry))
            sql_dat = dbcur.fetchall()
        return [record[0] for record in sql_dat]
    except Exception as err:
        record_error(
            dbcnx,
            pid,
            dtask='filter_pki',
            err_src='get_successful_pki_list',
            err_txt=str(err)
        )
        sys.exit(err)


def get_records_count(
    cnx,
    ins_tbl,
    purge_tbl,
    stage_schema=stage_schema,
    cache_schema=cache_schema,
    purge_schema=purge_schema,
):
    if ins_tbl:
        stage_count_sql = f''' SELECT
                                    COUNT(*)
                                FROM
                                    {stage_schema}.{ins_tbl};
                    '''
        cache_count_sql = f''' SELECT
                                    COUNT(*)
                                FROM
                                    {cache_schema}.{ins_tbl};
                    '''
    purge_count_sql = f''' SELECT
                                COUNT(*)
                            FROM
                                {purge_schema}.{purge_tbl};
                '''
    cnx.rollback()
    if ins_tbl:
        with cnx.cursor() as dbcur:
            dbcur.execute(stage_count_sql)
            stage_count = dbcur.fetchall()
            dbcur.execute(cache_count_sql)
            cache_count = dbcur.fetchall()
            dbcur.execute(purge_count_sql)
            purge_count = dbcur.fetchall()
        # print(stage_count, cache_count, purge_count)
        return stage_count[0][0], cache_count[0][0], purge_count[0][0]
    else:
        with cnx.cursor() as dbcur:
            dbcur.execute(purge_count_sql)
            purge_count = dbcur.fetchall()
        return purge_count[0][0]


def aio_filter_pki(
    pid,
    dburi,
    cpu_workers,
    pki_tables,
):
    pki_actions = []
    with ProcessPoolExecutor(max_workers=cpu_workers-1) as executor:
        pool_dictionary = {
            executor.submit(
                cmp_n_pop_purge,
                pid,
                pki_tab,
                dburi,
            ): pki_tab for pki_tab in pki_tables
        }
        for _future in fork_complete(pool_dictionary):
            key = pool_dictionary[_future]
            pki_actions.append(_future.result())
    return pki_actions


if __name__ == '__main__':
    if '-k' in sys.argv:
        sys.exit()

    try:
        pid = int(sys.argv[1])
    except Exception as err:
        sys.exit(f'PID - {err}')

    try:
        cfg = refresh_config()
        if not cfg['xport_cfg']['ingest_pki']:
            sys.exit()
        uri = cfg['dburi']
        cnx = pgconnector(uri)
        pki_tables = get_successful_pki_list(cnx, pid)
    except Exception as err:
        sys.exit(err)
    pki_tab_actions = aio_filter_pki(
        pid,
        dburi=uri,
        cpu_workers=cfg['cpu_workers'],
        pki_tables=pki_tables
    )
    try:
        pid_pki_actions = list(
            map(lambda doc: dict(doc, pid=pid), pki_tab_actions)
        )
        if len(pid_pki_actions) > 0:
            tab_columns = pid_pki_actions[0].keys()
            tab_dat = [[d for d in doc.values()] for doc in pid_pki_actions]
            ins_qry = f'''
                INSERT INTO
                    framework.pki_actions ({','.join(tab_columns)}) VALUES %s;
            '''
            with cnx.cursor() as dbcur:
                pg_execute_values(dbcur, ins_qry, tab_dat)
            cnx.commit()
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask='filter_pki',
            err_src='record_stats',
            err_txt=str(err)
        )
    finally:
        cnx.close()
