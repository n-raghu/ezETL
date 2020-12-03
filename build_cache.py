import sys

from dbops import record_error
from filter_pki import cmp_n_pop_purge
from dimlib import pgconnector, sql_query_cleanser, refresh_config


cache_schema = 'cached'
purge_schema = 'purged'
stage_schema = 'staging'

'''
1. Salesforce - No change
2. Build Cache - To create cached and purged schemas, also helpful in case to
rebuild. If any schema exists then skip
3. zip_consumer - Additionally, should create mother tables for PKI also
4. DV Prerequisites - No change
5. DV Filter PKI - Filter PKI and identify deleted records and rebase cache
6. DV Load changes
7. Exec Statements - No change
'''


def create_schema_tables_for_main_tbl(
    tbl_name,
    cnx,
    instance_list,
    create_purge_tbl=False,
    cache_schema=cache_schema,
    purge_schema=purge_schema
):
    comma_sep_columns_qry = f'''SELECT string_agg(result,',') AS comma_sep_cols
        FROM(
            SELECT array_to_string(ARRAY[fld_name ,fld_type], ' ')  AS result
            FROM framework.collectionmaps
            WHERE active=true AND collection_name='{tbl_name}'
        )A;'''
    cnx.rollback()
    with cnx.cursor() as dbcur:
        dbcur.execute(comma_sep_columns_qry)
        sql_dat = dbcur.fetchall()
    comma_sep_columns = [_[0] for _ in sql_dat][0]
    if create_purge_tbl:
        tbl_qry = f'''CREATE TABLE IF NOT EXISTS {purge_schema}.{tbl_name}_pki(
                    {comma_sep_columns}
                );'''
    else:
        tbl_qry = ''
    for ins in instance_list:
        ins_pki_tbl = f'{ins}_{tbl_name}_pki'
        tbl_qry += f'''CREATE TABLE IF NOT EXISTS {cache_schema}.{ins_pki_tbl}(
                {comma_sep_columns}
            );'''
    cnx.rollback()
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_query_cleanser(tbl_qry))
    cnx.commit()


if __name__ == '__main__':
    try:
        cfg = refresh_config()
        cnx = pgconnector(cfg['dburi'])
        with cnx.cursor() as dbcur:
            dbcur.execute(f'CREATE SCHEMA IF NOT EXISTS {cache_schema};')
            dbcur.execute(f'CREATE SCHEMA IF NOT EXISTS {purge_schema};')
        cnx.commit()
    except Exception as err:
        sys.exit(f'buildCache|InitializationError|{err}')

    sql_qry_active_tables = '''
                                SELECT
                                    collection_name
                                FROM
                                    framework.collectionmaps
                                WHERE
                                    active
                                AND
                                    src_app in ('lms', 'phishproof');
                            '''
    sql_qry_active_ins = '''
                                SELECT
                                    instancecode
                                FROM
                                    framework.instanceconfig
                                WHERE
                                    isactive;
                            '''

    try:
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_query_cleanser(sql_qry_active_tables))
            sql_dat = dbcur.fetchall()
        active_tables = {_[0] for _ in sql_dat}
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_query_cleanser(sql_qry_active_ins))
            sql_dat = dbcur.fetchall()
        active_instances = {_[0] for _ in sql_dat}
    except Exception as err:
        sys.exit(f'buildCache|ActivTables_ActivInstances|{err}')

    for tbl in active_tables:
        try:
            create_schema_tables_for_main_tbl(
                tbl,
                cnx,
                instance_list=active_instances,
                create_purge_tbl=True
            )
        except Exception as err:
            sys.exit(f'buildCache|create_schema_tbl_for_main_tbl|{tbl}|{err}')

    if cfg['agent']['skip_build_cache']:
        print('Skip Caching')
        sys.exit()

    staging_tbl_query = f'''
                            SELECT
                                tablename
                            FROM
                                pg_tables
                            WHERE
                                schemaname = '{stage_schema}'
                            AND
                                tablename LIKE '%_pki';
                        '''

    try:
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_query_cleanser(staging_tbl_query))
            sql_dat = dbcur.fetchall()
        staging_tables = [r[0] for r in sql_dat]
    except Exception as err:
        sys.exit(f'buildCache|staging_tables|{err}')

    for tbl in staging_tables:
        response = cmp_n_pop_purge(
            -1,
            tbl,
            cfg['dburi'],
            instance_list=active_instances
        )
        if not response['task_status']:
            sys.exit('Build Cache Failed')
