import sys
try:
    from yaml import safe_load as yml_safe_load
    from psycopg2 import connect as pgconnect

    from dbops import record_error
    from dimlib import sql_query_cleanser
except Exception as err:
    sys.exit(err)

dtask = 'dv_prerequisites'
try:
    with open('dimConfig.yml') as cfg_ins:
        raw_cfg = yml_safe_load(cfg_ins)
    pid = int(sys.argv[1])
except Exception as err:
    sys.exit(f'{dtask}|err')

try:
    _pgauth = f"{raw_cfg['eaedb']['user']}:{raw_cfg['eaedb']['passwd']}"
    _pghost = f"{raw_cfg['eaedb']['server']}:{raw_cfg['eaedb']['port']}"
    _pgdbo = f"{raw_cfg['eaedb']['db']}"
    dburi = f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}'
    db_schema = raw_cfg['eaedb']['schema']
    ingest_pki = raw_cfg['xport_cfg']['ingest_pki']
except Exception as err:
    sys.exit(f'{dtask}|import-config-err|{err}')

cnx = pgconnect(dburi)

try:
    sql_qry = '''SELECT fld_name
                FROM framework.collectionmaps
                WHERE collection_name = 'tbl_user_master';'''

    cnx.rollback()
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_query_cleanser(sql_qry))
        sql_dat = dbcur.fetchall()

    list_of_columns = [_[0] for _ in sql_dat]
    column_str = ','.join(list_of_columns)
except Exception as err:
    record_error(
        cnx,
        pid,
        dtask=dtask,
        err_src='columnString',
        err_txt=str(err)
    )
    sys.exit(f'{dtask}|columnString|{err}')

# Check Objects
try:
    sql_qry = '''
                SELECT
                    table_name
                FROM
                    information_schema.tables
                WHERE
                    table_schema = 'staging'
                AND
                    table_name IN ('tbl_user_master_deleted', 'tbl_user_master_deleted_pki');
            '''
    cnx.rollback()
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_query_cleanser(sql_qry))
        sql_dat = dbcur.fetchall()

    if len(sql_dat) == 0:
        print(f'{dtask}|No Tables found to merge')
        sys.exit()
except Exception as err:
    record_error(
        cnx,
        pid,
        dtask=dtask,
        err_src='checkMotherTables',
        err_txt=str(err)
    )
    sys.exit(f'{dtask}|checkMotherTables|{err}')

try:
    ins_list_qry = f'''
                    SELECT
                        DISTINCT instancecode
                    FROM
                        framework.tracker_collections
                    WHERE
                        pid = {pid}
                    AND
                        src_app = 'lms';
                    '''

    cnx.rollback()
    with cnx.cursor() as dbcur:
        dbcur.execute(ins_list_qry)
        sql_dat = dbcur.fetchall()

    ins_list = [_[0] for _ in sql_dat]
    sql_dat = None
except Exception as err:
    record_error(
        cnx,
        pid,
        dtask,
        err_src='fetchPIDInstanceList',
        err_txt=str(err)
    )

for ins in ins_list:
    try:
        sql_qry = f'INSERT INTO {db_schema}.{ins}_tbl_user_master({column_str})'
        sql_qry += f'SELECT {column_str} FROM {db_schema}.{ins}_tbl_user_master_deleted;'
        sql_qry += f'DROP TABLE IF EXISTS {db_schema}.{ins}_tbl_user_master_deleted;'

        cnx.rollback()
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_qry)
        cnx.commit()

        if ingest_pki:
            sql_qry = f'INSERT INTO {db_schema}.{ins}_tbl_user_master_pki({column_str})'
            sql_qry += f'SELECT {column_str} FROM {db_schema}.{ins}_tbl_user_master_deleted_pki;'
            sql_qry += f'DROP TABLE IF EXISTS {db_schema}.{ins}_tbl_user_master_deleted_pki;'

            cnx.rollback()
            with cnx.cursor() as dbcur:
                dbcur.execute(sql_qry)
            cnx.commit()
    except Exception as err:
        record_error(
            cnx=cnx,
            pid=pid,
            dtask=dtask,
            err_src='merge-deleted-tables',
            err_txt=f'{err}'
        )
        sys.exit(f'Unable to merge DELETED Tables to USER MASTER... {err}')

cnx.close()
