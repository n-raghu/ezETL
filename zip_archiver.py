import sys
from shutil import move as smove
from datetime import datetime as dtm

from dbops import record_error
from zipops import purge_worker_files
from dimlib import refresh_config, pgconnector

if len(sys.argv) > 1:
    try:
        pid = int(sys.argv[1])
    except Exception as err:
        sys.exit(err)
else:
    sys.exit('PID not provided...!')

'''
1. Afresh config
2. Purge Worker files
3. Load config & Create connection
4. Get recent PID from job tracker
5. Take zipset to archive
6. Move file and Update tracker
'''


def record_archiveset(
    cnx,
    pid,
    o_copy,
    a_copy,
    s_time,
    f_time=dtm.now(),
    ispurge=False,
):
    insert_qry = '''INSERT INTO framework.tracker_archiveset(
        pid,
        original_copy,
        archive_copy,
        start_time,
        finish_time,
        purge_status)'''
    select_qry = f"'{o_copy}', '{a_copy}', '{s_time}', '{f_time}', '{ispurge}'"
    try:
        sql_qry = f'{insert_qry} SELECT {pid}, {select_qry}'
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_qry)
        cnx.commit()
    except Exception as err:
        print(err)
    return None


cfg = refresh_config()
archive_path = cfg['xport_cfg']['archive_path']
dburi = cfg['dburi']

try:
    cnx = pgconnector(dburi)
except Exception as err:
    sys.exit(f'Unable to open connection|{err}')
finally:
    purge_worker_files(cfg)

with cnx.cursor() as dbcur:
    if pid == -1:
        sql_qry = f'''SELECT zip_set FROM framework.tracker_collections;'''
    else:
        sql_qry = f'''SELECT zip_set FROM framework.tracker_collections
            WHERE pid = {pid};'''
    try:
        dbcur.execute(sql_qry)
        sql_dat = dbcur.fetchall()
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask='zip_archiver',
            err_src='get-zipsets|all',
            err_txt=f'{err}'
        )
        sys.exit(f'Unable to get zipsets|{err}')

all_zipsets = {_[0] for _ in sql_dat}

with cnx.cursor() as dbcur:
    if pid == -1:
        sql_qry = f'''SELECT zip_set FROM framework.tracker_collections
        WHERE NOT ingest_success; '''
    else:
        sql_qry = f'''SELECT zip_set FROM framework.tracker_collections
        WHERE pid = {pid} AND NOT ingest_success '''
    try:
        dbcur.execute(sql_qry)
        sql_dat = dbcur.fetchall()
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask='zip_archiver',
            err_src='get-zipsets|failed',
            err_txt=f'{err}'
        )
        sys.exit(f'Unable to get zipsets|{err}')

fail_zipsets = {_[0] for _ in sql_dat}
archive_zipsets = all_zipsets - fail_zipsets
print(archive_zipsets)

try:
    zip_path = cfg['xport_cfg']['en_zip_path']
    archive_path = cfg['xport_cfg']['archive_path']
except Exception as err:
    record_error(
        cnx,
        pid,
        dtask='zip_archiver',
        err_src='Fetch XPORT config',
        err_txt=f'{err}'
    )
    sys.exit(f'Fetch XPORT config|{err}')

for _zipset in archive_zipsets:
    start_time = dtm.now()
    _archiveset = _zipset.replace(zip_path, archive_path)
    try:
        response = smove(_zipset, _archiveset)
        record_archiveset(
            cnx,
            pid,
            _zipset,
            _archiveset,
            start_time,
        )
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask='zip_archiver',
            err_src=_zipset,
            err_txt=f'{err}'
        )
        print(err)

cnx.close()
