import sys
from shutil import move as smove

from dimlib import refresh_config, pgconnector

'''
1. Afresh config
2. Load config & Create connection
3. Get recent PID from job tracker
4. Take zipset to archive
5. Move file and Update tracker
'''

cfg = refresh_config()
archive_path = cfg['archive_path']
dburi = cfg['dburi']
try:
    cnx = pgconnector(dburi)
except Exception as err:
    sys.exit(f'Unable to open connection: {err}')

with cnx.cursor() as dbcur:
    sql_qry = 'SELECT MAX(PID) AS maxpid FROM framework.tracker_jobs'
    try:
        dbcur.execute(sql_qry)
        sql_dat = dbcur.fetchall()
    except Exception as err:
        sys.exit(f'Error fetching pid: {err}')

pid = list(sql_dat)[0]
print(sql_dat)
print(pid)
if pid < 10000:
    sys.exit('Not eligible to archive')

with cnx.cursor() as dbcur:
    sql_qry = f'''SELECT zipset FROM framework.tracker_collections
        where pid = {pid}'''
    try:
        dbcur.execute(sql_dat)
        sql_dat = dbcur.fetchall()
    except Exception as err:
        sys.exit(f'Unable to get zipsets: {err}')

zipsets = list(sql_dat)
archive_qry = '''INSERT INO framework.tracker_archiveset(
    pid, original_copy, archive_copy, start_time, end_time, purge_status)'''

for _zip in zipsets:
    t1 = dtm.now()
    smove(_zip, archive_path_zip)
    t2 = dtm.now()
    with cnx.cursor() as dbcur:
        try:
            sql_qry = f'''{archive_qry} SELECT {pid}, '{_zip}',
                '{archive_path_zip}', '{t1}', '{t2}', false'''
            dbcur.execute(sql_qry)
            cnx.commit()
        except Exception as err:
            sys.exit(f'Unable to record archive status: {err}')
