import os
import sys
from glob import glob
from pprint import pprint
from time import time as ttime

from psycopg2.extras import RealDictCursor

from dimlib import refresh_config, pgconnector, sql_query_cleanser
from dbops import record_error

try:
    if len(sys.argv) > 1:
        debug = False
        pid = int(sys.argv[1])
    else:
        debug = True
        pid = int(ttime())
except Exception:
    sys.exit('Provide correct PID')

try:
    cfg = refresh_config()
    dburi = cfg['dburi']
    cnx = pgconnector(dburi)
    file_drop_location = cfg['xport_cfg']['en_zip_path']
    dtask = 'record_file_stats'
except Exception as err:
    sys.exit(f'CNXFail|{err}')


def ins_q_count(file_list):
    try:
        q_potter = 0
        q_lms = 0
        q_pp = 0
        for _file in file_list:
            file_properties = _file.split('_')[1]
            if 'potter' in file_properties:
                q_potter += 1
            elif 'lms' in file_properties:
                q_lms += 1
            elif 'phishproof' in file_properties:
                q_pp += 1
        return q_potter, q_lms, q_pp
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask=dtask,
            err_src='ins_q_count',
            err_txt=f'{err}'
        )


all_ins_files = {}
all_instance_list = [
    a for a in os.listdir(file_drop_location) if os.path.isdir(os.path.join(file_drop_location, a))
]
unwanted_ins_set = {'lost+found', 'ls', }
for _ins in unwanted_ins_set:
    if _ins in all_instance_list:
        all_instance_list.remove(_ins)

for _ins in all_instance_list:
    _location = f'{file_drop_location}{_ins}/*.gpg'
    _files = glob(_location)
    q_potter, q_lms, q_pp = ins_q_count(_files)
    all_ins_files[_ins] = {
        'q_total': len(_files),
        'q_lms': q_lms,
        'q_phishproof': q_pp,
        'q_potter': q_potter,
    }

sql_qry = f'''SELECT COALESCE(MAX(pid),0)
                FROM framework.file_stats '''
try:
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_qry)
        _dat = dbcur.fetchall()
    last_pid = _dat[0][0]
except Exception as err:
    record_error(
        cnx=cnx,
        pid=pid,
        dtask=dtask,
        err_src='get-last-pid',
        err_txt=f'{err}'
    )
    print(f'LastPIDError|{err}')

print('')
print('')
print(f'Last PID: {last_pid}. ', end='')
print(f'New PID: {pid}. ', end='')

if debug:
    print('Eligibility: Debug')
elif not (pid - last_pid) > 999:
    print('Eligibility: No')
    sys.exit()
else:
    print('Eligibility: Yes')

sql_qry = f'''SELECT instancecode, src_app, count(original_copy) AS zip_set_knt
                FROM(
                    SELECT DISTINCT ftc.instancecode
                        ,fta.original_copy ,ftc.src_app
                    FROM framework.tracker_archiveset AS fta
                    JOIN framework.tracker_collections AS ftc
                    ON fta.pid = ftc.pid AND fta.original_copy = ftc.zip_set
                        WHERE fta.pid > {last_pid})ngroup
                            GROUP BY instancecode, src_app; '''

try:
    with cnx.cursor(cursor_factory=RealDictCursor) as dbcur:
        dbcur.execute(sql_qry)
        _dat = dbcur.fetchall()
    sql_dat = list(_dat)
    sql_dat_check = True
except Exception as err:
    sql_dat_check = False
    record_error(
        cnx=cnx,
        pid=pid,
        dtask=dtask,
        err_src='get-consumed-count',
        err_txt=f'{err}'
    )

for _ins, _val in all_ins_files.items():
    try:
        lms_consumed = 0
        pp_consumed = 0
        potter_consumed = 0
        for _dict in sql_dat:
            if _ins == _dict['instancecode']:
                if _dict['src_app'] == 'lms':
                    lms_consumed = _dict['zip_set_knt']
                elif _dict['src_app'] == 'phishproof':
                    pp_consumed = _dict['zip_set_knt']
                elif _dict['src_app'] == 'potter':
                    potter_consumed = _dict['zip_set_knt']
        ins_consume_knt = lms_consumed + pp_consumed + potter_consumed
    except Exception:
        ins_consume_knt = 0

    if debug:
        try:
            _dict = {
                'total': ins_consume_knt,
                'lms': lms_consumed,
                'pp': pp_consumed,
                'potter': potter_consumed,
            }
            all_ins_files[_ins]['consumed_total'] = _dict['total']
            all_ins_files[_ins]['consumed_lms'] = _dict['lms']
            all_ins_files[_ins]['consumed_phishproof'] = _dict['pp']
            all_ins_files[_ins]['consumed_potter'] = _dict['potter']
            continue
        except Exception as err:
            print(f'UpdateDictError|{err}')

    try:
        sql_qry = f'''INSERT INTO framework.file_stats(
            pid, last_record_pid, instancecode, total_queue,
            lms_queue, pp_queue, potter_queue, total_consumed,
            lms_consumed, pp_consumed, potter_consumed
        )
        SELECT {pid}, {last_pid}, '{_ins}', {_val['q_total']},
                {_val['q_lms']}, {_val['q_phishproof']}, {_val['q_potter']},
                {ins_consume_knt}, {lms_consumed},
                {pp_consumed}, {potter_consumed}; '''
        sql_qry = sql_query_cleanser(sql_qry)
        cnx.rollback()
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_qry)
        cnx.commit()
    except Exception as err:
        record_error(
            cnx=cnx,
            pid=pid,
            dtask=dtask,
            err_src='record-stats',
            err_txt=f'{_ins}|{err}'
        )

pprint(all_ins_files)
print('')
