import os
import sys
from glob import iglob
import subprocess as sbp
from time import sleep as ziz
from datetime import datetime as dtm
from datetime import timedelta as tdt

from dbops import record_error
from dimlib import refresh_config, pgconnector, sql_query_cleanser


def check_files_n_proceed(gpg_path):
    fpath = f'{gpg_path}/**/*.gpg'
    dat_files = iglob(fpath)
    try:
        chk = next(dat_files)
        chk = True
    except Exception:
        chk = False
    return chk


def pid_worker(pid, jobid, pid_path, full_dv_mode=False, launcher='python'):
    with open(f'{pid_path}{pid}_{jobid}.log', 'w') as zip_log_file:
        with open(f'{pid_path}{pid}_{jobid}.err', 'w') as zip_err_file:
            job_start_time = dtm.utcnow()
            sbp_args = [launcher, jobid, str(pid)]
            if full_dv_mode:
                sbp_args.extend(['-i', '-k'])
            else:
                sbp_args.append('-f')
            proc = sbp.Popen(
                sbp_args,
                stderr=zip_err_file,
                stdout=zip_log_file,
            )
            io_wait = proc.communicate()
            job_end_time = dtm.utcnow()
    with open(f'{pid_path}{pid}_{jobid}.err', 'r') as zip_err_file:
        err_txt = zip_err_file.read()
        if not err_txt:
            job_status = True
        else:
            job_status = False
    return job_status, job_start_time, job_end_time


def consolidate_pid_file(cnx, pid, dtask, pid_path, jlist):
    job_sep = '-'
    pid_log_file = f'{pid_path}{pid}.log'
    for _job in jlist:
        try:
            with open(pid_log_file, 'a') as pid_file:
                with open(f'{pid_path}{pid}_{_job}.err', 'r') as log_file:
                    _info = log_file.read()
                if not _info:
                    _info = 'No Errors directed to console'
                pid_file.write(f'jobid: {_job}\n')
                pid_file.write('ERR:\n')
                pid_file.write(_info)
                pid_file.write('\n')
                pid_file.write('OUT:\n')
                with open(f'{pid_path}{pid}_{_job}.log', 'r') as log_file:
                    _info = log_file.read()
                pid_file.write(_info)
                pid_file.write('\n')
                pid_file.write(f'{64 * job_sep}')
            os.remove(f'{pid_path}{pid}_{_job}.log')
            os.remove(f'{pid_path}{pid}_{_job}.err')
        except Exception as err:
            record_error(
                cnx,
                pid,
                dtask=dtask,
                err_src='consolidate-pid-chunks',
                err_txt=f'{err}'
            )
            with open('agent.err', 'a') as agent_err:
                agent_err.write(f'{pid}|{_job}|{dtm.utcnow()}|{err}\n\n')
    return pid_log_file


def update_job_tracker(
    cnx,
    pid,
    j_id,
    j_status,
    j_start_time,
    j_end_time
):
    ins_qry = f'''INSERT INTO framework.tracker_jobs(
        pid,job_success,jobid,start_time,finish_time
    )
    SELECT {pid},{j_status},'{j_id}','{job_start_time}','{j_end_time}';'''
    ins_qry = sql_query_cleanser(ins_qry)
    try:
        with cnx.cursor() as dbcur:
            dbcur.execute(ins_qry)
            cnx.commit()
        return False
    except Exception as err:
        return err


dtask = 'agent'
line_sep = '-'
cycle_number = 0

while True:
    print('')
    utnow = dtm.utcnow()
    cstnow = utnow - tdt(hours=5)
    pid = int(dtm.timestamp(utnow))
    cycle_number += 1
    try:
        cfg = refresh_config()
        full_dv_min_hr = cfg['agent']['full_dv_min_hour']
        full_dv_max_hr = cfg['agent']['full_dv_max_hour']
        full_dv_mode = full_dv_min_hr <= cstnow.hour <= full_dv_max_hr
        cnx = pgconnector(cfg['dburi'])
        ziz_time = cfg['agent']['snooze_time']
        pid_path = cfg['agent']['pid_files']
        latency = cfg['agent']['latency']
        sfdc_latency = cfg['agent']['sfdc_latency']
    except Exception as err:
        sys.exit(f'{pid}|Agent unable to open connection|{err}')

    if cycle_number == 1:
        job_status, _, _ = pid_worker(
            -1,
            'build_cache.py',
            pid_path,
        )
        if job_status:
            print('Build Cache Succeeded')
        else:
            sys.exit('Unable to pre-build Cache')

    print(f'{64 * line_sep}')
    print(f'Cycle #{cycle_number} with {pid} started at {dtm.utcnow()}')
    print(f'UTC: {utnow}, CST: {cstnow}, Full PKI: {full_dv_mode}')

    try:
        if not check_files_n_proceed(cfg['xport_cfg']['en_zip_path']):
            print(f'No Files Found. Snoozing for {ziz_time}')
            ziz(ziz_time)
            continue

        job_list = []
        sql_qry = f'''SELECT * FROM
                    framework.launchpad({sfdc_latency},{latency},{pid})
                        ORDER BY job_order; '''
        with cnx.cursor() as dbcur:
            dbcur.execute(sql_qry)
            sql_dat = dbcur.fetchall()
        if not len(sql_dat) > 0:
            print(f'No Jobs Found. Snoozing for {ziz_time}')
            print(f'{64 * line_sep}')
            print('')
            ziz(ziz_time)
            continue
        print('Jobs Executed: ', flush=True, end='')
        for _tuple in sql_dat:
            # if full_dv_mode and _tuple[1] == 'filter_pki.py':
            #    continue
            job_status, job_start_time, job_end_time = pid_worker(
                pid,
                jobid=_tuple[1],
                pid_path=pid_path,
                launcher=_tuple[0],
                full_dv_mode=full_dv_mode
            )
            print(f'{_tuple[1]}, ', flush=True, end='')
            job_list.append(_tuple[1])
            fail_update_status = update_job_tracker(
                cnx,
                pid,
                j_id=_tuple[1],
                j_status=job_status,
                j_start_time=job_start_time,
                j_end_time=job_end_time
            )
            if fail_update_status:
                print(f'Unable to update tracker|{fail_update_status}')
        print('<END>')
    except Exception as err:
        print(f'{pid}|DB Launchpad Error|{err}')
        record_error(
            cnx,
            pid,
            dtask='agent',
            err_src='AIO-Launchpad',
            err_txt=f'{err}'
        )
        ziz(ziz_time)
        continue

    consolidate_pid_file(
        cnx,
        pid,
        dtask=dtask,
        pid_path=pid_path,
        jlist=job_list,
    )
    cnx.close()

    print(f'Cycle Finished at {dtm.utcnow()}. Snoozing for {ziz_time}')
    print(f'{64 * line_sep}')
    ziz(ziz_time)
