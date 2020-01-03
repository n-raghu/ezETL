import os
import sys
import subprocess as sbp
from time import sleep as ziz
from datetime import datetime as dtm

pid = int(dtm.timestamp(dtm.utcnow()))


def pid_worker(pid, jobid, launcher='python'):
    with open(f'{pid}_{jobid}.log', 'w') as zip_log_file:
        with open(f'{pid}_{jobid}.err', 'w') as zip_err_file:
            proc = sbp.Popen(
                [
                    launcher,
                    jobid,
                    str(pid),
                ],
                stderr=zip_err_file,
                stdout=zip_log_file,
            )
    return proc.communicate()


def consolidate_pid_file(pid, jlist):
    job_sep = '-'
    pid_log_file = f'{pid}.log'
    for _job in jlist:
        try:
            with open(pid_log_file, 'w') as pid_file:
                with open(f'{pid}_{_job}.err', 'r') as log_file:
                    _info = log_file.read()
                if not _info:
                    _info = 'No Errors directed to console'
                pid_file.write(f'jobid: {_job}\n')
                pid_file.write('ERR:\n')
                pid_file.write(_info)
                pid_file.write('\n')
                pid_file.write('OUT:\n')
                with open(f'{pid}_{_job}.log', 'r') as log_file:
                    _info = log_file.read()
                pid_file.write(_info)
                pid_file.write('\n')
                pid_file.write(f'{64 * job_sep}')
            os.remove(f'{pid}_{_job}.log')
            os.remove(f'{pid}_{_job}.err')
        except Exception as err:
            with open('agent.err', 'a') as agent_err:
                agent_err.write(str(err))
    return pid_log_file


while True:
    pid = int(dtm.timestamp(dtm.utcnow()))
    job_list = [
        'tester.py'
    ]

    for _job in job_list:
        pid_worker(
            pid=pid,
            jobid=_job,
        )

    consolidate_pid_file(
        pid=pid,
        jlist=job_list,
    )
    ziz(9)
