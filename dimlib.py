import re
import os
import sys
from functools import wraps
from time import time, sleep
from yaml import safe_load as yml_safe_load


def refresh_config():
    cfg = {}
    with open('config.yml') as _cfg_obj:
        raw_cfg = yml_safe_load(_cfg_obj)
    try:
        _pgauth = f"{raw_cfg['datastore']['user']}:{raw_cfg['datastore']['passwd']}"
        _pghost = f"{raw_cfg['datastore']['server']}:{raw_cfg['datastore']['port']}"
        _pgdbo = f"{raw_cfg['datastore']['db']}"
        path_zip_dat = f"{raw_cfg['xport_cfg']['zip_path']}"
        if not path_zip_dat.endswith('/'):
            path_zip_dat += '/'
        path_zip_archive = f"{raw_cfg['xport_cfg']['archive_path']}"
        if not path_zip_archive.endswith('/'):
            path_zip_archive += '/'
        cfg = {
            'dburi': f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}',
            'cpu_workers': raw_cfg['max_workers'],
            'xport_cfg': raw_cfg['xport_cfg'],
        }
    except Exception as err:
        sys.exit(f'dimlib|import-config-err|{err}')
    return cfg


def timetracer(myfun):

    @wraps(myfun)
    def _wrapper(*args, **kwargs):
        _t1 = time()
        _outcome = myfun(*args, **kwargs)
        print(f'{myfun.__name__} completed in {round((time() - _t1), 1)}, ', end='')
        print(f'PID - {os.getpid()}, Parent PID - {os.getppid()}')
        return _outcome
    return _wrapper
