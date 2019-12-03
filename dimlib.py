import os
import sys
from glob import iglob
from zipfile import ZipFile
from time import perf_counter as tpc
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector

cfg = {}
with open('story.yml') as _cfg_obj:
    raw_cfg = yml_safe_load(_cfg_obj)

try:
    _pgauth = f"{raw_cfg['datastore']['uid']}:{raw_cfg['datastore']['pwd']}"
    _pghost = f"{raw_cfg['datastore']['host']}:{raw_cfg['datastore']['port']}"
    _pgdbo = f"{raw_cfg['datastore']['dbn']}"
    cfg = {
        'csv_sep': raw_cfg['field_separator']
        'fmt_file_extn': raw_cfg['fmt_extension']
        'dat_file_extn': raw_cfg['file_extension']
        'pguri': f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}'
        'max_cpu_workers': raw_cfg['max_workers']
        'dat_file_path': raw_cfg['file_path_S3']
    }
    del _pgauth
    del _pgdbo
    del _pghost
except Exception as err:
    sys.exit(f'import-config-err: {err}')


def file_path_splitter(dat_file_with_path, file_path):
    index = dat_file_with_path.find(file_path)
    if index != -1 and index + len(file_path) < len(dat_file_with_path):
        return dat_file_with_path[index + len(file_path):]
    else:
        return None
