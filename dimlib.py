import os
import sys
from glob import iglob
from zipfile import ZipFile
from collections import OrderedDict as odict

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector


def refresh_config():
    cfg = {}
    with open('story.yml') as _cfg_obj:
        raw_cfg = yml_safe_load(_cfg_obj)
    try:
        _pgauth = f"{raw_cfg['datastore']['uid']}:{raw_cfg['datastore']['pwd']}"
        _pghost = f"{raw_cfg['datastore']['host']}:{raw_cfg['datastore']['port']}"
        _pgdbo = f"{raw_cfg['datastore']['dbn']}"
        cfg = {
            'dburi': f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}',
            'cpu_workers': raw_cfg['max_workers'],
            'db_schema': raw_cfg['db_schema'],
            'xport_cfg': raw_cfg['xport_cfg'],
        }
        del _pgauth
        del _pgdbo
        del _pghost
    except Exception as err:
        sys.exit(f'import-config-err: {err}')
    return cfg


def file_path_splitter(dat_file_with_path, file_path):
    index = dat_file_with_path.find(file_path)
    if index != -1 and index + len(file_path) < len(dat_file_with_path):
        return dat_file_with_path[index + len(file_path):]
    else:
        return None
