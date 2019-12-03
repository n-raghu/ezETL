import os
import sys
from glob import iglob
from zipfile import ZipFile
from time import perf_counter as tpc
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector


with open('story.yml') as cfg_obj:
    cfg = yml_safe_load(cfg_obj)

try:
    csv_sep = cfg['field_separator']
    fmt_file_extn = cfg['fmt_extension']
    dat_file_extn = cfg['file_extension']
    _pgauth = f"{cfg['datastore']['uid']}:{cfg['datastore']['pwd']}"
    _pghost = f"{cfg['datastore']['host']}:{cfg['datastore']['port']}"
    _pgdbo = f"{cfg['datastore']['dbn']}"
    pguri = f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}'
    max_cpu_workers = cfg['max_workers']
    S3_PATH = cfg['file_path_S3']
    del _pgauth
    del _pgdbo
    del _pghost
except Exception as err:
    sys.exit(f'import-config-err: {err}')


def file_to_tbl(urx, one_set, mother_schema_set):
    _t = tpc()
    pgx = pgconnector(urx)
    dat_file_fmt = fmt_to_json(one_set['fmt_file'])
    create_ins_tbl(
        pgx,
        mother_tbl=one_set['mother_tbl'],
        ins_tbl=one_set['ins_tbl'],
        mother_tbl_structure=mother_schema_set[0],
        ins_tbl_structure=dat_file_fmt[0],
    )
    tbl_header = get_csv_structure(one_set['dat_file'])
    pg_cp_statement = f"COPY {one_set['ins_tbl']}({tbl_header}) FROM STDIN WITH CSV HEADER DELIMITER AS '{csv_sep}' "
    with open(one_set['dat_file'], 'r') as dat_obj:
        csv_dat = StrIOGenerator(dat_obj)
        with pgx.cursor() as pgcur:
            pgcur.copy_expert(sql=pg_cp_statement, file=csv_dat)
    pgx.commit()
    pgx.close()
    return tpc() - _t


def file_path_splitter(dat_file_with_path, file_path):
    index = dat_file_with_path.find(file_path)
    if index != -1 and index + len(file_path) < len(dat_file_with_path):
        return dat_file_with_path[index + len(file_path):]
    else:
        return None
