import os
import sys
from glob import iglob
from time import perf_counter as tpc

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector

from iogen import StrIOGenerator

S3_PATH = 'dat/'
with open('story.yml') as cfg_obj:
    cfg = yml_safe_load(cfg_obj)

try:
    dat_file_extn = cfg['file_extension']
    _pgauth = f"{cfg['datastore']['uid']}:{cfg['datastore']['pwd']}"
    _pghost = f"{cfg['datastore']['host']}:{cfg['datastore']['port']}"
    _pgdbo = f"{cfg['datastore']['dbn']}"
    pguri = f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}'
    S3_PATH += f'*.{dat_file_extn}'
    del _pgauth
    del _pgdbo
    del _pghost
except Exception as err:
    sys.exit(f'import-config-err: {err}')

all_files = [
    _ for _ in iglob(S3_PATH, recursive=True)
]


def get_csv_structure(xfile):
    with open(xfile, 'r') as file_itr:
        head = file_itr.readline()
    return ','.join(head.split('|'))


def create_stage_tbl(pgx, tbl):
    tbl_statement = f'''
    CREATE TABLE IF NOT EXISTS {tbl}(
        f_stamp TIMESTAMP WITHOUT TIME ZONE,
        f_str TEXT,
        f_int INT,
        f_float FLOAT,
        f_bool BOOLEAN) '''
    try:
        with pgx.cursor() as pgcur:
            pgcur.execute(tbl_statement)
            pgx.commit()
        return True
    except Exception as err:
        return err


def file_to_tbl(urx, dat_file):
    pgx = urx
    dat_file_name = os.path.splitext(os.path.basename(dat_file))[0]
    create_stage_tbl(pgx, dat_file_name)
    tbl_header = get_csv_structure(dat_file)
    pg_cp_statement = f"COPY {dat_file_name}({tbl_header}) FROM STDIN WITH CSV HEADER DELIMITER AS '|' "
    with open(dat_file, 'r') as dat_obj:
        print(f'{dat_file} ACCESSED.')
        csv_dat = StrIOGenerator(dat_obj)
        with pgx.cursor() as pgcur:
            pgcur.copy_expert(sql=pg_cp_statement, file=csv_dat)
    pgx.commit()
    pgx.close()
    return f'{dat_file} complete'


t1 = tpc()
for flat_file in all_files:
    file_to_tbl(
        pgconnector(pguri),
        flat_file
    )
print(f'Total Time Taken: {tpc() - t1}')
