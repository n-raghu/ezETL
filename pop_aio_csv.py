import os
import sys
from glob import iglob
from time import perf_counter as tpc
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector

from iogen import StrIOGenerator

S3_PATH = 'dat/'
with open('story.yml') as cfg_obj:
    cfg = yml_safe_load(cfg_obj)

try:
    csv_sep = cfg['field_separator']
    dat_file_extn = cfg['file_extension']
    _pgauth = f"{cfg['datastore']['uid']}:{cfg['datastore']['pwd']}"
    _pghost = f"{cfg['datastore']['host']}:{cfg['datastore']['port']}"
    _pgdbo = f"{cfg['datastore']['dbn']}"
    pguri = f'postgresql://{_pgauth}@{_pghost}/{_pgdbo}'
    max_cpu_workers = cfg['max_workers']
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
    _t = tpc()
    pgx = pgconnector(urx)
    dat_file_name = os.path.splitext(os.path.basename(dat_file))[0]
    create_stage_tbl(pgx, dat_file_name)
    tbl_header = get_csv_structure(dat_file)
    pg_cp_statement = f"COPY {dat_file_name}({tbl_header}) FROM STDIN WITH CSV HEADER DELIMITER AS '{csv_sep}' "
    with open(dat_file, 'r') as dat_obj:
        csv_dat = StrIOGenerator(dat_obj)
        with pgx.cursor() as pgcur:
            pgcur.copy_expert(sql=pg_cp_statement, file=csv_dat)
    pgx.commit()
    pgx.close()
    return tpc() - _t


def heart():
    with ProcessPoolExecutor(max_workers=max_cpu_workers) as executor:
        pool_dictionary = {
            executor.submit(
                file_to_tbl,
                pguri,
                flat_file
            ): flat_file for flat_file in all_files
        }
        for future in fork_complete(pool_dictionary):
            key = pool_dictionary[future]
            print(f'{key} - {future.result()}')


if __name__ == '__main__':
    t1 = tpc()
    heart()
    print(f'Total Time Taken: {tpc() - t1}')