import os
import sys
from glob import iglob
from time import perf_counter as tpc
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector

from iogen import StrIOGenerator

S3_DIR = 'dat/'
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
    S3_PATH = f'{S3_DIR}*.{dat_file_extn}'
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
    tbl_mappers = []
    json_file = f'{S3_DIR}{tbl}.json'
    with open(json_file, 'r') as jfile:
        tbl_field_list = yml_safe_load(jfile)
    print('YML FILE PARSED.')
    for field_set in tbl_field_list:
        if 'bit' == field_set['COL_TYPE'].lower():
            field_set['db_col'] = 'BOOLEAN'
        elif 'date' == field_set['COL_TYPE'].lower():
            field_set['db_col'] = 'DATE'
        elif 'time' == field_set['COL_TYPE'].lower():
            field_set['db_col'] = 'TIME'
        elif 'datetime' == field_set['COL_TYPE'].lower():
            field_set['db_col'] = 'TIMESTAMP'
        elif 'int' in field_set['COL_TYPE']:
            field_set['db_col'] = 'BIGINT'
        else:
            field_set['db_col'] = 'TEXT'
        field_set['TBL_COLUMN'] = field_set['TBL_COLUMN'].lower()
        tbl_mappers.append(field_set)
    tbl_ddl_statement = f'CREATE TABLE IF NOT EXISTS {tbl}('
    for map_set in tbl_mappers:
        tbl_ddl_statement += f"{map_set['TBL_COLUMN']} {map_set['db_col']},"
    if tbl_ddl_statement.endswith(','):
        tbl_ddl_statement = tbl_ddl_statement[:-1]
    tbl_ddl_statement = f'{tbl_ddl_statement})'
    try:
        with pgx.cursor() as pgcur:
            pgcur.execute(tbl_ddl_statement)
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
    pg_cp_statement = f"COPY {dat_file_name}({tbl_header.lower()}) FROM STDIN WITH DELIMITER '{csv_sep}' CSV HEADER null 'NULL' "
    try:
        with open(dat_file, 'r') as dat_obj:
            csv_dat = StrIOGenerator(dat_obj)
            print(dat_obj)
            with pgx.cursor() as pgcur:
                pgcur.copy_expert(sql=pg_cp_statement, file=dat_obj,)
        pgx.commit()
    except Exception as err:
        sys.exit(err)
    finally:
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