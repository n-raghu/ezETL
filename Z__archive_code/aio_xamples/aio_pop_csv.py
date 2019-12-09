import os
import sys
from glob import iglob
from time import perf_counter as tpc
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from yaml import safe_load as yml_safe_load
from psycopg2 import connect as pgconnector

from iogen import StrIOGenerator

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


def get_csv_structure(xfile):
    with open(xfile, 'r') as file_itr:
        head = file_itr.readline()
    return ','.join(head.split('|'))


def create_ins_tbl(
    pgx,
    mother_tbl,
    ins_tbl,
    mother_tbl_structure,
    ins_tbl_structure
):
    tbl_columns = ''
    ins_only_columns = set(ins_tbl_structure) - set(mother_tbl_structure)
    for fld in ins_only_columns:
        tbl_columns += f'{fld} {ins_tbl_structure[fld]},'
    if tbl_columns.endswith(','):
        tbl_columns = tbl_columns[:-1]
    tbl_statement = f''' CREATE TABLE IF NOT EXISTS {ins_tbl}(
        {tbl_columns}
        ) INHERITS ({mother_tbl});'''
    try:
        with pgx.cursor() as pgcur:
            pgcur.execute(tbl_statement)
            pgx.commit()
        return True
    except Exception as err:
        return err


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


def create_mother_tables(pguri):
    cnx = pgconnector(pguri)
    with open('mother_tbl_schema.json', 'r') as jfile:
        schema_set = yml_safe_load(jfile)
    tbl_statements = []
    for tbl in schema_set:
        this_tbl_fields = ''
        for column_name, data_type in tbl.items():
            this_tbl_fields += f'{column_name} {data_type},'
        if this_tbl_fields.endswith(','):
            this_tbl_fields = this_tbl_fields[:-1]
        tbl_statements.append(
            f'CREATE TABLE IF NOT EXISTS user_master({this_tbl_fields})'
        )
    with cnx.cursor() as pgcur:
        for stmt in tbl_statements:
            pgcur.execute(stmt)
    cnx.commit()
    del tbl_statements
    cnx.close()
    return schema_set


def heart(file_set, mother_schema_set):
    with ProcessPoolExecutor(max_workers=max_cpu_workers) as executor:
        pool_dictionary = {
            executor.submit(
                file_to_tbl,
                pguri,
                one_set,
                mother_schema_set,
            ): one_set for one_set in file_set
        }
        for future in fork_complete(pool_dictionary):
            key = pool_dictionary[future]
            print(f'{key} - {future.result()}')


def get_file_set(file_path):
    file_set = []
    all_files = list(
        iglob(
            f'{file_path}**/*.{dat_file_extn}',
            recursive=True
        )
    )
    print(all_files)
    for dat_file in all_files:
        ins_file_name = file_path_splitter(dat_file, file_path)
        print(ins_file_name)
        _ilist = ins_file_name.split('/')
        ins_code = _ilist[0]
        _flist = dat_file.split('.')
        _file = _flist[0]
        _mother_tbl = os.path.splitext(os.path.basename(dat_file))[0]
        file_set.append(
            {
                'icode': ins_code,
                'dat_file': dat_file,
                'fmt_file': f'{_file}.{fmt_file_extn}',
                'mother_tbl': _mother_tbl,
                'ins_tbl': f'{ins_code}_{_mother_tbl}',
            }
        )
    return file_set


def file_path_splitter(dat_file_with_path, file_path):
    index = dat_file_with_path.find(file_path)
    if index != -1 and index + len(file_path) < len(dat_file_with_path):
        return dat_file_with_path[index + len(file_path):]
    else:
        return None


def fmt_to_json(jsonfile):
    with open(jsonfile, 'r') as jfile:
        return yml_safe_load(jfile)


if __name__ == '__main__':
    t1 = tpc()
    dv_schema = create_mother_tables(pguri)
    storage_set = get_file_set(S3_PATH)
    heart(storage_set, dv_schema)
    print(f'Total Time Taken: {tpc() - t1}')
