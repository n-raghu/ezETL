import sys
from random import shuffle
from zipfile import ZipFile
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed

from psycopg2 import connect as pgconnector

from iogen import StrIOGenerator
from dimlib import refresh_config, timetracer
from dbops import get_active_tables, create_ins_tbl
from zipops import build_file_set, file_scanner, fmt_to_json, \
    reporting_dtypes, get_csv_structure


def launchpad(
    cfg,
    active_tables,
    dat_sep,
    quote_pattern,
    cpu_workers,
    dburi,
    dtypes,
    file_set,
):
    with ProcessPoolExecutor(max_workers=cpu_workers) as executor:
        pool_dict = {
            executor.submit(
                build_file_set,
                cfg,
                dump,
                active_tables
            ): dump for dump in file_set
        }
    file_catalogue = list()
    for _future in as_completed(pool_dict):
        file_catalogue.extend(_future.result())

    shuffle(file_catalogue)
    with ProcessPoolExecutor(max_workers=cpu_workers) as executor:
        pool_dictionary = {
            executor.submit(
                zip_to_tbl,
                dat_sep,
                quote_pattern,
                dburi,
                dtypes,
                one_set,
            ): one_set for one_set in file_catalogue
        }
    print('')
    print('')
    print(pool_dictionary)


@timetracer
def zip_to_tbl(
    dat_sep,
    quote_pattern,
    urx,
    dtypes,
    one_set,
):
    cnx = pgconnector(urx)
    tbl_json = fmt_to_json(
        zipset=one_set['dataset'],
        jsonfile=one_set['fmt_file'],
        dtdct=dtypes,
    )
    create_ins_tbl(
        cnx,
        mother_tbl=one_set['mother_tbl'],
        ins_tbl=one_set['ins_tbl'],
        ins_tbl_map=tbl_json,
    )
    null_pattern = 'NULL'
    tbl_header = get_csv_structure(
        zipset=one_set['dataset'],
        datfile=one_set['dat_file']
    )
    pg_cp_statement = f"""
                        COPY \
                            {one_set['ins_tbl']}({tbl_header}) \
                        FROM \
                            STDIN \
                        WITH \
                            CSV HEADER \
                            DELIMITER '{dat_sep}' \
                            NULL '{null_pattern}' \
                            QUOTE '{quote_pattern}' \
                    """
    with ZipFile(one_set['dataset'], 'r') as zfile:
        with zfile.open(one_set['dat_file'], 'r') as cfile:
            chunk = StrIOGenerator(cfile, text_enc='latin_1')
            with cnx.cursor() as dbcur:
                dbcur.copy_expert(sql=pg_cp_statement, file=chunk)
    cnx.commit()
    cnx.close()


if __name__ == '__main__':
    cfg = refresh_config()
    cnx = pgconnector(cfg['dburi'])
    file_dumps = file_scanner(cfg['xport_cfg']['zip_path'])
    active_tables = get_active_tables(cnx)
    ingestion_info = []
    for dump in file_dumps:
        ingestion_info.extend(
            build_file_set(cfg, dump, active_tables)
        )
    launchpad(
        cfg=cfg,
        active_tables=active_tables,
        cpu_workers=3,
        dat_sep=cfg['xport_cfg']['field_separator'],
        quote_pattern=cfg['xport_cfg']['dat_quote'],
        dburi=cfg['dburi'],
        dtypes=reporting_dtypes(),
        file_set=file_dumps,
    )
    cnx.close()
