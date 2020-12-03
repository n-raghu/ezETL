import sys
from zipfile import ZipFile
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed

from psycopg2 import connect as pgconnector

from dimlib import refresh_config, timetracer
from zipops import build_file_set, file_scanner, fmt_to_json, reporting_dtypes, get_csv_structure
from dbops import get_active_tables, create_ins_tbl
from iogen import StrIOGenerator

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
    for dset in ingestion_info:
        zip_to_tbl(
            dat_sep=cfg['xport_cfg']['field_separator'],
            quote_pattern=cfg['xport_cfg']['dat_quote'],
            urx=cfg['dburi'],
            dtypes=reporting_dtypes(),
            one_set=dset
        )
