from datetime import datetime as dtm
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from iogen import StrIOGenerator
from dimlib import file_path_splitter
from dimlib import os, sys, tpc, ZipFile
from dimlib import refresh_config, pgconnector
from zipops import fmt_to_json, reporting_dtypes
from dbops import create_ins_tbl, create_mother_tables
from zipops import build_file_set, fmt_to_json, get_csv_structure


def zip_to_tbl(csv_sep, db_schema, urx, dtypes, one_set, ):
    pgx = pgconnector(urx)
    tbl_json = fmt_to_json(
        zipset=one_set['dataset'],
        jsonfile=one_set['fmt_file'],
        dtdct=dtypes,
    )
    create_ins_tbl(
        pgx,
        mother_tbl=one_set['mother_tbl'],
        ins_tbl=one_set['ins_tbl'],
        db_schema=db_schema,
        ins_tbl_map=tbl_json,
    )
    tbl_header = get_csv_structure(
        one_set['dataset'],
        one_set['dat_file'],
    )
    pg_cp_statement = f"COPY stage.{one_set['ins_tbl']}({tbl_header}) FROM STDIN WITH DELIMITER '{csv_sep}' CSV HEADER NULL 'NULL' QUOTE '`' "
    try:
        with ZipFile(one_set['dataset'], 'r') as zfile:
            with zfile.open(one_set['dat_file'], 'r') as dat_obj:
                csv_dat = StrIOGenerator(
                    binary_chunk=dat_obj,
                    text_enc='cp1252',
                )
                with pgx.cursor() as pgcur:
                    pgcur.copy_expert(sql=pg_cp_statement, file=csv_dat)
        pgx.commit()
        job_flag = True
    except Exception as err:
        print(err)
        job_flag = False
    finally:
        pgx.close()
    if job_flag:
        return f"Finished at {dtm.now()} - {one_set['ins_tbl']} - {one_set['dataset']}"
    else:
        return f"Failed at {dtm.now()} - {one_set['ins_tbl']} - {one_set['dataset']}"


def aio_launchpad(
    csv_sep,
    cpu_workers,
    dburi,
    db_schema,
    dtypes,
    file_set,
):
    with ProcessPoolExecutor(max_workers=cpu_workers) as executor:
        pool_dictionary = {
            executor.submit(
                zip_to_tbl,
                csv_sep,
                db_schema,
                dburi,
                dtypes,
                one_set,
            ): one_set for one_set in file_set
        }
        for future in fork_complete(pool_dictionary):
            key = pool_dictionary[future]
            print(future.result())


if __name__ == '__main__':
    pid = int(dtm.timestamp(dtm.utcnow()))
    try:
        cfg = refresh_config()
        dtypes_dict = reporting_dtypes()
        db_schema = cfg['db_schema']
        storage_set = build_file_set(cfg)
        mother_tbl_list = list(
            {
                _['mother_tbl'] for _ in storage_set
            }
        )
        create_mother_tables(cfg['dburi'], storage_set)
        aio_launchpad(
            csv_sep=cfg['xport_cfg']['field_separator'],
            cpu_workers=cfg['cpu_workers'],
            db_schema=cfg['db_schema'],
            dburi=cfg['dburi'],
            dtypes=dtypes_dict,
            file_set=storage_set,
        )
    except Exception as err:
        with open(pid, 'w') as pid_file:
            pid_file.write(err)
