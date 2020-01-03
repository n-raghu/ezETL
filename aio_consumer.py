from glob import iglob
from datetime import datetime as dtm
from time import time as ttime
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from iogen import StrIOGenerator
from dimlib import file_path_splitter, file_decrypter
from dimlib import os, sys, ZipFile, tpt
from dimlib import refresh_config, pgconnector, GPG
from zipops import fmt_to_json, reporting_dtypes
from dbops import create_ins_tbl, create_mother_tables
from dbops import collectiontracker, record_job_success
from zipops import build_file_set, get_csv_structure


def zip_to_tbl(
    csv_sep,
    null_pattern,
    quote_pattern,
    db_schema,
    urx,
    dtypes,
    one_set,
):
    start_time = dtm.now()
    pgx = pgconnector(urx)
    tbl_json = fmt_to_json(
        zipset=one_set['dataset'],
        jsonfile=one_set['fmt_file'],
        dtdct=dtypes,
    )
    ins_tbl_status = create_ins_tbl(
        pgx,
        mother_tbl=one_set['mother_tbl'],
        ins_tbl=one_set['ins_tbl'],
        db_schema=db_schema,
        ins_tbl_map=tbl_json,
    )
    if not ins_tbl_status:
        sys.exit(1)
    tbl_header = get_csv_structure(
        one_set['dataset'],
        one_set['dat_file'],
    )
    pg_cp_statement = f"COPY stage.{one_set['ins_tbl']}({tbl_header}) FROM STDIN WITH DELIMITER '{csv_sep}' CSV HEADER NULL '{null_pattern}' QUOTE '{quote_pattern}' "
    try:
        with ZipFile(one_set['dataset'], 'r') as zfile:
            with zfile.open(one_set['dat_file'], 'r') as dat_obj:
                csv_dat = StrIOGenerator(
                    binary_chunk=dat_obj,
                    text_enc='latin_1',
                )
                with pgx.cursor() as pgcur:
                    pgcur.copy_expert(sql=pg_cp_statement, file=csv_dat)
        pgx.commit()
        finish_time = dtm.now()
        record_job_success(
            pgx,
            pid,
            ins_tbl=one_set['ins_tbl'],
            start_time=start_time,
            end_time=finish_time,
        )
    except Exception as err:
        sys.exit(err)
    return f"Finished at {dtm.now()} - {one_set['ins_tbl']} - {one_set['dataset']}"


def aio_decrypter(key_file, key_passcode, en_file_set):
    with open(key_file, 'r') as kfile:
        key = kfile.read()
    gpg_instance = GPG()
    passcode = key_passcode
    gpg_instance.import_keys(key)
    with ProcessPoolExecutor(max_workers=3) as executor:
        pool_dictionary = {
            executor.submit(
                file_decrypter,
                en_file,
                passcode,
                gpg_instance,
            ): en_file for en_file in en_file_set
        }
        for _future in fork_complete(pool_dictionary):
            key = pool_dictionary[_future]


def aio_launchpad(
    csv_sep,
    null_pattern,
    quote_pattern,
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
                null_pattern,
                quote_pattern,
                db_schema,
                dburi,
                dtypes,
                one_set,
            ): one_set for one_set in file_set
        }
        for _future in fork_complete(pool_dictionary):
            key = pool_dictionary[_future]
            print(_future.result())


if __name__ == '__main__':
    if len(sys.argv) > 1:
        pid = int(sys.argv[1])
    else:
        pid = int(dtm.timestamp(dtm.utcnow()))
    print(f'{pid} generated for this cycle.')
    try:
        cfg = refresh_config()
        all_enc_files = list(
            iglob(
                f"{cfg['xport_cfg']['en_zip_path']}**/*.gpg",
                recursive=True,
            )
        )
        aio_decrypter(
            f"{cfg['xport_cfg']['key_file']}",
            key_passcode=cfg['xport_cfg']['en_key_passcode'],
            en_file_set=all_enc_files,
        )
        dtypes_dict = reporting_dtypes()
        db_schema = cfg['db_schema']
        storage_set = build_file_set(cfg)
        collectiontracker(
            dburi=cfg['dburi'],
            pid=pid,
            storageset=storage_set,
        )
        create_mother_tables(cfg['dburi'], storage_set)
        t1 = ttime()
        aio_launchpad(
            csv_sep=cfg['xport_cfg']['field_separator'],
            null_pattern=cfg['xport_cfg']['null_pattern'],
            quote_pattern=cfg['xport_cfg']['dat_quote'],
            cpu_workers=cfg['cpu_workers'],
            db_schema=cfg['db_schema'],
            dburi=cfg['dburi'],
            dtypes=dtypes_dict,
            file_set=storage_set,
        )
        print(f'Time Elapsed: {ttime() - t1}')
    except Exception as err:
        with open(str(pid), 'w') as pid_file:
            pid_file.write(str(err))
            sys.exit(err)
