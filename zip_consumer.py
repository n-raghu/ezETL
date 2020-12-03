from glob import iglob
from random import shuffle
from heapq import nsmallest
from time import time as ttime
from datetime import datetime as dtm
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as fork_complete

from iogen import StrIOGenerator
from dimlib import os, sys, ZipFile, tpt
from dimlib import refresh_config, pgconnector, GPG
from dimlib import file_path_splitter, file_decrypter
from build_cache import create_schema_tables_for_main_tbl
from zipops import build_file_set, get_csv_structure, get_rowversion
from dbops import collectiontracker, record_job_success, record_error
from zipops import fmt_to_json, reporting_dtypes, record_stmt_to_file
from dbops import create_ins_tbl, create_mother_tables, get_active_tables


def zip_to_tbl(
    debug_mode,
    csv_sep,
    quote_pattern,
    db_schema,
    urx,
    dtypes,
    one_set,
    fetch_rowversion=False,
):
    start_time = dtm.utcnow()
    _timer = ttime()
    try:
        pgx = pgconnector(urx)
        tbl_json = fmt_to_json(
            zipset=one_set['dataset'],
            jsonfile=one_set['fmt_file'],
            dtdct=dtypes,
        )
        if one_set['build_ins_tbl']:
            ins_tbl_status = create_ins_tbl(
                pgx,
                mother_tbl=one_set['mother_tbl'],
                ins_tbl=one_set['ins_tbl'],
                db_schema=db_schema,
                ins_tbl_map=tbl_json,
            )
            if not ins_tbl_status:
                print('INS Table failed')
                sys.exit(1)
        if one_set['build_cache_tbl']:
            create_schema_tables_for_main_tbl(
                one_set['tbl_name'],
                pgx,
                instance_list=[one_set['icode']]
            )
        tbl_header = get_csv_structure(
            one_set['dataset'],
            one_set['dat_file'],
        )
    except Exception as err:
        if not debug_mode:
            record_error(
                pgx,
                pid,
                dtask='zip_consumer',
                err_src=f"{one_set['dat_file']}|{one_set['dataset']}",
                err_txt=f"zip_to_tbl|fetchHeaders|{one_set['ins_tbl']}|{err}"
            )
        pgx.close()
        return f"Failed - {one_set['ins_tbl']} | zip_to_tbl|Create INS TBL and fetch headers|{err}"

    if one_set['app_name'] == 'lms':
        null_pattern = 'NULL'
        pg_cp_statement = f"COPY {db_schema}.{one_set['ins_tbl']}({tbl_header}) FROM STDIN WITH DELIMITER '{csv_sep}' CSV HEADER NULL '{null_pattern}' QUOTE '{quote_pattern}' "
    else:
        null_pattern = '\\N'
        pg_cp_statement = f"COPY {db_schema}.{one_set['ins_tbl']}({tbl_header}) FROM STDIN WITH DELIMITER '{csv_sep}' CSV HEADER NULL '{null_pattern}' QUOTE '{quote_pattern}' ESCAPE '\\' "

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
        finish_time = dtm.utcnow()
        _rowversion = False
        if not debug_mode:
            if fetch_rowversion and not one_set['pki_tbl']:
                _rowversion = get_rowversion(
                    zipset=one_set['dataset'],
                    stampid=one_set['stampid'],
                    app_name=one_set['app_name'],
                    tbl_name=one_set['mother_tbl']
                )
                if not type(_rowversion).__name__ == 'int':
                    record_error(
                        pgx,
                        pid,
                        dtask=dtask,
                        err_src=f"{one_set['dat_file']}|{one_set['dataset']}",
                        err_txt=f"fetchRowversion|{one_set['ins_tbl']}|{_rowversion}"
                    )
                    _rowversion = False
            record_job_success(
                pgx,
                pid,
                ins_tbl=one_set['ins_tbl'],
                start_time=start_time,
                end_time=finish_time,
                rowversion=_rowversion,
            )
    except Exception as err:
        if not debug_mode:
            record_error(
                pgx,
                pid,
                dtask='zip_consumer',
                err_src=f"{one_set['dat_file']}|{one_set['dataset']}",
                err_txt=f"zip_to_tbl|{one_set['ins_tbl']}|{err}"
            )
        else:
            print(f"zip_to_tbl|{one_set['ins_tbl']}|{err}")
        pgx.close()
        return f"Failed - {one_set['ins_tbl']}"
    pgx.close()
    return f"Finished in {round(ttime() - _timer, 1)}s | {one_set['ins_tbl']}"


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
    debug_mode,
    csv_sep,
    quote_pattern,
    cpu_workers,
    dburi,
    db_schema,
    dtypes,
    file_set,
    rowversion
):
    shuffle(file_set)
    with ProcessPoolExecutor(max_workers=cpu_workers) as executor:
        pool_dictionary = {
            executor.submit(
                zip_to_tbl,
                debug_mode,
                csv_sep,
                quote_pattern,
                db_schema,
                dburi,
                dtypes,
                one_set,
                rowversion,
            ): one_set for one_set in file_set
        }
        for _future in fork_complete(pool_dictionary):
            key = pool_dictionary[_future]
            print(_future.result())


if __name__ == '__main__':
    # Read CONFIG
    dtask = 'zip_consumer'
    try:
        cfg = refresh_config()
        dburi = cfg['dburi']
        db_schema = cfg['db_schema']
        cnx = pgconnector(dburi)
    except Exception as err:
        sys.exit(f'CNXFail|{err}')

    # Read Command Line Arguments
    try:
        if len(sys.argv) > 1:
            debug = False
            pid = int(sys.argv[1])
            drop_cache_schema = '-k' in sys.argv
        else:
            debug = True
            drop_cache_schema = False
            cfg['xport_cfg']['ingest_pki'] = False
            pid = int(ttime())
            print(f'0. zip_consumer starated in DEBUG mode.', end='')
            print(f' {pid} assigned for this run.')
    except Exception as err:
        sys.exit(f'Read Command Line Arguments|{err}')

    # Get Active INSTANCE List
    print('1. Fetch active instance list', end='')
    try:
        _sql = '''SELECT instancecode
            FROM framework.instanceconfig
                WHERE isactive;'''
        with cnx.cursor() as dbcur:
            dbcur.execute(_sql)
            sql_dat = dbcur.fetchall()
        active_instance_list = list(
            {_[0] for _ in sql_dat}
        )
        print(f'{active_instance_list}')
    except Exception as err:
        if not debug:
            record_error(
                cnx=cnx,
                pid=pid,
                dtask=dtask,
                err_src='get-active-instances',
                err_txt=str(err)
            )
        else:
            sys.exit(f'GetActiveINS|{err}')

    # Queue up eligible Files
    print('2. Queue up eligible files')
    eligible_enc_files = []
    set_unwanted_chars = {'', ' '}
    for _ins in active_instance_list:
        try:
            all_ins_files = list(iglob(
                f"{cfg['path_zip_dat']}{_ins}/*.gpg",
                recursive=True,
            ))
            this_ins_apps = {_.split('_')[1] for _ in all_ins_files}
            this_ins_apps = this_ins_apps - set_unwanted_chars
            _queue = []
            this_ins_app_files = {}
            for _app in this_ins_apps:
                this_ins_app_files[_app] = {_ for _ in all_ins_files if _app in _}
            for app in this_ins_app_files:
                app_file_set = this_ins_app_files[app]
                file_with_key_values = {}
                for _file in app_file_set:
                    _tmp_list = _file.split('/')
                    _key = _tmp_list[len(_tmp_list) - 1]
                    _val = file_with_key_values.get(_key, [])
                    _val.append(_file)
                    file_with_key_values[_key] = _val
                smallest_key = nsmallest(
                    1,
                    iterable=list(file_with_key_values.keys())
                )[0]
                _queue.append(file_with_key_values[smallest_key][0])
            eligible_enc_files.extend(_queue)
        except Exception as err:
            if not debug:
                record_error(
                    cnx=cnx,
                    pid=pid,
                    dtask=dtask,
                    err_src=_ins,
                    err_txt=f'FileQueue|{err}'
                )
            else:
                print(f'FileQueue|{_ins}|{err}')
            continue

    # Check eligible files and create worker files
    print('3. Worker files')
    try:
        if not len(eligible_enc_files) > 0:
            print('No files to ingest.')
            sys.exit()
        aio_decrypter(
            f"{cfg['xport_cfg']['key_file']}",
            key_passcode=cfg['xport_cfg']['en_key_passcode'],
            en_file_set=eligible_enc_files,
        )
        for _ in eligible_enc_files:
            print(f'- {_:>4}')
        del eligible_enc_files
    except Exception as err:
        record_error(
            cnx,
            pid,
            dtask=dtask,
            err_src='AIO-Decryptor',
            err_txt=f'{err}'
        )
        sys.exit(f'AIO-Decryptor|{err}')

    # Cycle File List
    print('4. Generate Cycle files')
    try:
        cycle_file_list = []
        dtypes_dict = reporting_dtypes()
        active_tables = get_active_tables(dburi)

        all_zip_files = list(
            iglob(
                f"{cfg['path_zip_dat']}**/*.{cfg['xport_cfg']['worker_xtn']}",
                recursive=True,
            ),
        )
        for decrypted_zip_file in all_zip_files:
            try:
                storage_set = build_file_set(
                    cfg,
                    worker_file=decrypted_zip_file,
                    active_collections=active_tables,
                    create_cache_tbl=True if cfg['xport_cfg']['ingest_pki'] and not drop_cache_schema else False
                )
                cycle_file_list.extend(storage_set)
            except Exception as err:
                if not debug:
                    record_error(
                        cnx=cnx,
                        pid=pid,
                        dtask=dtask,
                        err_src=decrypted_zip_file,
                        err_txt=f'build_cycle_files|{err}'
                    )
                else:
                    print(f'{decrypted_zip_file}|build_cycle_files|{err}')
        record_stmt_to_file(
            f"{cfg['agent']['pid_files']}{pid}",
            cycle_file_list
        )
    except Exception as err:
        if not debug:
            record_error(
                cnx=cnx,
                pid=pid,
                dtask=dtask,
                err_src='cycle_file_list',
                err_txt=f'{err}'
            )
        else:
            sys.exit(f'cycle_file_list|{err}')

    # Create Mother Tables
    print('5. Create Mother Tables')
    try:
        mother_table_list = []
        _check_set = set()
        for _set in cycle_file_list:
            if _set['mother_tbl'] not in _check_set:
                doc = _set.copy()
                d = next((row for row in active_tables if row['collection'] == _set['tbl_name']), None)
                if d:
                    doc.update(d)
                    mother_table_list.append(doc)
                    _check_set.add(_set['mother_tbl'])
        create_mother_tables(dburi, mother_table_list)
    except Exception as err:
        if not debug:
            record_error(
                cnx=cnx,
                pid=pid,
                dtask=dtask,
                err_src='create_mother_tables',
                err_txt=f'{err}'
            )
        else:
            print(f'create_mother_tables|{err}')

    # Launchpad
    print(f'6. Launchpad - {len(cycle_file_list)} files')
    try:
        if not debug:
            collectiontracker(
                dburi=dburi,
                pid=pid,
                storageset=cycle_file_list,
            )
            if drop_cache_schema:
                sql_recreate_schema = f'''
                    DROP SCHEMA IF EXISTS {cfg['cache_schema']} CASCADE;
                    CREATE SCHEMA IF NOT EXISTS {cfg['cache_schema']}; '''
                with cnx.cursor() as dbcur:
                    dbcur.execute(sql_recreate_schema)
                cnx.commit()
        if not cfg['xport_cfg']['ingest_pki']:
            print('')
            print('')
            print('INGEST_PKI DISABLED')
            print('')
        aio_launchpad(
            debug_mode=debug,
            csv_sep=cfg['xport_cfg']['field_separator'],
            quote_pattern=cfg['xport_cfg']['dat_quote'],
            cpu_workers=cfg['cpu_workers'],
            db_schema=db_schema,
            dburi=dburi,
            dtypes=dtypes_dict,
            file_set=cycle_file_list,
            rowversion=cfg['xport_cfg']['record_rowversion'],
        )
    except Exception as err:
        if not debug:
            record_error(
                cnx,
                pid,
                dtask=dtask,
                err_src='Launchpad',
                err_txt=f'{err}'
            )
        else:
            sys.exit(f'Launchpad|{err}')
    finally:
        cnx.close()
