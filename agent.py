from dimlib import os, sys, tpc, ZipFile
from dimlib import refresh_config, pgconnector
from iogen import StrIOGenerator
from zipops import build_file_set, fmt_to_json, get_csv_structure
from dimlib import file_path_splitter
from dbops import create_ins_tbl, create_mother_tables
from dimlib import ProcessPoolExecutor, fork_complete


def zip_to_tbl(csv_sep, db_schema, urx, one_set, ):
    pgx = pgconnector(urx)
    tbl_json = fmt_to_json(
        one_set['dataset'],
        one_set['fmt_file'],
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
    pg_cp_statement = f"COPY stage.{one_set['ins_tbl']}({tbl_header}) FROM STDIN WITH CSV HEADER DELIMITER AS '{csv_sep}' "
    with ZipFile(one_set['dataset'], 'r') as zfile:
        with zfile.open(one_set['dat_file'], 'r') as dat_obj:
            csv_dat = StrIOGenerator(dat_obj)
            with pgx.cursor() as pgcur:
                pgcur.copy_expert(sql=pg_cp_statement, file=csv_dat)
    pgx.commit()
    pgx.close()
    return f"{one_set['ins_tbl']} - {one_set['dataset']}"


def aio_launchpad(
    csv_sep,
    cpu_workers,
    dburi,
    db_schema,
    file_set
):
    with ProcessPoolExecutor(max_workers=cpu_workers) as executor:
        pool_dictionary = {
            executor.submit(
                zip_to_tbl,
                csv_sep,
                db_schema,
                dburi,
                one_set,
            ): one_set for one_set in file_set
        }
        for future in fork_complete(pool_dictionary):
            key = pool_dictionary[future]
            print(future.result())


if __name__ == '__main__':
    cfg = refresh_config()
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
        file_set=storage_set,
    )
