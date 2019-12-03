from dimlib import cfg
from dimlib import os, sys
from iogen import StrIOGenerator
from dimlib import file_path_splitter
from dimtraces import error_trace, dimlogger
from dimlib import ProcessPoolExecutor, fork_complete
from dbops import pgconnector, create_ins_tbl, create_mother_tables


def zip_to_tbl(urx, one_set, mother_schema_set):
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


def heart(file_set, mother_schema_set):
    with ProcessPoolExecutor(max_workers=max_cpu_workers) as executor:
        pool_dictionary = {
            executor.submit(
                zip_to_tbl,
                pguri,
                one_set,
                mother_schema_set,
            ): one_set for one_set in file_set
        }
        for future in fork_complete(pool_dictionary):
            key = pool_dictionary[future]
            print(f'{key} - {future.result()}')


if __name__ == '__main__':
    t1 = tpc()
    dv_schema = create_mother_tables(pguri)
    storage_set = get_file_set(S3_PATH)
    heart(storage_set, dv_schema)
    print(f'Total Time Taken: {tpc() - t1}')
