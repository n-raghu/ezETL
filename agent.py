from dimlib import os, sys, tpc
from dimlib import refresh_config, pgconnector
from iogen import StrIOGenerator
from zipops import build_file_set
from dimlib import file_path_splitter
from dbops import create_ins_tbl, create_mother_tables

if __name__ == '__main__':
    t1 = tpc()
    cfg = refresh_config()
    db_schema = cfg['db_schema']
    storage_set = build_file_set(cfg)
    mother_tbl_list = list(
        {
            _['mother_tbl'] for _ in storage_set
        }
    )
    create_mother_tables(cfg['dburi'], storage_set)
    print(f'Total Time Taken: {tpc() - t1}')
