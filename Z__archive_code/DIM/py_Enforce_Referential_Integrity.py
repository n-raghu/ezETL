'''
Module to Enforce Soft Referential Integrity on all tables of Data Vault on main Postgres connection
Author: Vadiraj Bhatt
Date : 20-Sep-2019
'''

import sqlalchemy as sa
import pandas as pd
import os
import yaml
from urllib.parse import quote_plus
from multiprocessing import Process, Lock, JoinableQueue as Queue
from datetime import datetime

tablequeue = Queue(maxsize=10)


def enforce_ref_integrity(l, i, tgt_schema, pg_conn, tablequeue):
    try:
        while True:
            table_name,child_table,parent_table = tablequeue.get()

            l.acquire()

            print(
                "{} : Start : Enforce Soft Referential Integrity for Table: {} Using {}"
                .format(datetime.now(), child_table,parent_table))

            l.release()

            pg_conn.execute(table_name)

            l.acquire()

            print(
                "{} : End : Enforce Soft Referential Integrity for Table: {} Using {}".
                format(datetime.now(), child_table,parent_table))

            l.release()

            tablequeue.task_done()

    except Exception as e:
        print('Exception in Enforcing Soft Referential Integrity: ' + str(e))


def loadConn(db_name):
    try:
        with open(os.path.expanduser("dimConfig.yml"), 'r') as stream:
            data = yaml.safe_load(stream)
            return '{}'.format(
                data[db_name]['schema']), '{}://{}:{}@{}:{}/{}'.format(
                    data[db_name]['connection'],
                    quote_plus(data[db_name]['user']),
                    quote_plus(data[db_name]['passwd']),
                    data[db_name]['server'], data[db_name]['port'],
                    data[db_name]['db'])
    except Exception as e:
        print('Exception in loading connection string: ' + str(e))
        return -1


def main():
    try:
        tgt_schema, tgt_conn_string = loadConn('eaedb')
        tgt_conn = sa.create_engine(
            tgt_conn_string, isolation_level="AUTOCOMMIT")

        max_threads = 1

        l = Lock()

        for i in range(max_threads):
            proc_iterate = Process(
                target=enforce_ref_integrity,
                args=(l, i, 'bi_db', tgt_conn, tablequeue))
            proc_iterate.daemon = True
            proc_iterate.start()

        table_list = pd.read_sql(
            "SELECT parent_table,child_table,'DELETE FROM ' || table_schema || '.' || child_table || ' src WHERE NOT EXISTS ( SELECT 1 FROM ' || table_schema || '.' || parent_table || ' tgt WHERE src.' || column_name || '=' || 'tgt.' || column_name || ');' as delete_statement FROM elt.vw_relationships", tgt_conn)

        for parent_table,child_table,table in table_list.values:
            tablequeue.put([table,child_table,parent_table])

        tablequeue.join()

    except Exception as e:
        print("Exception in Enforcing Soft Referential Integrity: " + str(e))


if __name__ == "__main__":
    main()
