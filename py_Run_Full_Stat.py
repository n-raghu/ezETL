'''
Module to Run FULL VACUUM ANALYZE on all tables of given schema on main Postgres connection
Author: Vadiraj Bhatt
Date : 27-Jun-2019
'''

import sqlalchemy as sa
import pandas as pd
import os
import yaml
from urllib.parse import quote_plus
from multiprocessing import Process, Lock, JoinableQueue as Queue
from datetime import datetime

tablequeue = Queue(maxsize=10)


def runFullStats(l, i, tgt_schema, pg_conn, tablequeue):
    try:
        while True:
            table_name = tablequeue.get()

            l.acquire()

            print(
                "{} : Start : Thread Num: {} Run Full Statistics for Table: {}"
                .format(datetime.now(), i, table_name))

            l.release()

            pg_conn.execute("VACUUM FULL ANALYZE {}.{}".format(
                tgt_schema, table_name))

            l.acquire()

            print(
                "{} : End : Thread Num: {} Run Full Statistics for Table: {}".
                format(datetime.now(), i, table_name))

            l.release()

            tablequeue.task_done()

    except Exception as e:
        print('Exception in Running Full Statistics: ' + str(e))


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

        max_threads = 8

        l = Lock()

        for i in range(max_threads):
            proc_iterate = Process(
                target=runFullStats,
                args=(l, i, 'bi_db', tgt_conn, tablequeue))
            proc_iterate.daemon = True
            proc_iterate.start()

        table_list = pd.read_sql(
            "SELECT table_name FROM elt.tbl_elt_master where active", tgt_conn)

        for table in table_list.table_name.values:
            tablequeue.put(table)

        tablequeue.join()

    except Exception as e:
        print("Exception in Running Full Statistics: " + str(e))


if __name__ == "__main__":
    main()
