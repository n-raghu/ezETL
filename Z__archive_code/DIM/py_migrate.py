import sqlalchemy as sa
import pandas as pd
import csv
import os
import yaml
from urllib.parse import quote_plus
from multiprocessing import Process, Lock, JoinableQueue as Queue, cpu_count

from datetime import datetime
from io import StringIO
import traceback

databufferqueue = Queue(maxsize=10)
tablequeue = Queue(maxsize=10)


def buildBuffers(batch_ts,
                 l,
                 i,
                 tgt_schema,
                 pg_conn,
                 tablequeue,
                 chunksize=100000):
    try:
        while True:
            instancecode, ms_conn_string,srcn_schema,table_name = tablequeue.get()

            if ms_conn_string!='':

                sql_srv = True if str(ms_conn_string)[0:5]=='mssql' else False

                ms_conn = sa.create_engine(ms_conn_string)

                srcn_schema = (srcn_schema+'.dbo') if sql_srv else srcn_schema
                src_schema = 'dbo' if sql_srv else srcn_schema

                query = "SELECT column_name , data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema='{}' and table_name='{}' order by ordinal_position".format(src_schema, table_name)

                meta = pd.read_sql(query, ms_conn)

                if meta.shape[0] == 0:
                    l.acquire()
                    print("Thread Num: {} Instance: {} Table doesn't exist in this instance: {}".
                          format(i, instancecode,table_name))
                    l.release()
                    tablequeue.task_done()
                else:

                    if str(ms_conn_string)[0:5] == 'mssql':
                        meta.data_type = meta.data_type.replace([
                            'datetime', 'tinyint', 'bit', 'varchar', 'char',
                            'nvarchar', 'uniqueidentifier', 'timestamp'
                        ], [
                            'timestamp without time zone', 'smallint', 'boolean',
                            'text', 'text', 'text', 'text', 'text'
                        ])

                    if str(ms_conn_string)[0:5] == 'mysql':
                        meta.data_type = meta.data_type.replace([
                            'varchar', 'char', 'nvarchar', 'datetime', 'longtext',
                            'mediumtext', 'tinyint'
                        ], [
                            'text', 'text', 'text', 'timestamp', 'text', 'text',
                            'smallint'
                        ])

                    meta = meta.assign(column_def=meta.column_name.str.lower() + ' ' + meta.data_type)

                    select_query = "SELECT '{}' as instancecode,'{}' as row_timestamp,{} FROM {}.{}".format(instancecode,batch_ts,meta.column_name.str.cat(sep=','),srcn_schema,table_name)

                    #pg_conn.execute('DROP TABLE IF EXISTS staging.{}'.format(table_name))

                    create_table = 'CREATE UNLOGGED TABLE IF NOT EXISTS staging2.' + table_name + '(instancecode text,row_timestamp timestamp without time zone,' + meta.column_def.str.cat(sep=',') + ');'


                    #pg_conn.execute("CREATE SCHEMA IF NOT EXISTS staging2")


                    pg_conn.execute(create_table)

                    ms_con = ms_conn.raw_connection()
                    ms_cur = ms_con.cursor()
                    ms_cur.arraysize = chunksize
                    cursor = ms_cur.execute(select_query)
                    batch_num = 1
                    while True:
                        batch = ms_cur.fetchmany(chunksize)
                        if not batch:
                            break
                        s_buf = StringIO()
                        writer = csv.writer(s_buf)
                        writer.writerows(batch)
                        s_buf.seek(0)
                        databufferqueue.put([batch_num, table_name, s_buf])
                        l.acquire()
                        print("Thread Num: {} Batch: {} Queued Buffer for Table: {}".
                              format(i, batch_num, table_name))
                        l.release()
                        batch_num = batch_num + 1

                    ms_cur.close()
                    ms_con.close()
                    tablequeue.task_done()
            else:
                tablequeue.task_done()

    except Exception as e:
        print('Exception in building buffers: ' + str(e))
        print(traceback.format_exc())


def processBuffers(l, i, pg_conn, tgt_schema, databufferqueue):
    try:
        while True:
            pg_con = pg_conn.raw_connection()
            pg_cur = pg_con.cursor()
            batch_num, table_name, s_buf = databufferqueue.get()
            pg_cur.copy_expert(
                'COPY staging2.' + table_name +
                ' FROM STDIN WITH CSV', s_buf)
            s_buf.close()
            pg_con.commit()
            l.acquire()
            print("Thread Num: {} Batch: {} Processed Buffer for Table: {}".
                  format(i, batch_num, table_name))
            l.release()
            pg_cur.close()
            pg_con.close()
            databufferqueue.task_done()
    except Exception as e:
        print('Exception in processing buffers: ' + str(e))
        print(traceback.format_exc())


def loadConn(db_name):
    try:
        if db_name=='reportingdb':
            db_name = 'eaedb'
            with open(os.path.expanduser("./dimConfig.yml"), 'r') as stream:
                data = yaml.safe_load(stream)
                return 'local', data[db_name]['schema'], '{}://{}:{}@{}:{}/{}'.format(data[db_name]['connection'],quote_plus(data[db_name]['user']),quote_plus(data[db_name]['passwd']),data[db_name]['server'], data[db_name]['port'],data[db_name]['db']),'reportingdb'
        else:
            instance, tgt_schema, tgt_conn_string, _ = loadConn('reportingdb')
            pg_conn = sa.create_engine(tgt_conn_string)
            conn_info = pd.read_sql("SELECT instancetype,uid,pwd,hostip,hport,dbname,instancecode,app from framework.instanceconfig where isactive and instanceid={}".format(db_name),pg_conn)

            if conn_info.shape[0] != 0:
                conn_info = conn_info.iloc[0]

                if conn_info.instancetype=='mysql':
                    conn_info.instancetype='mysql+mysqlconnector'

                conn_string = "{}://{}:{}@{}:{}/{}".format(conn_info.instancetype,conn_info.uid,conn_info.pwd,conn_info.hostip,conn_info.hport,conn_info.dbname)

                if conn_info.instancetype=='mssql':
                    conn_string = conn_string + "?driver=ODBC+Driver+17+for+SQL+Server"

                return conn_info.instancecode , conn_info.dbname, conn_string , conn_info.app
            else:
                return '','','',''

    except Exception as e:
        print('Exception in loading connection string: ' + str(e))
        print(traceback.format_exc())
        return -1


def main():
    try:

        batch_time = datetime.utcnow()

        application_name = 'py_Load_ReportingDB_Staging'

        pg_instance,tgt_schema, tgt_conn_string, _ = loadConn('reportingdb')

        tgt_conn = sa.create_engine(
            tgt_conn_string, isolation_level="AUTOCOMMIT")


        max_threads = cpu_count() * 4

        l = Lock()
        for i in range(max_threads):
            buf_producer = Process(
                target=buildBuffers,
                args=(batch_time,l, i, tgt_schema, tgt_conn,tablequeue))
            buf_producer.daemon = True
            buf_producer.start()

        for i in range(max_threads):
            buf_consumer = Process(
                target=processBuffers,
                args=(l, i, tgt_conn, tgt_schema, databufferqueue))
            buf_consumer.daemon = True
            buf_consumer.start()


        print('***{} : Started Building Buffers'.format(datetime.now()))
        print('***{} : Started Processing Buffers'.format(datetime.now()))

        tgt_conn.execute("DROP SCHEMA IF EXISTS staging2 CASCADE")
        tgt_conn.execute("CREATE SCHEMA staging2")

        instance_list = pd.read_sql("SELECT instanceid FROM framework.instanceconfig where isactive",tgt_conn)

        for instance_id in instance_list.instanceid.values:

            instance, schema, conn_string, app = loadConn(instance_id)

            app = 'ilms' if app=='lms' else app

            stage_list = pd.read_sql("SELECT DISTINCT staging_table FROM elt.tbl_elt_master where active=true and Lower(LEFT(table_src,position('.' in table_src)-1))='{}'".format(app),tgt_conn)

            for table_name in stage_list.staging_table.values:

                tablequeue.put([instance,conn_string,schema,table_name])


        tablequeue.join()
        print('***{} : Finished Building Buffers'.format(datetime.now()))
        databufferqueue.join()
        print('***{} : Finished Processing Buffers'.format(datetime.now()))

    except Exception as e:
        print("Exception in extracting Staging Layer: " + str(e))
        print(traceback.format_exc())


if __name__ == "__main__":
    main()
