'''
Module to Load all active Data Vault nodes from staging tables based on configurations in elt.tbl_elt_master and elt.tbl_elt_mapping
Author: Vadiraj Bhatt
Date : 27-Jun-2019
'''

import sys
import sqlalchemy as sa
import pandas as pd
import os
import yaml
import multiprocessing as mp
import contextlib as cl
import traceback

from urllib.parse import quote_plus
from datetime import datetime

def logInfo(pid,tgt_conn_string, l, logqueue):

    pg_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Logger'}).raw_connection()

    cur = pg_conn.cursor()

    cur.execute("PREPARE loginfo(int,text,timestamp,text,text) AS INSERT INTO logs.tbl_log(pid,message_type,stamptime,activity,table_name) VALUES($1,$2,$3,$4,$5);");

    while True:

        try:

            time,message_type,message,table_name = logqueue.get()

            l.acquire()
            print("{} : {} : {} : {} : {}".format(pid,message_type,time, message,table_name))
            l.release()


            cur.execute("EXECUTE loginfo({},'{}','{}','{}','{}')".format(pid,message_type,time,message.replace("'","''"), table_name))


        except Exception as e:
            l.acquire()
            print("Exception in Logging Process: {}".format(str(e)))
            l.release()

        finally:
            logqueue.task_done()

    cur.close()
    pg_conn.close()



def loadDV(proc_num, tgt_conn_string, tablequeue ,stage_count ,stage_marks ,logqueue ,pid):

    pg_conn = sa.create_engine(tgt_conn_string,pool_pre_ping=True,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Load Data Vault'}).connect()

    while True:

        try:

            table_name, recreateindex,stage_table,table_type = tablequeue.get()

            if stage_count[stage_table].counts.sum() > 0:

                dv_count = pd.read_sql("SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = 'bi_db.{}'::regclass".format(table_name), pg_conn)

                timestamps = "{" + ",".join(stage_count[stage_table].row_timestamp.astype(str).values) + "}"


                if recreateindex and stage_count[stage_table].counts.sum() > dv_count.estimate.sum() * 0.50 : # Recreate Index Only If rows to be processed are more than 50% of Total Table Size

                    logqueue.put([datetime.utcnow(),'Info',"Dropping Indexes From Table {}".format(table_name),table_name])

                    pg_conn.execute("SELECT elt.udf_drop_indices('{}')".format(table_name))

                    logqueue.put([datetime.utcnow(),'Info',"Dropped Indexes From Table {}".format(table_name),table_name])


                logqueue.put([datetime.utcnow(),'Info',"Loading DV Node {}".format(table_name),table_name])

                pg_conn.execute("SELECT elt.udf_load_dv_node_v2('{}','{}')".format(table_name,timestamps))

                logqueue.put([datetime.utcnow(),'Info',"Loaded DV Node {}".format(table_name),table_name])

                if not stage_marks[stage_table]:
                    pg_conn.execute("UPDATE logs.tbl_stage_batches SET processed=true WHERE stage_table='{}' AND row_timestamp=ANY('{}'::timestamp[])".format(stage_table,timestamps))


                #if stage_count[stage_table].counts.sum() > dv_count.estimate.sum() * 0.10: # Run Stats Only If Rows processed are more than 10% of Total Table Size

                logqueue.put([datetime.utcnow(),'Info',"Refreshing Statistics Of Table {}".format(table_name),table_name])

                pg_conn.execute("VACUUM ANALYZE bi_db.{}".format(table_name))

                logqueue.put([datetime.utcnow(),'Info',"Refreshed Statistics Of Table {}".format(table_name),table_name])

                if recreateindex and stage_count[stage_table].counts.sum() > dv_count.estimate.sum() * 0.50: # Recreate Index Only If rows to be processed are more than 50% of Total Table Size

                    logqueue.put([datetime.utcnow(),'Info',"Creating Indexes On Table {}".format(table_name),table_name])

                    pg_conn.execute("SELECT elt.udf_create_indices('{}')".format(table_name))

                    logqueue.put([datetime.utcnow(),'Info',"Created Indexes On Table {}".format(table_name),table_name])

            if table_type=='satellite': # Enddate satellite records for upstream deleted records

                logqueue.put([datetime.utcnow(),'Info',"Enddating Satellites for Deleted Records for DV Node {}".format(table_name),table_name])

                pg_conn.execute("SELECT elt.udf_enddate_satellites_for_deleted_records('{}','{}',{})".format(table_name,datetime.utcnow(),pid))

                logqueue.put([datetime.utcnow(),'Info',"Enddated Satellites for Deleted Records for DV Node {}".format(table_name),table_name])


        except Exception as e:
            if not stage_marks[stage_table]:
                pg_conn.execute("UPDATE logs.tbl_stage_batches SET processed=false WHERE stage_table='{}' AND row_timestamp=ANY('{}'::timestamp[])".format(stage_table,timestamps))
                stage_marks[stage_table]=True


            logqueue.put([datetime.utcnow(),'Error','Exception in Loading DataVault Node: {} with Staging Table {} - {}'.format(table_name,stage_table,str(e)),table_name])

        finally:
            tablequeue.task_done()

    pg_conn.close()


def checkStageTable(pid,i, tgt_conn_string, tablequeue,stage_marks,stagecount,logqueue):

    pg_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Check Staging Table Counts'}).connect()

    while True:
        try:
            table_name = tablequeue.get()

            logqueue.put([datetime.utcnow(),'Info',"Checking Row Counts of Staging Table {}".format(table_name),table_name])

            pg_conn.execute("ANALYZE staging.{}".format(table_name))

            stage_count = pd.read_sql("SELECT {} as pid,'{}' as stage_table,row_timestamp,count(*) counts FROM staging.{} GROUP BY row_timestamp order by row_timestamp".format(pid,table_name,table_name), pg_conn)

            for i in range(stage_count.shape[0]):
                with cl.suppress(sa.exc.IntegrityError):
                    stage_count.iloc[[i]].to_sql('tbl_stage_batches',con=pg_conn,if_exists='append',schema='logs',index=False)

            old_count = pd.read_sql("SELECT pid,stage_table,row_timestamp,counts FROM logs.tbl_stage_batches WHERE stage_table='{}' AND not processed order by row_timestamp".format(table_name),pg_conn)

            stagecount[table_name]=old_count

            stage_marks[table_name]=False

            logqueue.put([datetime.utcnow(),'Info',"Checked Row Counts of Staging Table {}".format(table_name),table_name])

        except Exception as e:
            logqueue.put([datetime.utcnow(),'Error','Exception in Checking Row Counts of Staging Table: {} - {}'.format(table_name, str(e)), table_name])

        finally:
            tablequeue.task_done()

    pg_conn.close()


def dropStageTable(i, tgt_conn_string, tablequeue,logqueue):

    pg_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Drop Staging Tables'}).connect()

    while True:
        try:
            table_name = tablequeue.get()
            logqueue.put([datetime.utcnow(),'Info',"Dropping Staging Table {}".format(table_name),table_name])
            pg_conn.execute("DROP TABLE IF EXISTS staging.{}".format(table_name))
            pg_conn.execute("DROP TABLE IF EXISTS staging.{}_pki".format(table_name))
            logqueue.put([datetime.utcnow(),'Info',"Dropped Staging Table {}".format(table_name),table_name])

        except Exception as e:
            logqueue.put([datetime.utcnow(),'Error','Exception in Dropping Staging Table: {} - {}'.format(table_name, str(e)), table_name])

        finally:
            tablequeue.task_done()

    pg_conn.close()


def loadConfig():
    try:
        db_name = 'eaedb'
        with open(os.path.expanduser("dimConfig.yml"), 'r') as stream:
            data = yaml.safe_load(stream)
            return data['dv_load']['drop_staging'], data['dv_load']['recreate_index'], data[db_name]['schema'], '{}://{}:{}@{}:{}/{}'.format(data[db_name]['connection'],quote_plus(data[db_name]['user']),quote_plus(data[db_name]['passwd']),data[db_name]['server'], data[db_name]['port'],data[db_name]['db'])
    except Exception as e:
        print('Exception in loading configs from dimConfig.yml: ' + str(e))
        raise e

def main():
    try:

        if(len(sys.argv)<2):
            sys.exit('PID not provided!')
        else:
            pid=int(sys.argv[1])

        stagequeue = mp.JoinableQueue(maxsize=10)
        dvqueue = mp.JoinableQueue(maxsize=10)
        dropstagequeue = mp.JoinableQueue(maxsize=10)
        logqueue = mp.JoinableQueue(maxsize=10)

        manager = mp.Manager()
        stage_count = manager.dict()
        stage_marks = manager.dict()

        drop_staging, recreate_index, tgt_schema, tgt_conn_string = loadConfig()

        tgt_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120, 'application_name' : 'Python Job - Main Process for Data Vault Load'}).connect()

        max_threads = mp.cpu_count()

        l = mp.Lock()

        proc_iterate = mp.Process(target=logInfo,args=(pid,tgt_conn_string,l,logqueue))
        proc_iterate.daemon = True
        proc_iterate.start()

        for i in range(max_threads):
            proc_iterate = mp.Process(target=checkStageTable,args=(pid,i, tgt_conn_string,stagequeue,stage_marks,stage_count,logqueue))
            proc_iterate.daemon = True
            proc_iterate.start()

        logqueue.put([datetime.utcnow(),'Info','Data Vault Master Process Started','Master'])


        table_list = pd.read_sql("SELECT a.table_name,{} as recreateindex,staging_table,table_type FROM elt.tbl_elt_master a WHERE active and EXISTS(SELECT 1 FROM information_schema.tables b WHERE table_schema='staging' AND a.staging_table=b.table_name) ORDER BY 1".format(recreate_index),tgt_conn)

        tgt_conn.close()

        for stage_table in table_list.staging_table.unique():

            stagequeue.put(stage_table)

        stagequeue.close()
        stagequeue.join()

        for i in range(max_threads):
            proc_iterate = mp.Process(target=loadDV,args=(i, tgt_conn_string,dvqueue,stage_count,stage_marks,logqueue,pid))
            proc_iterate.daemon = True
            proc_iterate.start()

        for table, recreateindex,stage_table,table_type in table_list.values:

            dvqueue.put([table, recreateindex,stage_table,table_type])

        dvqueue.close()
        dvqueue.join()

        if drop_staging:

            for i in range(max_threads):
                proc_iterate = mp.Process(target=dropStageTable,args=(i, tgt_conn_string,dropstagequeue,logqueue))
                proc_iterate.daemon = True
                proc_iterate.start()

            for stage_table in table_list.staging_table.unique():

                if not stage_marks[stage_table]:
                    dropstagequeue.put(stage_table)

            dropstagequeue.close()
            dropstagequeue.join()

        logqueue.put([datetime.utcnow(),'Info','Data Vault Master Process Ended','Master'])

        logqueue.close()
        logqueue.join()


    except Exception as e:

        print("Exception in Master Data Vault Process: " + str(e))
        print(traceback.format_exc())


if __name__ == "__main__":
    main()