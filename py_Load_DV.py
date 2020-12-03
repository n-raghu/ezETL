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

import argparse

from urllib.parse import quote_plus
from datetime import datetime


def logInfo(pid,tgt_conn_string, l, logqueue):

    pg_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Logger'}).raw_connection()

    cur = pg_conn.cursor()

    cur.execute("PREPARE loginfo(int,text,timestamp,text,text) AS INSERT INTO logs.tbl_log(pid,message_type,stamptime,activity,table_name,status) VALUES($1,$2,$3,$4,$5,$6);");

    while True:

        try:

            time,message_type,message,table_name,status = logqueue.get()

            l.acquire()
            print("{} : {} : {} : {} : {} : {}".format(pid,message_type,time, message,table_name,status))
            l.release()


            cur.execute("EXECUTE loginfo({},'{}','{}','{}','{}','{}')".format(pid,message_type,time,message.replace("'","''"), table_name,status))


        except Exception as e:
            l.acquire()
            print("Exception in Logging Process: {}".format(str(e)))
            l.release()

        finally:
            logqueue.task_done()

    cur.close()
    pg_conn.close()



def loadDV(proc_num, tgt_conn_string, tablequeue ,stage_count ,stage_marks ,logqueue ,pid,process_pki,process_purged_pki):

    pg_conn = sa.create_engine(tgt_conn_string,pool_pre_ping=True,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Load Data Vault'}).connect()

    while True:

        try:

            table_name, recreateindex,stage_table,table_type,table_exists,full_pki_exists,incremental_pki_exists = tablequeue.get()

            if table_exists and stage_count[stage_table].counts.sum() > 0:

                dv_count = pd.read_sql("SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = 'bi_db.{}'::regclass".format(table_name), pg_conn)

                timestamps = "{" + ",".join(stage_count[stage_table].row_timestamp.astype(str).values) + "}"


                if recreateindex and stage_count[stage_table].counts.sum() > dv_count.estimate.sum() * 0.50 : # Recreate Index Only If rows to be processed are more than 50% of Total Table Size

                    logqueue.put([datetime.utcnow(),'Info',"Dropping Indexes From Table".format(table_name),table_name,'Start'])

                    pg_conn.execute("SELECT elt.udf_drop_indices('{}')".format(table_name))

                    logqueue.put([datetime.utcnow(),'Info',"Dropping Indexes From Table".format(table_name),table_name,'End'])


                logqueue.put([datetime.utcnow(),'Info',"Loading DV Node".format(table_name),table_name,'Start'])

                pg_conn.execute("SELECT elt.udf_load_dv_node_v2('{}','{}')".format(table_name,timestamps))

                logqueue.put([datetime.utcnow(),'Info',"Loading DV Node".format(table_name),table_name,'End'])

                if not stage_marks[stage_table]:
                    pg_conn.execute("UPDATE logs.tbl_stage_batches SET processed=true WHERE stage_table='{}' AND row_timestamp=ANY('{}'::timestamp[])".format(stage_table,timestamps))


                #if stage_count[stage_table].counts.sum() > dv_count.estimate.sum() * 0.10: # Run Stats Only If Rows processed are more than 10% of Total Table Size

                logqueue.put([datetime.utcnow(),'Info',"Refreshing Statistics".format(table_name),table_name,'Start'])

                pg_conn.execute("VACUUM ANALYZE bi_db.{}".format(table_name))

                logqueue.put([datetime.utcnow(),'Info',"Refreshing Statistics".format(table_name),table_name,'End'])

                if recreateindex and stage_count[stage_table].counts.sum() > dv_count.estimate.sum() * 0.50: # Recreate Index Only If rows to be processed are more than 50% of Total Table Size

                    logqueue.put([datetime.utcnow(),'Info',"Creating Indexes".format(table_name),table_name,'Start'])

                    pg_conn.execute("SELECT elt.udf_create_indices('{}')".format(table_name))

                    logqueue.put([datetime.utcnow(),'Info',"Creating Indexes".format(table_name),table_name,'End'])

            if table_type=='satellite' and process_pki and full_pki_exists: # Enddate satellite records for upstream deleted records

                logqueue.put([datetime.utcnow(),'Info',"Enddating Satellites for Deleted Records with Full PKI".format(table_name),table_name,'Start'])

                pg_conn.execute("SELECT elt.udf_enddate_satellites_for_deleted_records_csv('{}','{}',{})".format(table_name,datetime.utcnow(),pid))

                logqueue.put([datetime.utcnow(),'Info',"Enddating Satellites for Deleted Records with Full PKI".format(table_name),table_name,'End'])


            if table_type=='satellite' and process_purged_pki and incremental_pki_exists: # Enddate satellite records for upstream deleted records

                logqueue.put([datetime.utcnow(),'Info',"Enddating Satellites for Incremental Deleted Records".format(table_name),table_name,'Start'])

                pg_conn.execute("SELECT elt.udf_enddate_satellites_for_purged_records('{}','{}',{})".format(table_name,datetime.utcnow(),pid))

                logqueue.put([datetime.utcnow(),'Info',"Enddating Satellites for Incremental Deleted Records".format(table_name),table_name,'End'])

        except Exception as e:
            logqueue.put([datetime.utcnow(),'Error','Exception in Loading DataVault Node: {} with Staging Table {} - {}'.format(table_name,stage_table,str(e)),table_name,'Error'])

            if not stage_marks[stage_table]:
                pg_conn.execute("UPDATE logs.tbl_stage_batches SET processed=false WHERE stage_table='{}' AND row_timestamp=ANY('{}'::timestamp[])".format(stage_table,timestamps))
                stage_marks[stage_table]=True

        finally:
            tablequeue.task_done()

    pg_conn.close()


def checkStageTable(pid,i, tgt_conn_string, tablequeue,stage_marks,stagecount,logqueue):

    pg_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Check Staging Table Counts'}).connect()

    while True:
        try:
            table_name = tablequeue.get()

            logqueue.put([datetime.utcnow(),'Info',"Checking Row Counts of Staging Table".format(table_name),table_name,'Start'])

            pg_conn.execute("ANALYZE staging.{}".format(table_name))

            stage_count = pd.read_sql("SELECT {} as pid,'{}' as stage_table,row_timestamp,count(*) counts FROM staging.{} GROUP BY row_timestamp order by row_timestamp".format(pid,table_name,table_name), pg_conn)

            for i in range(stage_count.shape[0]):
                with cl.suppress(sa.exc.IntegrityError):
                    stage_count.iloc[[i]].to_sql('tbl_stage_batches',con=pg_conn,if_exists='append',schema='logs',index=False)

            old_count = pd.read_sql("SELECT pid,stage_table,row_timestamp,counts FROM logs.tbl_stage_batches WHERE stage_table='{}' AND not processed order by row_timestamp".format(table_name),pg_conn)

            stagecount[table_name]=old_count

            stage_marks[table_name]=False

            logqueue.put([datetime.utcnow(),'Info',"Checking Row Counts of Staging Table".format(table_name),table_name,'End'])

        except Exception as e:
            logqueue.put([datetime.utcnow(),'Error','Exception in Checking Row Counts of Staging Table: {} - {}'.format(table_name, str(e)), table_name,'Error'])

        finally:
            tablequeue.task_done()

    pg_conn.close()


def recordStageTableCounts(pid,i, tgt_conn_string, tablequeue,logqueue):

    pg_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Record Staging Table Counts'}).connect()

    while True:
        try:
            source_app,table_name,has_instancecode,table_type = tablequeue.get()

            logqueue.put([datetime.utcnow(),'Info',"Recording Row Counts of Staging Table".format(table_name),table_name,'Start'])

            #pg_conn.execute("ANALYZE staging.{}".format(table_name))

            query = "SELECT '{}' as source_app,'{}' as table_type, {} as pid,'{}' as stage_table,count(*) counts,row_timestamp,CURRENT_TIMESTAMP recordtime,instancecode FROM staging.{} GROUP BY instancecode,row_timestamp".format(source_app,table_type,pid,table_name,table_name)

            if not has_instancecode:
                query = "SELECT '{}' as source_app,'{}' as table_type, {} as pid,'{}' as stage_table,count(*) counts,row_timestamp,CURRENT_TIMESTAMP recordtime,'' as instancecode FROM staging.{} GROUP BY row_timestamp".format(source_app,table_type,pid,table_name,table_name)

            stage_count = pd.read_sql(query, pg_conn)

            for i in range(stage_count.shape[0]):
                with cl.suppress(sa.exc.IntegrityError):
                    stage_count.iloc[[i]].to_sql('tbl_stage_counts',con=pg_conn,if_exists='append',schema='logs',index=False)

            logqueue.put([datetime.utcnow(),'Info',"Recording Row Counts of Staging Table".format(table_name),table_name,'End'])

        except Exception as e:
            logqueue.put([datetime.utcnow(),'Error','Exception in Recording Row Counts of Staging Table: {} - {}'.format(table_name, str(e)), table_name,'Error'])

        finally:
            tablequeue.task_done()

    pg_conn.close()


def dropStageTable(i, tgt_conn_string, tablequeue,logqueue,process_pki,process_purged_pki,stage_marks):

    pg_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120,'application_name' : 'Python Job - Drop Staging Tables'}).connect()

    while True:
        try:
            table_name,table_exists,full_pki_exists,incremental_pki_exists = tablequeue.get()
            if table_exists and not stage_marks[table_name]:
                logqueue.put([datetime.utcnow(),'Info',"Dropping Staging Table".format(table_name),table_name,'Start'])
                pg_conn.execute("DROP TABLE IF EXISTS staging.{} CASCADE".format(table_name))
                logqueue.put([datetime.utcnow(),'Info',"Dropping Staging Table".format(table_name),table_name,'End'])

            if process_pki and full_pki_exists:
                logqueue.put([datetime.utcnow(),'Info',"Dropping Full PKI Table".format(table_name),table_name + "_pki",'Start'])
                pg_conn.execute("DROP TABLE IF EXISTS staging.{}_pki CASCADE".format(table_name))
                logqueue.put([datetime.utcnow(),'Info',"Dropping Full PKI Table".format(table_name),table_name + "_pki",'End'])

            if process_purged_pki and incremental_pki_exists:
                logqueue.put([datetime.utcnow(),'Info',"Truncating Incremental PKI Table".format(table_name),table_name + "_pki",'Start'])
                pg_conn.execute("TRUNCATE purged.{}_pki CASCADE".format(table_name))
                logqueue.put([datetime.utcnow(),'Info',"Truncating Incremental PKI Table".format(table_name),table_name + "_pki",'End'])

        except Exception as e:
            logqueue.put([datetime.utcnow(),'Error','Exception in Dropping Staging Table: {} - {}'.format(table_name, str(e)), table_name,'Error'])

        finally:
            tablequeue.task_done()

    pg_conn.close()


def loadConfig():
    try:
        db_name = 'eaedb'
        with open(os.path.expanduser("dimConfig.yml"), 'r') as stream:
            data = yaml.safe_load(stream)
            return data['dv_load']['record_stage_count'],data['max_workers'],data['dv_load']['drop_staging'], data['dv_load']['recreate_index'], data[db_name]['schema'], '{}://{}:{}@{}:{}/{}'.format(data[db_name]['connection'],quote_plus(data[db_name]['user']),quote_plus(data[db_name]['passwd']),data[db_name]['server'], data[db_name]['port'],data[db_name]['db'])
    except Exception as e:
        print('Exception in loading configs from dimConfig.yml: ' + str(e))
        raise e

def main():
    try:

        parser = argparse.ArgumentParser(description='Python program to load Data Vault Nodes based on configurations mentioned in dimConfig.yml')

        parser.add_argument('pid', metavar='PID',type=int, help='Process id, this will be logged in log tables')
        parser.add_argument('-i','--enforce-integrity',dest='enforce_integrity', help='Enforce Integrity across Data Vault, default is False',action='store_true')
        parser.add_argument('-k','--process-pki',dest='process_pki', help='Process Full PKI Tables, default is False',action='store_true')
        parser.add_argument('-f','--process-incremental-pki',dest='process_purged_pki', help='Process Incremental PKI Tables, default is False',action='store_true')
        parser.add_argument('-e','--disable-execute-statements',dest='execute_statements', help='Disable Execution of Post Processing Steps, default is False',action='store_false')
        args = parser.parse_args()

        enforce_integrity = args.enforce_integrity
        process_pki = args.process_pki
        pid = args.pid
        process_purged_pki = args.process_purged_pki
        execute_statements = args.execute_statements

        allstagequeue = mp.JoinableQueue(maxsize=10)
        stagequeue = mp.JoinableQueue(maxsize=10)
        dvqueue = mp.JoinableQueue(maxsize=10)
        dropstagequeue = mp.JoinableQueue(maxsize=10)
        logqueue = mp.JoinableQueue(maxsize=10)

        manager = mp.Manager()
        stage_count = manager.dict()
        stage_marks = manager.dict()

        record_stage_count, max_threads, drop_staging, recreate_index, tgt_schema, tgt_conn_string = loadConfig()

        tgt_conn = sa.create_engine(tgt_conn_string,isolation_level="AUTOCOMMIT",connect_args={'connect_timeout': 120, 'application_name' : 'Python Job - Main Process for Data Vault Load'})

        #max_threads = mp.cpu_count()

        l = mp.Lock()

        proc_iterate = mp.Process(target=logInfo,args=(pid,tgt_conn_string,l,logqueue))
        proc_iterate.daemon = True
        proc_iterate.start()

        for i in range(max_threads):
            proc_iterate = mp.Process(target=checkStageTable,args=(pid,i, tgt_conn_string,stagequeue,stage_marks,stage_count,logqueue))
            proc_iterate.daemon = True
            proc_iterate.start()

        logqueue.put([datetime.utcnow(),'Info','Data Vault Master Process','Master','Start'])

        table_list = pd.read_sql('''
            SELECT a.table_name,true as recreateindex,staging_table,a.table_type,
            b.table_name is not null as table_exists,
            c.table_name is not null as full_pki_exists,
            d.table_name is not null as incremental_pki_exists
            FROM elt.tbl_elt_master a
            LEFT JOIN (SELECT table_name FROM information_schema.tables b WHERE table_schema ='staging') b ON a.staging_table=b.table_name
            LEFT JOIN (SELECT table_name FROM information_schema.tables b WHERE table_schema ='staging') c ON a.id_table=c.table_name
            LEFT JOIN (SELECT table_name FROM information_schema.tables b WHERE table_schema ='purged') d ON a.id_table=d.table_name
            WHERE active ORDER BY 1
            '''.format(recreate_index),tgt_conn)

        for stage_table in table_list[table_list.table_exists].staging_table.unique():

            stagequeue.put(stage_table)

        stagequeue.close()
        stagequeue.join()

        if record_stage_count:

            stage_list = pd.read_sql('''SELECT source_app,staging_table,has_instancecode,table_type FROM
            (SELECT source_app,staging_table,has_instancecode,'staging' as table_type FROM elt.tbl_elt_master where active UNION SELECT source_app,id_table,has_instancecode,'ids' FROM elt.tbl_elt_master where active) A
            WHERE staging_table in (SELECT table_name FROM information_schema.tables WHERE table_schema='staging')
            ''',tgt_conn)

            for i in range(max_threads):
                proc_iterate = mp.Process(target=recordStageTableCounts,args=(pid,i, tgt_conn_string,allstagequeue,logqueue))
                proc_iterate.daemon = True
                proc_iterate.start()

            for index,stage_table in stage_list.iterrows():

                allstagequeue.put([stage_table.source_app,stage_table.staging_table,stage_table.has_instancecode,stage_table.table_type])

            allstagequeue.close()
            allstagequeue.join()

        for i in range(max_threads):
            proc_iterate = mp.Process(target=loadDV,args=(i, tgt_conn_string,dvqueue,stage_count,stage_marks,logqueue,pid,process_pki,process_purged_pki))
            proc_iterate.daemon = True
            proc_iterate.start()

        for table, recreateindex,stage_table,table_type,table_exists,full_pki_exists,incremental_pki_exists in table_list.values:

            dvqueue.put([table, recreateindex,stage_table,table_type,table_exists,full_pki_exists,incremental_pki_exists])

        dvqueue.close()
        dvqueue.join()

        if drop_staging:

            for i in range(max_threads):
                proc_iterate = mp.Process(target=dropStageTable,args=(i, tgt_conn_string,dropstagequeue,logqueue,process_pki,process_purged_pki,stage_marks))
                proc_iterate.daemon = True
                proc_iterate.start()

            for stage_table,table_exists,full_pki_exists,incremental_pki_exists in table_list[["staging_table","table_exists","full_pki_exists","incremental_pki_exists"]].drop_duplicates().values:

                    dropstagequeue.put([stage_table,table_exists,full_pki_exists,incremental_pki_exists])

            dropstagequeue.close()
            dropstagequeue.join()



        dv_load_successful = pd.read_sql("SELECT count(*)=0 as status FROM logs.tbl_log WHERE message_type='Error' and pid={}".format(pid),tgt_conn)

        stage_load_successful = pd.read_sql("SELECT count(*)=0 as status FROM framework.tracker_collections where not ingest_success and pid={}".format(pid),tgt_conn)


        if dv_load_successful.iloc[0].status and stage_load_successful.iloc[0].status:

            if enforce_integrity:

                logqueue.put([datetime.utcnow(),'Info','Enforcing Soft Referential Integrity','Enforce Integrity','Start'])
                tgt_conn.execute("SELECT elt.udf_clean_soft_ref_integrity()")
                logqueue.put([datetime.utcnow(),'Info','Enforcing Soft Referential Integrity','Enforce Integrity','End'])


            if execute_statements:

                exec_statements = pd.read_sql("SELECT query_text,run_with_nightly_job FROM elt.tbl_exec_statements WHERE active ORDER BY priority",tgt_conn)

                for statement,run_with_nightly_job in exec_statements.values:

                    if (not run_with_nightly_job) or (run_with_nightly_job and process_pki):
                        logqueue.put([datetime.utcnow(),'Info','Running Execute Statement',statement,'Start'])
                        tgt_conn.execute(statement)
                        logqueue.put([datetime.utcnow(),'Info','Running Execute Statement',statement,'End'])

        logqueue.put([datetime.utcnow(),'Info','Data Vault Master Process','Master','End'])

        logqueue.close()
        logqueue.join()

    except Exception as e:

        print("Exception in Master Data Vault Process: " + str(e))
        print(traceback.format_exc())


if __name__ == "__main__":
    main()