import sys

if len(sys.argv)<2:
    sys.exit('PID not provided...')
else:
    try:
        pid=int(sys.argv[1])
    except ValueError as err:
        sys.exit(err)

import requests as req
import json
from dimlib import pgcnx as dbeng
from dimlib import DatabaseError, psyDataError, OperationalError, IntegrityError, ProgrammingError, InternalError, alchemyEXC
from dimlib import dtm, pdf, cfg, logError, rsq, dataSession, DataError, gc_purge
from pandas import concat as pConcat, Series as pSeries, merge as pMerge
from collections import OrderedDict as odict,Iterable as cIterable
from calendar import monthrange
from datetime import timedelta as tdt

gc_purge()
app='salesforce'
row_timestamp=dtm.utcnow()
uri='postgresql://' +cfg['eaedb']['user']+ ':' +cfg['eaedb']['passwd']+ '@' +cfg['eaedb']['server']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
eaeSchema=cfg['eaedb']['schema']
pgx=dbeng(uri)

try:
    ndays=cfg['salesforce']['days_frequency']
    TokenPoint=cfg['salesforce']['token']
    tokenParams={'grant_type':'password','client_id':cfg['salesforce']['cid'],'client_secret':cfg['salesforce']['secrect']
    ,'username':cfg['salesforce']['uid'],'password':cfg['salesforce']['pwd']}
    _fmt=cfg['salesforce']['dateformat']
    default_start_date=str(cfg['salesforce']['start_date'])
    tokenHeadR={'content-type':'application/x-www-form-urlencoded'}
    r=req.post(TokenPoint,params=tokenParams,headers=tokenHeadR)
    _record=[{'app':app,'endpoint':r.url,'headers':json.dumps(tokenHeadR),'params':json.dumps(tokenParams),'responsecode':str(r),'responseok':r.ok,'requesttime':dtm.utcnow()}]
    if not r.ok:
        logError(pid,app,'TokenError|' +str(r.text),uri)
    pdf(_record).to_sql('api_traces',pgx,index=False,if_exists='append',schema='framework')
    dataHeadR={'accept':cfg['salesforce']['data_ctype'],'content-type':cfg['salesforce']['data_ctype']
            ,'Authorization':'Bearer {}'.format(json.loads(r.text)['access_token'])}
except Exception as err:
    logError(pid, app, 'HeaderPointError|' +str(err), uri)
    sys.exit(err)

try:
    mapper=rsq('SELECT * FROM framework.api_salesforce_mappers WHERE active=true',pgx)
    sfdc_collections=list(set(list(mapper['sfdc_collection'])))
    tab_shape=mapper[['sfdc_column','sfdc_collection','collection']].copy()
    tab_shape['columns']=tab_shape.groupby(['collection','sfdc_collection'])['sfdc_column'].transform(lambda x:','.join(x))
    del tab_shape['sfdc_column']
    tab_shape.drop_duplicates(inplace=True)
    utnow=dtm.utcnow().date()
except Exception as err:
    logError(pid, app, 'MapperPointError|' +str(err), uri)
    sys.exit(err)

def pushChunk(pid, uri, api_frame, sfdc_collection, dat_collection, col_columns, start_period, end_period=dtm.utcnow().date()):
    chunk_status=True
    try:
        pg_cnx=dbeng(uri)
        tracker=pdf({'pid':[pid], 'collection':[sfdc_collection], 'start_date':[start_period], 'end_date':[end_period]})
        if len(api_frame)>0:
            del api_frame['attributes']
            api_frame.columns=map(str.lower,api_frame.columns)
            api_frame['row_timestamp']=row_timestamp
            columnList=col_columns.lower().split(',')
            columnList.append('row_timestamp')
            api_frame[columnList].to_sql(dat_collection,pg_cnx,index=False,if_exists='append',schema=eaeSchema)
        tracker['status']=True
        tracker.to_sql('api_salesforce_tracker',pg_cnx,index=False,if_exists='append',schema='framework')
        pg_cnx.dispose()
    except (DataError, DatabaseError, psyDataError, OperationalError, IntegrityError, ProgrammingError, InternalError, alchemyEXC.SQLAlchemyError, Exception) as err:
        chunk_status=False
        logError(pid,app,'PushChunkError|' +dat_collection+ '|' +str(err),uri)
        tracker['status']=False
        tracker.to_sql('api_salesforce_tracker',pg_cnx,index=False,if_exists='append',schema='framework')
        print(tracker)
    return chunk_status

for _col_ in sfdc_collections:
    try:
        api_query_link='v45'
        columns_list=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_column']]['sfdc_column'])
        column_str=','.join(columns_list)
        db_collection=list(mapper.loc[(mapper['sfdc_collection']==_col_),['collection']]['collection'])[0]
        api_domain_url=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_domain_url']]['sfdc_domain_url'])[0]
        api_data_point=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_data_point']]['sfdc_data_point'])[0]
        api_version_link=api_domain_url+api_data_point
    except Exception as err:
        logError(pid, app, 'FetchCollectionParams|' +_col_+ '|' +str(err), uri)
        continue

    while True:
        try:
            break_itr=False
            _col_max_q="SELECT COALESCE(MAX(end_date),'" +default_start_date+ "') AS col_max_val FROM framework.api_salesforce_tracker WHERE status=true and collection='" +_col_+ "' "
            eSession=dataSession(uri)
            col_start_date=list(eSession.execute(_col_max_q))[0]['col_max_val']
            eSession.close()
            if (utnow -col_start_date).days >ndays:
                col_max_date = col_start_date+tdt(days=ndays)
                whereClause='+where+LastModifiedDate+>=+' +str(col_start_date)+ 'T00:00:00Z+and+LastModifiedDate+<=+' +str(col_max_date)+ 'T23:59:59Z'
            else:
                break_itr=True
                whereClause='+where+LastModifiedDate+>+' +str(col_start_date)+ 'T00:00:01Z'
            filters=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_filters']]['sfdc_filters'])[0]
            if filters:
                filters=filters.replace(' ','+')
                whereClause=whereClause +'+and+'+ filters
            api_query_link=api_version_link+ '/?q=select+' +column_str+ '+from+' +_col_+ whereClause
            R=req.get(api_query_link,headers=dataHeadR)
            _record=[{'pid':pid,'app':app,'endpoint':api_query_link,'headers':json.dumps(dataHeadR),'params':None,'responsecode':str(R),'responseok':R.ok,'requesttime':dtm.utcnow()}]
            if not R.ok:
                logError(pid,app,'SalesForceRequestError|' +api_query_link+ '|' +_col_+ '|' +str(R.text),uri)
                break
            pdf(_record).to_sql('api_traces',pgx,index=False,if_exists='append',schema='framework')
            api_dat=pdf(json.loads(R.text)['records'])
            while True:
                R_txt=json.loads(R.text)
                if 'nextRecordsUrl' in R_txt:
                    R=req.get(api_domain_url+str(R_txt['nextRecordsUrl']), headers=dataHeadR)
                    if not R.ok:
                        logError(pid,app,'SalesForceExtraRequestError|' +api_query_link+ '|' +_col_+ '|' +str(R.text),uri)
                    xtra_frame=pdf(json.loads(R.text)['records'])
                    api_dat=api_dat.append(xtra_frame.copy(deep=True) ,sort=False, ignore_index=True)
                else:
                    break
            if break_itr:
                last_chunk_not_reqd_xclusive_break=pushChunk(pid, uri, api_dat.copy(deep=True), _col_, db_collection, column_str, col_start_date)
                break
            else:
                res_chk=pushChunk(pid, uri, api_dat.copy(deep=True), _col_, db_collection, column_str, col_start_date, col_max_date)
                if not res_chk:
                    break
        except Exception as err:
            logError(pid,app,'SalesForceExceptionError|' +api_query_link+ '|' +_col_+ '|' +str(err),uri)
            break

pgx.dispose()
gc_purge()