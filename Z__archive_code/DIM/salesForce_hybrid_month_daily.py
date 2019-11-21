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
from dimlib import DatabaseError, psyDataError, OperationalError, IntegrityError, ProgrammingError, InternalError
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

def pushChunk(pid, uri, api_frame, dat_collection, col_columns, start_period, end_period=dtm.utcnow().date()):
    try:
        pg_cnx=dbeng(uri)
        if len(api_frame)>0:
            print('Found Data')
            del api_frame['attributes']
            api_frame.columns=map(str.lower,api_frame.columns)
            api_frame['row_timestamp']=row_timestamp
            columnList=col_columns.lower().split(',')
            columnList.append('row_timestamp')
            api_frame[columnList].to_sql(dat_collection,pg_cnx,index=False,if_exists='append',schema=eaeSchema)
        tracker=pdf([],columns=['collection','start_date','end_date','pid'])
        tracker=tracker.append({'pid':pid, 'collection':dat_collection, 'start_date':start_period, 'end_date':end_period},ignore_index=True)
        tracker.to_sql('api_salesforce_tracker',pg_cnx,index=False,if_exists='append',schema='framework')
        pg_cnx.dispose()
    except (DataError, DatabaseError, psyDataError, OperationalError, IntegrityError, ProgrammingError, InternalError, Exception) as err:
        logError(pid,app,'PushChunkError|' +dat_collection+ '|' +str(err),uri)
    return None

for _col_ in sfdc_collections:
    api_query_link='v45'
    columns_list=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_column']]['sfdc_column'])
    column_str=','.join(columns_list)
    api_version_link=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_api']]['sfdc_api'])[0]
    try:
        while True:
            break_itr=False
            _col_max_q="SELECT COALESCE(MAX(end_date),'" +default_start_date+ "') AS col_max_val FROM framework.api_salesforce_tracker WHERE collection='" +_col_+ "' "
            eSession=dataSession(uri)
            col_start_date=list(eSession.execute(_col_max_q))[0]['col_max_val']
            print(str(col_start_date) +' > '+ _col_)
            if (utnow -col_start_date).days >31:
                start_year=col_start_date.year
                start_month=col_start_date.month
                first_day,last_day=monthrange(start_year,start_month)
                if col_start_date.day == last_day:
                    col_start_date += tdt(days=1)
                    start_year=col_start_date.year
                    start_month=col_start_date.month
                    first_day,last_day=monthrange(start_year,start_month)
                col_max_date=dtm(start_year, start_month, last_day).date()
                whereClause='+where+LastModifiedDate+>=+' +str(col_start_date)+ 'T00:00:00Z+and+LastModifiedDate+<=+' +str(col_max_date)+ 'T23:59:59Z'
            else:
                break_itr=True
                col_max_date=dtm.utcnow()
                whereClause='+where+LastModifiedDate+>+' +str(col_start_date)+ 'T00:00:01Z'
            filters=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_filters']]['sfdc_filters'])[0]
            if filters:
                filters=filters.replace(' ','+')
                whereClause=whereClause +'+and+'+ filters
            api_query_link=api_version_link+ '/?q=select+' +column_str+ '+from+' +_col_+ whereClause
            R=req.get(api_query_link,headers=dataHeadR)
            _record=[{'app':app,'endpoint':api_query_link,'headers':json.dumps(dataHeadR),'params':None,'responsecode':str(R),'responseok':R.ok,'requesttime':dtm.utcnow()}]
            if not R.ok:
                logError(pid,app,'SalesForceRequestError|' +api_query_link+ '|' +_col_+ '|' +str(R.text),uri)
            pdf(_record).to_sql('api_traces',pgx,index=False,if_exists='append',schema='framework')
            api_dat=pdf(json.loads(R.text)['records'])
            while True:
                R_txt=json.loads(R.text)
                if 'nextRecordsUrl' in R_txt:
                    R=req.get(api_version_link+'/'+str(R_txt['nextRecordsUrl']), headers=dataHeadR)
                    xtra_frame=pdf(json.loads(R.text)['records'])
                    api_dat=api_dat.append(xtra_frame.copy(deep=True) ,sort=False, ignore_index=True)
                else:
                    break
            if break_itr:
                pushChunk(pid, uri, api_dat.copy(deep=True), _col_, column_str, col_start_date)
                break
            else:
                pushChunk(pid, uri, api_dat.copy(deep=True), _col_, column_str, col_start_date, col_max_date)
    except Exception as err:
        logError(pid,app,'SalesForceExceptionError|' +api_query_link+ '|' +_col_+ '|' +str(err),uri)
        sys.exit(err)

pgx.dispose()
gc_purge()