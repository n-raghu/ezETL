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
from dimlib import pgcnx as dbeng,dtm,pdf,cfg,logError,rsq,dataSession,DataError
from pandas import concat as pConcat,Series as pSeries,merge as pMerge
from collections import OrderedDict as odict,Iterable as cIterable

api=odict()
app='salesforce'
row_timestamp=dtm.utcnow()
uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
eaeSchema=cfg['eaedb']['schema']
pgx=dbeng(uri)
TokenPoint=cfg['salesforce']['token']
tokenParams={'grant_type':'password','client_id':cfg['salesforce']['cid'],'client_secret':cfg['salesforce']['secrect']
    ,'username':cfg['salesforce']['uid'],'password':cfg['salesforce']['pwd']}
_fmt=cfg['salesforce']['dateformat']
default_start_date=str(cfg['salesforce']['start_date'])
tokenHeadR={'content-type':'application/x-www-form-urlencoded'}
r=req.post(TokenPoint,params=tokenParams,headers=tokenHeadR)
_record=[{'app':app,'endpoint':r.url,'headers':json.dumps(tokenHeadR),'params':json.dumps(tokenParams),'responsecode':str(r),'responseok':r.ok,'requesttime':dtm.utcnow()}]
pdf(_record).to_sql('api_traces',pgx,index=False,if_exists='append',schema='framework')
dataHeadR={'accept':cfg['salesforce']['data_ctype'],'content-type':cfg['salesforce']['data_ctype']
        ,'Authorization':'Bearer {}'.format(json.loads(r.text)['access_token'])}

mapper=rsq('SELECT * FROM framework.api_salesforce_mappers WHERE active=true',pgx)
tracker=pdf([],columns=['collection','start_date','end_date'])
sfdc_collections=list(set(list(mapper['sfdc_collection'])))

for _col_ in sfdc_collections:
    _col_max_q="SELECT CAST(COALESCE(MAX(end_date),'" +default_start_date+ "') AS TEXT) AS col_max_val FROM framework.api_salesforce_tracker WHERE collection='" +_col_+ "' "
    eSession=dataSession(uri)
    _col_max=list(eSession.execute(_col_max_q))[0]['col_max_val'] +'T00:00:00Z'
    api_version_link=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_api']]['sfdc_api'])[0]
    columns_list=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_column']]['sfdc_column'])
    column_str=','.join(columns_list)
    whereClause='+where+LastModifiedDate+>+' +_col_max
    filters=list(mapper.loc[(mapper['sfdc_collection']==_col_),['sfdc_filters']]['sfdc_filters'])[0]
    if filters:
        filters=filters.replace(' ','+')
        whereClause=whereClause +'+and+'+ filters
    api_query_link=api_version_link+ '/?q=select+' +column_str+ '+from+' +_col_+ whereClause
    R=req.get(api_query_link,headers=dataHeadR)
    _record=[{'app':app,'endpoint':api_query_link,'headers':json.dumps(dataHeadR),'params':None,'responsecode':str(R),'responseok':R.ok,'requesttime':dtm.utcnow()}]
    pdf(_record).to_sql('api_traces',pgx,index=False,if_exists='append',schema='framework')
    api[_col_]=pdf(json.loads(R.text)['records'])
    if len(api[_col_])>0:
        api[_col_].columns=map(str.lower,api[_col_].columns)
        api[_col_]['row_timestamp']=row_timestamp
        del api[_col_]['attributes']
        tracker=tracker.append({'collection':_col_,'start_date':_col_max,'end_date':dtm.utcnow().date()},ignore_index=True)

if len(tracker)>0:
    trackerCollection=list(set(list(tracker['collection'])))
    cmapR=mapper[['sfdc_column','sfdc_collection','collection']].copy()
    cmapR['columns']=cmapR.groupby(['collection','sfdc_collection'])['sfdc_column'].transform(lambda x:','.join(x))
    del cmapR['sfdc_column']
    cmapR.drop_duplicates(inplace=True)
    collectionList=list(zip(cmapR['collection'],cmapR['sfdc_collection'],cmapR['columns']))
    for tup in collectionList:
        _dataCol_,_sfdcCol_,_columns_=tup
        if _sfdcCol_ not in trackerCollection:
            continue
        try:
            columnList=_columns_.lower().split(',')
            columnList.append('row_timestamp')
            api[_sfdcCol_][_columns_.lower().split(',')].to_sql(_dataCol_,pgx,index=False,if_exists='append',schema=eaeSchema)
        except (DataError,AssertionError,ValueError,IOError,IndexError,KeyError) as err:
            print(err)
    tracker['pid']=pid
    tracker.to_sql('api_salesforce_tracker',pgx,index=False,if_exists='append',schema='framework')

del api
pgx.dispose()
