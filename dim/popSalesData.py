import requests as req
import json
from dimlib import pgcnx as dbeng,dtm,pdf,cfg,logError,dataSession,rsq
from pandas import concat as pConcat,Series as pSeries,merge as pMerge,to_datetime as pDT
from collections import OrderedDict as odict

uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
TokenPoint=cfg['salesforce']['token']
DataPoint=cfg['salesforce']['data']
tokenParams={'grant_type':'password','client_id':cfg['salesforce']['cid'],'client_secret':cfg['salesforce']['secrect']
    ,'username':cfg['salesforce']['uid'],'password':cfg['salesforce']['pwd']}
dataParams={'start_date':'05/01/2019','end_date':'05/02/2019'}
tokenHeadR={'content-type':'application/x-www-form-urlencoded'}

r=req.post(TokenPoint,params=tokenParams,headers=tokenHeadR)

dataHeadR={'accept':cfg['salesforce']['data_ctype'],'content-type':cfg['salesforce']['data_ctype']
        ,'Authorization':'Bearer {}'.format(json.loads(r.text)['access_token'])}

pgx=dbeng(uri)
api=odict()

mapper=rsq('SELECT * FROM framework.api_mappers WHERE active=true',pgx)
tfo=mapper[['point_name','endpoint']].copy(deep=True)
tfo.drop_duplicates('point_name',keep='first',inplace=True)
points=list(zip(tfo['point_name'],tfo['endpoint']))
mapper['point_col']=mapper['point_col'].str.lower()

# Fetch data from active API
for pnt in points:
 idi,hit=pnt
 R=req.post(hit,data=json.dumps(dataParams),headers=dataHeadR)
 api[idi]=pdf(json.loads(R.text))
 api[idi].columns=map(str.lower,api[idi].columns)

# List all collections
collectionList=mapper['collection']
collectionList=list(set(collectionList))

# Create list of tables to be created
tabFrame=mapper[['point_name','point_col']].copy(deep=True)
tabFrame['point_col']=tabFrame.groupby('point_name')['point_col','point_name'].transform(lambda x:','.join(x))
tabFrame.drop_duplicates(inplace=True)
tabList=list(zip(tabFrame['point_name'],tabFrame['point_col']))

# Create Table Frames
dataCollectionList=[]
for tab in tabList:
 idi,col=tab
 col=col.split(',')
 parseCol=[{k.split('.')[0]:k.split('.')[1]} for k in col if "." in k]
 if len(parseCol)>0:
  _nu={}
  for _itr in parseCol:
   for _k,_v in _itr.items():
    key=_nu.get(_k,[])
    key.append(_v)
    _nu[_k]=key
  unpackCols=list(_nu.keys())
  tabColumns=[_ for _ in col if '.' not in _]
  apiKeys=list(set(list(mapper['api_keycol'])))
  fetchColFromAPI=unpackCols+apiKeys+tabColumns
  fetchColFromAPI=list(set(fetchColFromAPI))
  tmpFrame=api[idi][fetchColFromAPI].copy(deep=True)
  rows=[]
  _=tmpFrame.apply(lambda row: [rows.append({','.join(apiKeys):row[','.join(apiKeys)], unpackCols[0]:nn}) for nn in row[unpackCols[0]]],axis=1)
  _frm_=pdf(rows)
  _frm_=pConcat([_frm_,_frm_[unpackCols[0]].apply(pSeries)],axis=1).drop(unpackCols[0],axis=1)
  _frm_.columns=map(str.lower,_frm_.columns)
  dataCollectionList.append(_frm_[tabColumns])
 else:


  rows.clear()


# Push Table Frames to datastore

# Purge data objects
del collectionList
del mapper
del api
