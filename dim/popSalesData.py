import requests as req
import json
from dimlib import pgcnx as dbeng,dtm,pdf,cfg,logError,rsq
from pandas import concat as pConcat,Series as pSeries,merge as pMerge
from collections import OrderedDict as odict,Iterable as cIterable

app='salesforce'
pgx=dbeng(uri)
api=odict()
uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
TokenPoint=cfg['salesforce']['token']
DataPoint=cfg['salesforce']['data']
tokenParams={'grant_type':'password','client_id':cfg['salesforce']['cid'],'client_secret':cfg['salesforce']['secrect']
    ,'username':cfg['salesforce']['uid'],'password':cfg['salesforce']['pwd']}
dataParams={'start_date':'05/01/2019','end_date':'05/02/2019'}
tokenHeadR={'content-type':'application/x-www-form-urlencoded'}
r=req.post(TokenPoint,params=tokenParams,headers=tokenHeadR)
_record=[{'app':app,'endpoint':r.url,'headers':json.dumps(tokenHeadR),'params':json.dumps(tokenParams),'responsecode':str(r),'responseok':r.ok,'requesttime':dtm.utcnow()}]
pdf(_record).to_sql('api_traces',index=False,if_exists='append',schema='framework')
dataHeadR={'accept':cfg['salesforce']['data_ctype'],'content-type':cfg['salesforce']['data_ctype']
        ,'Authorization':'Bearer {}'.format(json.loads(r.text)['access_token'])}

def listFlatter(l):
    for el in l:
        if isinstance(el, cIterable) and not isinstance(el, (str, bytes)):
            yield from listFlatter(el)
        else:
            yield el

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
 _record=[{'app':app,'endpoint':idi,'headers':json.dumps(dataHeadR),'params':json.dumps(dataParams),'responsecode':str(R),'responseok':R.ok,'requesttime':dtm.utcnow()}]
 pdf(_record).to_sql('api_traces',index=False,if_exists='append',schema='framework')
 api[idi].columns=map(str.lower,api[idi].columns)

# Create list of tables to be created
tabFrame=mapper[['point_name','point_col','collection']].copy(deep=True)
tabFrame['point_col']=tabFrame.groupby('collection')['point_col','point_name'].transform(lambda x:','.join(x))
tabFrame.drop_duplicates(inplace=True)
tabList=list(zip(tabFrame['point_name'],tabFrame['point_col'],tabFrame['collection']))

# Create Table Frames
for tab in tabList:
 idi,columnSTR,tabName=tab
 columnList=columnSTR.split(',')
 parseCol=[{k.split('.')[0]:k.split('.')[1]} for k in columnList if "." in k]
 if len(parseCol)>0:
  _nu={}
  for _itr in parseCol:
   for _k,_v in _itr.items():
    key=_nu.get(_k,[])
    key.append(_v)
    _nu[_k]=key
  unpackCols=list(_nu.keys())
  tabColumns=[_ for _ in columnList if '.' not in _]
  apiKey=list(mapper['api_keycol'])[0]
  fetchColFromAPI=unpackCols+[apiKey]+tabColumns
  fetchColFromAPI=list(set(fetchColFromAPI))
  fullFrame=api[idi][fetchColFromAPI].copy(deep=True)
  dataTabFrame=pdf([])
  for _col_ in unpackCols:
   rows=[]
   _=fullFrame.apply(lambda row: [rows.append({apiKey:row[apiKey],_col_:nn}) for nn in row[_col_]],axis=1)
   _frm_=pdf(rows)
   _frm_=pConcat([_frm_,_frm_[_col_].apply(pSeries)],axis=1).drop(_col_,axis=1)
   _frm_.columns=map(str.lower,_frm_.columns)
   if len(dataTabFrame)>0:
    dataTabFrame=pMerge(dataTabFrame,_frm_,on='opportunityid',how='inner')
   else:
    dataTabFrame=_frm_.copy(deep=True)
  if len(dataTabFrame)>0:
   dataTabFrame=pMerge(dataTabFrame,fullFrame,on='opportunityid',how='inner')
  else:
   dataTabFrame=fullFrame
  tabColumns+=list(_nu.values())
  tabColumns=list(listFlatter(tabColumns))
  dataTabFrame.drop_duplicates()
  dataTabFrame[tabColumns].to_sql(tabName,index=False,if_exists='replace',schema='framework')
 else:
  api[idi][columnList].to_sql(tabName,index=False,if_exists='replace',schema='framework')

del api
