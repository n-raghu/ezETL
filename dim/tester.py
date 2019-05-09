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

for pnt in points:
 idi,hit=pnt
 R=req.post(hit,data=json.dumps(dataParams),headers=dataHeadR)
 api[idi]=pdf(json.loads(R.text))
 api[idi].columns=map(str.lower,api[idi].columns)

collections=mapper['collection']
collections=list(set(collections))

tabFrame=mapper[['point_name','point_col']].copy(deep=True)
tabFrame['point_col']=tabFrame.groupby('point_name')['point_col','point_name'].transform(lambda x:','.join(x))
tabFrame.drop_duplicates(inplace=True)
tabList=list(zip(tabFrame['point_name'],tabFrame['point_col']))

collectionList=[]
for tab in tabList:
 idi,col=tab
 col=col.split(',')
 collectionList.append(api[idi][col])
