import requests as req
import json
from dimlib import pgcnx as dbeng,dtm,pdf,cfg,logError,rsq
from pandas import concat as pConcat,Series as pSeries,merge as pMerge
from collections import OrderedDict as odict,Iterable as cIterable

app='salesforce'
api=odict()
uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
eaeSchema=cfg['eaedb']['schema']
pgx=dbeng(uri)
TokenPoint=cfg['salesforce']['token']
tokenParams={'grant_type':'password','client_id':cfg['salesforce']['cid'],'client_secret':cfg['salesforce']['secrect']
    ,'username':cfg['salesforce']['uid'],'password':cfg['salesforce']['pwd']}
_fmt=cfg['salesforce']['dateformat']
_x=rsq(''' SELECT requesttime FROM framework.api_traces WHERE app='salesforce' ''',pgx)
_sdate=_x['requesttime'].max()
row_timestamp=dtm.utcnow()

if isinstance(_sdate,dtm):
    _sdate=_sdate.strftime(_fmt)
else:
    _sdate=cfg['salesforce']['start_date']
_fmt='%' +'/%'.join(_fmt.split('/'))
dataParams={'start_date':_sdate,'end_date':dtm.utcnow().strftime(_fmt)}
dataParams={'start_date':'01/01/2016','end_date':'05/16/2019'}
tokenHeadR={'content-type':'application/x-www-form-urlencoded'}
r=req.post(TokenPoint,params=tokenParams,headers=tokenHeadR)
_record=[{'app':app,'endpoint':r.url,'headers':json.dumps(tokenHeadR),'params':json.dumps(tokenParams),'responsecode':str(r),'responseok':r.ok,'requesttime':dtm.utcnow()}]
pdf(_record).to_sql('api_traces',pgx,index=False,if_exists='append',schema='framework')
dataHeadR={'accept':cfg['salesforce']['data_ctype'],'content-type':cfg['salesforce']['data_ctype']
        ,'Authorization':'Bearer {}'.format(json.loads(r.text)['access_token'])}

def listFlatter(l):
    for el in l:
        if isinstance(el, cIterable) and not isinstance(el, (str, bytes)):
            yield from listFlatter(el)
        else:
            yield el

mapper=rsq('SELECT * FROM framework.api_mappers WHERE active=true',pgx)
tfo=mapper[['point_name','endpoint','collection']].copy(deep=True)
tfo.drop_duplicates('point_name',keep='first',inplace=True)
points=list(zip(tfo['point_name'],tfo['collection'],tfo['endpoint']))

# Fetch data from active API
for pnt in points:
 idi,col,hit=pnt
 hit='https://cs70.salesforce.com/services/data/v45.0/query/?q='+hit
 R=req.get(hit,headers=dataHeadR)
 api[idi]=pdf(json.loads(R.text)['records'])
 _record=[{'app':app,'endpoint':idi,'headers':json.dumps(dataHeadR),'params':None,'responsecode':str(R),'responseok':R.ok,'requesttime':dtm.utcnow()}]
 pdf(_record).to_sql('api_traces',pgx,index=False,if_exists='append',schema='framework')
 api[idi].columns=map(str.lower,api[idi].columns)
 api[idi]['row_timestamp']=row_timestamp
 api[idi].to_sql(col,pgx,index=False,if_exists='replace',schema=eaeSchema)
