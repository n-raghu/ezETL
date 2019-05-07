import requests as req
import json
from dimlib import pgcnx as dbeng,dtm,pdf,pconcat,pSeries,cfg,tdt

uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
TokenPoint=cfg['salesforce']['token']
DataPoint=cfg['salesforce']['token']
tokenParams={'grant_type':'password','client_id':cfg['salesforce']['cid'],'client_secret':cfg['salesforce']['csecrect']
    ,'username':cfg['salesforce']['uid'],'password':cfg['salesforce']['pwd']}
dataParams={'start_date':'05/01/2019','end_date':'05/02/2019'}
tokenHeadR={'content-type':cfg['salesforce']['token_ctype']}

r=req.post(TokenPoint,params=tokenParams,headers=tokenHeadR)
dataHeadR={'content-type':cfg['salesforce']['data_ctype'],'Authorization':'Bearer {}'.format(json.loads(r.text)['access_token'])}
R=req.post(DataPoint,data=json.dumps(dataParams),headers=dataHeadR)
dataR=json.loads(R.text)

oppTable=[]
oppMasterTable=[]
accTable=[]
prdTable=[]
pgx=dbeng(uri)

for data in dataR:
    dat_prd_raw={key:data[key] for key in ['OpportunityId','LineItems']}
    dat_opp=data.copy()
    del dat_opp['LineItems']
    line_items=dat_prd_raw['LineItems']
    for line in line_items:
        oppTable.append({'opportunityid':dat_prd_raw['OpportunityId'],'productdata':line})
    accTable.append(dat_opp)

if len(accTable)>0:
    accFrame=pdf(accTable)
    accFrame.to_sql('salesforce_accounts',pgx,if_exists='append',index=False,schema=cfg['eaedb']['schema'])

if len(oppTable)>0:
    oppFrameRaw=pdf(oppTable)
    prdFrame=pconcat([oppFrameRaw,oppFrameRaw['productdata'].apply(pSeries)],axis=1).drop('productdata',axis=1)
    prdFrame.to_sql('salesforce_opportunitydata',pgx,if_exists='append',index=False,schema=cfg['eaedb']['schema'])
