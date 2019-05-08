import requests as req
import json
from dimlib import pgcnx as dbeng,dtm,pdf,cfg,logError
from pandas import concat as pConcat,Series as pSeries,merge as pMerge,to_datetime

def pushData():
    return False

def pushLog(point,app='salesforce'):
    logFrame=pdf({'app':['salesforce'],'endpoint':['point'],'pid':['pid'],'params':[json.dumps('Params')],'headers':[json.dumps('headR')]})
    return logFrame.to_sql('api_responses')

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

R=req.post(DataPoint,data=json.dumps(dataParams),headers=dataHeadR)
dataR=json.loads(R.text)

oppTable=[]
accTable=[]
pgx=dbeng(uri)

for data in dataR:
    dat_prd_raw={key:data[key] for key in ['OpportunityId','LineItems']}
    dat_opp=data.copy()
    del dat_opp['LineItems']
    line_items=dat_prd_raw['LineItems']
    for line in line_items:
        oppTable.append({'opportunityid':dat_prd_raw['OpportunityId'],'productdata':line})
    accTable.append(dat_opp)

if len(accTable)>0 or len(oppTable)>0:
    accFrame=pdf(accTable)
    oppFrameRaw=pdf(oppTable)
    prdFrame=pConcat([oppFrameRaw,oppFrameRaw['productdata'].apply(pSeries)],axis=1).drop('productdata',axis=1)
    accFrame['opportunityid']=accFrame['OpportunityId']
    del accFrame['OpportunityId']
    fOf=pMerge(prdFrame,accFrame,on='opportunityid',how='inner')
    dateCols=['AccountExpiryDate','CloseDate','LicenseEndDate','LicenseStartDate']
    for col in dateCols:
        fOf[col]=to_datetime(fOf[col],format='%Y-%m-%d')
    accFrame=fOf[['AccountExpiryDate','AccountName','AccountId','TypeOfPartner']].copy(deep=True)
    accFrame.drop_duplicates(subset='AccountId',keep='first',inplace=True)
    fOf['pid']=int(dtm.timestamp(dtm.utcnow()))
    fOf['app']='salesforce'
    fOf.to_sql('api_datalogs',pgx,index=False,if_exists='append',schema='framework')
    fOf.drop(['AccountExpiryDate','AccountName','TypeOfPartner','app','pid'],axis=1,inplace=True)
    accFrame.to_sql('salesforce_accounts',pgx,index=False,if_exists='append',schema=cfg['eaedb']['schema'])
    fOf.to_sql('salesforce_opportunities',pgx,index=False,if_exists='append',schema=cfg['eaedb']['schema'])
