import requests as req
from datetime import datetime as dtm
from pandas import DataFrame as pdf, concat as pconcat, Series as pSeries
from sqlalchemy import create_engine as dbeng
import json

uri='postgresql://eaeuser:eaeuser@172.16.1.151:5432/reportingdb'
AccessPoint='https://cs70.salesforce.com/services/oauth2/token'
DataPoint='https://cs70.salesforce.com/services/apexrest/getClosedWonOpportunities'

inParams={'grant_type':'password'
    ,'client_id':'3MVG9PE4xB9wtoY8w4VZoDIfOOua2XA_iJw9cGi270bVwJAJ6S4iZ5mcpffviN.GJ_Fx0.UF_eimRFRVmRxeL'
    ,'client_secret':'1F35E6ED29FB35F484B562F02236479E92F16B71BE5A5B1CED787AD42EBB7D2D'
    ,'username':'divs@kcloudtechnologies.com.elearning'
    ,'password':'Passion@3owcF8Vw8VpqodZY7v9boU3Ph'
    }
accessHeadR={'content-type':'application/x-www-form-urlencoded'}
r=req.post(AccessPoint,params=inParams,headers=accessHeadR)
dataHeadR={'Content-Type':'application/json','Authorization':'Bearer {}'.format(r.text['access_token'])}
R=req.get(DataPoint,headers=dataHeadR)
dataR=json.loads(R.text)

oppTable=[]
accTable=[]
pgx=dbeng(uri)

for data in dataR:
    dat_prd_raw={key:data[key] for key in ['OpportunityId', 'LineItems']}
    dat_opp=data.copy()
    del dat_opp['LineItems']
    line_items=dat_prd_raw['LineItems']
    for line in line_items:
        oppTable.append({'opportunityid':dat_prd_raw['OpportunityId'],'productdata':line})
    accTable.append(dat_opp)

if len(accTable)>0:
    accFrame=pdf(accTable)
    accFrame.to_sql('salesforce_accounts',pgx,if_exists='append',index=False,schema='framework')

if len(oppTable)>0:
    oppFrameRaw=pdf(oppTable)
    prdFrame=pconcat([oppFrameRaw,oppFrameRaw['productdata'].apply(pSeries)],axis=1).drop('productdata',axis=1)
    prdFrame.to_sql('salesforce_opportunitydata',pgx,if_exists='append',index=False,schema='framework')

pFile='h_' +str(int(dtm.timestamp(dtm.utcnow())))
with open(pFile,'w+') as oFile:
    oFile.write(str(dtm.utcnow())+ ' <<< Callout\n')
    oFile.write(str(r.headers))
