'''
    DATA TRANSFORMATION SERVICES(DTS) FOR EAE
    REQUIRE INPUTS FROM 'dtsConfig.yaml' WHICH IS PRESENT IN THE SAME DIRECTORY
'''

import sys,yaml
from backupManager import *
from idxTracker import *
from datetime import datetime
from collections import OrderedDict
from uuid import uuid1 as uid
from subprocess import Popen,PIPE
from time import sleep as ziz
from pymongo import MongoClient as mClient, IndexModel as idxModel, ASCENDING as asc, DESCENDING as dsc

# LOAD CONFIG FILE
with open('dtsConfig.yaml','r') as ymlFile:
  cfg=yaml.load(ymlFile)

authMode=cfg['eaedb']['authentication']
eaeIns=cfg['eaedb']['host'] +':'+ str(cfg['eaedb']['port'])
eaedw=cfg['eaedb']['db']
eaeKey=cfg['eaedb']['pass_key']
eaeAcc=cfg['eaedb']['user']
eaeSalt=cfg['eaedb']['passwd']

def eaedbKonnexions(pwd,uname=eaeAcc):
  if(uname==0 or pwd==0):
    mongoKonnect=mClient(eaeIns)
  else:
    mongoKonnect=mClient(eaeIns,username=uname,password=pwd)
  a=mongoKonnect[eaedw].logDTSAdminActions
  e=mongoKonnect[eaedw].logDTSErrors
  i=mongoKonnect[eaedw].indexTracker
  o=mongoKonnect[eaedw]
  return a,e,i,o

if(authMode):
  from cryptography.fernet import Fernet as fNet
  eaeKey=fNet(eaeKey)
  eaePass=eaeKey.decrypt(eaeSalt.encode()).decode()
  opsLL=['mongodump','-h',eaeIns,'--authenticationDatabase','admin','-u',eaeAcc,'-p',eaePass,'-d',eaedw,'--out']

else:
  eaePass=0
  opsLL=['mongodump','-h',eaeIns,'-d',eaedw,'--out']

# CONSTANT VARIABLES
dbTuple=eaedbKonnexions(eaePass)
dba,dbe,dbi,dbo=dbTuple                                 # Extract Tuple for internal use
ixpacks=(OrderedDict,idxModel,asc,dsc,datetime)         # pack functions and classes for functional use
bkpacks=(OrderedDict,PIPE,Popen,opsLL,datetime)
opsLen=len(opsLL)

# BACKUP
def backupJOB(bkSet,retro,bkPath,opt=1):
  bkCFG=(opt,retro,bkSet,bkPath)
  ss=backupAgent(bkCFG,bkpacks,dbTuple)
  rerun=False
  if(len(opsLL)!=opsLen):
    opsLL.pop()
  cbk,pbk,pat=ss

  print(pbk)
  print(pat)
  
  if(cbk!=False):
    cbk.update(OrderedDict({'_id':uid()}))
    dba.insert_one(cbk)
    if(cbk['status']==False):
      rerun=True
    else:
      rerun=False
  if(pbk!=False):
    for purger in pbk:
      if('purgeIssue' in purger.keys()):
        dba.update_one({'backupSetupID':purger['backupSetupID']},{'$set':{'purgeIssue':purger['purgeIssue'],'purged':purger['purged']}})
      else:
        dba.update_one({'backupSetupID':purger['backupSetupID']},{'$set':{'purged':purger['purged']}})
  else:
    patrol=OrderedDict()
    patrol['_id']=uid()
    dba.insert_one(patrol)
  return rerun

# PROGRAMMING FOR SMART SCHEDULER
while(True):
  getNext=False
  with open('dtsConfig.yaml') as configFile:
    reConfig=yaml.load(configFile)
  job_bkp_stop=reConfig['backup']['disabled']
  job_index_stop=reConfig['index']['disabled']
  job_bkp_defer=reConfig['backup']['deferred']
  job_index_defer=reConfig['index']['deferred']
  if(job_bkp_stop & job_index_stop):
    print('RECEIVED EXIT CODE. ')
    ziz(3)
    break
  elif(job_bkp_defer & job_index_defer):
    print('DEFERRED FOR NEXT 30 MINS. ')
    ziz(1801)
    continue
  defaultTime={'min':14,'sec':14}
  backup_days=reConfig['backup']['day_of_week']
  index_days=reConfig['index']['day_of_week']
  backup_times=reConfig['backup']['frequency']
  backup_path=reConfig['backup']['path']
  dtNow=datetime.utcnow()
  if(dtNow.weekday() in backup_days):
    job_bkp=True
  if(dtNow.weekday() in index_days):
    job_index=True
  else:
    print('NO JOB. ')
    ziz(3600)
    continue
  if(job_index):
    print('IDX Tracking. ')
    xx=idxTracker(dbTuple,ixpacks)
  if(job_bkp):
    dtNow=datetime.utcnow()
    backup_times=list(set(backup_times))
    backup_times.sort()
    tim=dtNow.hour
    if(tim not in backup_times or dtNow.minute>11):
      for tim in backup_times:
        if(tim>dtNow.hour):
          nextSchedule=datetime(dtNow.year,dtNow.month,dtNow.day,tim,defaultTime['min'],defaultTime['sec'])
          getNext=True
          break
        elif(dtNow.hour==23):
          nextSchedule='TOMORROW'
          getNext=True
          break
        else:
          continue
    else:
      nextSchedule=datetime(dtNow.year,dtNow.month,dtNow.day,tim,defaultTime['min'],defaultTime['sec'])
    if(getNext):
      zTime=36+(60*(60-dtNow.minute))
      print('WAITING. NEXT SCHEDULE AT ' +str(nextSchedule)+ '. ')
      ziz(zTime)
      continue
    else:
      zTime=int((nextSchedule-dtNow).total_seconds())
      print('RIGHT ON TIME. READY TO STRIKE. ')
      ziz(zTime)
      print('BACKUP INITIATED. ')
      bst=nextSchedule.hour
      ret=reConfig['backup']['retention']
      rerun=backupJOB(bst,ret,backup_path)
      ziz(60)
      if(rerun):
        print('PREPARING TO TRIGGER A FAILED ATTEMPT. ')
        ziz(600)
        backupJOB(bst,ret)
      print('CYCLE COMPLETED. ')
