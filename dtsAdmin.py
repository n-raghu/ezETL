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

# INITIAL CHECK IF PROCESS ALREADY RUNNING ON KERNEL


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
  ops=['mongodump','-h',eaeIns,'--authenticationDatabase','admin','-u',eaeAcc,'-p',eaePass,'-d',eaedw,'--out']

else:
  eaePass=0
  ops=['mongodump','-h',eaeIns,'-d',eaedw,'--out']

# CONSTANT VARIABLES
bkpLock=False
dbTuple=eaedbKonnexions(eaePass)
dba,dbe,dbi,dbo=dbTuple                       # Extract Tuple for internal use
ixpacks=(OrderedDict,idxModel,asc,dsc,datetime)        # pack functions and classes for functional use
bkpacks=(OrderedDict,PIPE,Popen,ops,datetime)

# BACKUP
def backupJOB(bkSet,retro,pat=0):
  bkc=(bkSet,retro,pat)
  ss=backupAgent(bkc,bkpacks,dbTuple)
  cbk,pbk,pat=ss
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
        dba.update_one({'backSetupID':purger['backSetupID']},{'$set':{'purgeIssue':purger['purgeIssue'],'purged':purger['purged']}})
      else:
        dba.update_one({'backSetupID':purger['backSetupID']},{'$set':{'purged':purger['purged']}})
  else:
    patrol=OrderedDict()
    patrol['_id']=uid()
    dba.insert_one(patrol)
  return rerun

# PROGRAMMING FOR SMART SCHEDULER
while(True):
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
  dtNow=datetime.now()
  if(dtNow.weekday() in backup_days):
    job_bkp=True
  else:
    bkpLock=False
  if(dtNow.weekday() in index_days):
    job_index=True
  else:
    print('NO JOB. ')
    ziz(3600)
  if(job_index and bkpLock==False):
    print('IDX Tracking. ')
    xx=idxTracker(dbTuple,ixpacks)
    print(xx)
  if(job_bkp):
    backup_times=list(set(backup_times))
    backup_times.sort()
    for tim in backup_times:
      if(tim>dtNow.hour):
        nextSchedule=datetime(dtNow.year,dtNow.month,dtNow.day,tim,defaultTime['min'],defaultTime['sec'])
        break
      else:
        continue
  zTime=int((nextSchedule-dtNow).total_seconds())
  if(zTime>3900):
    bkpLock=False
    print('SCHEDULE TOO LONG. RESTING. ')
    ziz(3600)
    continue
  elif(zTime>1600):
    print('SCHEDULED. WAITING FOR RIGHT TIME. ')
    ziz(zTime-110)
    bkpLock=True
    continue
  else:
    print('RIGHT ON TIME. ')
    ziz(zTime)
    bst=nextSchedule.hour
    ret=reConfig['backup']['retention']
    rerun=backupJOB(bst,ret)
    if(rerun):
      print('PREPARING TO TRIGGER A FAILED ATTEMPT. ')
      ziz(600)
      backupJOB(bst,ret)
    bkpLock=False
    print('CYCLE COMPLETED. RESTING. ')
    ziz(3100)