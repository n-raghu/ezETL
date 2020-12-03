'''
    DATA TRANSFORMATION SERVICES(DTS) FOR FARM STATISTICS
'''

from dtsFarmLib import *

ptime=dtm.utcnow()
lmsFork='forkLMS'
phiFork='forkPHISH'

# LOAD YML FILE
with open("dtsConfig.yaml", 'r') as ymlFile:
 cfg=yaml.load(ymlFile)

# LOAD CONFIG
eaeIns=cfg['eaedb']['host'] +':'+ str(cfg['eaedb']['port'])
eaedw=cfg['eaedb']['db']
eaeKey=cfg['eaedb']['pass_key']
eaeAcc=cfg['eaedb']['user']
eaeSalt=cfg['eaedb']['passwd']
authMode=cfg['eaedb']['authentication']

if(authMode):
  from cryptography.fernet import Fernet as fNet
  eaeKey=fNet(eaeKey)
  eaePass=eaeKey.decrypt(eaeSalt)
  uri='mongodb://'+eaeAcc+ ':' +str(eaePass)+ '@' +eaeIns
  mongoKonnect=mClient(uri)

else:
  mongoKonnect=mClient(eaeIns)

# CONNECTIONS
logStore=farmLogStore()
dba=mongoKonnect[eaedw][logStore['dbd']]
dbe=mongoKonnect[eaedw][logStore['dxx']]


# PANEL OPTIONS
def panel(opt):
 if(opt==1 or opt=='1' or opt=='lmsCourseStats' or opt=='lmsCourseStats.py'):
  efl='lmsCourseStats.py'
 elif(opt==0 or opt=='0' or opt=='snapShotManager' or opt=='snapShotManager.py'):
  efl='snapShotManager.py'
 else:
  sys.exit('Invalid Option')
 return efl

def dtsWorker(paramStr):
 global ptime,dba,eaeIns,eaedw,eaeAcc,eaePass
 pid=str(bson.objectid.ObjectId())
 taskStatus=True
 dba.insert_one({'_id':pid,'status': False,'timeStarted':ptime,'task':paramStr})
 print(str(ptime)+ ' <<< CREATING NEW THREAD FOR ' +paramStr)
 if(authMode):
  proc=Popen(['python',paramStr,eaeIns,eaedw,eaeAcc,eaePass],stdout=PIPE,stderr=PIPE)
 else:
  proc=Popen(['python', paramStr,eaeIns,eaedw],stdout=PIPE,stderr=PIPE)
 proc.wait()
 gist,pErr=proc.communicate()

 gist=gist.decode()
 pErr=pErr.decode()

# LOG TO FARM-DB
 if(pErr==''):
  print(str(dtm.utcnow())+ ' <<< THREAD CLOSED. INTERNAL ERRORS WILL BE LOGGED IF FOUND. ')
  etime=dtm.utcnow()
 else:
  taskStatus=False
  dba.update_one({'_id':pid}, {'$set':{'error':pErr}})
  print(str(dtm.utcnow())+ ' <<< THREAD FAILED. PROCESS ID : ' +pid)
  dbe.insert_one({'script':paramStr,'type':'OBO','refer':pid,'times':dtm.utcnow(),'_id':str(bson.objectid.ObjectId())})
  etime=dtm.utcnow()
 dba.update_one({'_id':pid}, {'$set':{'timeFinished':etime,'status': taskStatus,'gist':gist}})
 return proc

if(len(sys.argv)==2):
  if(sys.argv[1]==lmsFork):
    print('DTS-AGENT FORKING LMS SCHEDULER')
    if(authMode):
     Popen(['python','lmsForker.py',eaeIns,eaedw,eaeAcc,eaePass])
    else:
     Popen(['python','lmsForker.py',eaeIns,eaedw])
  elif(sys.argv[1]==phiFork):
    print('DTS-AGENT FORKING PHISHPROOF SCHEDULER')
    if(authMode):
     Popen(['python','phiForker.py',eaeIns,eaedw,eaeAcc,eaePass])
    else:
     Popen(['python','phiForker.py',eaeIns,eaedw])
  else:
    chc=sys.argv[1]
    eaeOut=dtsWorker(panel(chc))

else:
 sys.exit('Invalid number of params')

#TERMINATE
mongoKonnect.close()
