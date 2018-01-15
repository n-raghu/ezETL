import os

def main():
  print('1. CREATE BACKUP AND REFRESH CATALOGUE')
  print('2. PURGE OLD BACKUP SET FROM CATALOGUE')
  print('3. PATROL')
  print('Enter Option: ')
  opt=int(input())
  if(opt==1 or opt==2 or opt==3):
    if(opt==1 or opt==2):
      print('Enter Number of Live Retention Sets: ')
      retention=int(input())
      print('Enter path: ')
      path=str(input())
      datetime=dtm.datetime
      seid=datetime.now()
      setid=seid.hour
      bkConfig=(opt,retention,setid,path)
      bkpacks=(clc.OrderedDict,sbp.PIPE,sbp.Popen,ops,datetime)
      dbTup=(a,e,i,o)
    elif(opt==3):
      backupAgent()
  else:
    print('INVALID OPTION')
  return 0

def backupAgent(bkc,bkpacks,dbTup):
  global ops,PIPE,Popen,OrderedDict,datetime,dba
  OrderedDict,PIPE,Popen,ops,datetime=bkpacks
  dba,dbe,dbi,dbo=dbTup
  opt,retention,backupSetID,path=bkc
  dtNow=datetime.today().date()
  (y,m,d)=str(dtNow).split('-')[:3]
  setID=y+m+d
  backupSetID=int(setID+str(backupSetID))
  cbk=False
  pbk=False
  pat=False
  if(opt==1 or opt=='1'):
    cbk=createBackup(path,backupSetID)
    pbk=purgeBackup(retention)
  elif(opt==2 or opt=='2'):
    pbk=purgeBackup(retention)
  elif(opt==3 or opt=='3'):
    pat=backupPatrol()
  else:
    print('INVALID OPTION')
  return cbk,pbk,pat

def createBackup(path,bkpid):
  bkStat=OrderedDict()
  bkStat['timestarted']=datetime.now()
  bkStat['backupSetID']=bkpid
  bkStat['purged']=False
  bkStat['model']='backup'
  bkStat['path']=path
  dbDIR=path+'/'+bkpid
  if(os.path.exists(dbDIR)):
    timeNow=datetime.now()
    oldDBName=dbDIR+str(timeNow.hour)
    proc=Popen(['mv','-f',dbDIR,oldDBName],stderr=PIPE,stdout=PIPE)
    proc.wait()
    pErr,gist=proc.communicate()
    bkStat['issue']={'gist':gist.decode(),'error':pErr.decode()}
  ops.append(path)
  proc=Popen(ops,stderr=PIPE,stdout=PIPE)
  proc.wait()
  pErr,gist=proc.communicate()
  pErr=pErr.decode()
  gist=gist.decode()
  if(pErr==''):
    bkStat['status']=True
  else:
    bkStat['status']=False
    bkStat['error']=pErr
  bkStat['gist']=gist
  bkStat['timefinished']=datetime.now()
  return bkStat

def purgeBackup():
  catalogue=list(dba.find({'purged':False},{'backupSetID':1}).sort('backupSetID',-1))
  if(len(catalogue)>retention):
    catalogue.sort(reverse=True)
    purgeFile=catalogue[retention:]
    purgeSet=list(dba.find({'backupSetID':{'$in':purgeFile}},{'backupSetID':1,'path':1}))
    purgeList=[]
    for lsd,pSet in purgeSet:
      proc=Popen(['rm','-r',pSet],stderr=PIPE,stdout=PIPE)
      proc.wait()
      pErr,gist=proc.communicate()
      pErr=pErr.decode()
      if(pErr==''):
        bkStat=OrderedDict({'backupSetID':lsd,'purged':True})
      else:
        bkStat=OrderedDict({'backupSetID':lsd,'purgeIssue':pErr,'purged':True})
      purgeList.append(bkStat)
  return purgeList

def backupPatrol():
  docs=list(dba.find({'purged':True},{'path':1}))
  for doc in docs:
    if(os.path.exists(doc)):
      proc=Popen(['rm','-r',doc],stderr=PIPE,stdout=PIPE)
      proc.wait()
      pErr,gist=proc.communicate()
      pat=True
    else:
      continue
  return pat

if __name__=='__main__':
  import sys, collections as clc, pymongo as pym, datetime as dtm, subprocess as sbp
  print('BACKUP MANAGER SHELL INVOKED...')
  print('DO NOTE, INVOKING FROM "dtsAdmin" IS THE BEST PRACTISE ...')
  if(len(sys.argv)!=5):
    sys.exit('Invalid Arguments')
  mongoConStr=sys.argv[1]
  eaedw=sys.argv[2]
  mongoKonnect=pym.MongoClient(mongoConStr,username=sys.argv[3],password=sys.argv[4])
  a=mongoKonnect[eaedw].logDTSAdminActions
  e=mongoKonnect[eaedw].logDTSErrors
  i=mongoKonnect[eaedw].indexTracker
  o=mongoKonnect[eaedw]
  ops=['mongodump','-h',mongoConStr,'--authenticationDatabase','admin','-u',sys.argv[3],'-p',sys.argv[4],'-d',eaedw,'--out']