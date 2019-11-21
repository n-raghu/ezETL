from dtsFarmLib import *
from farmQueries import *

if(len(sys.argv)==5):
  authMode=True
elif(len(sys.argv)==3):
  authMode=False
else:
  sys.exit('Invalid number of params.')

mongoConStr=sys.argv[1]
eaedw=sys.argv[2]

if(authMode):
  uri='mongodb://'+str(sys.argv[3])+ ':' +str(sys.argv[4])+ '@' +mongoConStr
  mongoConnect=mClient(uri)
else:
  mongoConnect=mClient(mongoConStr)

dataStore=dataStore()
cStore=cacheStore()
logStore=farmLogStore()
settings=FarmSettings()

dbi=mongoConnect[eaedw][logStore['dbi']]
dbe=mongoConnect[eaedw][logStore['dbc']]
dxx=mongoConnect[eaedw][logStore['dxx']]
bcc=mongoConnect[eaedw][cStore['_hs']]

sqlInsList=dbi.find({'status':'active','instance_type':'phishproof','database_type':'mysql'},{'instancename':1,'database':1,'instancecode':1})
print('Total PHISHPROOF instances fetched : '+str(sqlInsList.count())+'. ')

if(sqlInsList.count()==0):
    sys.exit()

uid=uid()
hetroStats=[]

for ins in sqlInsList:
 sqlIns=ins['instancename']
 sqlip=ins['database']['serverIP']
 sqlport=ins['database']['port']
 sqlInsCode=ins['instancecode']
 sqlpwd=ins['database']['password']
 sqluid=ins['database']['username']
 sqldb=ins['database']['databasename']
 try:
  sqlConnect=psql.connect(host=sqlip,user=sqluid,passwd=sqlpwd,db=sqldb,cursorclass=pcr.DictCursor,charset="utf8mb4")
 except psql.Error as err:
  xiid=str(bson.objectid.ObjectId())
  dxx.insert_one({'error':str(err),'instancecode':sqlInsCode,'times':dtm.utcnow(),'env':'FARM','_id':xiid,'script':'phiHetroStats','type':'exception'})
  print('Error Connecting to '+sqlIns+' instance. See Error Logs for more details. ')
  continue
 sqlCur=sqlConnect.cursor()

 queries=faq_phiHetro()
 camps=odict()
 dtNow=dtm.utcnow()
 dataKeyList=['templates','mails','victims','clicks','forms','opened','attachments',]

 sqlCur.execute(queries[0])
 sqlCur.execute(queries[1])
 dataTub=sqlCur.fetchall()
 for tup in dataTub:
     doc=odict()
     idi=int(str(tup['YYY'])+str(tup['DOY']))
     doc['instancecode']=sqlInsCode
     doc['instancename']=sqlIns
     doc['batchCode']=uid
     doc['doy']=tup['DOY']
     doc['calendaryear']=tup['YYY']
     doc['calendarday']=tup['SDT']
     doc['calendarmonth']=tup['M']
     doc['woy']=tup['WOY']
     doc['weekofmonth']=tup['WOM']
     doc['campaigns']=tup['KNT']
     doc['createdUTC']=dtNow
     camps[idi]=doc
 sqlCur.close()
 sqlCur=sqlConnect.cursor()
 qloop=copy(queries[2:len(queries)-1])

 for i in range(len(dataKeyList)):
     sqlCur=sqlConnect.cursor()
     sqlCur.execute(qloop[i])
     dataTub=sqlCur.fetchall()
     for tup in dataTub:
         doc=odict()
         idi=int(str(tup['YYY'])+str(tup['DOY']))
         if(tup['KNT']):
             count=tup['KNT']
         else:
             count=0
         if idi in camps.keys():
             camp=camps.get(idi,odict())
             camp[dataKeyList[i]]=count
     sqlCur.close()

 sqlCur=sqlConnect.cursor()
 sqlCur.execute(queries[len(queries)-1])
 sqlConnect.close()
 hetroStats.append(camps.values())

qpipe=[{'$project':{'instancecode':1,'instancename':1,'createdUTC':1,'doy':1,'woy':1,'calendaryear':1,'calendarmonth':1,'weekofmonth':1,'calendarday':1,'campaigns':1,'templates':1,'mails':1,'victims':1,'clicks':1,'attachments':1,'forms':1,'opened':1}}
,{'$out':dataStore['hsd']}]

if(len(hetroStats)>0):
    bcc.drop()
    for stats in hetroStats:
        bcc.insert_many(stats)
    cur=bcc.find()
    frame=DataFrame(list(cur))
    frame.fillna(0,inplace=True)
    bcc.drop()
    bcc.insert_many(frame.to_dict('r'))
    bcc.aggregate(qpipe,allowDiskUse=True)

mongoConnect.close()
