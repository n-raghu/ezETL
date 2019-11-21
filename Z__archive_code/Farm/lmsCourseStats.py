from dtsFarmLib import *
from farmQueries import faq_lmsCourseCompletion

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
bcc=mongoConnect[eaedw][cStore['_cc']]
odbcDriver=settings['odbcDriver']

print('Connected to : ' +mongoConStr)
sqlInsList=dbi.find({'status':'active','instance_type':'lms','database_type':'sql'},{'instancename':1, 'instancecode':1,'database':1})
print('SQL Instances Found : ' +str(sqlInsList.count())+ '. ')

if(sqlInsList.count()==0):
    sys.exit()

uid=uid()
courseCollection=[]

for ins in sqlInsList:
 sqlIns=ins['instancename']
 sqlip=ins['database']['serverIP']
 sqlport=ins['database']['port']
 sqlpwd=ins['database']['password']
 sqlInsCode=ins['instancecode']
 sqluid=ins['database']['username']
 sqldb=ins['database']['databasename']
 sqlConStr='DRIVER={'+odbcDriver+'};SERVER='+sqlip+','+str(int(sqlport))+';DATABASE='+sqldb+';UID='+sqluid+';PWD=' +sqlpwd
 try:
  sqlConnect=podbc.connect(sqlConStr)
 except podbc.Error as err:
  xiid=str(bson.objectid.ObjectId())
  threadError=True
  dxx.insert_one({'error':str(err),'instancecode':sqlInsCode,'times':dtm.utcnow(),'env':'FARM','_id':xiid,'script':'courseStats','type':'exception'})
  print('Error Connecting to ' +sqlIns+ ' instance. See Error Logs for more details. ')
  continue
 print('Connected to instance: ' +sqlIns+ '. ')
 sqlCur=sqlConnect.cursor()

 courseDict=odict()
 queries=faq_lmsCourseCompletion()
 dataTub=[]

 sqlCur.execute(queries[0])
 sqlCur.execute(queries[1])
 sqlCur.execute(queries[2])
 tub=sqlCur.fetchall()
 dataTub.append(deepcopy(tub))
 sqlCur.execute(queries[3])
 tub=sqlCur.fetchall()
 dataTub.append(deepcopy(tub))
 sqlCur.execute(queries[4])
 tub=False

 dtNow=dtm.utcnow()
 for tup in dataTub[0]:
 	doc=odict()
 	idi=int(str(tup[0])+str(tup[1]))
 	doc['instancecode']=sqlInsCode
 	doc['instancename']=sqlIns
 	doc['calendaryear']=tup[0]
 	doc['doy']=tup[1]
 	doc['calendarday']=tup[2]
 	doc['assigned']=tup[5]
 	doc['completed']=0
 	doc['weekofmonth']=tup[3]
 	doc['calendarmonth']=tup[4]
 	doc['woy']=tup[6]
 	doc['createdUTC']=dtNow
 	doc['batchCode']=uid
 	courseDict[idi]=doc

 for tup in dataTub[1]:
 	idi=int(str(tup[0])+str(tup[1]))
 	if(idi in courseDict.keys()):
 		courses=courseDict.get(idi,odict())
 		courses['completed']=tup[5]
 	else:
 		doc=odict()
 		doc['instancecode']=sqlInsCode
 		doc['instancename']=sqlIns
 		doc['calendaryear']=tup[0]
 		doc['doy']=tup[1]
 		doc['calendarday']=tup[2]
 		doc['assigned']=0
 		doc['completed']=tup[5]
 		doc['weekofmonth']=tup[3]
 		doc['createdUTC']=dtNow
 		doc['calendarmonth']=tup[4]
 		doc['woy']=tup[6]
 		doc['batchCode']=uid

 dataTub=True
 sqlCur.close()
 sqlConnect.close()
 courseCollection.append(courseDict.values())

qpipe=[{'$project':{'instancecode':1,'instancename':1,'createdUTC':1,'doy':1,'woy':1,'calendaryear':1,'calendarmonth':1,'weekofmonth':1,'calendarday':1,'assignedcount':'$assigned','completedcount':'$completed'}}
,{'$out':dataStore['ccd']}]

if(len(courseCollection)>0):
    bcc.drop()
    for col in courseCollection:
        bcc.insert_many(col)
    bcc.aggregate(qpipe,allowDiskUse=True)

mongoConnect.close()
