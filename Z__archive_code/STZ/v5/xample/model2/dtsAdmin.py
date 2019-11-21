import sys

try:
	import bson, collections as clc, pypyodbc as pod, pymongo as pom, time as ti
	from numpy import array as arrow
	from datetime import datetime as dtm
except ImportError as e:
	sys.exit(e)

eaeIns='172.16.1.179:36006'
mongoConnect=pom.MongoClient(eaeIns)
dba=mongoConnect.eaedw.InstanceConfiguration
sqlInsList = dba.find({'status': 'active', 'instance_type': 'lms', 'database_type': 'sql'},{'instancename':1, 'database':1})
sqlConnections=[]

for ins in sqlInsList:
	sqlIns=ins['instancename']
	sqlip=ins['database']['serverIP']
	sqlport=ins['database']['port']
	sqlpwd=ins['database']['password']
	sqluid=ins['database']['username']
	sqldb=ins['database']['databasename']
	sqlConStr='DRIVER={MSSQL-NC1311};SERVER=' +sqlip+ ',' +str(int(sqlport))+ ';DATABASE=' +sqldb+ ';UID=' +sqluid+ ';PWD=' +sqlpwd
	sqlConnect=pod.connect(sqlConStr)
	sqlConnections.append(sqlConnect)

def connections():
	global sqlConnections,mongoConnect
	return mongoConnect,sqlConnections