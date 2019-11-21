import sys

try:
	from pymongo import MongoClient as mClient
	from pypyodbc import connect as sqlCNX
	import json
	from numpy import array as arrow
	from datetime import datetime as dtm
	import time
	from collections import OrderedDict as odict
	from cryptography.fernet import Fernet as fnt
except ImportError as e:
	sys.exit(e)

class eaeAdapter():
	def __init__(mem):
		mem.port=36006
		mem.ins='172.16.1.179'
		mem.klust=False
		mem.kik='4PUsWh3L-QjOGtygsx47eLyqVZzRDKdP2DFf6p-2GBA='
		mem.net=True
		mem.uid=''
		mem.auth=False
		mem.pwd=''
	def mKonnect(mem):
		if(mem.net==False):
			sys.exit('INSTANCE NOT IN NETWORK')
		eaeIns=mem.ins+':'+str(mem.port)
		if(mem.auth==True):
			key=mem.kik.encode()
			f=fnt(key)
			kon=mClient(eaeIns)
		elif(mem.auth==False):
			kon=mClient(eaeIns)
		else:
			sys.exit('INSTANCE OUT OF NETWORK OR INVALID AUTH MODE')
		return kon

class eaeFuse(eaeAdapter):
	def __init__(mem):
		super(eaeFuse, mem).__init__()
		mem.mongoConnect=super(eaeFuse,mem).mKonnect()
		mem.dbi=mem.mongoConnect.eaedw.InstanceConfiguration
		mem.dbx=mem.mongoConnect.eaedw.logBugs
		mem.dbf=mem.mongoConnect.eaedw.logForkerActions
		mem.dbs=mem.mongoConnect.eaedw.customerStats
		mem.dbc=mem.mongoConnect.eaedw.changeTracker
		mem.studentData=mem.mongoConnect.eaedw.studentData
		mem.phiCampData=mem.mongoConnect.eaedw.phiCampData
		mem.phiCampStats=mem.mongoConnect.eaedw.phiCampStats

class appFuse(eaeFuse):
	def __init__(mem):
		super(appFuse, mem).__init__()
		mem.lmsInsList=mem.dbi.find({'status': 'active', 'instance_type': 'lms', 'database_type': 'sql'},{'instancename':1,'database':1,'instancecode':1})
		mem.phiInsList=mem.dbi.find({'status': 'active', 'instance_type': 'phishproof', 'database_type': 'mysql'},{'instancecode':1,'instancename':1,'database':1})
	def lmsFuse(mem):
		lmsConnections=[]
		for ins in mem.lmsInsList:
			sqlip=ins['database']['serverIP']
			sqlport=ins['database']['port']
			sqlpwd=ins['database']['password']
			sqluid=ins['database']['username']
			sqldb=ins['database']['databasename']
			sqlConStr='DRIVER={MSSQL-NC1311};SERVER=' +sqlip+ ',' +str(int(sqlport))+ ';DATABASE=' +sqldb+ ';UID=' +sqluid+ ';PWD=' +sqlpwd
			lmsConnections.append(odict([('ins',ins['instancename']),('code',ins['instancecode']),('cnx',sqlCNX(sqlConStr))]))
		return lmsConnections
	def phiFuse(mem):
		phiConnections=[]
		for ins in mem.phiInsList:
			phiConnections.append(1)
		return phiConnections
