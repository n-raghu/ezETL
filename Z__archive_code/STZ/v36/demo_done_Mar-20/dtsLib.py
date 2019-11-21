import sys

try:
	from pymongo import MongoClient as mClient
	from pypyodbc import connect as sqlCNX
	import json
	from numpy import array
	from datetime import datetime as dtm
	import ray
	from copy import *
	import yaml
	from pandas import DataFrame,read_sql,options,read_sql_query as pandas_query
	from collections import OrderedDict as odict
	from time import sleep as ziz,time
	from cryptography.fernet import Fernet as fnt
except ImportError as e:
	sys.exit(e)

try:
	with open('dtsConfig.yaml','r') as yamlFile:
		yFile=yaml.load(yamlFile)
except FileNotFoundError as err:
	sys.exit(err)

class eaeAdapter():
	def __init__(mem):
		try:
			mem.port=yFile['eaedb']['port']
			mem.ins=yFile['eaedb']['host']
			mem.auth=yFile['eaedb']['authentication']
			mem.key=yFile['eaedb']['pass_key']
			mem.pwd=yFile['eaedb']['passwd']
			mem.uid=yFile['eaedb']['user']
			mem.eae=yFile['eaedb']['db']
		except KeyError as err:
			sys.exit(err)
	def mKonnect(mem):
		try:
			if(mem.auth):
				fKey=fnt(mem.key)
				eaePass=fKey.decrypt(mem.pwd)
				konStr=mem.ins+ ':' +str(mem.port)+ ',username=' +mem.uid+ ',password=eaePass'
			elif(mem.auth==False):
				konStr=mem.ins+ ':' +str(mem.port)
			else:
				sys.exit('BIT type value expected in [eaedb][authentication]. ')
		except ValueError:
			sys.exit('INSTANCE OUT OF NETWORK OR INVALID AUTH MODE. ')
		try:
			kon=mClient(konStr)
		except:
			print('Connection Issues. Check DB connections. ')
		return kon

class eaeFuse(eaeAdapter):
	def __init__(mem):
		super(eaeFuse, mem).__init__()
		mem.mongoConnect=super(eaeFuse,mem).mKonnect()
		mem.dbi=mem.mongoConnect[mem.eae].InstanceConfiguration
		mem.dbx=mem.mongoConnect[mem.eae].logDTSErrors
		mem.dbf=mem.mongoConnect[mem.eae].logForkerActions
		mem.dbs=mem.mongoConnect[mem.eae].customerStats
		mem.dbc=mem.mongoConnect[mem.eae].changeTracker
		mem.studentData=mem.mongoConnect[mem.eae].studentData
		mem.phishCampData=mem.mongoConnect[mem.eae].phishCampData
		mem.phishDashboardData=mem.mongoConnect[mem.eae].phishDashboardData
		mem.phishDashboardParams=mem.mongoConnect[mem.eae].phishDashboardParams
	def eaeObjects(mem):
		eaeObj=(mem.dbi,mem.dbx,mem.dbf,mem.dbs,mem.dbc,mem.studentData,mem.phishCampData,mem.phishDashboardData,mem.phishDashboardParams)
		return eaeObj

class appFuse(eaeFuse):
	def __init__(mem):
		super(appFuse, mem).__init__()
		mem.lmsInsList=mem.dbi.find({'status':'active','instance_type':'lms','database_type': 'sql'},{'instancename':1,'database':1,'instancecode':1})
		mem.phiInsList=mem.dbi.find({'status':'active','instance_type':'phishproof','database_type':'mysql'},{'instancecode':1,'instancename':1,'database':1})
	def lmsFuse(mem):
		lmsConnections=[]
		for ins in mem.lmsInsList:
			sqlip=ins['database']['serverIP']
			sqlport=ins['database']['port']
			sqlpwd=ins['database']['password']
			sqluid=ins['database']['username']
			sqldb=ins['database']['databasename']
			sqlConStr='DRIVER={DTS};SERVER=' +sqlip+ ',' +str(int(sqlport))+ ';DATABASE=' +sqldb+ ';UID=' +sqluid+ ';PWD=' +sqlpwd
			lmsConnections.append(odict([('ins',ins['instancename']),('code',ins['instancecode']),('cnx',sqlConStr)]))
		return lmsConnections
	def phiFuse(mem):
		phiConnections=[]
		for ins in mem.phiInsList:
			phiConnections.append(1)
		return phiConnections
