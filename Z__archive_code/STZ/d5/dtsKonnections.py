'''
CLASSIFIED CONNECTORS FOR ALL SOURCES
'''

import dtsLib as lib

dtsDriver='DTS'

# Load Config File
try:
    with open('dtsConfig.yaml','r') as ymlFile:
        dtsConfig=lib.yml.load(ymlFile)
except OSError as err:
    lib.logImpError(err)
    lib.thwart(err)


class eaeAdapter():
	def __init__(mem):
		mem.port=dtsConfig['eae']['port']
		mem.eaedw=str(dtsConfig['eae']['db'])
		mem.ins=dtsConfig['eae']['host']
		mem.auth=dtsConfig['eae']['auth']
	def mKonnect(mem):
		eaeIns=mem.ins+':'+str(mem.port)
		if(mem.auth==True):
			key=mem.kik.encode()
			f=lib.fnt(key)
			kon=lib.mClient(eaeIns)
		elif(mem.auth==False):
			kon=lib.mClient(eaeIns)
		else:
			lib.thwart('INSTANCE OUT OF NETWORK OR INVALID AUTH MODE')
		return kon

class eaeFuse(eaeAdapter):
	def __init__(mem):
		super(eaeFuse, mem).__init__()
		mem.mongoConnect=super(eaeFuse,mem).mKonnect()
		mem.dbi=mem.mongoConnect[mem.eaedw].InstanceConfiguration
		mem.dbx=mem.mongoConnect[mem.eaedw].logBugs
		mem.dbf=mem.mongoConnect[mem.eaedw].logForkerActions
		mem.dbs=mem.mongoConnect[mem.eaedw].customerStats
		mem.dbc=mem.mongoConnect[mem.eaedw].changeTracker
		mem.studentData=mem.mongoConnect[mem.eaedw].studentData
		mem.phiCampData=mem.mongoConnect[mem.eaedw].phiCampData
		mem.phiCampStats=mem.mongoConnect[mem.eaedw].phiCampStats

class appFuse(eaeFuse):
	def __init__(mem):
		super(appFuse, mem).__init__()
		mem.lmsInsList=mem.dbi.find({'status':'active','instance_type':'lms','database_type':'sql'},{'instancename':1,'database':1,'instancecode':1})
		mem.phiInsList=mem.dbi.find({'status':'active','instance_type':'phishproof','database_type':'mysql'},{'instancecode':1,'instancename':1,'database':1})
	def lmsFuse(mem):
		lmsConnections=[]
		for ins in mem.lmsInsList:
			sqlip=ins['database']['serverIP']
			sqlport=ins['database']['port']
			sqlpwd=ins['database']['password']
			sqluid=ins['database']['username']
			sqldb=ins['database']['databasename']
			sqlConStr='DRIVER='+dtsDriver+';SERVER=' +sqlip+ ',' +str(int(sqlport))+ ';DATABASE=' +sqldb+ ';UID=' +sqluid+ ';PWD=' +sqlpwd
			try:
				sqlConnexion=lib.sqlClient(sqlConStr)
			except lib.sqlError as err:
				continue
			lmsConnections.append(lib.odict([('ins',ins['instancename']),('code',ins['instancecode']),('cnx',sqlConnexion)]))
		return lmsConnections
	def phiFuse(mem):
		phiConnections=[]
		for ins in mem.phiInsList:
			phiConnections.append(1)
		return phiConnections