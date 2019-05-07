try:
	import yaml as y
	from collections import OrderedDict as odict
	from pypyodbc import connect as sqlCnx
	from pandas import read_sql_query as rsq,DataFrame as pdf,concat as pconcat,Series as pSeries
	from sqlalchemy import create_engine as pgcnx
	from psycopg2 import connect as pgconnect
	from sqlalchemy.sql import text as alchemyText
	from pandas.core.groupby.groupby import DataError
	import ray as r
	from datetime import datetime as dtm,timedelta as tdt
	from sqlalchemy.orm import sessionmaker
	from io import StringIO
	from mysql.connector import connect as mysqlCNX, Error as mysqlErr
except ImportError:
	raise ImportError(' Module(s) not installed...')

with open('dimConfig.yml') as ymlFile:
	cfg=y.safe_load(ymlFile)

def dwCNX(tinyset=False):
	uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
	eaeSchema=cfg['eaedb']['schema']
	if tinyset:
		csize=cfg['pandas']['tinyset']
	else:
		csize=cfg['pandas']['bigset']
	return csize,eaeSchema,uri

def dataSession(urx):
	cnxPGX=pgcnx(urx)
	SessionClass=sessionmaker(bind=cnxPGX)
	Session=SessionClass()
	return Session

def objects_sql(urx,itype):
	insList_io=[]
	cnxPGX=pgcnx(urx)
	SessionClass=sessionmaker(bind=cnxPGX)
	Session=SessionClass()
	insData=cnxPGX.execute("SELECT * FROM framework.instanceconfig WHERE isactive=true AND instancetype='" +itype+ "' ")
	colFrame_io=rsq("SELECT icode,instancetype,app,collection,s_table,rower,stg_cols,pkitab,pki_cols FROM framework.live_instancecollections() WHERE instancetype='" +itype+ "' ",cnxPGX)
	for cnx in insData:
		dat=odict(cnx)
		insDict=odict()
		insDict['icode']=dat['instancecode']
		if itype=='mssql':
			insDict['sqlConStr']='DRIVER={'+cfg['drivers']['mssql']+'};SERVER='+dat['hostip']+','+str(int(dat['hport']))+';DATABASE='+dat['dbname']+';UID='+dat['uid']+';PWD='+dat['pwd']+';MARS_Connection=Yes'
		elif itype=='salesforce':
			insDict['sqlConStr']='salesforce'
		else:
			insDict['user']=dat['uid']
			insDict['password']=dat['pwd']
			insDict['database']=dat['dbname']
			insDict['host']=dat['hostip']
			insDict['port']=dat['hport']
		insList_io.append(insDict)
	return odict([('frame',colFrame_io),('insList',insList_io),('session',Session)])

def logError(pid,jobid,err_message,uri):
	pgx=pgcnx(uri)
	err_json={'pid':[pid],'jobid':[jobid],'error':[err_message],'error_time':[dtm.utcnow()]}
	errFrame=pdf.from_dict(err_json)
	errFrame.to_sql('errorlogs',pgx,if_exists='append',index=False,schema='framework')
	pgx.dispose()
	return None
