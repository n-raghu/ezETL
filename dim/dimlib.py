import yaml as y
from collections import OrderedDict as odict
from pypyodbc import connect as sqlCnx
from pandas import read_sql_query as rsq,DataFrame as pdf
from sqlalchemy import create_engine as pgcnx
from sqlalchemy.sql import text as alchemyText
from datetime import datetime as dtm
from sqlalchemy.orm import sessionmaker

with open('dimConfig.yml') as ymlFile:
	cfg=y.load(ymlFile)

def dwCNX(tinyset=False):
	uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
	eaeSchema=cfg['eaedb']['schema']
	if tinyset:
		csize=cfg['pandas']['tinyset']
	else:
		csize=cfg['pandas']['bigset']
	return csize,eaeSchema,uri

if(cfg['pandas']['parallelism']):
	import ray as r

def objects_mssql(urx):
	insList_io=[]
	cnxPGX=pgcnx(urx)
	insData=cnxPGX.execute("SELECT * FROM framework.instanceconfig WHERE isactive=true AND instancetype='mssql' ")
	colFrame_io=rsq("SELECT icode,instancetype,collection,s_table,rower,stg_cols,pkitab,pki_cols FROM framework.live_instancecollections() WHERE instancetype='mssql' ",cnxPGX)
	for cnx in insData:
		dat=odict(cnx)
		insDict=odict()
		insDict['icode']=dat['instancecode']
		insDict['sqlConStr']='DRIVER={'+cfg['drivers']['mssql']+'};SERVER='+dat['hostip']+','+str(int(dat['hport']))+';DATABASE='+dat['dbname']+';UID='+dat['uid']+';PWD='+dat['pwd']
		insList_io.append(insDict)
	return odict([('frame',colFrame_io),('insList',insList_io)])
