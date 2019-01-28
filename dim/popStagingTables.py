debug=False

import sys
if all([len(sys.argv)<2, debug==False]):
	sys.exit('PID not provided... ')
elif debug:
	pid=-1
else:
	pid=int(sys.argv[1])

import yaml as y
from collections import OrderedDict as odict
from pypyodbc import connect as sqlCnx
from pandas import read_sql_query as rsq,DataFrame as pdf
from sqlalchemy import create_engine as pgcnx
import ray as r
from datetime import datetime as dtm

with open('dimConfig.yaml') as ymlFile:
	cfg=y.load(ymlFile)
r.init()
uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
eaeSchema=cfg['eaedb']['schema']
csize=cfg['chunksize']
tracker=pdf([],columns=['status','instancecode','collection','timestarted','timefinished','rowversion'])
objFrame=[]

def recordRowVersions():
	global tracker
	if tracker.empty:
		print('Tracker is Empty ')
		issue=True
	else:
		pgx=pgcnx(uri)
		tracker['pid']=pid
		tracker['instancetype']='mssql'
		tracker.fillna(-1,inplace=True)
		tracker.to_sql('chunktraces',pgx,if_exists='append',index=False,schema='framework')
		ixx=tracker.groupby(['instancecode','collection'],sort=False)['rowversion'].transform(max)==tracker['rowversion']
		tracker[ixx].to_sql('collectiontracker',pgx,if_exists='append',index=False,schema='framework')
		del tracker
		issue=False
	return issue

def objects_mssql():
	insList_io=[]
	cnxPGX=pgcnx(uri)
	insData=cnxPGX.execute("SELECT * FROM framework.instanceconfig WHERE isactive=true AND instancetype='mssql' ")
	colFrame_io=rsq("SELECT * FROM framework.activecollections() WHERE instancetype='mssql' ",cnxPGX)
	for cnx in insData:
		dat=odict(cnx)
		insDict=odict()
		insDict['icode']=dat['instancecode']
		insDict['sqlConStr']='DRIVER={'+cfg['drivers']['mssql']+'};SERVER='+dat['hostip']+','+str(int(dat['hport']))+';DATABASE='+dat['dbname']+';UID='+dat['uid']+';PWD='+dat['pwd']
		insList_io.append(insDict)
	return odict([('frame',colFrame_io),('insList',insList_io)])

mssql_dict=objects_mssql()
insList=mssql_dict['insList']
colFrame=mssql_dict['frame']

@r.remote
def popCollections(icode,connexion,iFrame):
	pgx=pgcnx(uri)
	sqx=sqlCnx(connexion)
	trk=pdf([],columns=['status','collection','timestarted','timefinished','rowversion'])
	for idx,rowdata in iFrame.iterrows():
		rco=rowdata['collection']
		trk=trk.append({'status':False,'collection':rco,'timestarted':dtm.utcnow()},ignore_index=True)
		sql="SELECT '" +icode+ "' as instancecode,*,CAST(sys_ROWVERSION AS BIGINT) AS ROWER FROM " +rco+ "(NOLOCK) WHERE CAST(sys_ROWVERSION AS BIGINT) > " +str(int(rowdata['rower']))
		for chunk in rsq(sql,sqx,chunksize=csize):
			chunk.to_sql(rowdata['s_table'],pgx,if_exists='append',index=False,schema=eaeSchema)
			trk=trk.append({'collection':rco,'rowversion':chunk['rower'].max(),'timestarted':dtm.utcnow()},ignore_index=True)
		trk.loc[(trk['collection']==rco),['status']]=True
		trk.loc[(trk['collection']==rco),['timefinished']]=dtm.utcnow()
	del chunk
	sqx.close()
	trk['instancecode']=icode
	return trk

print('Active Instances Found: ' +str(len(insList)))
if all([debug==False,len(insList)>0]):
	for ins in insList:
		cnxStr=ins['sqlConStr']
		instancecode=ins['icode']
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['instancetype']=='mssql'),['collection','s_table','rower']]
		objFrame.append(popCollections.remote(instancecode,cnxStr,iFrame))
	r.wait(objFrame)
	for obj in objFrame:
		tracker=tracker.append(r.get(obj),sort=False,ignore_index=True)
	del objFrame
	r.shutdown()
	recordRowVersions()
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
