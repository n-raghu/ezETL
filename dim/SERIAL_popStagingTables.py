debug=False

import sys
if all([len(sys.argv)<2, debug==False]):
	sys.exit('PID not provided... ')
elif debug:
	pid=-1
else:
	pid=int(sys.argv[1])

from dimlib import *

uri=uri
csize=cfg['pandas']['chunksize']
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

mssql_dict=objects_mssql(uri)
insList=mssql_dict['insList']
colFrame=mssql_dict['frame']
print(colFrame)

def popCollections(icode,connexion,iFrame):
	pgx=pgcnx(uri)
	sqx=sqlCnx(connexion)
	chunk=pdf([],columns=['model'])
	trk=pdf([],columns=['status','collection','timestarted','timefinished','rowversion'])
	for idx,rowdata in iFrame.iterrows():
		rco=rowdata['collection']
		scols=rowdata['stg_cols']
		rower=str(int(rowdata['rower']))
		trk=trk.append({'status':False,'collection':rco,'timestarted':dtm.utcnow()},ignore_index=True)
		sql="SELECT '" +icode+ "' as instancecode," +scols+ ",CONVERT(BIGINT,sys_ROWVERSION) AS ROWER FROM " +rco+ "(NOLOCK) WHERE CONVERT(BIGINT,sys_ROWVERSION) > " +rower
		for chunk in rsq(sql,sqx,chunksize=csize):
			chunk.to_sql(rowdata['s_table'],pgx,if_exists='append',index=False,schema=eaeSchema)
			trk=trk.append({'collection':rco,'rowversion':chunk['rower'].max(),'timestarted':dtm.utcnow()},ignore_index=True)
		trk.loc[(trk['collection']==rco),['status']]=True
		trk.loc[(trk['collection']==rco),['timefinished']]=dtm.utcnow()
	del chunk
	sqx.close()
	pgx.dispose()
	trk['instancecode']=icode
	return trk

print('Active Instances Found: ' +str(len(insList)))
if all([debug==False,len(insList)>0]):
	for ins in insList:
		cnxStr=ins['sqlConStr']
		instancecode=ins['icode']
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['instancetype']=='mssql'),['collection','s_table','rower','stg_cols']]
		obFrame.append(popCollections(instancecode,cnxStr,iFrame))
	r.wait(objFrame)
	for obj in objFrame:
		tracker=tracker.append(obj,sort=False,ignore_index=True)
	del objFrame
	recordRowVersions()
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
