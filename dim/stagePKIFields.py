debug=False

import sys
if all([len(sys.argv)<2, debug==False]):
	sys.exit('PID not provided... ')
elif debug:
	pid=-1
else:
	pid=int(sys.argv[1])

from dimlib import *

r.init()
csize,eaeSchema,uri=dwCNX(tinyset=False)
print(uri)
objFrame=[]
tracker=pdf([],columns=['instancecode','collection','primekeys','kount','starttime','endtime','status'])

mssql_dict=objects_mssql(uri)
insList=mssql_dict['insList']
colFrame=mssql_dict['frame']

@r.remote
def popCollections(icode,connexion,iFrame):
	pgx=pgcnx(uri)
	trk=pdf([],columns=['collection','primekeys','kount','starttime','endtime','status'])
	try:
		sqx=sqlCnx(connexion)
	except podbc.Error as err:
		print('Error Connecting to ' +icode+ ' instance. See Error Logs for more details. ')
		logError(pid,'stagePKIFields',err,uri)
		return trk
	for idx,rowdata in iFrame.iterrows():
		rco=rowdata['collection']
		cachecollection=rowdata['pkitab']
		primeKeys=iFrame.loc[(iFrame['collection']==rco)]['pki_cols'].values.tolist()[0]
		knt=0
		stime=dtm.utcnow()
		sql="SELECT '" +icode+ "' as instancecode," +primeKeys+ " FROM " +rco+ "(NOLOCK) "
		try:
			for chunk in rsq(sql,sqx,chunksize=csize):
				chunk.to_sql(cachecollection,pgx,if_exists='append',index=False,schema=eaeSchema)
				knt+=len(chunk)
		except (DataError,AssertionError,ValueError,IOError,IndexError) as err:
			trk=trk.append({'status':False,'collection':rco,'kount':knt,'primekeys':primeKeys,'starttime':stime,'endtime':dtm.utcnow()},sort=False,ignore_index=True)
		trk=trk.append({'status':True,'collection':rco,'kount':knt,'primekeys':primeKeys,'starttime':stime,'endtime':dtm.utcnow()},sort=False,ignore_index=True)
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
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['instancetype']=='mssql'),['collection','pkitab','pki_cols']]
		objFrame.append(popCollections.remote(instancecode,cnxStr,iFrame))
	r.wait(objFrame)
	for obj in objFrame:
		tracker=tracker.append(r.get(obj),sort=False,ignore_index=True)
	del objFrame
	r.shutdown()
	pgx=pgcnx(uri)
	tracker['pid']=pid
	tracker.to_sql('cachetraces',pgx,if_exists='append',index=False,schema='framework')
	pgx.dispose()
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
