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
tracker=pdf([],columns=['instancecode','collection','primekeys','kount','starttime','endtime'])

insQuery=''' SELECT tab.TABLE_NAME as collection,col.COLUMN_NAME as cache_indexcolumn
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tab
	JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE col ON tab.CONSTRAINT_NAME=col.CONSTRAINT_NAME AND tab.TABLE_NAME=col.TABLE_NAME
WHERE Constraint_Type = 'PRIMARY KEY' '''

mssql_dict=objects_mssql(uri)
insList=mssql_dict['insList']
colFrame=mssql_dict['frame']

@r.remote
def popCollections(icode,connexion,iFrame):
	pgx=pgcnx(uri)
	sqx=sqlCnx(connexion)
	tabKeys=rsq(insQuery,sqx)
	tabKeys['collection']=tabKeys['collection'].str.lower()
	tabKeys['pid']=pid
	tabKeys.to_sql('lmscollections',pgx,if_exists='replace',index=False,schema=eaeSchema)
	trk=pdf([],columns=['collection','primekeys','kount','starttime','endtime'])
	for idx,rowdata in iFrame.iterrows():
		rco=rowdata['collection']
		cachecollection=rowdata['pkitab']
		fldList=tabKeys.loc[(tabKeys['collection']==rco)]['cache_indexcolumn'].values.tolist()
		primeKeys=','.join(fldList)
		knt=0
		stime=dtm.utcnow()
		sql="SELECT '" +icode+ "' as instancecode," +primeKeys+ " FROM " +rco+ "(NOLOCK) "
		for chunk in rsq(sql,sqx,chunksize=csize):
			chunk.to_sql(cachecollection,pgx,if_exists='append',index=False,schema=eaeSchema)
			knt+=len(chunk)
		trk=trk.append({'collection':rco,'kount':knt,'primekeys':primeKeys,'starttime':stime,'endtime':dtm.utcnow()},sort=False,ignore_index=True)
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
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['instancetype']=='mssql'),['collection','pkitab']]
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
