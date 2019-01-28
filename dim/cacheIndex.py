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
csize=cfg['cachesize']
objFrame=[]
tracker=pdf([],columns=['instancecode','primekey'])

insQuery=''' SELECT tab.TABLE_NAME as collection,col.COLUMN_NAME as cache_indexcolumn
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tab
	JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE col ON tab.CONSTRAINT_NAME=col.CONSTRAINT_NAME AND tab.TABLE_NAME=col.TABLE_NAME
WHERE Constraint_Type = 'PRIMARY KEY' '''

uri=uri
eaeSchema=eaeSchema
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
	tabKeys.to_sql('lmscollections',pgx,if_exists='append',index=False,schema=eaeSchema)
	trk=pdf([],columns=['instancecode','primekey'])
	for idx,rowdata in iFrame.iterrows():
		rco=rowdata['collection']
		cachecollection='cache_'+rowdata['s_table']
		fldList=tabKeys.loc[(tabKeys['collection']==rco)]['cache_indexcolumn'].values.tolist()
		primeKeys=','.join(fldList)
		sql="SELECT '" +icode+ "' as instancecode," +primeKeys+ " FROM " +rco+ "(NOLOCK) "
		chk=rsq(sql,sqx)
		for chunk in rsq(sql,sqx,chunksize=csize):
			chunk.to_sql(cachecollection,pgx,if_exists='append',index=False,schema=eaeSchema)
			trk.append(chunk,sort=False,ignore_index=True)
	del chunk
	sqx.close()
	return chk

print('Active Instances Found: ' +str(len(insList)))
if all([debug==False,len(insList)>0]):
	for ins in insList:
		cnxStr=ins['sqlConStr']
		instancecode=ins['icode']
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['instancetype']=='mssql'),['collection','s_table']]
		objFrame.append(popCollections.remote(instancecode,cnxStr,iFrame))
	r.wait(objFrame)
	for obj in objFrame:
		tracker=tracker.append(r.get(obj),sort=False,ignore_index=True)
	del objFrame
	tracker['hash']=list(map(lambda x: hah.sha1(x.encode()).hexdigest(), tracker['instancecode']))
	r.shutdown()
	print(tracker)
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
