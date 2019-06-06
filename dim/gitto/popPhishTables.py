debug=False

import sys
if all([len(sys.argv)<2, debug==False]):
	sys.exit('PID not provided... ')
elif debug:
	pid=-1
else:
	pid=int(sys.argv[1])

from dimlib import *

appVariables={'app':'phishproof','instancetype':'mysql','module':'popPhishTables'}
csize,eaeSchema,uri=dwCNX(tinyset=True)
row_timestamp=dtm.utcnow()
tracker=pdf([],columns=['status','instancecode','collection','timestarted','timefinished','chunkstart','chunkfinish','rowversion'])
objFrame=[]

def recordRowVersions():
	global tracker
	if tracker.empty:
		print('Tracker is Empty ')
		issue=True
	else:
		pgx=pgcnx(uri)
		tracker['pid']=pid
		tracker['instancetype']=appVariables['instancetype']
		tracker.fillna(-1,inplace=True)
		tracker[['status','instancecode','collection','chunkstart','chunkfinish','rowversion','pid']].to_sql('chunktraces',pgx,if_exists='append',index=False,schema='framework')
		tracker.drop(['chunkstart','chunkfinish'],axis=1,inplace=True)
		ixx=tracker.groupby(['instancecode','collection'],sort=False)['rowversion'].transform(max)==tracker['rowversion']
		tracker[ixx].to_sql('collectiontracker',pgx,if_exists='append',index=False,schema='framework')
		del tracker
		pgx.dispose()
		issue=False
	return issue

sql_dict=objects_sql(uri,appVariables['app'],appVariables['instancetype'])
insList=sql_dict['insList']
colFrame=sql_dict['frame']

def copyCollectionShape(one_ins,uri):
	try:
		sqx=mysqlCNX(user=one_ins['user'],passwd=one_ins['password'],port=int(one_ins['port']),db=one_ins['database'],host=one_ins['host'])
	except (mysqlOpErr,mysqlErr) as err:
		print('Error Connecting to ' +one_ins['host']+ ' instance. See Error Logs for more details. ')
		logError(pid,appVariables['module'],str(err),uri)
		return False

	nuSession=dataSession(uri)
	nuSession.execute("DELETE FROM framework.tabshape WHERE app='" +appVariables['app']+ "' ")
	nuSession.commit()
	nuSession.close()
	dbQL="SELECT COLUMN_NAME as column_str,DATA_TYPE as datatype,TABLE_NAME as collection FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema='" +one_ins['database']+ "'"
	dbShape=rsq(dbQL,sqx)
	dbShape['collection']=dbShape['collection'].str.lower()
	dbShape['column_str']=dbShape['column_str'].str.lower()
	dbShape['instancecode']=one_ins['icode']
	dbShape['app']=appVariables['app']
	dbShape.to_sql('tabshape',pgcnx(uri),if_exists='append',index=False,schema='framework')
	sqx.close()
	del dbShape
	return None

def createCollections(app_appVariables,sql_table,stage_table,schema_name,uri):
	ddql=" SELECT framework.createcollection('" +sql_table+ "','" +stage_table+ "','" +schema_name+ "','" +app_appVariables+ "')"
	nuSession=dataSession(uri)
	nuSession.execute(ddql)
	nuSession.commit()
	nuSession.close()
	return ddql

def pushChunk(instancecode,pgTable,tabSchema,pgURI,nuchk,chk):
	pgsql="COPY " +tabSchema+ "." +pgTable+ " FROM STDIN WITH CSV DELIMITER AS '\t' "
	chk['row_timestamp']=row_timestamp
	nuCHK=nuchk.append(chk,sort=False,ignore_index=True)
	csv_dat=StringIO()
	nuCHK.to_csv(csv_dat,header=False,index=False,sep='\t')
	csv_dat.seek(0)
	kon=pgconnect(pgURI)
	pgcursor=kon.cursor()
	pgcursor.copy_expert(sql=pgsql,file=csv_dat)
	kon.commit()
	pgcursor.close()
	csv_dat=None
	del chk
	del nuCHK
	del nuchk
	kon.close()
	return True

def popCollections(icode,one_ins,iFrame):
	pgx=pgcnx(uri)
	trk=pdf([],columns=['collection','chunkstart','chunkfinish','rowversion','status','timestarted','timefinished'])
	try:
		sqx=mysqlCNX(user=one_ins['user'],passwd=one_ins['password'],port=int(one_ins['port']),db=one_ins['database'],host=one_ins['host'],use_pure=False)
	except (mysqlErr,mysqlOpErr) as err:
		print('Error Connecting to ' +icode+ ' instance. See Error Logs for more details. ')
		logError(pid,appVariables['module'],str(err),uri)
		return trk
	chunk=pdf([],columns=['model'])
	for idx,rowdata in iFrame.iterrows():
		astart=dtm.utcnow()
		noChange=True
		rco=rowdata['collection']
		s_table=rowdata['s_table']
		scols=rowdata['stg_cols']
		rower=str(int(rowdata['rower']))
		sql='SELECT * FROM ' +eaeSchema+ '.' +rco+ ' LIMIT 0'
		nuChunk=rsq(sql,pgx)
		sql="SELECT *,UNIX_TIMESTAMP(sys_ROWVERSION) AS rower FROM " +rco+ " WHERE UNIX_TIMESTAMP(sys_ROWVERSION)>" +rower
		try:
			chunk=rsq(sql,sqx)
			cstart=dtm.utcnow()
			chunk.columns=map(str.lower,chunk.columns)
			pushChunk(icode,s_table,eaeSchema,uri,nuChunk.copy(deep=True),chunk.copy(deep=True))
			trk=trk.append({'status':False,'collection':rco,'rowversion':chunk['rower'].max(),'chunkfinish':dtm.utcnow(),'chunkstart':cstart},ignore_index=True)
			col_list=[]
			if len(chunk)>0:
				noChange=False
				t_kon=pgconnect(uri)
				alterQL="SELECT COLUMN_NAME AS col_name FROM information_schema.columns WHERE table_name='" +s_table+ "' AND table_schema='" +eaeSchema+ "' AND data_type='numeric' "
				alterFrame=rsq(alterQL,t_kon)
				t_kon.close()
				col_list=list(alterFrame['col_name'])
			if len(col_list)>0:
				alterSQL='ALTER TABLE ' +eaeSchema +'.'+ s_table
				for _col_ in col_list:
					alterSQL+=' ALTER COLUMN ' +_col_+ ' TYPE BOOL USING CASE WHEN ' +_col_+ '=0 THEN false WHEN ' +_col_+ '>0 THEN true ELSE null END,'
				if alterSQL.endswith(','):
					alterSQL=alterSQL[:-1]
				nuSession=dataSession(uri)
				nuSession.execute(alterSQL)
				nuSession.commit()
				nuSession.close()
			trk.loc[(trk['collection']==rco),['status']]=True
		except (DataError,AssertionError,ValueError,IOError,IndexError) as err:
			logError(pid,appVariables['module'],'Chunk Error: ' +str(rco)+ ' ' +str(err),uri)
			trk.loc[(trk['collection']==rco),['status']]=False
		trk.loc[(trk['collection']==rco),['timefinished']]=dtm.utcnow()
		trk.loc[(trk['collection']==rco),['timestarted']]=astart
		if noChange:
			utnow=dtm.utcnow()
			trk=trk.append({'collection':rco,'rowversion':rower,'chunkfinish':utnow,'chunkstart':utnow,'status':True,'timestarted':astart,'timefinished':utnow},ignore_index=True)
	del chunk
	sqx.close()
	pgx.dispose()
	trk['instancecode']=icode
	return trk

print('Active Instances Found: ' +str(len(insList)))
if all([debug==False,len(insList)>0]):
	oneINS=insList[0]
	copyCollectionShape(oneINS,uri)
	print('Table Shape copied... ')
	oneFrame=colFrame.loc[(colFrame['icode']==oneINS['icode']) & (colFrame['app']==appVariables['app']),['collection','s_table']]
	collectionZIP=list(zip(oneFrame['collection'],oneFrame['s_table']))
	tmpList=[]
	for iZIP in collectionZIP:
		iSQL_Tab,iStage_Tab=iZIP
		tmpList.append(createCollections(appVariables['app'],iSQL_Tab,iStage_Tab,eaeSchema,uri))
	print('Staging Tables created... ')
	for ins in insList:
		instancecode=ins['icode']
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['app']==appVariables['app']),['collection','s_table','rower','stg_cols']]
		objFrame.append(popCollections(instancecode,ins,iFrame))
	for obj in objFrame:
		tracker=tracker.append(obj,sort=False,ignore_index=True)
	del objFrame
	print(tracker)
	recordRowVersions()
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
