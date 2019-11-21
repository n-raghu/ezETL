debug=False

import sys
if all([len(sys.argv)<2, debug==False]):
	sys.exit('PID not provided... ')
elif debug:
	pid=-1
else:
	pid=int(sys.argv[1])

from dimlib import *

appVariables={'app':'potter','instancetype':'mysql','module':'popPotterTables'}
csize,eaeSchema,uri=dwCNX(tinyset=True)
row_timestamp=dtm.utcnow()
tracker=pdf([],columns=['status','instancecode','collection','timestarted','timefinished','chunkstart','chunkfinish','rowversion'])
objFrame=[]
record_agent_pulse=True
sql_dict=objects_sql(uri,appVariables['app'],appVariables['instancetype'])
insList=sql_dict['insList']
colFrame=sql_dict['frame']

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

def copyCollectionShape(one_ins,uri):
	try:
		sqx=mysqlCNX(user=one_ins['user'],password=one_ins['password'],port=int(one_ins['port']),database=one_ins['database'],host=one_ins['host'])
	except (mysqlOpErr,mysqlErr) as err:
		print('Error Connecting to ' +one_ins['host']+ ' instance. See Error Logs for more details. ')
		logError(pid,appVariables['module'],'CopyTabShapeError|' +str(err),uri)
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
	return True

def createCollections(app_appVariables,sql_table,stage_table,schema_name,uri):
	nuSession=dataSession(uri)
	nuSession.execute('DROP TABLE IF EXISTS ' +schema_name+ '.' +stage_table+ '_work')
	nuSession.commit()
	nuSession.close()
	ddql=" SELECT framework.createcollection('" +sql_table+ "','" +stage_table+ "_work','" +schema_name+ "','" +app_appVariables+ "')"
	nuSession=dataSession(uri)
	nuSession.execute(ddql)
	nuSession.commit()
	nuSession.close()
	return ddql

def pushChunk(instancecode,pgTable,tabSchema,pgURI,nuchk,chk):
	pgsql="COPY " +tabSchema+ "." +pgTable+ " FROM STDIN WITH CSV DELIMITER AS '\t' "
	chk['row_timestamp']=row_timestamp
	nuCHK=nuchk.append(chk,sort=False,ignore_index=True)
	try:
		csv_dat=StringIO()
		nuCHK.replace({'\t':'<TabSpacer>','\n':'<NewLiner>','\r':'<CarriageReturn>'},regex=True,inplace=True)
		nuCHK.to_csv(csv_dat,header=False,index=False,sep='\t')
		csv_dat.seek(0)
		kon=pgconnect(pgURI)
		pgcursor=kon.cursor()
		pgcursor.copy_expert(sql=pgsql,file=csv_dat)
		kon.commit()
		pgcursor.close()
		csv_dat=None
		kon.close()
	except (DatabaseError,psyDataError,OperationalError,IntegrityError,ProgrammingError,InternalError,DataError,AssertionError,ValueError,IOError,IndexError,TypeError,Exception) as err:
		logError(pid,'Potter','PushChunkError|' +str(pgTable)+ '|' +str(err),pgURI)
		errType=type(err).__name__
		if errType in [DatabaseError,psyDataError,OperationalError,IntegrityError,ProgrammingError,InternalError,DataError]:
			try:
				nuCHK.to_sql(pgTable,pgx,if_exists='append',index=False,schema=tabSchema)
			except (DataError,AssertionError,ValueError,IOError,IndexError,TypeError) as unknownError:
				logError(pid,'Potter','ExceptionDataFrameError|' +str(pgTable)+ '|' +str(unknownError),pgURI)
	del nuCHK
	del chk
	del nuchk
	return True

def popCollections(icode,one_ins,iFrame):
	pgx=pgcnx(uri)
	trk=pdf([],columns=['collection','chunkstart','chunkfinish','rowversion','status','timestarted','timefinished'])
	try:
		sqx=mysqlCNX(user=one_ins['user'],password=one_ins['password'],port=int(one_ins['port']),database=one_ins['database'],host=one_ins['host'],use_pure=False)
	except (mysqlErr,mysqlOpErr) as err:
		print('Error Connecting to ' +icode+ ' instance. See Error Logs for more details. ')
		logError(pid,appVariables['module'],'POPCollectionError|' +str(err),uri)
		return trk
	chunk=pdf([],columns=['model'])
	for idx,rowdata in iFrame.iterrows():
		astart=dtm.utcnow()
		noChange=True
		rco=rowdata['collection']
		recordpulse(record_agent_pulse, pid, str(rco), icode +' - Start', uri)
		s_table_org=rowdata['s_table']
		s_table=s_table_org+'_work'
		scols=rowdata['stg_cols']
		rower=str(int(rowdata['rower']))
		sql='SELECT * FROM ' +eaeSchema+ '.' +s_table+ ' LIMIT 0'
		nuChunk=rsq(sql,pgx)
		sql="SELECT " +scols+ ",UNIX_TIMESTAMP(sys_ROWVERSION) AS rower FROM " +rco+ " WHERE UNIX_TIMESTAMP(sys_ROWVERSION)>" +rower
		try:
			chunk=rsq(sql,sqx)
			cstart=dtm.utcnow()
			chunk.columns=map(str.lower,chunk.columns)
			pushChunk(icode,s_table,eaeSchema,uri,nuChunk.copy(deep=True),chunk.copy(deep=True))
			recordpulse(record_agent_pulse, pid, str(rco), icode +' - Worker', uri)
			col_list=[]
			if len(chunk)>0:
				noChange=False
				trk=trk.append({'status':False,'collection':rco,'rowversion':chunk['rower'].max(),'chunkfinish':dtm.utcnow(),'chunkstart':cstart},ignore_index=True)
				recordpulse(record_agent_pulse, pid, str(rco), icode +' - ChunkCreated', uri)
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
				recordpulse(record_agent_pulse, pid, str(rco), icode +' - Conversion Set', uri)
				nuSession=dataSession(uri)
				nuSession.execute('CREATE TABLE IF NOT EXISTS ' +eaeSchema+ '.' +s_table_org+ ' AS SELECT * FROM ' +eaeSchema+ '.' +s_table+ ' WHERE 1=0')
				nuSession.commit()
				nuSession.close()
				nuSession=dataSession(uri)
				ddql="select string_agg(column_name,',') from information_schema.columns where table_schema='" +eaeSchema+ "' and table_name='" +s_table_org+ "'";
				tbl_column_list=list(nuSession.execute(ddql))
				nuSession.close()
				tbl_column_list=tbl_column_list[0][0]
				recordpulse(record_agent_pulse, pid, str(s_table_org), str(tbl_column_list), uri)
				nuSession=dataSession(uri)
				nuSession.execute('INSERT INTO ' +eaeSchema+ '.' +s_table_org+ '(' +tbl_column_list+ ') SELECT ' +tbl_column_list+ ' FROM ' +eaeSchema+ '.' +s_table)
				nuSession.commit()
				nuSession.close()
				recordpulse(record_agent_pulse, pid, str(rco), icode +' - Dump Working Table', uri)
			nuSession=dataSession(uri)
			nuSession.execute('DROP TABLE IF EXISTS ' +eaeSchema+ '.' +s_table)
			nuSession.commit()
			nuSession.close()
			recordpulse(record_agent_pulse, pid, str(rco), icode +' - Success', uri)
			trk.loc[(trk['collection']==rco),['status']]=True
		except (DataError,AssertionError,ValueError,IOError,IndexError) as err:
			logError(pid,appVariables['module'],'ChunkError|' +str(rco)+ ' ' +str(err),uri)
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

recordpulse(record_agent_pulse, pid, 'Potter', str(len(insList)), uri)
if all([debug==False,len(insList)>0]):
	oneINS=insList[0]
	copyStatus=copyCollectionShape(oneINS,uri)
	if not copyStatus:
		logError(pid,appVariables['module'],'Copy Status Flag False' ,uri)
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