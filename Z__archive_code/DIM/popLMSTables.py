debug=False

import sys
if all([len(sys.argv)<2, debug==False]):
	sys.exit('PID not provided... ')
elif debug:
	pid=-1
else:
	pid=int(sys.argv[1])

from dimlib import *

r.shutdown()
r.init(include_webui=False)
objFrame=[]
row_timestamp=dtm.utcnow()
csize,eaeSchema,uri=dwCNX(tinyset=True)
appVariables={'app':'lms','instancetype':'mssql','module':'popLMSTables'}
sql_dict=objects_sql(uri,appVariables['app'],appVariables['instancetype'])
insList=sql_dict['insList']
colFrame=sql_dict['frame']
tracker=pdf([],columns=['pid','status','instancecode','instancetype','collection','chunkstart','chunkfinish','rowversion'])
record_agent_pulse=True

def copyCollectionShape(icode,cnxStr,uri):
	pgx=pgcnx(uri)
	try:
		sqx=sqlCnx(cnxStr)
	except podbc.Error as err:
		logError(pid,appVariables['module'],'CopyTabShapeError| ' +str(err),uri)
		return False
	nuSession=dataSession(uri)
	nuSession.execute("DELETE FROM framework.tabshape WHERE app='" +appVariables['app']+ "' ")
	nuSession.commit()
	nuSession.close()
	dbQL="SELECT '" +icode+ "' as instancecode,COLUMN_NAME as column_str,DATA_TYPE as datatype,TABLE_NAME as collection FROM INFORMATION_SCHEMA.COLUMNS ORDER BY TABLE_NAME DESC"
	dbShape=rsq(dbQL,sqx)
	dbShape['collection']=dbShape['collection'].str.lower()
	dbShape['column_str']=dbShape['column_str'].str.lower()
	dbShape['app']=appVariables['app']
	dbShape.replace('timestamp','varchar',True)
	dbShape.to_sql('tabshape',pgx,if_exists='append',index=False,schema='framework')
	sqx.close()
	pgx.dispose()
	del dbShape
	return True

@r.remote
def recordRowVersions(trk):
	tracker=trk.copy(deep=True)
	issue=False
	if tracker.empty:
		issue=True
	else:
		pgx=pgcnx(uri)
		tracker['pid']=pid
		tracker.fillna(-1,inplace=True)
		ixx=tracker.groupby(['instancecode','collection'],sort=False)['rowversion'].transform(max)==tracker['rowversion']
		tracker[ixx].to_sql('collectiontracker',pgx,if_exists='append',index=False,schema='framework')
		del tracker
		pgx.dispose()
	return issue

@r.remote
def createCollections(sql_table,stage_table,schema_name,uri):
	pgx=pgcnx(uri)
	app_code=appVariables['app']
	ddql=" SELECT framework.createcollection('" +sql_table+ "','" +stage_table+ "','" +schema_name+ "','" +app_code+ "' )"
	nuSession=dataSession(uri)
	nuSession.execute(ddql)
	nuSession.commit()
	nuSession.close()
	pgx.dispose()
	return None

@r.remote
def pushChunk(pgTable,tabSchema,pgURI,nuchk,chk,model_traker,ins_code,ins_type,p_id):
	pgx=pgcnx(pgURI)
	try:
		chunk_traker=model_traker.append({'pid':p_id,'status':True,'collection':pgTable,'rowversion':chk['rower'].max(),'chunkfinish':dtm.utcnow(),'chunkstart':dtm.utcnow(),'instancecode':ins_code,'instancetype':ins_type},ignore_index=True)
		pgsql="COPY " +tabSchema+ "." +pgTable+ " FROM STDIN WITH CSV DELIMITER AS '\t' "
		chk['row_timestamp']=row_timestamp
		nuCHK=nuchk.append(chk,sort=False,ignore_index=True)
		csv_dat=StringIO()
		nuCHK.replace({'\t':'<TabSpacer>','\n':'<NewLiner>','\r':'<CarriageReturn>'},regex=True,inplace=True)
		nuCHK.to_csv(csv_dat,header=False,index=False,sep='\t')
		csv_dat.seek(0)
		kon=pgconnect(pgURI)
		pgcursor=kon.cursor()
		pgcursor.copy_expert(sql=pgsql,file=csv_dat)
		kon.commit()
		chunk_traker['chunkfinish']=dtm.utcnow()
		chunk_traker.to_sql('chunktraces',pgx,if_exists='append',index=False,schema='framework')
	except (DatabaseError,psyDataError,OperationalError,IntegrityError,ProgrammingError,InternalError,DataError,AssertionError,ValueError,IOError,IndexError,TypeError,Exception) as err:
		logError(pid,'popLMSTables','PushChunkError|' +str(pgTable)+ '|' +str(err),pgURI)
		errType=type(err).__name__
		if errType in [DatabaseError,psyDataError,OperationalError,IntegrityError,ProgrammingError,InternalError,DataError]:
			try:
				nuCHK.to_sql(pgTable,pgx,if_exists='append',index=False,schema=tabSchema)
			except (DataError,AssertionError,ValueError,IOError,IndexError,TypeError) as unknownError:
				logError(pid,'popLMSTables','ExceptionDataFrameError|' +str(pgTable)+ '|' +str(unknownError),pgURI)
				chunk_traker['status']=False
				chunk_traker.to_sql('chunktraces',pgx,if_exists='append',index=False,schema='framework')
		else:
			chunk_traker['status']=False
			chunk_traker.to_sql('chunktraces',pgx,if_exists='append',index=False,schema='framework')
	pgcursor.close()
	csv_dat=None
	del chk
	del nuCHK
	del nuchk
	del chunk_traker
	kon.close()
	pgx.dispose()
	return None

def popCollections(icode,connexion,iFrame,model_trk,pid):
	pgx=pgcnx(uri)
	ins_response=True
	try:
		sqx=sqlCnx(connexion)
	except podbc.Error as err:
		print('Error Connecting to ' +icode+ ' instance. See Error Logs for more details. ')
		logError(pid,appVariables['module'],'POPCollectionError| ' +str(err),uri)
		return False
	chunk=model_trk.copy()
	allFrames=[]
	for idx,rowdata in iFrame.iterrows():
		noChange=True
		table_response=True
		rco=rowdata['collection']
		recordpulse(record_agent_pulse, pid, str(rco), icode +' - Started', uri)
		if rco == 'tbl_user_master_deleted':
			s_table='tbl_user_master'
		else:
			s_table=rowdata['s_table']
		scols=rowdata['stg_cols']
		rower=str(int(rowdata['rower']))
		sql='SELECT * FROM ' +eaeSchema+ '.' +s_table+ ' LIMIT 0'
		nuChunk=rsq(sql,pgx)
		sql="SELECT '" +icode.lower()+ "' as instancecode," +scols+ ",CONVERT(BIGINT,sys_ROWVERSION) AS ROWER FROM " +rco+ "(NOLOCK) WHERE CONVERT(BIGINT,sys_ROWVERSION) > " +rower
		tbl_start_stamp=dtm.utcnow()
		allFrames.clear()
		try:
			for chunk in rsq(sql,sqx,chunksize=csize):
				allFrames.append(pushChunk.remote(s_table,eaeSchema,uri,nuChunk.copy(deep=True),chunk.copy(deep=True),model_trk.copy(),icode,appVariables['instancetype'],pid))
				noChange=False
			r.wait(allFrames,timeout=3600.1)
			tbl_finish_stamp=dtm.utcnow()
			if noChange:
				recordpulse(record_agent_pulse, pid, str(rco), icode+' - NoChange', uri)
				utnow=dtm.utcnow()
				table_tracker=pdf({'pid':[pid],'collection':[rco],'rowversion':[rower],'status':[True],'timestarted':[tbl_start_stamp],'timefinished':[utnow],'instancecode':[icode],'instancetype':[appVariables['instancetype']]})
			else:
				table_tracker=rsq("SELECT status,instancecode,instancetype,rowversion FROM framework.chunktraces WHERE pid=" +str(pid)+ " AND collection='" +s_table+ "' AND status=true ",pgx)
				table_tracker['timefinished']=dtm.utcnow()
				table_tracker['timestarted']=tbl_start_stamp
				table_tracker['collection']=rco
			allFrames.append(recordRowVersions.remote(table_tracker))
			recordpulse(record_agent_pulse, pid, str(rco), icode+' - Success', uri)
		except Exception as err:
			logError(pid,appVariables['module'],'ChunkError|' +str(rco)+ '|' +str(err),uri)
			recordpulse(record_agent_pulse, pid, str(rco), icode +' - Fail', uri)
		gc_purge()
	del chunk
	r.wait(allFrames)
	allFrames.clear()
	sqx.close()
	pgx.dispose()
	return ins_response

print('Active Instances Found: ' +str(len(insList)))
recordpulse(record_agent_pulse, pid, 'LMSInstances', str(len(insList)), uri)
if all([debug==False,len(insList)>0]):
	for oneINS in insList:
		copyStatus=copyCollectionShape(oneINS['icode'],oneINS['sqlConStr'],uri)
		if copyStatus:
			break
	recordpulse(record_agent_pulse, pid, 'TabShape', str(copyStatus), uri)
	if not copyStatus:
		sys.exit('Unable to get metadata...')
	print('Table Shape copied... ')
	oneFrame=colFrame.loc[(colFrame['icode']==oneINS['icode']) & (colFrame['instancetype']=='mssql'),['collection','s_table']]
	collectionZIP=list(zip(oneFrame['collection'],oneFrame['s_table']))

# Create Staging Tables
	iStage_Tab_Created_List = []
	for iZIP in collectionZIP:
		iSQL_Tab,iStage_Tab=iZIP
		if iStage_Tab in iStage_Tab_Created_List:
			continue
		else:
			iStage_Tab_Created_List.append(iStage_Tab)
			objFrame.append(createCollections.remote(iSQL_Tab,iStage_Tab,eaeSchema,uri))
	r.wait(objFrame)
	objFrame.clear()
	del iStage_Tab_Created_List
	print('Staging Tables created... ')

	for ins in insList:
		cnxStr=ins['sqlConStr']
		instancecode=ins['icode']
		recordpulse(record_agent_pulse, pid, instancecode, str(cnxStr), uri)
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['instancetype']=='mssql'),['collection','s_table','rower','stg_cols']]
		objFrame.append(popCollections(instancecode,cnxStr,iFrame,tracker,pid))
	r.shutdown()
	del objFrame
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
