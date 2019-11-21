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
record_agent_pulse=True

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
		logError(pid,appVariables['module'],'CopyTabShape Error ' +str(err),uri)
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
	ddql1='DROP TABLE IF EXISTS ' +schema_name+ '.' +stage_table+ '_work'
	nuSession.execute(ddql1)
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
			chunk['instancecode']=icode
			pushChunk(icode,s_table,eaeSchema,uri,nuChunk.copy(deep=True),chunk.copy(deep=True))
			recordpulse(record_agent_pulse, pid, str(rco), icode +' - Worker Done', uri)
			col_list=[]
			if len(chunk)>0:
				noChange=False
				trk=trk.append({'status':False,'collection':rco,'rowversion':chunk['rower'].max(),'chunkfinish':dtm.utcnow(),'chunkstart':cstart},ignore_index=True)
				recordpulse(record_agent_pulse, pid, str(rco), icode +' - ChunkCreated', uri)
			t_kon=pgconnect(uri)
			alterQL="SELECT COLUMN_NAME AS col_name FROM information_schema.columns WHERE table_name='" +s_table+ "' AND table_schema='" +eaeSchema+ "' AND data_type='double precision' "
			alterFrame=rsq(alterQL,t_kon)
			t_kon.close()
			col_list=list(alterFrame['col_name'])
			if len(col_list)>0:
				alterSQL='ALTER TABLE ' +eaeSchema +'.'+ s_table
				for _col_ in col_list:
					alterSQL+=' ALTER COLUMN ' +_col_+ ' TYPE INT,'
				if alterSQL.endswith(','):
					alterSQL=alterSQL[:-1]
				nuSession=dataSession(uri)
				nuSession.execute(alterSQL)
				nuSession.commit()
				nuSession.close()
				recordpulse(record_agent_pulse, pid, str(rco), icode +' - Conversion Done', uri)
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
			trk.loc[(trk['collection']==rco),['status']]=True
		except (DataError,AssertionError,ValueError,IOError,IndexError,TypeError,Exception) as err:
			logError(pid,appVariables['module'],'Chunk Error: ' +str(rco)+ ' ' +str(err),uri)
			trk.loc[(trk['collection']==rco),['status']]=False
		trk.loc[(trk['collection']==rco),['timefinished']]=dtm.utcnow()
		trk.loc[(trk['collection']==rco),['timestarted']]=astart
		if noChange:
			recordpulse(record_agent_pulse, pid, str(rco), icode +' - No Change', uri)
			utnow=dtm.utcnow()
			trk=trk.append({'collection':rco,'rowversion':rower,'chunkfinish':utnow,'chunkstart':utnow,'status':True,'timestarted':astart,'timefinished':utnow},ignore_index=True)
		recordpulse(record_agent_pulse, pid, str(rco), icode +' - Success', uri)
	del chunk
	sqx.close()
	pgx.dispose()
	trk['instancecode']=icode
	return trk

recordpulse(record_agent_pulse, pid, 'PhisProofInstances', str(len(insList)), uri)
if all([debug==False,len(insList)>0]):
	tmpList=[]
	for oneINS in insList:
		copyStatus=copyCollectionShape(oneINS,uri)
		if copyStatus:
			break
	recordpulse(record_agent_pulse, pid, 'TabShape', str(copyStatus), uri)
	if not copyStatus:
		sys.exit('Unable to connect to Phish Instances...')
	oneFrame=colFrame.loc[(colFrame['icode']==oneINS['icode']) & (colFrame['app']==appVariables['app']),['collection','s_table']]
	collectionZIP=list(zip(oneFrame['collection'],oneFrame['s_table']))

# Create Staging Tables
	iStage_Tab_Created_List = []
	for iZIP in collectionZIP:
		iSQL_Tab,iStage_Tab=iZIP
		if iStage_Tab in iStage_Tab_Created_List:
			continue
		else:
			try:
				iStage_Tab_Created_List.append(iStage_Tab)
				objFrame.append(createCollections.remote(appVariables['app'],iSQL_Tab,iStage_Tab,eaeSchema,uri))
			except Exception as err:
				logError(pid,appVariables['module'],'createCollections-1: ' +str(error),uri)
	r.wait(objFrame)
	objFrame.clear()
	del iStage_Tab_Created_List
	print('Staging Tables created... ')

	_ccc_ = 1
	for ins in insList:
		_ccc_ += 1
		instancecode=ins['icode']
		recordpulse(record_agent_pulse, pid, instancecode, 'Connecting...', uri)
		iFrame=colFrame.loc[(colFrame['icode']==instancecode) & (colFrame['app']==appVariables['app']),['collection','s_table','rower','stg_cols']]
		objFrame.append(popCollections(instancecode,ins,iFrame))
		for iZIP in collectionZIP:
			iSQL_Tab,iStage_Tab=iZIP
			try:
				tmpList.append(createCollections(appVariables['app'],iSQL_Tab,iStage_Tab,eaeSchema,uri))
			except Exception as error:
				logError(pid,appVariables['module'],'createCollections-' +_ccc_+ ': ' +str(error),uri)

	for obj in objFrame:
		tracker=tracker.append(obj,sort=False,ignore_index=True)
	del objFrame
	print(tracker)
	recordRowVersions()
	del tmpList
elif debug:
	print('Ready to DEBUG... ')
else:
	print('No Active Instances Found.')
