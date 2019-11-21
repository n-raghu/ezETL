import sys
from dimlib import *

debug = False

if all([len(sys.argv) < 2, not debug]):
    sys.exit('PID not provided. ')
elif debug:
    pid = -1
else:
    pid = int(sys.argv[1])


r.shutdown()
r.init(include_webui=False)
csize, eaeSchema, uri = dwCNX(tinyset=False)
objFrame = []
appVariables = {
    'app': 'lms',
    'instancetype': 'mssql',
    'module': 'stageLMSPKIFields'
}
tracker = pdf(
    [],
    columns=[
        'pid',
        'status',
        'instancecode',
        'collection',
        'primekeys',
        'kount',
        'starttime',
        'endtime',
    ]
)

mssql_dict = objects_sql(
    uri,
    appVariables['app'],
    appVariables['instancetype']
)
insList = mssql_dict['insList']
colFrame = mssql_dict['frame']


@r.remote
def createCollections(sql_table, stage_table, schema_name, uri):
    app_code = appVariables['app']
    ddql = f"SELECT framework.createcollection('{sql_table}','{stage_table}','{schema_name}','{app_code}')"
    nuSession = dataSession(uri)
    nuSession.execute(ddql)
    nuSession.commit()
    nuSession.close()
    return None


def drop_staging_pki_tables(frame_collections):
    pgx = pgcnx(uri)
    try:
        sql = ''
        for idx, rowdata in frame_collections.iterrows():
            sql += f"DROP TABLE IF EXISTS {eaeSchema}.{rowdata['pkitab']}; "
        event_session = dataSession(uri)
        event_session.execute(sql)
        event_session.commit()
        event_session.close()
        del frame_collections
    except Exception as err:
        logError(
            pid,
            'stageLMSPKIFields',
            f'DropPKITabs| {str(err)}',
            uri
        )
    return None


@r.remote
def pushChunk(p_id, prime_keys, pgURI, ins_code, tabSchema, pgTable, db_table, chk, model_traker):
    pgx = pgcnx(pgURI)
    chunkResponse = False
    try:
        chunk_traker = model_traker.append(
            {
                'pid': p_id,
                'status': False,
                'instancecode': ins_code,
                'collection': pgTable,
                'primekeys': prime_keys,
                'kount': len(chk),
                'starttime': dtm.utcnow(),
                'endtime': dtm.utcnow()
            },
            ignore_index=True
        )
        pgsql = f"COPY {tabSchema}.{pgTable} FROM STDIN WITH CSV DELIMITER AS '\t' "
        db_table = db_table.append(chk, sort=False, ignore_index=True)
        csv_dat = StringIO()
        db_table.replace(
            {
                '\t': '<TabSpacer>',
                '\n': '<NewLiner>',
                '\r': '<CarriageReturn>'
            },
            regex=True,
            inplace=True
        )
        db_table.to_csv(
            csv_dat,
            header=False,
            index=False,
            sep='\t'
        )
        csv_dat.seek(0)
        kon = pgconnect(pgURI)
        pgcursor = kon.cursor()
        pgcursor.copy_expert(
            sql=pgsql,
            file=csv_dat
        )
        kon.commit()
        chunk_traker['endtime'] = dtm.utcnow()
        pgcursor.close()
        kon.close()
        chunkResponse = True
    except Exception as err:
        logError(
            pid,
            'lms',
            f'PushChunkError| {pgTable}| {str(err)}',
            pgURI
        )
        errType = type(err).__name__
        if errType in [
            DatabaseError,
            psyDataError,
            OperationalError,
            IntegrityError,
            ProgrammingError,
            InternalError,
            DataError
        ]:
            try:
                nuCHK.to_sql(
                    pgTable,
                    pgx,
                    if_exists='append',
                    index=False,
                    schema=tabSchema
                )
            except (
                DataError,
                AssertionError,
                ValueError,
                IOError,
                IndexError,
                TypeError
            ) as unknownError:
                logError(
                    pid,
                    'lms',
                    f'ExceptionDataFrameError| {pgTable}| {str(unknownError)}',
                    pgURI
                )
    csv_dat = None
    del chk
    del db_table
    chunk_traker.to_sql(
        'cachetraces',
        pgx,
        if_exists='append',
        index=False,
        schema='framework'
    )
    pgx.dispose()
    return chunkResponse


@r.remote
def popCollections(icode, connexion, iFrame, model_trk, pid):
    pgx = pgcnx(uri)
    table_response = True
    try:
        sqx = sqlCnx(connexion)
    except podbc.Error as err:
        print(f'Error Connecting to {icode} instance.')
        logError(
            pid,
            appVariables['module'],
            f'POPCollectionError| {str(err)}',
            uri
        )
        return trk
    for idx, rowdata in iFrame.iterrows():
        rco = rowdata['collection']
        cachecollection = rowdata['pkitab']
        primeKeys = iFrame.loc[
            (iFrame['collection'] == rco)
        ]['pki_cols'].values.tolist()[0]
        knt = 0
        stime = dtm.utcnow()
        sql = f'SELECT * FROM {eaeSchema}.{cachecollection} LIMIT 0'
        print(sql)
        db_table_structure = rsq(sql, pgx)
        sql = f"SELECT '{icode}' as instancecode,{primeKeys} FROM {rco}(NOLOCK) "
        allFrames = []
        try:
            for chunk in rsq(sql, sqx, chunksize=csize):
                allFrames.append(
                    pushChunk.remote(
                        pid,
                        primeKeys,
                        uri,
                        icode,
                        eaeSchema,
                        cachecollection,
                        db_table_structure.copy(),
                        chunk.copy(deep=True),
                        model_trk.copy(),
                    )
                )
            r.wait(allFrames, timeout=3600.1)
            allFramesResponse = [r.get(_) for _ in allFrames]
            if False in allFramesResponse:
                table_response = False
            else:
                nuSession = dataSession(uri)
                nuSession.execute(f"UPDATE framework.cachetraces SET status=true WHERE pid={pid} AND collection='{cachecollection}' ")
                nuSession.commit()
                nuSession.close()
        except Exception as err:
            logError(
                pid,
                appVariables['module'],
                f'ChunkError| {rco}| {str(err)}',
                uri
            )
            table_response = False
    del chunk
    sqx.close()
    pgx.dispose()
    return table_response


print(f'Active Instances Found: {len(insList)}')
if all([not debug, len(insList) > 0]):
    drop_staging_pki_tables(colFrame.copy(deep=True))

# Create Staging Tables
    iStage_Tab_Created_List = []
    collectionZIP = list(zip(colFrame['collection'], colFrame['pkitab']))
    for iZIP in collectionZIP:
        iSQL_Tab, iStage_Tab = iZIP
        if iStage_Tab in iStage_Tab_Created_List:
            continue
        else:
            iStage_Tab_Created_List.append(iStage_Tab)
            objFrame.append(
                createCollections.remote(
                    iSQL_Tab,
                    iStage_Tab,
                    eaeSchema,
                    uri
                )
            )
    r.wait(objFrame)
    objFrame.clear()
    del collectionZIP
    del iStage_Tab_Created_List
    print('Staging Tables created... ')

    for ins in insList:
        cnxStr = ins['sqlConStr']
        instancecode = ins['icode']
        iFrame = colFrame.loc[
            (colFrame['icode'] == instancecode) & (colFrame['instancetype'] == appVariables['instancetype']),
            ['collection', 'pkitab', 'pki_cols']
        ]
        objFrame.append(
            popCollections.remote(
                instancecode,
                cnxStr,
                iFrame,
                tracker.copy(),
                pid
            )
        )
    r.wait(objFrame)
    list_response = [r.get(_) for _ in objFrame]
    r.shutdown()
    if False in list_response:
        sys.exit('One or more jobs Failed.')
elif debug:
    print('Ready to DEBUG... ')
else:
    print('No Active Instances Found.')
