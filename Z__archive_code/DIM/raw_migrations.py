import os
import sys

try:
    import yaml as y
    from collections import OrderedDict as odict
    from sqlalchemy import create_engine as pgcnx
    from sqlalchemy.sql import text as alchemyText
    from datetime import datetime as dtm
    from glob import iglob
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    raise ImportError(' Module(s) not installed...')

with open('dimConfig.yml') as ymlFile:
        cfg = y.safe_load(ymlFile)

migration_ddql='CREATE SCHEMA IF NOT EXISTS framework; CREATE TABLE IF NOT EXISTS framework.migrations(module TEXT,sql_type TEXT, sid INT GENERATED ALWAYS AS IDENTITY,migration_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP);'
uri='postgresql://' +cfg['eaedb']['user']+ ':' +cfg['eaedb']['passwd']+ '@' +cfg['eaedb']['server']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']

objects_DDL=str(cfg['migrations'])+'/*.SQL'
migrations_DDL=[odict([('filename',os.path.basename(x)),('fpath',x)]) for x in iglob(objects_DDL,recursive=True)]
objects_DDL=str(cfg['migrations'])+'/*.sql'
migrations_DDL+=[odict([('filename',os.path.basename(x)),('fpath',x)]) for x in iglob(objects_DDL,recursive=True)]

pgx=pgcnx(uri)
session=sessionmaker(bind=pgx)
pgxSession=session()
pgxSession.execute(migration_ddql)
pgxSession.commit()
current_migrations=[x['module'] for x in list(pgxSession.execute(''' SELECT module FROM framework.migrations '''))]

ddlSet=list(set([x['filename'] for x in migrations_DDL])-set(current_migrations))

if len(ddlSet)>0:
    ddlSet.sort()
    for iModule in ddlSet:
        sqlFile=[x['fpath'] for x in migrations_DDL if x['filename']==iModule][0]
        try:
            with open(sqlFile) as sqlWrapper:
                sqlModule=sqlWrapper.read()
                pgxSession.execute(sqlModule)
                pgxSession.commit()
                pgxSession.execute("INSERT INTO framework.migrations(module) SELECT '" +iModule+  "'")
                pgxSession.commit()
        except (SQLAlchemyError,IOError) as err:
            print('Failed...')
            print('Module : ' +iModule)
            sys.exit(str(err)+ '. Check migrations table for successfully applied modules')

pgxSession.close()
pgx.dispose()

print('New Modules applied: ' +str(len(ddlSet)))
if len(ddlSet)>0:
    print('------------------------')
    for i in ddlSet:
        print(i)
