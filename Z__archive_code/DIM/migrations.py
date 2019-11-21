import os
import sys
from io import BytesIO, TextIOWrapper

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

try:
    with open('dimConfig.yml') as ymlFile:
        cfg = y.safe_load(ymlFile)
        migration_path_objects = str(cfg['migrations'])+'/*.SQL'
        uri = f"postgresql://{cfg['eaedb']['user']}:{cfg['eaedb']['passwd']}@{cfg['eaedb']['server']}:{str(int(cfg['eaedb']['port']))}/{cfg['eaedb']['db']}"
except Exception as err:
    sys.exit('Config Error')

record_obj = True
BEGIN_OBJ = '--- BEGIN OBJECTIVE'
END_OBJ = '--- END OBJECTIVE'

migration_ddql = 'CREATE SCHEMA IF NOT EXISTS framework;'
migration_ddql += '''CREATE TABLE IF NOT EXISTS framework.migrations(
    module TEXT,
    sql_type TEXT,
    sid INT GENERATED ALWAYS AS IDENTITY,
    migration_time TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP); '''

all_files = [
    odict(
        [
            ('filename', os.path.basename(x)),
            ('fpath', x)
        ]
    )
    for x in iglob(migration_path_objects, recursive=True)
]
migration_path_objects = str(cfg['migrations'])+'/*.sql'
all_files += [
    odict(
        [
            ('filename', os.path.basename(x)),
            ('fpath', x)
        ]
    )
    for x in iglob(migration_path_objects, recursive=True)
]

pgx = pgcnx(uri)
session = sessionmaker(bind=pgx)
pgx_ssn = session()
pgx_ssn.execute(migration_ddql)
pgx_ssn.commit()
past_successful_migrations = [
    x['module'] for x in list(
        pgx_ssn.execute(''' SELECT module FROM framework.migrations ''')
    )
]
pgx_ssn.close()

ddlSet = set([x['filename'] for x in all_files])-set(past_successful_migrations)
ddlSet = list(ddlSet)

if len(ddlSet) > 0:
    ddlSet.sort()
    for _mod in ddlSet:
        obj_text = ''
        obj_check = False
        obj_found = False
        sql_file = [x['fpath'] for x in all_files if x['filename'] == _mod][0]
        sql_code = TextIOWrapper(
            BytesIO(),
            line_buffering=True,
            encoding='utf-8'
        )
        try:
            with open(sql_file) as sql_file_obj:
                for _line_ in sql_file_obj:
                    if _line_.strip().upper() == BEGIN_OBJ:
                        obj_check = True
                        obj_found = True
                        continue
                    if _line_.strip().upper() == END_OBJ:
                        obj_check = False
                        continue
                    if obj_check:
                        obj_text += f'{_line_.strip()} |||'
                    else:
                        sql_code.write(_line_)

                sql_code.seek(0)
                if not obj_found:
                    file_number = int(
                        [
                            _ for _ in _mod.split('_') if _.isdigit()
                        ][0]
                    )
                    if file_number <= 79:
                        print(f'Warning: Missing Objective for {_mod}')
                    else:
                        sys.exit(f'{_mod}: OBJECTIVE NOT FOUND.')
                session = sessionmaker(bind=pgx)
                pgx_ssn = session()
                pgx_ssn.execute(sql_code.read())
                pgx_ssn.commit()
                record_mig = f"INSERT INTO framework.migrations(module) SELECT '{_mod}'"
                if record_obj and obj_found:
                    record_mig = f"INSERT INTO framework.migrations(module,objective) SELECT '{_mod}','{obj_text}'"
                pgx_ssn.execute(record_mig)
                pgx_ssn.commit()
                pgx_ssn.close()

        except Exception as err:
            print('Failed...')
            print(f'Module : {_mod}')
            sys.exit(f'{str(err)}. Check migrations table for successfully applied modules')

pgx.dispose()
print(f'New Modules applied: {len(ddlSet)}')
if len(ddlSet) > 0:
    print('------------------------')
    for i in ddlSet:
        print(i)
