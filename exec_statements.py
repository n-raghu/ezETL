# Job is deprecated as Load DV is dynamically handling the statements

import sys
from datetime import datetime as dtm

from dimlib import pgconnector, refresh_config

if len(sys.argv) < 2:
    sys.exit('PID not provided... ')
else:
    pid = int(sys.argv[1])


try:
    cfg = refresh_config()
    cnx = pgconnector(cfg['dburi'])
except Exception as err:
    sys.exit(f'Statements|Unable to open connection|{err}')

sql_qry = '''SELECT statement FROM framework.statements
         WHERE isactive=true ORDER BY statement_id '''

try:
    with cnx.cursor() as dbcur:
        dbcur.execute(sql_qry)
        list_of_statements = dbcur.fetchall()
    all_statements = [_[0] for _ in list_of_statements]
except Exception as err:
    sys.exit(f'Statements|Unable to fetch statements|{err}')

db_ins_stmt = '''INSERT INTO framework.tracker_statements(
    pid,query_success,query_statement,start_time,finish_time
    )'''
for stmt in all_statements:
    start_time = dtm.utcnow()
    print(stmt)
    try:
        with cnx.cursor() as dbcur:
            dbcur.execute(stmt)
        cnx.commit()
        db_stmt = f"SELECT {pid},true,'{stmt}','{start_time}','{dtm.utcnow()}';"
        with cnx.cursor() as dbcur:
            dbcur.execute(db_ins_stmt + db_stmt)
        cnx.commit()
    except Exception as err:
        db_stmt = f"SELECT {pid},false,'{stmt}','{start_time}','{dtm.utcnow()}';"
        with cnx.cursor() as dbcur:
            dbcur.execute(db_ins_stmt + db_stmt)
        print(f'Statement[{stmt}] failed with error|{err}')

cnx.close()
