from dimlib import *
from multiprocessing import JoinableQueue,Process

print(dtm.utcnow())
_,_,uri=dwCNX()
sql_object=objects_sql(uri,'mssql')
insList=sql_object['insList']
sqx=sqlCnx(insList[0]['sqlConStr'])
Q=JoinableQueue()
query='SELECT TOP 100000* FROM tbl_assign_course_student'

def buildQ(sqx,query):
    sqc=sqx.cursor()
    gen=sqc.execute(query)
    while True:
        x=gen.fetchmany(10000)
        if not x:
            break
        else:
            Q.put(x)
    return True

producer=Process(target=buildQ,args=sqx,query)
producer.start()
print(dtm.utcnow())
