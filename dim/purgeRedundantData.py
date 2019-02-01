import sys
if len(sys.argv)<2:
	sys.exit('PID not provided... ')
else:
	pid=int(sys.argv[1])

from dimlib import *
csize,eaeSchema,uri=dwCNX()

r.init()
@r.remote
def cleanseStagingTables(tab_name,schema_name,uri):
    pgx=pgcnx(uri)
    query="SELECT framework.purgeduplicaterows('" +tab_name+ "','" +schema_name+ "')"
    sessionClass=sessionmaker(bind=pgx)
    nuSession=sessionClass()
    nuSession.execute(query)
    nuSession.commit()
    nuSession.close()
    pgx.dispose()
    return None

pgx=pgcnx(uri)
pgTables=[k['stage_table'] for k in list(pgx.execute(''' SELECT stage_table FROM framework.collectionmaps WHERE isactive=true AND instancetype='mssql' '''))]
pgx.dispose()
objFrame=[]

for iTab in pgTables:
    objFrame.append(cleanseStagingTables.remote(iTab,eaeSchema,uri))

r.wait(objFrame)
