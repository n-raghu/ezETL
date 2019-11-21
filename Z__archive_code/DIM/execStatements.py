import sys
if len(sys.argv)<2:
	sys.exit('PID not provided... ')
else:
	pid=int(sys.argv[1])

from dimlib import dataSession,rsq,dwCNX,pgcnx,alchemyEXC,logError

_,_,uri=dwCNX()
pgx=pgcnx(uri)
getStatements=''' SELECT statement FROM framework.statements WHERE isactive=true ORDER BY statement_id '''
allStatements=rsq(getStatements,pgx)

if len(allStatements)>0:
    for idx,row in allStatements.iterrows():
        thisSession=dataSession(uri)
        try:
            _=row['statement']
            thisSession.execute(_)
            thisSession.commit()
        except alchemyEXC.SQLAlchemyError as err:
            logError(pid,'Addon Scripts',err,uri)
        thisSession.close()

pgx.dispose()
