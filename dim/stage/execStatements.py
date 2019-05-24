import sys
if len(sys.argv)<2:
	sys.exit('PID not provided... ')
else:
	pid=int(sys.argv[1])

from dimlib import dataSession,rsq,cfg,pgcnx,alchemyEXC,logError

uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
pgx=pgcnx(uri)
getStatements=''' SELECT statement FROM framework.statements WHERE isactive=true '''
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
