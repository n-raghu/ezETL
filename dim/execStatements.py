from dimlib import dataSession,rsq,cfg,pgcnx
import os

uri='postgresql://' +cfg['eaedb']['uid']+ ':' +cfg['eaedb']['pwd']+ '@' +cfg['eaedb']['host']+ ':' +str(int(cfg['eaedb']['port']))+ '/' +cfg['eaedb']['db']
pgx=pgcnx(uri)
getStatements=''' SELECT statement FROM framework.statements WHERE isactive=true '''
allStatements=rsq(getStatements,pgx)

if len(allStatements)>0:
    for idx,row in allStatements.iterrows():
        thisSession=dataSession(uri)
        thisSession.execute(row['statement'])
        thisSession.commit()
        thisSession.close()

pgx.dispose()
