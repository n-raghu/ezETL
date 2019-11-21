import sys
import smtplib
import subprocess as sbp
from os.path import basename
from time import sleep as ziz
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from email.mime.application import MIMEApplication
from dimlib import dwCNX, pdf, y, pgcnx, dtm, sessionmaker, recordpulse

print('Modules Imported...')
intLowLatencyStartTime=5
intHighLatencyStartTime=23

import sys
if len(sys.argv)>1:
    if sys.argv[1]=='testcycle':
        test_cycle=True
else:
    test_cycle=False

def getConfig():
    with open('dimConfig.yml') as yFile:
        cfg=y.safe_load(yFile)
    return cfg['log_directory'],cfg['agent'],cfg['smtp']

def mailStakeHolders(file_name_path, smtp_server, urx):
    pgx=pgcnx(urx)
    msg=MIMEMultipart()
    msg['From']='EAE Data Integration <eae@inspireme.com>'
    msg['To']='raghu.neerukonda@inspiredelearning.com'
    msg['Date']=formatdate(localtime=True)
    msg['Subject']='Summary of Data Integration to EAE Database.'
    msg.attach(MIMEText('Check Attachment.... Thank you'))
    with open(file_name_path,"rb") as fil:
        mimeFile=MIMEApplication(fil.read(),Name=basename(file_name_path))
    mimeFile['Content-Disposition'] = 'attachment; filename="%s"' % basename(file_name_path)
    msg.attach(mimeFile)
    s_smtp=smtplib.SMTP(smtp_server)
    s_smtp.starttls()
    s_smtp.sendmail(msg['From'],[msg['To']],msg.as_string())
    s_smtp.close()
    pgx.dispose()
    return None

def upsertPID(pid, uri, jobList):
    pgx=pgcnx(uri)
    pidFrame=pdf(jobList,columns=['jobid'])
    pidFrame['pid']=pid
    pidFrame.to_sql('tracker',pgx,if_exists='append',index=False,schema='framework')
    pidFrame['starttime']=dtm.utcnow()
    pidFrame['endtime']=dtm.utcnow()
    pidFrame['notes']='JOB in Queue'
    pidFrame.to_sql('volatiletracker',pgx,if_exists='replace',index=False,schema='framework')
    pgx.dispose()
    return pidFrame

if test_cycle:
    print('Test cycle invoked.')

_,_,uri=dwCNX()
session=sessionmaker(bind=pgcnx(uri))
nuSession=session()
nuSession.execute(''' CREATE TABLE IF NOT EXISTS framework.agentpulse(pid BIGINT, tbl_id INT GENERATED ALWAYS AS IDENTITY, db_stamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP, action TEXT, action_response TEXT)''')
nuSession.commit()
nuSession.close()

while True:
    log_dir,agentCFG,smtpCFG=getConfig()
    if agentCFG['kill']:
        recordpulse(agentCFG['recordpulse'], -1, 'Terminate', 'Terminate', uri)
        sys.exit('Received Agent Terminate')
        break
    elif agentCFG['defer']:
        recordpulse(agentCFG['recordpulse'], -1, 'Defer', 'DeferModeActivated: '+str(agentCFG['defertime']), uri)
        ziz(int(agentCFG['defertime']))
        continue

    csize,eaeSchema,uri=dwCNX(tinyset=True)
    pgx=pgcnx(uri)
    session=sessionmaker(bind=pgx)
    nuSession=session()
    jobsNow=list(nuSession.execute(''' SELECT * FROM framework.launchpad() ORDER BY job_order '''))
    pid=int(dtm.timestamp(dtm.utcnow()))
    nuSession.close()
    recordpulse(agentCFG['recordpulse'] ,pid, 'launchpad', str(jobsNow), uri)

    if len(jobsNow)>0:
        jList=[j['jid'] for j in jobsNow]
        pidFrame=upsertPID(pid,uri,jList)
        recordpulse(agentCFG['recordpulse'] ,pid, 'VolatileTracker', str(pidFrame), uri)
        nuSession=session()
        nuSession.execute(''' CREATE SCHEMA IF NOT EXISTS staging''')
        nuSession.commit()
        nuSession.close()
        pidFile=log_dir+'/'+str(pid)
        with open(pidFile,'w+') as pFile:
            pFile.write(str(dtm.utcnow())+ ' <<< Cycle Started\n')
        recordpulse(agentCFG['recordpulse'], pid, 'FileCreate', pidFile, uri)
        for job in jobsNow:
            jStartTime=dtm.utcnow()
            pidFrame.loc[pidFrame['jobid']==job['jid'],['notes']]='Job Executing...'
            pidFrame.to_sql('volatiletracker',pgx,if_exists='replace',index=False,schema='framework')
            with open(pidFile,'a') as pFile:
                proc=sbp.Popen([job['job_launcher'],job['jid'],str(pid)],stdout=pFile,stderr=sbp.PIPE)
                stdio,stder=proc.communicate()
                pFile.write('====================================================================\n')
            pidFrame.loc[pidFrame['jobid']==job['jid'],['endtime']]=dtm.utcnow()
            pidFrame.loc[pidFrame['jobid']==job['jid'],['starttime']]=jStartTime
            pidFrame.loc[pidFrame['jobid']==job['jid'],['notes']]='Job completed'
            pidFrame.to_sql('volatiletracker',pgx,if_exists='replace',index=False,schema='framework')
        with open(pidFile,'a+') as pFile:
            pFile.write('\n')
            pFile.write(str(dtm.utcnow())+ ' <<< Cycle completed')
        print(pidFrame)
        session=sessionmaker(bind=pgx)
        nuSession=session()
        nuSession.execute(' DELETE FROM framework.tracker WHERE pid=' +str(pid))
        nuSession.commit()
        nuSession.close()
        pidFrame.to_sql('tracker',pgx,if_exists='append',index=False,schema='framework')
        recordpulse(agentCFG['recordpulse'], pid, 'Tracker', 'TrackerUpdated', uri)
        pgx.dispose()
        if smtpCFG['enable']:
            recordpulse(agentCFG['recordpulse'], pid, 'InvokeMailStakeHolders', str(smptpCFG['server']), uri)
            mailStakeHolders(pidFile,smtpCFG['server'],uri)
        print(str(dtm.utcnow())+ ' Cycle Completed...')

    else:
        print('No JOBS in this cycle')

    if test_cycle:
        recordpulse(agentCFG['recordpulse'], pid, 'TestCycle', 'TestCycleTerminate', uri)
        break

    elif intLowLatencyStartTime < dtm.utcnow().hour < intHighLatencyStartTime:
        recordpulse(agentCFG['recordpulse'], pid, 'LowDelaySleep', str(agentCFG['lowdelay']), uri)
        ziz(agentCFG['lowdelay'])
    else:
        recordpulse(agentCFG['recordpulse'], pid, 'HighDelaySleep', str(agentCFG['highdelay']), uri)
        ziz(agentCFG['highdelay'])
