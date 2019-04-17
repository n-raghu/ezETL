import subprocess as sbp
from dimlib import dwCNX,pdf,y,pgcnx,dtm,sessionmaker
import smtplib
from os.path import basename
from time import sleep as ziz
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

print('Modules Imported...')
intLowLatencyStartTime=5
intHighLatencyStartTime=23

def getConfig():
    with open('dimConfig.yml') as yFile:
        cfg=y.safe_load(yFile)
    return cfg['log_directory'],cfg['agent'],cfg['smtp']

def mailStakeHolders(file_name_path,smtp_server,urx):
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

def upsertPID(pid,uri,jobList):
    pgx=pgcnx(uri)
    pidFrame=pdf(jobList,columns=['jobid'])
    pidFrame['pid']=pid
    pidFrame.to_sql('tracker',pgx,if_exists='append',index=False,schema='framework')
    pidFrame['starttime']=dtm.utcnow()
    pidFrame['endtime']=dtm.utcnow()
    pidFrame['notes']='JOB in Queue'
    pidFrame.to_sql('volatiletracker',pgx,if_exists='append',index=False,schema='framework')
    pgx.dispose()
    return pidFrame

while True:
    log_dir,agentCFG,smtpCFG=getConfig()
    if agentCFG['kill']:
        print('Received Agent Terminate')
        break
    elif agentCFG['defer']:
        ziz(59)
        continue
    csize,eaeSchema,uri=dwCNX(tinyset=True)
    pgx=pgcnx(uri)
    session=sessionmaker(bind=pgx)
    nuSession=session()
    jobsNow=list(nuSession.execute(''' SELECT * FROM framework.launchpad() ORDER BY  '''))
    nuSession.commit()
    nuSession.close()
    print(jobsNow)
    if len(jobsNow)>0:
        pid=int(dtm.timestamp(dtm.utcnow()))
        jList=[j['jid'] for j in jobsNow]
        pidFrame=upsertPID(pid,uri,jList)
        pgx=pgcnx(uri)
        pidFile=log_dir+'/'+str(pid)
        with open(pidFile,'w+') as pFile:
            pFile.write(str(dtm.utcnow())+ ' <<< Cycle Started\n')
        for job in jobsNow:
            jStartTime=dtm.utcnow()
            pidFrame.loc[pidFrame['jobid']==job['jid'],['notes']]='Job Executing...'
            proc=sbp.Popen([job['job_launcher'],job['jid'],str(pid)],stdout=sbp.PIPE,stderr=sbp.PIPE)
            proc.wait()
            pidFrame.loc[pidFrame['jobid']==job['jid'],['endtime']]=dtm.utcnow()
            pidFrame.loc[pidFrame['jobid']==job['jid'],['starttime']]=jStartTime
            pidFrame.loc[pidFrame['jobid']==job['jid'],['notes']]='Job completed'
            pidFrame.to_sql('volatiletracker',pgx,if_exists='append',index=False,schema='framework')
            stdio,stder=proc.communicate()
            with open(pidFile,'a+') as pFile:
                pFile.write(job['jid'])
                pFile.write(stdio.decode())
                pFile.write('\n')
                pFile.write(stder.decode())
        with open(pidFile,'a+') as pFile:
            pFile.write('\n')
            pFile.write(str(dtm.utcnow())+ ' <<< Cycle completed')
        print(pidFrame)
        session=sessionmaker(bind=pgx)
        nuSession=session()
        nuSession.execute(''' SELECT framework.updatetracker() ''')
        nuSession.commit()
        nuSession.close()
        pgx.dispose()
        if smtpCFG['enable']:
            mailStakeHolders(pidFile,smtpCFG['server'],uri)
        print(str(dtm.utcnow())+ ' Cycle Completed...')
    else:
        print('No JOBS in this schedule')
    if intLowLatencyStartTime < dtm.utcnow().hour < intHighLatencyStartTime:
        ziz(agentCFG['lowdelay'])
    else:
        ziz(agentCFG['highdelay'])
