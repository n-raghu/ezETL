import subprocess as sbp
from dimlib import *
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

log_dir='dimLogs/'
smtpServer='172.16.3.129'

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
    pidFrame['notes']='JOB queued...'
    pgx.dispose()
    return pidFrame

csize,eaeSchema,uri=dwCNX(tinyset=True)
pgx=pgcnx(uri)
jobsNow=[job['jid'] for job in list(pgx.execute(''' SELECT * FROM framework.launchpad() '''))]

if len(jobsNow)>0:
    pid=int(dtm.timestamp(dtm.utcnow()))
    pidFrame=upsertPID(pid,uri,jobsNow)
    pgx=pgcnx(uri)
    for job in jobsNow:
        jStartTime=dtm.utcnow()
        proc=sbp.Popen(['python',job+'.py',str(pid)],stdout=sbp.PIPE,stderr=sbp.PIPE)
        proc.wait()
        pidFrame.loc[pidFrame['jobid']==job,['endtime']]=dtm.utcnow()
        pidFrame.loc[pidFrame['jobid']==job,['starttime']]=jStartTime
        pidFrame.loc[pidFrame['jobid']==job,['notes']]='Job completed...'
        pidFrame.to_sql('volatiletracker',pgx,if_exists='append',index=False,schema='framework')
        stdio,stder=proc.communicate()
        pidFile=log_dir+str(pid)
        with open(pidFile,'w+') as pFile:
            pFile.write(job)
            pFile.write('Console OUT')
            pFile.write(stdio.decode())
            pFile.write('Console ERR')
            pFile.write(stder.decode())
    session=sessionmaker(bind=pgx)
    nuSession=session()
    nuSession.execute(''' SELECT framework.updatetracker() ''')
    nuSession.commit()
    nuSession.close()

pgx.dispose()
