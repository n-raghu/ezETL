import smtplib
from os.path import basename
from time import sleep as ziz
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from email.mime.application import MIMEApplication
from dimlib import dwCNX,pdf,rsq,y,pgcnx,dtm,sessionmaker

def getConfig():
    with open('dimConfig.yml') as yFile:
        cfg=y.safe_load(yFile)
    return cfg['smtp']

smtp=getConfig()
csize,eaeSchema,uri=dwCNX(tinyset=True)
stakes=rsq('SELECT * FROM framework.stakeholders WHERE active=true',pgcnx(uri))
tolist=list(stakes.loc[(stakes['tolist']==True),['recepient']]['recepient'])
cclist=list(stakes.loc[(stakes['cclist']==True),['recepient']]['recepient'])

msg=MIMEMultipart()
mailFile='mailFile'
msg['From']='EAE Data Integration <statzen@eae.inspireme.com>'
msg['To']=','.join(cclist)
msg['Date']=formatdate(localtime=True)
msg['Subject']='Summary of Data Integration to EAE Database.'
msg.attach(MIMEText('Check Attachment.... Thank you'))
with open(mailFile,"rb") as fil:
    mimeFile=MIMEApplication(fil.read(),Name=basename(mailFile))
mimeFile['Content-Disposition'] = 'attachment; filename="%s"' % basename(mailFile)
msg.attach(mimeFile)
s_smtp=smtplib.SMTP(smtp['server'])
s_smtp.starttls()
_response_=s_smtp.sendmail(msg['From'],[msg['To']],msg.as_string())
s_smtp.close()

print(_response_)
