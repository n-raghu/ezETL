from dtsFarmLib import *

options.display.width=180

# LOAD YML FILE
with open("dtsConfig.yaml", 'r') as ymlFile:
 cfg=yaml.load(ymlFile)

# LOAD CONFIG
eaeIns=cfg['eaedb']['host']+':'+str(cfg['eaedb']['port'])
eaedw=cfg['eaedb']['db']
eaeKey=cfg['eaedb']['pass_key']
eaeAcc=cfg['eaedb']['user']
eaeSalt=cfg['eaedb']['passwd']
authMode=cfg['eaedb']['authentication']

if(authMode):
  from cryptography.fernet import Fernet as fNet
  eaeKey=fNet(eaeKey)
  eaePass=eaeKey.decrypt(eaeSalt)
  uri='mongodb://'+eaeAcc+':'+str(eaePass)+'@'+eaeIns
  mongoKonnect=mClient(uri)

else:
  mongoKonnect=mClient(eaeIns)

dataCubes=dataCubes()
css=mongoKonnect[eaedw][dataCubes['css']]
cur=css.find({'isActive':True},{'snapshot':1,'ssize':1,'createdUTC':1,'_id':0,'expireUTC':1}).sort('createdUTC',-1)
dfo=DataFrame(list(cur))
dfo['createdUTC']=dfo['createdUTC'].dt.date
print(dfo)