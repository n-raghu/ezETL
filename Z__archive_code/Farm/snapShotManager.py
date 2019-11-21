from dtsFarmLib import *

if(len(sys.argv)==5):
  authMode=True
elif(len(sys.argv)==3):
  authMode=False
else:
  sys.exit('Invalid number of params.')

mongoConStr=sys.argv[1]
eaedw=sys.argv[2]

if(authMode):
  uri='mongodb://'+str(sys.argv[3])+':'+str(sys.argv[4])+'@'+mongoConStr
  mongoConnect=mClient(uri)
else:
  mongoConnect=mClient(mongoConStr)

dataCubes=dataCubes()
cStore=cacheStore()
modelMap=modelMap()
logStore=farmLogStore()
settings=FarmSettings()
cacheConnexions=odict()
snapVersions=odict()
killSnaps=[]
batchCodePurge=[]

fss=mongoConnect[eaedw][dataCubes['fss']]
css=mongoConnect[eaedw][dataCubes['css']]

for i in cStore:
    cacheConnexions[i]=mongoConnect[eaedw][cStore[i]]

ndays=settings['dataTowerFarm']
xtime=dtm.combine(dtm.today().date()+timedelta(days=ndays),dtm.min.time())

for cache,conex in cacheConnexions.items():
    cur=conex.find()
    newSS=DataFrame(list(cur))
    if(len(newSS)<1):
        continue
    bcode=newSS['batchCode'][0]
    cutc=newSS['createdUTC'][0]
    conex.drop()
    newSS.drop(['_id'],axis=1,inplace=True)
    newSS_copy=newSS.drop(['batchCode','createdUTC'],axis=1)
    model=modelMap[cache]
    nsize=len(newSS)
    if(css.count({'isActive':True,'snapshot':model})==0):
        fss.insert_many(newSS.to_dict('r'))
        css.insert_one(odict([('isActive',True),('snapshot',model),('ssize',nsize),('expireUTC',xtime),('batchCode',bcode),('createdUTC',cutc)]))
    else:
        snaps=list(css.find().distinct('batchCode'))
        for batcode in snaps:
            code=batcode
            cur=fss.find({'batchCode':code})
            oss=DataFrame(list(cur))
            if(len(oss)<1):
                continue
            oss.drop(['_id','batchCode','createdUTC'],axis=1,inplace=True)
            oss.sort_values(['instancecode','calendaryear','doy'],inplace=True)
            newSS_copy.sort_values(['instancecode','calendaryear','doy'],inplace=True)
            oss.reset_index(drop=True,inplace=True)
            newSS_copy.reset_index(drop=True,inplace=True)
            ezyCheck=oss.equals(newSS_copy)
            if(ezyCheck):
                snapVersions[ezyCheck]=code
                osize=len(oss)
        if True in snapVersions.keys():
            ocode=snapVersions[True]
            nsize=osize
        else:
            fss.insert_many(newSS.to_dict('r'))
            ocode=bcode
        css.insert_one(odict([('isActive',True),('snapshot',model),('ssize',nsize),('expireUTC',xtime),('batchCode',ocode),('createdUTC',cutc)]))

snaps=css.find()
for snap in snaps:
    stime=snap['expireUTC']
    dtNow=dtm.today().date()
    if(dtNow>=stime.date()):
        killSnaps.append(snap['_id'])

if(len(killSnaps)>0):
    css.update_many({'_id':{'$in':killSnaps}},{'$set':{'isActive':False,'expireUTC':dtm.today()}})

dataSnaps=list(fss.find().distinct('batchCode'))
actSnaps=list(css.find().distinct('batchCode'))

for snap in dataSnaps:
    if snap in actSnaps:
        x=True
    else:
        batchCodePurge.append(snap)

if(len(batchCodePurge)>1):
    fss.delete_many({'batchCode':{'$in':batchCodePurge}})

mongoConnect.close()