import courses as c, dtsAdmin as d, json as j

custo=[65]
cc=c.course()
oo=cc.obj()
key=[]
val=[]
valMap=[]
tmp_qkeys=[]
konnection=d.connections()

for o in oo:
	k=list(o.keys())
	v=list(o.values())
	k.pop(0)
	q=v.pop(0)
	valMap.append(q)
	key.append(k)
	val.append(v)
	tmp_qkeys.append([q+'.'+item for item in k])
qkeys=[i for l in tmp_qkeys for i in l]
qvals=[i for l in val for i in l]
tmp_qkeys=[]

fld=''
for i in qkeys:
	fld+=i+','
fld=fld[:-1]

sqlConnect=konnection[1]
mConnect=konnection[0]
dbo=mConnect.eaedw.forkTst

for kon in sqlConnect:
	sqlCur=kon.cursor()
	sqlCur.execute('SELECT '+fld+cc.joins+cc.where,custo)
	tub=sqlCur.fetchall()

insArray=[]
for b in tub:
	doc=d.odict()
	ii=0
	for v in range(len(b)):
		if(ii==4):
			ii=0
		doc[qvals[v]]=b[ii]
		ii+=1
	insArray.append(doc)

kount=dbo.insert_many(insArray)
print('Migrated : ' +str(len(insArray)))