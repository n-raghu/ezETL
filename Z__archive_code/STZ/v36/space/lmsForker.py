from dtsLib import *
from dataLMS import *
from reporting import *

options.display.width=180
ray.init()

instanceList=appFuse().lmsFuse()
customerQL=studentQuery()

rData=rData()
repos=repos()

frameSet=[]
dataTower=[]
dbSet=[]

zeroDataCustomer=0
customerCount=0
nReturn=0

@ray.remote
def rayTower(customerSQL,cnx,cusid,icode):
	cxx=sqlCNX(cnx)
	pdf=read_sql(customerSQL,cxx,params=(cusid,))
	pdf['icode']=icode
	cxx.close()
	return pdf


@ray.remote
def insOps(frm):
	eaeObjects=eaeFuse().eaeObjects()
	frm.groupby('userid')['groupid','groupname','groupuserid'].apply(lambda x: x.to_dict('r').to_frame('groups').reset_index(inplace=True)
	print(frm)
	eaeObjects['cache_students'].insert_many(frm.to_dict('records'))
	return 'X'

def getRedisData(frm):
	return ray.get(frm)

print(str(dtm.now()) +' <<< ...')
for instance in instanceList:
	connexion=instance['cnx']
	icode=instance['icode']
	instanceCustomers=sqlCNX(connexion).cursor().execute(customers()).fetchall()
	for customer in instanceCustomers:
		customerCount+=1
		frameSet.append(rayTower.remote(customerQL,connexion,customer[0],icode))

if(customerCount>0):
	if(customerCount>1):
		nReturn=1
	ray.wait(frameSet,num_returns=customerCount-nReturn)
	print(str(dtm.now()) +' <<< Fetch from Redis Server...')
	for frame in frameSet:
		dataTower.append(getRedisData(frame))

	print(str(dtm.now()) +' <<< Pushing to MongoDB...')
	for frame in range(len(dataTower)):
		if dataTower[frame].empty:
			zeroDataCustomer+=1
			continue
		else:
			dbSet.append(insOps.remote(dataTower[frame].fillna('NULL_LMSDB')))

	print(str(dtm.now()) +' <<< Data Tower Ready')
