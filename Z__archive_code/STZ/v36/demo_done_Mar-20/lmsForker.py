from dtsLib import *
from repoLMS import *

options.display.width=180
ray.init()

instanceList=appFuse().lmsFuse()

frameSet=[]
dataSet=[]
dbSet=[]
zeroDataCustomer=0
customerCount=0
nReturn=0

@ray.remote
def oneSynchronize(customerSQL,cnx,cusid):
	cxx=sqlCNX(cnx)
	pdf=read_sql(customerSQL,cxx,params=(cusid,))
	cxx.close()
	pdf.fillna('Missed')
	return pdf

@ray.remote
def insOps(frm):
	eaeObjects=eaeFuse().eaeObjects()
	eaeObjects[5].insert_many(frm.to_dict('records'))
	return 'X'

def getRedisData(frm):
	return ray.get(frm)

print(str(dtm.now()) +' <<< Creating Frames...')
for instance in instanceList:
	connexion=instance['cnx']
	customerQL=sqlQuery()
	customerQuery=''' SELECT fldCustId FROM TBL_CUSTOMER_LOOKUP WHERE bitIsEAEIntegration=1 AND charAccountType='P' AND fldCustomerFlag='A' '''
	instanceCustomers=sqlCNX(connexion).cursor().execute(customerQuery).fetchall()
	for customer in instanceCustomers:
		customerCount+=1
		frameSet.append(oneSynchronize.remote(customerQL,connexion,customer[0]))

if(customerCount>0):
	if(customerCount>1):
		nReturn=1
	ray.wait(frameSet,num_returns=customerCount-nReturn)
	print(str(dtm.now()) +' <<< Fetch from Redis Server...')
	for frame in frameSet:
		dataSet.append(getRedisData(frame))

	print(str(dtm.now()) +' <<< Pushing to MongoDB...')
	for frame in range(len(dataSet)):
		if dataSet[frame].empty:
			zeroDataCustomer+=1
			continue
		else:
			dbSet.append(insOps.remote(dataSet[frame].fillna('LMS')))

	ray.wait(dbSet,num_returns=customerCount-nReturn-zeroDataCustomer)

print(str(dtm.now()) +' <<< ...')