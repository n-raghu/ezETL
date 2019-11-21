from dtsLib import *
from repoLMS import *

ray.init()

frameSet=[]
instanceList=appFuse().lmsFuse()

@ray.remote
def oneSynchronize(customerSQL,cnx):
	pdf=read_sql(customerSQL,cnx)
	return pdf

def tstSynchronize(customerSQL,cnx):
	pdf=read_sql(customerSQL,cnx)
	return pdf

for instance in instanceList:
	connexion=instance['cnx']
	customerQuery=''' SELECT fldCustId FROM TBL_CUSTOMER_LOOKUP WHERE bitIsEAEIntegration=1 AND charAccountType='P' AND fldCustomerFlag='A' '''
	instanceCustomers=connexion.cursor().execute(customerQuery).fetchall()
	for customer in instanceCustomers:
		customerQL=query+ 'WHERE fldCustId=' +str(customer[0])
		frameSet.append(oneSynchronize.remote(customerQL,connexion))