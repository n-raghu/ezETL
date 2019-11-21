from pandas import read_sql
from pypyodbc import connect
import ray

ray.init()
sqlConStr='DRIVER={DTS};SERVER=172.16.3.243,1433;DATABASE=catholicDB;UID=ilmsuser;PWD=ilmsuser'
connexion=connect(sqlConStr)
customerQL=''' SELECT * FROM TBL_CUSTOMER_LOOKUP '''

@ray.remote
def rayTst(n):
	print(customerQL+str(n))

@ray.remote
def raySynchronize(customerSQL):
	connexion=connect(sqlConStr)
	pdf=read_sql(customerSQL,connexion)
	return pdf

def pandaSynchronize(customerSQL,cnx):
	pdf=read_sql(customerSQL,cnx)
	return pdf


pandasFrame=pandaSynchronize(customerQL,connexion)
rayFrame=raySynchronize.remote(customerQL)


'''
BEHAVIOR OBSERVED

Function 'rayTst' which is designed to test ray is working fine.
Function 'pandaSynchronize' executes successfully which indicates pandas library is working.

Function 'raySynchronize' is created integrating pandas library into ray but throws error

'''