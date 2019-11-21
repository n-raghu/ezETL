'''
DATA TRANSFORMATION SERVICES (DTS) FOR EAE
'''

errFile='logErrors'

def logImpError(err):
 f=open(errFile,'a')
 f.write(str(dtnow.now())+ ' :\n ')
 f.write(str(err))
 f.write('\n----------------------------------------\n')
 f.close()
 return 'X'


try:
# MANDATORY PACKAGES
	from collections import OrderedDict as odict
	from time import sleep as ziz
	from sys import exit as thwart
	from datetime import datetime as dtnow
	from glob import glob

# ANALYTIC PACKAGES
	from numpy import array as arrow
	from pandas import DataFrame,Series
	from pandas import read_sql_query as sqlQuery,to_datetime as PDT
	from cryptography.fernet import Fernet as fnt
	import yaml as yml

# CONNECTION PACS
	from pymongo import MongoClient as mClient
	from pypyodbc import connect as sqlClient, Error as sqlError
except ImportError as e:
	logImpError(e)
	exit(e)