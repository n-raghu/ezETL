import os,sys,bson,yaml
from subprocess import Popen,PIPE
from pymongo import MongoClient as mClient
from collections import OrderedDict as odict
import pypyodbc as podbc
import pymysql as psql, pymysql.cursors as pcr
from datetime import datetime as dtm, timedelta
from copy import copy,deepcopy
from uuid import uuid1 as uid
from pandas import DataFrame,options
from time import sleep as ziz
import numpy as np

def FarmSettings():
	doc=odict()
	doc['dataTowerFarm']=36
	doc['odbcDriver']='SS171'
	return doc

def farmLogStore():
	logStore=odict()
	logStore['dbi']='InstanceConfiguration'
	logStore['dbc']='customerStats'
	logStore['dbs']='instanceStats'
	logStore['dbf']='logForkerActions'
	logStore['dbd']='logDTSActions'
	logStore['dxx']='logDTSErrors'
	return logStore

def dataCubes():
	dtc=odict()
	dtc['fss']='dataTowerFarm'
	dtc['css']='catalogueSnapShot'
	return dtc

def dataStore():
	dStore=odict()
	dStore['hsd']='phishHetroStats'
	dStore['ccd']='courseCompletion'
	return dStore

def cacheStore():
	dStore=odict()
	dStore['_cc']='cacheCourseCompletion'
	dStore['_hs']='cachePhishHetroStats'
	return dStore

def modelMap():
	dStore=odict()
	dStore['_cc']='Course Completion'
	dStore['_hs']='Hetro Stats'
	return dStore
