'''
INDEX TRACKER FOR EAE-DB.

RECORDS INDEX STATS IN THE FIRST EXECUTION OF THE DAY.
'''

def main():
		packs=(clc.OrderedDict,pym.IndexModel,ASCENDING,DESCENDING,dtm.datetime)
		collectionSet=(a,e,i,o)
		idxTracker(collectionSet,packs)

def idxConstructor(idxSet):
	print('Constructor Invoked')
	buildSet=[]
	for kit in idxSet:
		nuKit=odc(kit)
		queryBuilder=[]
		for i,k in nuKit['definition'].items():
			if(int(k)==1):
				k=asc
			elif(int(k)==-1):
				k=dsc
			else:
				break
			tup=(str(i),k)
			queryBuilder.append(tup)
		if(nuKit['isUnique']==True):
			buildComponent=idxModel(queryBuilder,unique=True,name=nuKit['idx'])
		else:
			buildComponent=idxModel(queryBuilder,name=nuKit['idx'])
		buildSet.append(buildComponent)
		dbo[str(nuKit['collection'])].create_indexes(buildSet)
		dbi.update_one({'_id':str(nuKit['idx'])},{'$set':{'statistics':[{'times':datetime.utcnow(),'status':'Started Collecting Stats'}]}})
	return 'X'

def idxDestructor(idxSet):
	print('Destructor Invoked')
	buildSet=[]
	for kit in idxSet:
		nuKit=odc(kit)
		queryBuilder=[]
		for i,k in nuKit['definition'].items():
			if(int(k)==1):
				k=asc
			elif(int(k)==-1):
				k=dsc
			else:
				break
			tup=(str(i),k)
			queryBuilder.append(tup)
		if(nuKit['isUnique']==True):
			buildComponent=idxModel(queryBuilder,unique=True,name=nuKit['idx'])
		else:
			buildComponent=idxModel(queryBuilder,name=nuKit['idx'])
		buildSet.append(buildComponent)
		dbo[str(nuKit['collection'])].drop_indexes(buildSet)
		dbi.update_one({'_id':str(nuKit['idx'])},{'$set':{'statistics':{}}})
	return 'X'

def idxStats():
	tabList=dbi.distinct('collection')
	for tab in tabList:
		aggOut=dbo[tab].aggregate([{'$indexStats':{}}])
		for agg in aggOut:
			if(agg['name']!='_id_'):
				agg.update({'times':datetime.utcnow()})
				dbi.update_one({'_id':agg['name']},{'$push':{'statistics':agg}})
	return 'X'

def idxTracker(dbTup,sysPacks):
	global odc,dba,dbe,dbi,dbo,idxModel,asc,dsc,datetime
	dba,dbe,dbi,dbo=dbTup
	odc,idxModel,asc,dsc,datetime=sysPacks
	idxNewList=[]
	idxRetireList=[]
	idxCollection=dbi.find()
	for idxKit in idxCollection:
		if(idxKit['isActive']==False and idxKit['isDeleted']==True):
			continue																# INDEX DROPPED ALREADY
		elif(idxKit['isDeleted'])==True:
			idxRetireList.append(idxKit)											# INDEX TO BE DROPPED NOW
			continue
		elif(idxKit['isActive']==False):
			continue																# INDEX MARKED INACTIVE
		elif 'statistics' not in idxKit:
			idxNewList.append(idxKit)
			continue

	if(len(idxRetireList)>0):
		idxDestructor(idxRetireList)

	doc=list(dbi.find({'statistics.times':{'$exists':True}}).sort('statistics.times',-1).limit(1))
	if(len(doc)>0):
		d=doc[0]
		idxDate=max([x['times'] for x in d['statistics']]).date()
		sysDate=datetime.utcnow().date()
		if(idxDate<sysDate):
			print('Invoking Stats')
			idxStats()
		else:
			print('Nothing to update')

	if(len(idxNewList)>0):
		idxConstructor(idxNewList)
	return 'X'

if __name__=='__main__':
	import sys, collections as clc, pymongo as pym, datetime as dtm
	print('INDEX TRACKER IS INVOKED...')
	print('DO NOTE, INVOKING FROM "dtsAdmin" IS THE BEST PRACTISE ...')
	if(len(sys.argv)!=5):
		sys.exit('Invalid Arguments')
	mongoConStr=sys.argv[1]
	eaedw=sys.argv[2]
	mongoKonnect=pym.MongoClient(mongoConStr,username=sys.argv[3],password=sys.argv[4])
	a=mongoKonnect[eaedw].logDTSActions
	e=mongoKonnect[eaedw].logDTSErrors
	i=mongoKonnect[eaedw].indexTracker
	o=mongoKonnect[eaedw]