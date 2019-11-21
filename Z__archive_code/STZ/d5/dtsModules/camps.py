app='phish'
query='''SELECT * FROM TBL_CUSTOMERLOOKUP '''
clause='''WHERE ID=1 '''

def dataSet():
	global query, clause
	return query,clause