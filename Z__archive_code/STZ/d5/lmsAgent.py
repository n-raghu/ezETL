import dtsKonnections as dct
from dtsLib import arrow
from dtsModules import students as std

insList=[]
connexions=dct.appFuse().lmsFuse()

bizLogic,bizClause=std.dataSet()
bizQuery=bizLogic+bizClause

print(bizQuery)

for kon in connexions:
	cur=kon['cnx'].cursor()
	cur.execute(bizQuery)
	queryTuples=cur.fetchall()
	thisList=[]
	for tup in queryTuples:
		thisList.append(tup)
	insList.append(thisList)