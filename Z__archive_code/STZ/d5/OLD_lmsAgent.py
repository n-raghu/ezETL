import dtsKonnections as dct
from dtsLib import arrow
from dtsModules import *

connexions=dct.appFuse().lmsFuse()
print()
print(connexions[0]['code'])

cachedPacks=dir()
cachedPacks.sort(reverse=True)
lmsPacks=[ele for ele in cachedPacks if ele.isalpha()]
print(lmsPacks)

for i in range(len(lmsPacks)):
	print(lmsPacks[i].app)