from dtsLib import *
import ray

y=randint(1,100)
print(y)
ziz(5)
print('Invoking RAY. ')

ray.init()

@ray.remote
def first():
	k=y+10
	ziz(10)
	return k

@ray.remote
def king():
	ziz(5)
	return 'DONE. '

@ray.remote
def panFrame():
	panTMP=buildFrame()
	return panTMP

a=first.remote()
b=king.remote()
pdf=panFrame.remote()

