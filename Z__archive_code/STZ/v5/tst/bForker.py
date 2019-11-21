#!/eae/dts/EAE/bin/python

import bclass as b
cla=b.ops(5,6)
clb=b.ops(6,6,45,46,67,78,34,23,1)

cla.facto=3

print(cla.adi())
print(cla.mto(cla.facto))

print(clb.adi())
print(clb.mto(clb.facto))