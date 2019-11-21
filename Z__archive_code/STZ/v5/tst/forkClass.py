#!/eae/dts/EAE/bin/python

import classic as c
print(c.gtext('Sreeyansh'))

p21=c.point2()
p22=c.point2()

p21.atr.append('repeated and global')

print(p21.atr)
print(p22.atr)

p11=c.point1()
p12=c.point1()

p11.atr.append('unique')

print(p11.atr)
print(p12.atr)