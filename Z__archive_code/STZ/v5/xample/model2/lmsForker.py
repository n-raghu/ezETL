import courses as c, dtsAdmin as d

custo=[65]

q=c.course()
k=d.connections()

sqlConnect=k[1]
mConnect=k[0]

for kon in sqlConnect:
	sqlCur=kon.cursor()
	sqlCur.execute(q[0]+q[1]+q[2],custo)
	tub=sqlCur.fetchall()
	print(tub)