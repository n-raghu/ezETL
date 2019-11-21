def pdoc(fun):
	def fwrap(n):
		return '<p>{0}</p>'.format(fun(n))
	return fwrap

@pdoc
def gtext(ss):
	return 'Hi, {0}!!! Howz it going'.format(ss)

@pdoc
def gtext2(ss):
	return 'Hi, {0}!!! Extending the text'.format(ss)

class point2:
	atr=[]

class point1:
	def __init__(mem):
		mem.atr=[]

class MyClass:
	"This is my second class"
	"Class One"
	a = 10
	def func(self):
		print('Hello')

print(MyClass.a)

# Output: <function MyClass.func at 0x0000000003079BF8>
print(MyClass.func)

# Output: 'This is my second class'
print(MyClass.__doc__)