from numpy import array as arrow

class frame:
	def __init__(mem,*args):
		mem.xx=arrow(args)

class ops(frame):
	facto=1
	def adi(mem):
		return mem.xx.sum()
	def mto(mem,facto):
		return facto*mem.xx