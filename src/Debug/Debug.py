#! /usr/local/bin/python
#-*- coding: utf-8 -*-

from sys import stderr,stdout

class Debug:
	def __init__(self,classname):
		self.classname = classname
		self.stderr = stderr
		self.stdout = stdout

	def print_err(self,funname,msg):
		self.stderr.write('[%s.%s]: %s\n' % (self.classname,funname,msg))
		self.stderr.flush()

	def print_out(self,funname,msg):
		self.stdout.write('[%s.%s]: %s\n' % (self.classname,funname,msg))
		self.stdout.flush()


if __name__ == '__main__':
	dbg = Debug('Debug')
	dbg.print_err('print_err', 'some error occured')
	dbg.print_out('print_out', 'some information need to be displayed')