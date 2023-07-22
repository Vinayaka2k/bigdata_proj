#!/usr/bin/env python3

import sys
project_index = int(sys.argv[1])
select_index = int(sys.argv[2])
operator = sys.argv[3]
value = sys.argv[4]
query_type = sys.argv[5]
project_column = sys.argv[6]
datatype = sys.argv[7]
for line in sys.stdin:
	keys = line.split(",")
	
	if(datatype == 'int'):
		keys[select_index] = int(keys[select_index])
		value = int(value)

	if(query_type == "select"):
		print("%s,%s" % (project_column , keys[project_index]))

	else:

		if(operator == "=") :
			if(keys[select_index] == value):
				print("%s,%s" % (project_column , keys[project_index]))

		elif(operator == ">") :
			if(keys[select_index] > value):
				print("%s,%s" % (project_column , keys[project_index]))
		
		elif(operator == "<") :
			if(keys[select_index] < value):
				print("%s,%s" % (project_column , keys[project_index]))
