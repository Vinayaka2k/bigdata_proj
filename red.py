#!/usr/bin/env python3

import sys

total = 0
maximum = 0
counter = 0

aggr_function = sys.argv[1]

if(aggr_function.upper() == 'SUM') :
	for line in sys.stdin:
		if(line != '\t\n'):
			my_line = line.split(",")
			total = total + int(my_line[1])		
	print(total)

if(aggr_function.upper() == 'MAX') :
	for line in sys.stdin:
		if(line != '\t\n'):
			my_line = line.split(",")
			ele = int(my_line[1])		
			if(ele > maximum) :
				maximum = ele		
	print( "%s" % (maximum) ,end="" )

if(aggr_function.upper() == 'COUNT') :
	for line in sys.stdin:
		if(line != '\t\n'):
			counter = counter + 1	
	print(counter)

if(aggr_function == '#'):
	for line in sys.stdin:
		if(line != '\t\n'):
			print(line.split(",")[1])

