#!/usr/bin/env python3
#column validation in load
"""mapper.py"""
import os
from os import path
import csv
import sys

def main():
    while(True):
        query = input("miniHive> ")
        if(query == 'exit'):
            break
        query = query.strip()
        validateQuery(query)

def validateQuery(query):
    split = query.split(' ')
    if(split[0].upper() == 'LOAD'):
        validateLoadQuery(query)
    elif(split[0].upper() == 'DELETE'):
        validateDeleteQuery(query)
    elif(split[0].upper() == 'SELECT'):
        validateSelectQuery(query)

def validateLoadQuery(query):
    #load db/a.csv as (id:int) inpath local.csv
    querysplit = query.split(' ')
    if(len(querysplit) != 6):
        print("Invalid query!")
        return
    if(querysplit[2].upper() != 'AS' or querysplit[4].upper() != 'INPATH' or querysplit[3][0] != '(' or querysplit[3][-1] != ')'):
        print("Invalid query!")
        return
    # if(path.exists(querysplit[5]) == False):
    #     print(querysplit[5]," file doesn't exists in local path!")
    #     return
    else:
        databasetable = querysplit[1].split('/')
        if(not checkDatabase(databasetable[0])):
            database = "hadoop fs -mkdir /"+databasetable[0]
            os.system(database)
            print("Database created!")
        if(not checkTable(databasetable)):
            inputfile = querysplit[5]
            table = "hadoop fs -put "+inputfile+" /"+databasetable[0]+"/"+databasetable[1]
            os.system(table)
            print("Table created!")
            schema = databasetable[1]+"->"+querysplit[3]+"\n"
            tempfile = "temp.txt"
            t = open(tempfile,"w")
            t.write(schema)
            t.close()
            indexfile = "hadoop fs -appendToFile /home/hduser/"+tempfile+" /"+databasetable[0]+"/index.txt"
            os.system(indexfile)
            os.system("rm temp.txt")
        else:
            print("Table already exists!")
            return
def validateDeleteQuery(query):
    querysplit = query.split(' ')
    if(len(querysplit) != 2):
        print("Invalid query!")
        return
    if(checkDatabase(querysplit[1])):
        database = "hadoop dfs -rm -r /"+querysplit[1]
        os.system(database)
        print("Database deleted!")
    else:
        print("Database doesn't exists!")

def validateSelectQuery(query):
    #SELECT n1 FROM db/table.csv
    querysplit = query.split(' ')
    querylen = len(querysplit)
    if(querylen >= 4):
        aggrfunc = '#'
        value = '#'
        selectindex = -1
        operator = '#'
        datatype = '#'

        querytype = "select"
        databasetable = querysplit[3].split('/')
        if(querysplit[2].upper() != 'FROM' or len(databasetable) != 2 or databasetable[0] == '' or databasetable[1] == ''):
            print("Invalid query")
            return
        projectcolumn = querysplit[1]
        if(not checkDatabase(databasetable[0])):
            print("Database doesn't exists!")
            return
        if(not checkTable(databasetable)):
            print("Table doesn't exists!")
            return
        allschema = os.popen("hdfs dfs -cat /" + databasetable[0] + "/index.txt").read()
        lines = allschema.split('\n')
        for line in lines:
            schemacontent = line.split('->')
            if(schemacontent[0] == databasetable[1]):
                schema = schemacontent[1]
                break 
        temp = schema.replace('(','')
        schema = temp.replace(')','')
        attr = schema.split(",")
        flag1 = 0
        #n1:string,n2:string
        for col in attr :
            if(projectcolumn == col.split(':')[0]):
                projectindex = attr.index(col)
                flag1 = 1
                break
        if(not flag1):
            print(projectcolumn," column doesn't exists!")
            return
        flag = 0

        #to handle aggr query
        if((querylen >= 5 and querysplit[4].split('=')[0].upper() == 'AGGR') or (querylen > 6 and querysplit[6].split('=')[0].upper() == 'AGGR')):
            aggrfunc = query.split('aggr=')[1]
            dtype = attr[projectindex].split(':')[1]
            if(dtype == 'string' and aggrfunc.upper() != 'COUNT'):
                print(aggrfunc," aggregate operation can't be done on this column!")
                return
            flag = 1
        
        #to handle where query
        if(querylen >= 6 and querysplit[4].upper() == 'WHERE'):
            if ">" in querysplit[5] :
                operator  = ">"
            elif "=" in querysplit[5] :
                operator = "="
            elif "<" in querysplit[5] :
                operator = "<"
            else:
                print("Invalid query!")
                return
            querytype = 'where'
            condition = querysplit[5]
            condition_list = condition.split(operator)
            selectcolumn = condition_list[0]
            value = condition_list[1]
            if(selectcolumn == '' or value == ''):
                print("Invalid query!")
                return
            flag2 = 0
            for col in attr :
                if(selectcolumn == col.split(':')[0]):
                    selectindex = attr.index(col)
                    datatype = col.split(':')[1]
                    flag2 = 1
            if(not flag2):
                print(selectcolumn," column doesn't exists!")
                return
        elif(not flag and not querylen == 4):
            print("Invalid query!")
            return

        command = "hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -files /home/manoj/Desktop/BD/project/map.py,/home/manoj/Desktop/BD/project/red.py -mapper 'map.py " + str(projectindex)+ " " + str(selectindex) + " "+ operator +" "+ str(value) + " " +querytype  + " " + projectcolumn +" "+ datatype + "' -reducer 'red.py " + aggrfunc +"' -output /output/out1 -input /" + databasetable[0] + "/" + databasetable[1]
        os.system(command)

        os.system("hadoop dfs -cat /output/out1/part*")
        os.system("hadoop dfs -rm -r /output/out1")

    else:
        print("Invalid query!")

def checkDatabase(db):
    checkdbcmd = "hadoop dfs -test -d /"+db
    checkdb = os.system(checkdbcmd)
    if(checkdb == 0): #exists
        return 1
    return 0

def checkTable(databasetable):
    checktbcmd = "hadoop dfs -test -e /"+databasetable[0]+"/"+databasetable[1]
    checktb = os.system(checktbcmd)
    if(checktb == 0):
        return 1
    else:
        return 0
if __name__ == "__main__":
    main()