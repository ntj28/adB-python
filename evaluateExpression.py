import re
import pandas as pd
import numpy as np
import executionEngine
from DBEngine import dbGetSet



locStudent = './data/STUDENT.csv'
studentDB = pd.read_csv(locStudent)
studentDB1 = dbGetSet(studentDB)


locCourse ='./data/COURSE.csv'
CourseDB = pd.read_csv(locCourse)
CourseDB1 = dbGetSet(CourseDB)

locCourseOffering = './data/COURSEOFFERING.csv'
CourseOfferingDB = pd.read_csv(locCourseOffering)
CourseOfferingDB1 = dbGetSet(CourseOfferingDB)

locStudCourse = './data/STUDCOURSE.csv'
studCourseDB = pd.read_csv(locStudCourse)
studCourseDB1 = dbGetSet(studCourseDB)


locStudentHistory = './data/STUDENTHISTORY.csv'
studentHistoryDB = pd.read_csv(locStudentHistory)
studentHistoryDB1 = dbGetSet(studentHistoryDB)



def setData(tableName,data,readOnly):

    if tableName == 'STUDENT':
        if readOnly == False:
            studentDB1.set_data(data)
        return studentDB1.get_data()
    elif tableName == 'COURSE':
        if readOnly == False:
            CourseDB1.set_data(data)
        return CourseDB1.get_data()
    elif tableName == 'COURSEOFFERING':
        if readOnly == False:
            CourseOfferingDB1.set_data(data)
        return CourseOfferingDB1.get_data()
    elif tableName == 'STUDCOURSE':
        if readOnly == False:
            studCourseDB1.set_data(data)
        return studCourseDB1.get_data()
    elif tableName == 'STUDENTHISTORY':
        if readOnly == False:
            studentHistoryDB1.set_data(data)
        return studentHistoryDB1.get_data()


def evaluateQuery(query):

    resultsTable = 'null'

    if query[0] == 'select':
        resultsTable = selectQuery(query)


    elif query[0] == 'insert':
        insertQuery(query)

    elif query[0] == 'delete':
        deleteQuery(query)

    return resultsTable


def selectQuery(query):

    tempCol = re.split(",| ",query[1])

    cols=[]
    for col in tempCol :
        if len(col) != 0:
            cols.append(col)


    table = query[3]



    validity = checkTable(table)
    if checkTable(table):

        if not checkColumns(table,cols):
            print("Invalid column encountered select")

        else:
            where = 'null'
            orderBy = 'null'


            #Simple select
            if len(query) == 4:
                resultsTableData = executionEngine.selectQuery(table,cols,where,orderBy)



            #only where clause
            if len(query)==5:
                whereSelect= whereExtractor(table,query[4])
                if len(whereSelect) == 0:
                    print('Invalid column encountered in where clause')
                else:
                    where = whereSelect
                    resultsTableData = executionEngine.selectQuery(table,cols,where,orderBy)


            #only order by clause
            if len(query) == 7:
                orderBy = orderByExtractor(table,query[6])
                if len(orderBy) == 0:
                    print('Invalid column encountered in order by clause')
                else:
                    resultsTableData = executionEngine.selectQuery(table,cols,where,orderBy)

            #with where clause and order by
            if len(query) == 8 :
                whereSelect= whereExtractor(table,query[4])
                if len(whereSelect) == 0:
                    print('Invalid column encountered in where clause')
                else:
                    where = whereSelect
                    orderBy = orderByExtractor(table,query[7])
                    if len(orderBy) == 0:
                        print('Invalid column encountered in order by clause')
                    else:
                        resultsTableData = executionEngine.selectQuery(table,cols,where,orderBy)
        return resultsTableData

    else:
        print("Invalid table " + table)



def insertQuery(query):
    tableData = query[2]
    newStr = query[4].replace('(','')
    newStr = newStr.replace(')','')
    values = []

    for data in newStr.split(","):
        newStr = data.replace("'",'')
        values.append(newStr)

    newStr2 = query[2].replace('(',' ')
    newStr2 = newStr2.replace(')',' ')
    newStr2 = newStr2.replace("'",' ')
    tableTokens = re.split(",| ",newStr2)


    table =[]
    for token in tableTokens:
        if len(token) != 0:
            table.append(token)

    counter = 0
    cols = []
    for column in table:
        if counter != 0:
            cols.append(column)
        counter = counter + 1



    if checkTable(table[0]):
        if len(table) > 1:
            #with columns
            if not checkColumns(table[0],cols):
                print('invalid column encountered')
            else:
                if len(cols) != len(values):
                    print('values supplied did not match the number of columns')
                else:
                    if table[0] == 'STUDENT':
                        tableData = studentDB
                    executionEngine.insertQuery(table[0],cols,values)
        else:
            #without columns
            if table[0] == 'STUDENT':
                    tableData = studentDB
            executionEngine.insertQuery(table[0],cols, values)

    else:
        print('invalid table ' + table[0])



def deleteQuery(query):
    table = query[2]
    if checkTable(table):
        if (len(query)) == 4 :
            whereDelete = whereExtractor(table,query[3])
            if len(whereDelete) == 0:
                print('Invalid column encountered in where clause')
            else:
                where = whereDelete
                executionEngine.deleteQuery(table,where)
        else:
            where =[]
            executionEngine.deleteQuery(table,where)

            #whereDelete = whereExtractor(table,query[3])
            #if len(whereDelete) == 0:
            #	print('Invalid column encountered in where clause')
            #else:
            #	where = whereDelete
            #	executionEngine.deleteQuery(table,where)
        #else:
        #	print(query)
    else:
        print('invalid table ' + table)



def checkTable(tableArg):
    table = ['STUDENT','STUDENTHISTORY','COURSE','COURSEOFFERING','STUDCOURSE']
    valid = tableArg in table
    if valid == False:
        return  False
    else:
        return True


def checkColumns(tableArg, column):	
    table = {}
    table['STUDENT'] = ['*','StudNo', 'StudentName','Birthday','Degree','Major','UnitsEarned']
    table['STUDENTHISTORY'] =['*','StudNo','Description','Action','DateFiled','DateResolved']
    table['COURSE'] =['*','CNo','CTitle', 'CDesc','NoOfUnits','HasLab','SemOffered']
    table['COURSEOFFERING'] = ['*','Semester','AcadYear','CNo','Section','Time','MaxStud']
    table['STUDCOURSE'] = ['*','StudNo','CNo','CDesc','Semester','AcadYear']
    data = table[tableArg]

    error = ''
    for cols in column:
        valid = cols in data
        error = False
        if valid == False:
            error =False
            return False
            break
        else:
            error = True

    return error



def whereExtractor(table, query):

    logical_op = ['!','=','>','<']
    counter = 0
    index = -1

    if '!' in query:
        index = query.index('!')

    if '<' in query:
        index = query.index('<')

    if '>' in query:
        index = query.index('>')

    if '=' in query:
        index = query.index('=')


    starter = index +1
    valueCondition = ""

    for start in range(starter,len(query)):

        valueCondition = valueCondition + query[start]


    valueCondition = valueCondition.replace(';','')
    valueCondition = valueCondition.strip()

    #https://stackoverflow.com/questions/6116978/python-replace-multiple-strings
    rep = {"=":" = ", "<":" < ", ">" :" > ", "!" :" ! ", ">=":" > = ","<=":" < = "} # define desired replacements here

    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))




    tokens = []
    for data in query.split(" "):

        newStr = data.replace(';','')
        text = pattern.sub(lambda m: rep[re.escape(m.group(0))], newStr)

        if not text.isspace():

            split = text.split(" ")
            for tok in split:
                if len(tok) !=0:
                    tokens.append(tok)



    targets = ['where', 'or', 'and']

    cols =[]

    for target in targets:

        if target in tokens:
            colIndex = tokens.index(target) + 1
            cols.append(tokens[colIndex])


    if not checkColumns(table,cols):
        return []
    else:

        if '!' in tokens:
            index = tokens.index('!')

        if '<' in tokens:
            index = tokens.index('<')

        if '>' in tokens:
            index = tokens.index('>')

        if '=' in tokens:
            index = tokens.index('=')


        newToken = []
        for ind in range(index+1):

            newToken.append(tokens[ind])

        newToken.append(valueCondition)

        return newToken


def orderByExtractor(tableName,orderByQuery):
    tokens =[]

    for data in orderByQuery.split(" "):
        tokens.append(data)


    if not checkColumns(tableName,[tokens[0]]):

        return []
    else:
        return tokens






