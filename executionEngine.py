import pandas as pd
import numpy as np
import evaluateExpression
import datetime
import numpy as np


def readData(table):
	location = './data/'+ table +'.csv'
	data = pd.read_csv(location)
	return data

def dataTypeChecker(column):
	dataType = {}	
	dataType['StudNo'] = 'id'	
	dataType['StudentName'] = 'varchar'	
	dataType['Birthday'] = 'date'	
	dataType['Degree'] = 'varchar'	
	dataType['Major'] = 'varchar'	
	dataType['UnitsEarned'] = 'int'	
	dataType['Desciption'] = 'varchar'	
	dataType['Action'] = 'varchar'	
	dataType['DateFiled'] = 'date'	
	dataType['DateResolved'] = 'date'	
	dataType['CNo'] = 'int'	
	dataType['CTitle'] = 'varchar'	
	dataType['CDesc'] = 'varchar'	
	dataType['NoOfUnits'] = 'int'	
	dataType['HasLab']  = 'int'	
	dataType['SemOffered'] = 'enum'	
	dataType['Semester'] = 'varchar'	
	dataType['AcadYear'] = 'varchar'	
	dataType['Section'] = 'varchar'	
	dataType['Time'] = 'varchar'	
	dataType['MaxStud'] = 'int'
	typeData = dataType[column]

	return typeData


def getColumns(tableName):
	table = {}	
	table['STUDENT'] = ['StudNo', 'StudentName','Birthday','Degree','Major','UnitsEarned']
	table['STUDENTHISTORY'] =['StudNo','Description','Action','DateFiled','DateResolved']
	table['COURSE'] =['CNo','CTitle', 'CDesc','NoOfUnits','HasLab','SemOffered']
	table['COURSEOFFERING'] = ['Semester','AcadYear','CNo','Section','Time','MaxStud']
	table['STUDCOURSE'] = ['StudNo','CNo','Semester','AcadYear']
	cols = table[tableName]
	return cols

def lengthChecker(query):
	return len(query)

def selectQuery(tableName,columns,whereClause,orderby):	
	tableData = evaluateExpression.setData(tableName,[],True)
	table = {}		
	table['STUDENT'] = ['StudNo', 'StudentName','Birthday','Degree','Major','UnitsEarned']
	table['STUDENTHISTORY'] =['StudNo','Description','Action','DateFiled','DateResolved']
	table['COURSE'] =['CNo','CTitle', 'CDesc','NoOfUnits','HasLab','SemOffered']
	table['COURSEOFFERING'] = ['Semester','AcadYear','CNo','Section','Time','MaxStud']
	table['STUDCOURSE'] = ['StudNo','CNo','Semester','AcadYear']
	cols = table[tableName]
	

	if columns[0] =='*':
		columns = cols
		
	
	if whereClause == 'null' and orderby == 'null':	
		tableData = tableData[columns].replace(np.nan, '', regex=True)

		print(tableData.head(tableData.shape[0]))
		print('\nNumber of rows returned: ' + str(tableData.shape[0]))
		print('Number of columns returned: ' + str(tableData.shape[1]))
		
	#only order by specified in the  query
	elif whereClause == 'null' and orderby != 'null':		
		if orderby[1] == 'asc':		
			tableData = tableData.sort_values([orderby[0]], ascending=True)			
			
		else:
			tableData = tableData.sort_values([orderby[0]], ascending=False)			
		tableData = tableData[columns].replace(np.nan, '', regex=True)
		print(tableData)

	#only where clause is specified
	elif whereClause != 'null' and orderby == 'null' :
				
		if len(whereClause) == 4 :
			newStr = whereClause[3].replace("'",'')			
			newStr = newStr.replace('"','')


			if whereClause[2] == '=':
				if dataTypeChecker(whereClause[1]) == 'int':					
					tableData= tableData[tableData[whereClause[1]].astype(float) == int(newStr)]#.head()
					print(tableData[columns].replace(np.nan, '', regex=True))
				elif dataTypeChecker(whereClause[1]) == 'varchar':
					if len(newStr) > 50 :
						print('values must be less than 50 characters')
					else:					
						tableData= tableData[tableData[whereClause[1]] == newStr]#.head()
						print(tableData[columns].replace(np.nan, '', regex=True))
				elif dataTypeChecker(whereClause[1]) == 'id':
					if '-' in newStr:
						tokens = newStr.split('-')
						if len(tokens) != 2 :
							raise Exception('invalid format')
						else :
							tok1 = int(tokens[0])
							tok2 = int(tokens[1])
							if len(str(tok1)) != 4:
								raise Exception('invalid format')
							else:
								if len(str(tok2)) != 5:
									raise Exception('invalid format')
							tableData= tableData[tableData[whereClause[1]] == newStr]#.head()
							print(tableData[columns].replace(np.nan, '', regex=True))
				elif dataTypeChecker(whereClause[1])== 'date' or  dataTypeChecker(whereClause[1])== 'time':
					tableData= tableData[tableData[whereClause[1]] == newStr]#.head()
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					raise Exception('invalid format')

			elif whereClause[2] == '<':

				if dataTypeChecker(whereClause[1]) == 'int':					
					tableData= tableData[tableData[whereClause[1]].astype(float) < int(newStr)]#.head()
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform lesser than operation on non-numeric column')

			elif whereClause[2] == '>':
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) > int(newStr)]#.head()
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform greater than operation on non-numeric column')

				

		elif len(whereClause) == 5 :
			
			newStr = whereClause[4].replace("'",'')
			newStr = newStr.replace('"','')
			

			if whereClause[2] == '<':
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) <= int(newStr)]#.head()
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform greater than operation on non-numeric column')

				
			elif whereClause[2] == '>':
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) >= int(newStr)]#.head()
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform greater than operation on non-numeric column')

				
			
			elif whereClause[2] == '!':
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) != int(newStr)]#.head()
					print(tableData[columns].replace(np.nan, '', regex=True))
				elif dataTypeChecker(whereClause[1]) == 'varchar':
					if len(newStr) > 50 :
						print('values must be less than 50 characters')
					else:
						tableData= tableData[tableData[whereClause[1]] != newStr]#.head()
						print(tableData[columns].replace(np.nan, '', regex=True))
				

	#both where and order by are specified
	elif whereClause != 'null' and orderby != 'null' :
		
		if len(whereClause) == 4 :			
			newStr = whereClause[3].replace("'",'')			
			newStr = newStr.replace('"','')			
			newString = ''
			for char in range(0, len(newStr)-1):
				newString = newString + newStr[char]	

			newStr = newString

			if whereClause[2] == '=':
				
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) < int(newStr)]#.head()
					if orderby[1] == 'asc':						
						tableData = tableData.sort_values([orderby[0]], ascending=True)			
						
					else:
						tableData = tableData.sort_values([orderby[0]], ascending=False)
					print(tableData[columns].replace(np.nan, '', regex=True))
				elif dataTypeChecker(whereClause[1]) == 'varchar':
					if len(newStr) > 50 :
						print('values must be less than 50 characters')
					else:						
						tableData= tableData[tableData[whereClause[1]] == newStr ]#.head()
						if orderby[1] == 'asc':							
							tableData = tableData.sort_values([orderby[0]], ascending=True)			
							
						else:
							tableData = tableData.sort_values([orderby[0]], ascending=False)
						print(tableData[columns].replace(np.nan, '', regex=True))

			elif whereClause[2] == '<':

				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) < int(newStr)].head()
					if orderby[1] == 'asc':						
						tableData = tableData.sort_values([orderby[0]], ascending=True)			
						
					else:
						tableData = tableData.sort_values([orderby[0]], ascending=False)
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform lesser than operation on non-numeric column')

			elif whereClause[2] == '>':				
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) > int(newStr)]#.head()
					if orderby[1] == 'asc':						
						tableData = tableData.sort_values([orderby[0]], ascending=True)			
						
					else:
						tableData = tableData.sort_values([orderby[0]], ascending=False)
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform greater than operation on non-numeric column')
		elif len(whereClause) == 5 :			
			newStr = whereClause[4].replace("'",'')
			newStr = newStr.replace('"','')
			

			if whereClause[2] == '<':
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) <= int(newStr)]#.head()
					if orderby[1] == 'asc':						
						tableData = tableData.sort_values([orderby[0]], ascending=True)			
						
					else:
						tableData = tableData.sort_values([orderby[0]], ascending=False)
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform greater than operation on non-numeric column')

				
			elif whereClause[2] == '>':
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) >= int(newStr)]#.head()
					if orderby[1] == 'asc':						
						tableData = tableData.sort_values([orderby[0]], ascending=True)			
						
					else:
						tableData = tableData.sort_values([orderby[0]], ascending=False)
					print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					print('Cannot perform greater than operation on non-numeric column')

				
			elif whereClause[2] == '!':
				if dataTypeChecker(whereClause[1]) == 'int':
					tableData= tableData[tableData[whereClause[1]].astype(float) != int(newStr)]#.head()
					if orderby[1] == 'asc':						
						tableData = tableData.sort_values([orderby[0]], ascending=True)			
						
					else:
						tableData = tableData.sort_values([orderby[0]], ascending=False)
					print(tableData[columns].replace(np.nan, '', regex=True))
				elif dataTypeChecker(whereClause[1]) == 'varchar':
					if len(newStr) > 50 :
						print('values must be less than 50 characters')
					else:
						tableData= tableData[tableData[whereClause[1]] != newStr]#.head()
						if orderby[1] == 'asc':						
							tableData = tableData.sort_values([orderby[0]], ascending=True)			
							
						else:
							tableData = tableData.sort_values([orderby[0]], ascending=False)
						print(tableData[columns].replace(np.nan, '', regex=True))
	 

def splitWhereQ(query):

	targets = ['where', 'or', 'and']	
	tokens =[]	
	opIndex =[]
	if 'where' in query:
		whereIndex = query.index('where')

	if 'and' in query:
		opIndex.append(query.index('and'))

	if 'or' in query:
		opIndex.append(query.index('or'))

	
	
	operators = ['=','!','<','>']

	counter = 0
	for token in query:
	
		tokCombined =[]
		if counter+1 < len(query):
			if query[counter+1] in operators and token in operators:
				
				tokens.append(token + ''+query[counter+1])
				counter = counter + 1

			else:
				tokens.append(token)
				
		counter = counter + 1


	return tokens

def validateDate(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

def validateTime(date_text):
    try:
        datetime.datetime.strptime(date_text, '%H:%M')
    except ValueError:
        raise ValueError("Incorrect time format, should be H:M")



def insertDataTypeChecker(table,columns,values):
	if len(columns) == 0 :
		columns = getColumns(table)
	counter =-1

	cno=''
	semester=''
	acadyear=''
	section=''
	try:

		for value in values:
			counter = counter + 1
			if dataTypeChecker(columns[counter]) == 'int':
				int(value)			
			elif dataTypeChecker(columns[counter]) == 'varchar':
				if len(value) > 50:
					raise Exception('must be of 50 charcaters or less')

				if (columns[counter]=='CNo' and table=='COURSE'):

					tableData = evaluateExpression.setData(table,[],True)
					# tableData= tableData[tableData[columns[counter]].astype(float) == int(value)]
					tableData= tableData[tableData[columns[counter]] == value]#.head()
					cno=value
					if(tableData.shape[0]>0):
						raise Exception('CNo already exists.')
				if (columns[counter]=='Semester'):
					semester=value
				if (columns[counter]=='AcadYear'):
					acadyear=value
				if (columns[counter]=='Section'):
					section=value

			elif dataTypeChecker(columns[counter]) == 'date':
				datetime.datetime.strptime(value.strip(), '%Y-%m-%d')
			elif dataTypeChecker(columns[counter]) == 'enum':
				values = ['1st','2nd','Sum']
				value= value.strip()
				if not (value in values):					
					raise Exception('Acceptable values are { 1st, 2nd , Sum }' )


			elif dataTypeChecker(columns[counter]) == 'time':
				datetime.datetime.strptime(value, '%H:%M')
			elif dataTypeChecker(columns[counter]) == 'id':
				if '-' in value:
					tokens = value.split('-')
					if len(tokens) != 2 :
						raise Exception('invalid format')
					else :
						tok1 = int(tokens[0])
						tok2 = int(tokens[1])
						if len(str(tok1)) != 4:
							raise Exception('invalid format')
						else:
							if len(str(tok2)) != 5:
								raise Exception('invalid format')
					tableData = evaluateExpression.setData(table,[],True)
					tableData= tableData[tableData[columns[counter]] == value]#.head()
					# print('\nNumber of rows returned: ' + str(tableData.shape[0]))
					if(tableData.shape[0]>0):
						raise Exception('StudNo already exists.')

					# print(tableData[columns].replace(np.nan, '', regex=True))
				else:
					raise Exception('invalid format')
		# if (table=='COURSEOFFERING'):


	except Exception as error:
		print(error)
		if(str(error)=='StudNo already exists.'):
			print(error)
		elif(str(error)=='CNo already exists.'):
			print(error)
		else:
			if dataTypeChecker(columns[counter]) == 'int':
				print("value { "+ value +" } must be of  { int } format for the column { "+ columns[counter] +" } ")
			elif dataTypeChecker(columns[counter]) == 'varchar':
				print ('value {' + value + ' } must be 50 characters or less')
			elif dataTypeChecker(columns[counter])  == 'date':
				print('value {' + value + ' } must be in { YYYY-MM-DD } format')
			elif dataTypeChecker(columns[counter])  == 'time':
				print('value { ' + value + ' } must be in { H:M } format')
			elif dataTypeChecker(columns[counter])  == 'id':
				print('Student Number value { ' + value + ' } must be in { YYYY-XXXXX } format')
			elif dataTypeChecker(columns[counter])  == 'enum':
				print('Acceptable values are { 1st, 2nd , Sum }' )

def insertQuery(table,columns, values):
	tableData = evaluateExpression.setData(table,[],True)
	insertDataTypeChecker(table,columns,values)

	data = {}
	counter = 0
	if len(columns) != 0:
		for cols in columns:
			data[cols] = values[counter]
			counter = counter + 1
	else:
		cols = getColumns(table)		
		counter = 0
		for value in values:
			data[cols[counter]] = value
			counter = counter + 1	
	
	tableData = tableData.append([data],ignore_index=True)
	evaluateExpression.setData(table,tableData,False)
	


def deleteQuery(tableName, whereClause):
	tableData = evaluateExpression.setData(tableName,[], True)
	print(whereClause)

	if len(whereClause) == 0 :
		columns = getColumns(tableName)
		tableData= pd.DataFrame(columns=columns)#tableData.loc[tableData]
		evaluateExpression.setData(tableName,tableData,False)


	elif len(whereClause) == 4 :
		newStr = whereClause[3].replace("'",'')
		newStr = newStr.replace('"','')
		if whereClause[2] == '=':			
			if dataTypeChecker(whereClause[1]) == 'int':				
				tableData= tableData.loc[tableData[whereClause[1]].astype(float) != int(newStr)]#.head()
				
			else:				
				tableData= tableData.loc[tableData[whereClause[1]] != newStr]#.head()
			

		elif whereClause[2] == '<':

			if dataTypeChecker(whereClause[1]) == 'int':
				tableData= tableData.loc[tableData[whereClause[1]].astype(float) >= int(newStr)]#.head()
			
			else:
				print('Cannot perform lesser than operation on non-numeric column')

		elif whereClause[2] == '>':
			
			if dataTypeChecker(whereClause[1]) == 'int':
				tableData= tableData.loc[tableData[whereClause[1]].astype(float) <= int(newStr)]#.head()
				
			else:
				print('Cannot perform greater than operation on non-numeric column')

		evaluateExpression.setData(tableName,tableData,False)
	elif len(whereClause) == 5 :
		
		newStr = whereClause[4].replace("'",'')
		newStr = newStr.replace('"','')
		

		if whereClause[2] == '<':
			if dataTypeChecker(whereClause[1]) == 'int':
				tableData= tableData.loc[tableData[whereClause[1]].astype(float) > int(newStr)]#.head()
				
				
			else:
				print('Cannot perform greater than operation on non-numeric column')

			
		elif whereClause[2] == '>':
			if dataTypeChecker(whereClause[1]) == 'int':
				tableData= tableData.loc[tableData[whereClause[1]].astype(float) < int(newStr)]#.head()
				
			else:
				print('Cannot perform greater than operation on non-numeric column')

			
		elif whereClause[2] == '!':
			if dataTypeChecker(whereClause[1]) == 'int':
				tableData = tableData[tableData[whereClause[1]].astype(float) == int(newStr)]#.head()
			
			else:
				tableData = tableData[tableData[whereClause[1]] == newStr]#.head()
				
		evaluateExpression.setData(tableName,tableData,False)
