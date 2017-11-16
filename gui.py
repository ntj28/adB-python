import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import filedialog
import sys 
from tkinter import * 
from antlr4 import *
from SQLiteLexer import SQLiteLexer
from SQLiteParser import SQLiteParser
from SQLiteListener import SQLiteListener
from antlr4.error.ErrorListener import ErrorListener
from antlr4.tree.Trees import Trees
from antlr4.error.ErrorListener import ErrorListener
from InputStream import InputStream
import sqlparse
import os
import sys
import evaluateExpression
import executionEngine
import datetime
import pandas as pd
import time


pd.set_option('display.width', None)
pd.set_option('display.max_rows', None)


class MyErrorListener( ErrorListener ):

    def __init__(self):
        super(MyErrorListener, self).__init__()

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise Exception(offendingSymbol)
        


root = Tk()
root.title('aDB - Python-Based SQL Application')

def executeQuery():
    inputString = sqlEntry.get()

 
    if sqlEntry.index("end") != 0:     
        sqlEntry.delete(0, 'end')

    if outputQuery.index("end") != 0:

        outputQuery.configure(state='normal')
        outputQuery.delete(1.0, 'end')
        outputQuery.configure(state='disabled')
    
    
    if inputString == 'clear':
            os.system('clear')
    elif inputString == 'q':
        quit()
    
    elif inputString[-1] != ";":
        print ("semicolon missing")

    else:

           
        try :

            inputQ = InputStream(inputString)

            start_time = time.time()
            #use the lexer to check the tokens
            lexer = SQLiteLexer(inputQ)
            stream = CommonTokenStream(lexer)
            
            #initialize parser to check semantics and adds error listener
            parser = SQLiteParser(stream)
            parser._listeners = [ MyErrorListener() ]



            #start parsing
            tree = parser.parse()

       

            parsed = sqlparse.parse(inputString)[0]
            print(parsed)



            #initialize array of tokens
            tokenArray = []       
            for token in parsed.tokens :            
                data = str(token)
               
                #escapes whitespaces and semicolon
                if not data.isspace()  and data != ';':
                    tokenArray.append(data) 

            print(tokenArray)
            evaluateExpression.evaluateQuery(tokenArray)

            print("\n--- execution time in seconds : %s ---" % (time.time() - start_time))
                             



        except Exception as error:
            print("error : "+str(error))


def center(toplevel):
    toplevel.update_idletasks()
    w = toplevel.winfo_screenwidth()
    h = toplevel.winfo_screenheight()
    size = tuple(int(_) for _ in toplevel.geometry().split('+')[0].split('x'))
    x = w/2 - size[0]/2
    y = h/2 - size[1]/2
    toplevel.geometry("%dx%d+%d+%d" % (size + (x, y)))

def askopenfile():
        file =  filedialog.askopenfile(mode='r')
        
        start_time = time.time()
        data = pd.read_csv(file)



        tableData = evaluateExpression.setData(optionSelected.get(),[],True)
        tableData = tableData.append([data],ignore_index=True)
        evaluateExpression.setData(optionSelected.get(),tableData,False)
        if outputQuery.index("end") != 0:
              
                outputQuery.configure(state='normal')
                outputQuery.delete(1.0, 'end')
                outputQuery.configure(state='disabled')

        print("--- execution time in seconds : %s ---" % (time.time() - start_time))

       

def backup():
    directory = filedialog.askdirectory()
   

    start_time = time.time()

    #commit updates to csv files
    tableDataCourse = evaluateExpression.setData('COURSE',[],True)
    tableDataCourse.to_csv(directory + '/COURSE.csv', index=False)

    tableDataCourseOffering = evaluateExpression.setData('COURSEOFFERING',[],True)
    tableDataCourseOffering.to_csv(directory + '/COURSEOFFERING.csv', index=False)

    tableDataStudCourse = evaluateExpression.setData('STUDCOURSE',[],True)
    tableDataStudCourse.to_csv(directory + '/STUDCOURSE.csv', index=False)

    tableDataStudent = evaluateExpression.setData('STUDENT',[],True)
    tableDataStudent.to_csv(directory + '/STUDENT.csv', index=False)

    tableDataStudentHistory = evaluateExpression.setData('STUDENTHISTORY',[],True)
    tableDataStudentHistory.to_csv(directory + '/STUDENTHISTORY.csv', index=False)

    print("\n--- execution time in seconds : %s ---" % (time.time() - start_time))

def quit():
    #commit updates to csv files
    tableDataCourse = evaluateExpression.setData('COURSE',[],True)
    tableDataCourse.to_csv('./data/COURSE.csv', index=False)

    tableDataCourseOffering = evaluateExpression.setData('COURSEOFFERING',[],True)
    tableDataCourseOffering.to_csv('./data/COURSEOFFERING.csv', index=False)

    tableDataStudCourse = evaluateExpression.setData('STUDCOURSE',[],True)
    tableDataStudCourse.to_csv('./data/STUDCOURSE.csv', index=False)

    tableDataStudent = evaluateExpression.setData('STUDENT',[],True)
    tableDataStudent.to_csv('./data/STUDENT.csv', index=False)


    tableDataStudentHistory = evaluateExpression.setData('STUDENTHISTORY',[],True)
    tableDataStudentHistory.to_csv('./data/STUDENTHISTORY.csv', index=False)

    root.quit()

def displayTable(optionSelected):
    start_time = time.time()
    tableData = evaluateExpression.setData(optionSelected,[],True)
    if outputQuery.index("end") != 0:
               
                outputQuery.configure(state='normal')
                outputQuery.delete(1.0, 'end')
                outputQuery.configure(state='disabled')
        
    print(tableData.replace(np.nan, '', regex=True))
    print("\n--- execution time in seconds : %s ---" % (time.time() - start_time))
   



w, h = root.winfo_screenwidth(), root.winfo_screenheight()
root.geometry("%dx%d+0+0" % (w, h))


optionSelected = StringVar(root)
viewSelected = StringVar(root)
viewSelected.set("Tables")
optionSelected.set("COURSE")
sqlquery= Label(root, text="Enter SQL query or type 'q' to quit ! And press enter key to execute. You may also select a table to display its contents.", font = "Verdana 10 bold")
sqlquery.configure(background='SpringGreen2')


sqlquery.place(x = 50, y = 5 , width=1200, height=25)
sqlEntry = Entry(root)
sqlEntry.focus()
sqlEntry.place(x =50, y = 40 , width=1050, height=25)

viewtOptions = OptionMenu(root, viewSelected,"COURSE", "COURSEOFFERING", "STUDCOURSE", "STUDENT","STUDENTHISTORY",command=displayTable)
viewtOptions.place(x =1102, y= 40 , width=150, height=25)
scrollbarY = Scrollbar(root)
scrollbarY.pack(side=RIGHT)
scrollbarY.place(x =1235, y = 70 , width=15, height=550)

scrollbar = tk.Scrollbar(root,orient="horizontal")
outputQuery = Text(root, state='disabled',wrap=tk.NONE)
outputQuery['yscrollcommand'] =  scrollbarY.set
outputQuery['xscrollcommand'] =  scrollbar.set

scrollbarY.config( command = outputQuery.yview )
outputQuery.place(x =50, y = 70 , width=1187, height=550)
scrollbar.pack( side='bottom')
scrollbar.place(x =50, y = 620 , width=1200, height=15)
scrollbar.config(command=outputQuery.xview)
outputQuery.config()
importQuery= Label(root, text="Import Data", font = "Verdana 10 bold")
importQuery.configure(background='SpringGreen2')
importQuery.place(x = 50, y =650 , width=1200, height=25)
importOptions = OptionMenu(root, optionSelected, "COURSE", "COURSEOFFERING", "STUDCOURSE", "STUDENT","STUDENTHISTORY")
importOptions.place(x = 50, y =680 , width=350, height=25)
importButton = Button( text='Browse', command=askopenfile)
importButton.place(x = 475, y =680 , width=350, height=25)
backupButton = Button( text='Create Backup', command=backup)
backupButton.place(x = 900, y =680 , width=350, height=25)




def func(event):
    executeQuery()
root.bind('<Return>', func)


class PrintToT1(object): 
	def write(self, s):
	 	outputQuery.configure(state='normal')
	 	outputQuery.insert(END, s)
	 	outputQuery.configure(state='disabled')

sys.stdout = PrintToT1() 



mainloop() 
