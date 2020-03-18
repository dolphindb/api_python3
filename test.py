import dolphindb as ddb
import datetime as date
import pandas as pd
import numpy as np

caseCount = 0
failedCount = 0
succCount = 0

def AssertTrue(exp, title, testingCase):
    global caseCount
    global failedCount
    global succCount
    caseCount += 1
    if exp:
        print(testingCase, ' ' , title, ': success.')
        succCount += 1
    else:
        print(testingCase, title, ': failed.' )
        failedCount += 1

def printResult():
    print("testing finished: failed cases/all cases is ", failedCount, '/', caseCount)

s = ddb.session()
s.connect("localhost",8081,"admin","123456")

case = "testStringVector"
vector = s.run("`IBM`GOOG`YHOO");
AssertTrue((vector==['IBM','GOOG','YHOO']).all(), '1', case)
   
case = "testFunctionDef"
obj = s.run("def(a,b){return a+b}")
AssertTrue(len(obj)>0,"1",case)

case = "testSymbolVector"
vector = s.run("rand(`IBM`MSFT`GOOG`BIDU,10)")
AssertTrue(len(['IBM','GOOG','YHOO'])>0, '1', case)

case = "testIntegerVector"
vector = s.run("2938 2920 54938 1999 2333")
AssertTrue((vector==[2938,2920,54938,1999,2333]).all(), '1', case)

case = "testDoubleVector"
vector = s.run("rand(10.0,10)")
AssertTrue(len(vector)==10, '1', case)

case = "testDateVector"
vector = s.run("2012.10.01 +1..3")
AssertTrue((vector==[np.datetime64('2012-10-02', dtype='datetime64[D]'), np.datetime64('2012-10-03', dtype='datetime64[D]'), np.datetime64('2012-10-04', dtype='datetime64[D]')]).all(), '1', case)

case = "testDateVector"
vector = s.run("2012.10.01T15:00:04 + 2009..2011")
AssertTrue((vector==[np.datetime64('2012-10-01T15:33:33', dtype='datetime64[s]'), np.datetime64('2012-10-01T15:33:34', dtype='datetime64[s]'), np.datetime64('2012-10-01T15:33:35', dtype='datetime64[s]')]).all(), '1', case)

case = "testDateTimeVector"
vector = s.run("2012.10.01T15:00:04 + 2009..2011")
AssertTrue((vector==[np.datetime64('2012-10-01T15:33:33', dtype='datetime64[s]'), np.datetime64('2012-10-01T15:33:34', dtype='datetime64[s]'), np.datetime64('2012-10-01T15:33:35', dtype='datetime64[s]')]).all(), '1', case)

case = "testIntMatrix"
matx = s.run("1..6$2:3")
# print(matx[0])
# print(np.array([[1, 3, 5],[2, 4, 6]]))
AssertTrue((matx[0]==np.array([[1, 3, 5],[2, 4, 6]])).all(), '1', case)

case = "testIntMatrixWithLabel"
matx = s.run("cross(add,1..5,1..10)")
# print(matx)
# print(np.array([[1, 3, 5],[2, 4, 6]]))
AssertTrue((matx[0]==np.array([[ 2,  3,  4,  5,  6,  7,  8,  9, 10, 11],[ 3,  4,  5,  6,  7,  8,  9, 10, 11, 12],[ 4,  5,  6,  7,  8,  9, 10, 11, 12, 13], [ 5,  6,  7,  8,  9, 10, 11, 12, 13, 14],[ 6,  7,  8,  9, 10, 11, 12, 13, 14, 15]])).all(), '1', case)

case = "testTable"
script = '''n=20;
		syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN;
		mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price);
		select qty,price from mytrades where sym==`IBM;'''
		
table = s.run(script);
AssertTrue(table.shape[1]==2 , "1", case)

case = "testDictionary"
script = '''dict(1 2 3,`IBM`MSFT`GOOG)'''
dic = s.run(script);
# print(dic)
AssertTrue(dic[2]=='MSFT' , "1", case)
AssertTrue(dic[1]=='IBM' , "2", case)
AssertTrue(dic[3]=='GOOG' , "3", case)

# case = "testFunction"
# array = {1.5, 2.5, 7};
# result = s.run("sum", array);
# print(result)

case = "testFunction1"
s.upload({'a':[1.5,2.5,7]})
result = s.run("accumulate(+,a)");
AssertTrue((result==[1.5,4.0,11.0]).all(),'1',case)

case = "testFunction2"
s.run("login('admin','123456')");
s.run("def Foo5(a,b){ f=file('testsharp.csv','w');f.writeLine(string(a)); }");
args = [1.3,1.4];
result = s.run("Foo5",1.3,1.4);

case = "testAnyVector"
obj = s.run("{1, 2, {1,3, 5},{0.9, 0.8}}")
AssertTrue((obj[2]==[1,3,5]).all(),"1",case)


case ="testSet"
obj = s.run("set(1+3*1..3)")
AssertTrue(obj=={10, 4, 7},"1", case)

case="testMatrixUpload"
a = s.run("cross(+, 1..5, 1..5)")
b = s.run("1..25$5:5")
s.upload({'a':a,'b':b});
mtx = s.run('a+b')
print(mtx[0])
print(mtx[1])
print(mtx[2])
printResult()