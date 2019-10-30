import dolphindb as ddb
import datetime as date
import pandas as pd
import numpy as np
s = ddb.session()
s.connect("localhost",8081,"admin","123456")
dates = '2019.08.01..2019.08.10'
syms = '`IBM`AAPL`MSFT`GS`YHOO`TS`TSL`C`MC`HA'
script='data = table(1:0,`date`sym`price, [DATE,SYMBOL,DOUBLE])'
print(script)
t = s.run(script)
##创建分布式分区表
userName = 'admin'
pwd = '123456'
symRange = 'cutPoints({a},3)'.format(a=syms)
dbPath = 'dfs://demo'
tableName = 'testTableName'
if(s.existsDatabase(dbPath)):
    s.dropDatabase(dbPath)
lgnScript = 'login("{a}","{b}")'.format(a=userName,b=pwd)
dbDateScript='dbDate=database("",VALUE,{a})'.format(a=dates)
dbSymScript='dbSym=database("",RANGE,{b})'.format(b=symRange)
script= '{login};{db1};{db2};db = database("{db}",COMPO,[dbDate,dbSym])'.format(login=lgnScript, db1=dbDateScript, db2=dbSymScript, db=dbPath)
s.run(script)
## 向分布式数据库写入数据
script='db.createPartitionedTable(data,`{a}, `date`sym)'.format(a=tableName)
print(script)
s.run(script)

loadScript = 'tb = loadTable("{a}","{b}")'.format(a=dbPath,b=tableName)
s.run(loadScript)
script = 'tableInsert{tb}'
print(script)
dataTable = pd.DataFrame({'date':[np.datetime64('2019-08-01', dtype='datetime64[D]'),np.datetime64('2019-08-02', dtype='datetime64[D]')],'sym':['MSFT','GS'],'price':[90,89]})
s.run(script, dataTable)

script='select * from loadTable("{a}","{b}")'.format(a=dbPath,b=tableName)
re = s.run(script)
print(re)
