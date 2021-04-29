# Python API for DolphinDB

DolphinDB Python API supports Python 3.6 - 3.8.

本教程目录如下：

- [1. Execute DolphinDB script and call DolphinDB functions](#1-运行dolphindb脚本及其函数调用)
    - [1.1 Establish DolphinDB connection](#11-建立dolphindb连接)
    - [1.2 Execute DolphinDB script](#12-运行dolphindb脚本)
    - [1.3 Call DolphinDB functions](#13-运行dolphindb函数)
- [2. Upload Python objects to DolphinDB server](#2-上传本地对象到dolphindb服务器)
    - [2.1 使用session的upload方法上传](#21-使用session的upload方法上传)
    - [2.2 Upload with function `table`](#22-使用table函数上传) 
- [3. Import data into DolphinDB databases](#3-导入数据到dolphindb数据库)
    - [3.1 Import data into in-memory tables](#31-把数据导入到内存表中)
    - [3.2 Import data into partitioned databases](#32-把数据导入到分区数据库中)
    - [3.3 Import data into partitioned in-memory tables](#33-把数据导入到内存的分区表中)
- [4. Load data from DolphinDB databases](#4-从dolphindb数据库中加载数据)
    - [4.1 Load data with function `loadTable`](#41-使用loadtable函数) 
    - [4.2 Load data with function `loadTableBySQL`](#42-使用loadtablebysql函数) 
    - [4.3 Data type conversion when downloading data from DolphinDB to Python](#43-从dolphindb下载数据到python时的数据转换) 
- [5. Append data to DolphinDB tables](#5-追加数据到dolphindb数据表)
    - [5.1 Append data to in-memory tables](#51-追加数据到dolphindb内存表)
    - [5.2 Append data to DFS tables](#52-追加数据到分布式表)
- [6. Database and table operations](#6-数据库和表操作)
    - [6.1 数据库和表的操作方法说明](#61-数据库和表的操作方法说明) 
    - [6.2 Database operations](#62-数据库操作)
    - [6.3 Table operations](#63-表操作)
- [7. SQL queries](#7-sql查询)
    - [7.1 select](#71-select)
    - [7.2 top](#72-top)
    - [7.3 where](#73-where)
    - [7.4 groupby](#74-groupby)
    - [7.5 contextby](#75-contextby)
    - [7.6 Table joins](#76-表连接)
    - [7.7 executeAs](#77-executeas)
    - [7.8 回归运算](#78-回归运算)
- [8. Python Streaming API](#8-python-streaming-api)
    - [8.1 指定订阅端口号](#81-指定订阅端口号)  
    - [8.2 Subscription](#82-订阅和反订阅)
    - [8.3 Streaming appications](#83-流数据应用)
- [9. 更多实例](#9-更多实例)
    - [9.1 动量交易策略](#91-动量交易策略) 
    - [9.2 时间序列操作](#92-时间序列操作) 

DolphinDB Python API in essense encapsulates a subset of DolphinDB's scripting language. It converts Python script to DolphinDB script to be executed on the DolphinDB server. The result can either be saved on DolphinDB server or be serialized to a Python client object. 

The examples in this tutorial use a csv file: [example.csv](data/example.csv).

## 1 Execute DolphinDB script and DolphinDB functions

### 1.1 Establish DolphinDB connection

Python interacts with DolphinDB through a Session object. The most commonly used Session class methods are as follows:

| Method        | Explanation          |
|:------------- |:-------------|
|connect(host,port,[username,password])|Connect a session to DolphinDB server|
|login(username,password,[enableEncryption])|log in DolphinDB server|
|run(script)|Execute script on DolphinDB server|
|run(functionName,args)|Call functions on DolphinDB server|
|upload(DictionaryOfPythonObjects)|Upload Python objects to DolphinDB server|
|undef(objName,objType)|Undefine an object in DolphinDB to release memory|
|undefAll()|Undefine all objects in DolphinDB to release memory|
|close()|Close the session|

In the following script, we first create a session in Python, then connect the session to a DolphinDB server with specified domain name/IP address and port number. We need to start a DolphinDB server before running the following Python script.

```python
import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848)
# output
True
```

Use the following script to connect to DolphinDB server with username and password:
```python
s.connect("localhost", 8848, YOUR_USER_NAME, YOUR_PASS_WORD)
```
or
```python
s.connect("localhost", 8848)
s.login(YOUR_USER_NAME,YOUR_PASS_WORD)
```
If a session was initialized without username and password, we can use the method `login` to log in DolphinDB server. The default username is 'admin' and the default password is '123456'. By default, the user name and password are encrypted during connection.

### 1.2 Execute DolphinDB script

All DolphinDB script can be executed through the `run(script)` method. If the script returns an object in DolphinDB, it will be converted to an object in Python. If the script fails to run, there will be a corresponding error prompt.
```python
a=s.run("`IBM`GOOG`YHOO")
repr(a)
# output
"array(['IBM', 'GOOG', 'YHOO'], dtype='<U4')"
```

User-defined functions can be generated with the `run` method:
```python
s.run("def getTypeStr(input){ \nreturn typestr(input)\n}")
```

The maximum length of the text in the `run` method is 65,535 bytes. For multiple lines of script, we can wrap them inside triple quotes for clarity. For example:
```python
script="""
def getTypeStr(input){
    return typestr(input)
}
"""
s.run(script)
s.run("getTypeStr", 1);
# output
'LONG'
```

### 1.3 Call DolphinDB functions

In addition to executing script, the `run` method can directly call DolphinDB built-in or user-defined functions on a remote DolphinDB server. For this usage, the first parameter of the `run` method is the function name and the subsequent parameters are the parameters of the function.

#### 1.3.1 Parameter passing

The following example shows a Python program calling DolphinDB built-in function `add` through method `run`. The `add` function has 2 parameters: x and y. Depending on whether the values of the parameters have been assigned at DolphinDB server, there are 3 ways to call the function:

- Both parameters have been assigned value at DolphinDB server

If both x and y have been assigned value at DolphinDB server in the Python program,
```python
s.run("x = [1,3,5];y = [2,4,6]")
```

then just use `run(script)`:
```python
a=s.run("add(x,y)")
repr(a)
# output
'array([3, 7, 11], dtype=int32)'
```

- Only one parameter has been assigned value at DolphinDB server

If only x has been assigned value at DolphinDB server in the Python program
```python
s.run("x = [1,3,5]")
```

and y is to be assigned value when calling `add`, we need to use [Partial Application](https://www.dolphindb.com/help/PartialApplication.html) to fix parameter x to function `add`. 
```python
import numpy as np

y=np.array([1,2,3])
result=s.run("add{x,}", y)
repr(result)
# output
'array([2,5,8])'

result.dtype
# output
dtype('int64')
```

- Both parameters are to be assigned value

```python
import numpy as np

x=np.array([1.5,2.5,7])
y=np.array([8.5,7.5,3])
result=s.run("add", x, y)
repr(result)
# output
'array([10., 10., 10.])'

result.dtype
# output
dtype('float64')
```

#### 1.3.2 Data types and forms of parameters

When calling DolphinDB's built-in functions through `run`, the parameters uploaded can be scalar, list, dict, numpy objects, pandas DataFrame and Series, etc.

> Note:
> 1. NumPy arrays can only be 1D or 2D. 
> 2. If a pandas DataFrame or Series object has an index, the index will be lost after the object is uploaded to DolphinDB. To keep the index, use the pandas DataFrame function `reset_index`.
> 3. If a parameter of a DolphinDB function is of temporal type, it should be converted to numpy.datetime64 type before uploading.

The following examples introduce the use of various types of Python objects as parameters. 

- list objects

  Add 2 Python lists with DolphinDB function `add`:
  ```python
  s.run("add",[1,2,3,4],[1,2,1,1])
  # output
  array([2, 4, 4, 5])
  ```

- NumPy objects

  - np.int
    ```python
    import numpy as np
    s.run("add{1,}",np.int(4))
    # output
    5
    ```
  - np.datetime64
   
    Python API将datetime64格式的数据转换成DolphinDB中对应的时间数据类型。对应关系如下表。
  
    |DolphinDB Type        | datetime64  |
    |:------------- |:-------------|
    |DATE|'2019-01-01'|
    |MONTH|'2019-01'|
    |DATETIME|'2019-01-01T20:01:01'|
    |TIMESTAMP|'2019-01-01T20:01:01.122'|
    |NANOTIMESTAMP|'2019-01-01T20:01:01.122346100'|

    ```python
    import numpy as np
    s.run("typestr",np.datetime64('2019-01-01'))
    # output
    'DATE'
    
    s.run("typestr",np.datetime64('2019-01'))
    # output
    'MONTH'
    
    s.run("typestr",np.datetime64('2019-01-01T20:01:01'))
    # output
    'DATETIME'
    
    s.run("typestr",np.datetime64('2019-01-01T20:01:01.122'))
    # output
    'TIMESTAMP'
    
    s.run("typestr",np.datetime64('2019-01-01T20:01:01.1223461'))
    # output
    'NANOTIMESTAMP'
    ```
    
    As TIME, MINUTE, SECOND and NANOTIME types in DolphinDB don't have information about dates, datetime64 type cannot be converted into these types directly in Python API. To generate these data types in DolphinDB from Python, we can upload the datetime64 type to DolphinDB server and then get rid of the date information. 
    
    ```python
    import numpy as np
    ts = np.datetime64('2019-01-01T20:01:01.1223461')
    s.upload({'ts':ts})
    s.run('a=nanotime(ts)')
    
    s.run('typestr(a)')
    # output
    'NANOTIME'
    
    s.run('a')
    # output
    numpy.datetime64('1970-01-01T20:01:01.122346100')
    ```
    Please note that in the last step of the example above, when the NANOTIME type in DolphinDB is downloaded to Python, Python will automatically add 1970-01-01 as the date part.
    
  - list of np.datetime64 objects

    ```python
    import numpy as np
    a=[np.datetime64('2019-01-01T20:00:00.000000001'), np.datetime64('2019-01-01T20:00:00.000000001')]
    s.run("add{1,}",a)
    # output
    array(['2019-01-01T20:00:00.000000002', '2019-01-01T20:00:00.000000002'], dtype='datetime64[ns]')
    ```

- pandas objects

  If a pandas DataFrame or Series object has an index, the index will be lost after the object is uploaded to DolphinDB. 

  - Series
    ```python
    import pandas as pd
    import numpy as np
    a = pd.Series([1,2,3,1,5],index=np.arange(1,6,1))
    s.run("add{1,}",a)
    # output
    array([2, 3, 4, 2, 6])
    ```

  - DataFrame
    ```python
    import pandas as pd
    import numpy as np
    a=pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 
                    'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 
                    'x': np.int32([5, 4, 3, 2, 1])},
                    index={'one','two','three','four','five'})
    s.upload({'a':a})
    s.run("typestr",a)
    # output
    'IN-MEMORY TABLE'
    
    s.run('a')
    # output
        id  value x
    0   1    7.8  5
    1   2    4.6  4
    2   3    5.1  3
    3   4    9.6  2
    4   3    0.1  1 
    ```

### 2 Upload local objects to DolphinDB server

If we need to use a local object repeatedly in DolphinDB, we can upload the local object to the DolphinDB server. When uploading, we need to specify the variable name.

### 2.1 Upload with Session method `upload`

The Python API provides method `upload` to upload Python objects to the DolphinDB server. The input of the method `upload` is a Python dictionary object. The key of the dictionary is the variable name in DolphinDB, and the value is a Python object, which can be Numbers, Strings, Lists, DataFrame, etc.

- Upload Python list

```python
a = [1,2,3.0]
s.upload({'a':a})
a_new = s.run("a")
print(a_new)
# output
[1. 2. 3.]

a_type = s.run("typestr(a)")
print(a_type)
# output
ANY VECTOR
```
Note that a Python list with different data types such as a=[1,2,3.0] will be recognized as an ANY vector after being uploaded to DolphinDB. For such cases, it is recommended to use numpy.array instead of list. With numpy.array, we can specify a single data type through a=numpy.array([1,2,3.0],dtype=numpy.double), so that after uploading, "a" is a vector of DOUBLE type.

- Upload NumPy array

```python
import numpy as np

arr = np.array([1,2,3.0],dtype=np.double)
s.upload({'arr':arr})
arr_new = s.run("arr")
print(arr_new)
# output
[1. 2. 3.]

arr_type = s.run("typestr(arr)")
print(arr_type)
# output
FAST DOUBLE VECTOR
```

- Upload pandas DataFrame

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.value.avg()"))
# output
5.44
```

### 2.2 Upload with method `table`

In Python, we can use the method `table` to create a DolphinDB table object and upload it to the server. The input of the method `table` can be a dictionary, DataFrame or table name in DolphinDB.

* Upload dict

The script below defines a function `createDemoDict()` to create a dictionary. 

```python
import numpy as np

def createDemoDict():
    return {'id': [1, 2, 2, 3],
            'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
            'price': [22, 3.5, 21, 26]}
```

Upload the dictionary to the DolphinDB server with the method `table`, and name the table as "testDict", then we can read the table with the method `loadTable` provided by the API.

```python
import numpy as np

# save the table to DolphinDB server as table "testDict"
dt = s.table(data=createDemoDict(), tableAliasName="testDict")

# load table "testDict" on DolphinDB server 
print(s.loadTable("testDict").toDF())

# output
   id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0
```

* Upload pandas DataFrame

The script below defines function `createDemoDataFrame()` to create a pandas DataFrame. 

```python
import pandas as pd

def createDemoDataFrame():
    data = {'cid': np.array([1, 2, 3], dtype=np.int32),
            'cbool': np.array([True, False, np.nan], dtype=np.bool),
            'cchar': np.array([1, 2, 3], dtype=np.int8),
            'cshort': np.array([1, 2, 3], dtype=np.int16),
            'cint': np.array([1, 2, 3], dtype=np.int32),
            'clong': np.array([0, 1, 2], dtype=np.int64),
            'cdate': np.array(['2019-02-04', '2019-02-05', ''], dtype='datetime64[D]'),
            'cmonth': np.array(['2019-01', '2019-02', ''], dtype='datetime64[M]'),
            'ctime': np.array(['2019-01-01 15:00:00.706', '2019-01-01 15:30:00.706', ''], dtype='datetime64[ms]'),
            'cminute': np.array(['2019-01-01 15:25', '2019-01-01 15:30', ''], dtype='datetime64[m]'),
            'csecond': np.array(['2019-01-01 15:00:30', '2019-01-01 15:30:33', ''], dtype='datetime64[s]'),
            'cdatetime': np.array(['2019-01-01 15:00:30', '2019-01-02 15:30:33', ''], dtype='datetime64[s]'),
            'ctimestamp': np.array(['2019-01-01 15:00:00.706', '2019-01-01 15:30:00.706', ''], dtype='datetime64[ms]'),
            'cnanotime': np.array(['2019-01-01 15:00:00.80706', '2019-01-01 15:30:00.80706', ''], dtype='datetime64[ns]'),
            'cnanotimestamp': np.array(['2019-01-01 15:00:00.80706', '2019-01-01 15:30:00.80706', ''], dtype='datetime64[ns]'),
            'cfloat': np.array([2.1, 2.658956, np.NaN], dtype=np.float32),
            'cdouble': np.array([0., 47.456213, np.NaN], dtype=np.float64),
            'csymbol': np.array(['A', 'B', '']),
            'cstring': np.array(['abc', 'def', ''])}
    return pd.DataFrame(data)
```

Upload the DataFrame to DolphinDB server with method `table`, name it as "testDataFrame", then we can read the table with method `loadTable` provided by the API.

```python
import pandas as pd

# save the table to DolphinDB server as table "testDataFrame"
dt = s.table(data=createDemoDataFrame(), tableAliasName="testDataFrame")

# load table "testDataFrame" on DolphinDB server 
print(s.loadTable("testDataFrame").toDF())

# output
>>> print(s.loadTable("testDataFrame").toDF())
   cid  cbool  cchar  cshort  cint  ...             cnanotimestamp    cfloat    cdouble csymbol cstring
0    1   True      1       1     1  ... 2019-01-01 15:00:00.807060  2.100000   0.000000       A     abc
1    2  False      2       2     2  ... 2019-01-01 15:30:00.807060  2.658956  47.456213       B     def
2    3   True      3       3     3  ...                        NaT       NaN        NaN
```

### 2.3 上传的数据表的的生命周期

`table`和`loadTable`函数必须返回一个Python本地变量，即Python端的变量和server端有一个一一对应的关系。假设server端表对象为t0, 释放server端对象有三种方法：取消server端定义(undef)，将server端对象置空，或取消本地变量对server端对象的引用。

```
t0=s.table(data=createDemoDict(), tableAliasName="t1")

s.undef("t0", "VAR")
```
or 
```
s.run("t0=NULL")
```
or
```
t0=None
```

当Python端通过session.table函数将数据上传到server之后, DolphinDB会为Python端的变量建立一个变量对server端table变量的引用。当Python端对server端table变量引用消失后，server端的table会自动释放。

下面代码显示将一个表上传到server，然后通过toDF()加载来拿到数据。

```
t1=s.table(data=createDemoDict(), tableAliasName="t1")

print(t1.toDF())

#output
   id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0
```

如果重复下面这个语句，就会发生找到不到t1的异常。原因是Python端对Server端表t1的原有引用已经取消，在重新给Python端t1分配DolphinDB的表对象前, 
DolphinDB要对session中的对应的表t1进行释放（通过函数`undef`取消它在session中的定义），所以会出现无法找到t1的异常。

```
t1=s.table(data=createDemoDict(), tableAliasName="t1")
print(t1.toDF())

#output

<Server Exception> in run: Can't find the object with name t1

```

那么如何避免这种情况呢？将这个table对象赋值给另一个Python本地变量就不会出现找不到t1的情况。但这里的代价是server端保存了两份同样的table对象，因为Python端有两个引用：t1和t2。

```
t2=s.table(data=createDemoDict(), tableAliasName="t1")
print(t2.toDF())

#output
   id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0
```

如果需要反复通过同一个本地变量指向相同的或者不同的上传表，更合理的方法是不指定表名。此时会为用户随机产生一个临时表名。这个表名可以通过t1.tableName()来获取。这里可能会产生一个疑惑，那么server端是不是会产生很多表对象，造成内存溢出。由于python端使用了同一个变量名，
所以在重新上传数据的时候，系统会将上一个表对象释放掉(TMP_TBL_876e0ce5)，而用一个新的table对象TMP_TBL_4c5647af来对应Python端的t1，所以Server端始终只有一个对应的表对象。


```
t1=s.table(data=createDemoDicts())
print(t1.tableName())

#output
TMP_TBL_876e0ce5


print(t1.toDF())

#output
   id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0

t1=s.table(data=createDemoDict())

print(t1.tableName())

#output
'TMP_TBL_4c5647af'

print(t1.toDF())

#output
   id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0


```

同理，通过`loadTable`来加载一个磁盘分区表到内存的原理也是必须赋值给一个Python本地变量，建立起Python本地变量和server端一一对应的关系。 首先运行一下DolphinDB脚本：

```
db = database("dfs://testdb",RANGE, [1, 5 ,11])
t1=table(1..10 as id, 1..10 as v)
db.createPartitionedTable(t1,`t1,`id).append!(t1)
```

然后运行以下Python脚本:
```
pt1=s.loadTable(tableName='t1',dbPath="dfs://testdb")
```

上面脚本即是在server端创建了一个磁盘分区表，然后通过session函数`loadTable`来讲该表导入内存，并将该表对象赴给本地变量pt1。注意到这里t1并不是server端表对象的名字，
而是磁盘分区表的名字，是用于讲数据库testdb中，讲分区表`t1`加载到内存中的。实际的表对象的名字，需要通过 pt1.tableName()来得到。

```
print(pt1.tableName())
'TMP_TBL_4c5647af'
```

如果一个表对象只是一次性使用，尽量不要使用上传机制。直接通过函数调用来完成，表对象作为函数的一个参数。函数调用不会缓存数据，函数调用结束，所有数据都释放，没有副作用，而且只有一次网络传输，降低网络延迟。








```
df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.value.avg()"))

# output
5.44
```

#### 2.4.2 With function `table`

A DolphinDB table object can be created with the `table` method of a session. The input of the `table` method can be a dictionary, a DataFrame, or a table name on the DolphinDB server. 

```
# save the table to DolphinDB server as table "test"
dt = s.table(data={'id': [1, 2, 2, 3],
                   'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                   'price': [22, 3.5, 21, 26]}).executeAs("test")

# load table "test" on DolphinDB server 
print(s.loadTable("test").toDF())

# output
   id  price   sym
0   1   22.0  AAPL
1   2    3.5  AMZN
2   2   21.0  AMZN
3   3   26.0     A

```
























### 2 Import data to DolphinDB server

There are 3 types of DolphinDB databases: in-memory database, local file system database and DFS (Distributed File System) database. DFS automatically manages data storage and replicas. DolphinDB is a distributed database system and achieves optimal performance in DFS mode. Therefore, we highly recommend to use DFS mode. Please refer to the tutorial [multi_machine_cluster_deploy](https://github.com/dolphindb/Tutorials_EN/blob/master/multi_machine_cluster_deploy.md) for details. For simplicity, we also use databases in the local file system in some examples. 

#### 2.1 Import data as an in-memory table

To import text files into DolphinDB as an in-memory table, use session method `loadText`. It returns a DolphinDB table object in Python, which corresponds to an in-memory table on the DolphinDB server. The DolphinDB table object in Python has a method `toDF` to convert it to a pandas DataFrame.

Please note that to use method `loadText` to load a text file as an in-memory table, table size must be smaller than available memory.

```
WORK_DIR = "C:/DolphinDB/Data"

# return a DolphinDB table object in Python
trade=s.loadText(WORK_DIR+"/example.csv")

# convert the imported DolphinDB table object into a pandas DataFrame
df = trade.toDF()
print(df)

# output
      TICKER        date       VOL        PRC        BID       ASK
0       AMZN  1997.05.16   6029815   23.50000   23.50000   23.6250
1       AMZN  1997.05.17   1232226   20.75000   20.50000   21.0000
2       AMZN  1997.05.20    512070   20.50000   20.50000   20.6250
3       AMZN  1997.05.21    456357   19.62500   19.62500   19.7500
4       AMZN  1997.05.22   1577414   17.12500   17.12500   17.2500
5       AMZN  1997.05.23    983855   16.75000   16.62500   16.7500
...
13134   NFLX  2016.12.29   3444729  125.33000  125.31000  125.3300
13135   NFLX  2016.12.30   4455012  123.80000  123.80000  123.8300

```
The default delimiter for function `loadText` is comma ",". We can also use other delimiters. For example, to import a tabular text file:
```
t1=s.loadText(WORK_DIR+"/t1.tsv", '\t')
```

#### 2.2 Import data into a partitioned database

To load data files that are larger than available memory into DolphinDB, we can load data into a partitioned database.

#### 2.2.1 Create a partitioned database

After we create a partitioned database, we cannot modify its partitioning scheme. To make sure the examples below do not use a pre-defined database "valuedb", check if it exists. If it exists, delete it.

```
if s.existsDatabase(WORK_DIR+"/valuedb"):
    s.dropDatabase(WORK_DIR+"/valuedb")
```

Now create a value-based partitioned database "valuedb" with a session method `database`. As "example.csv" only has data for 3 stocks, we use a VALUE partition with stock ticker as the partitioning column. The parameter "partitions" indicates the partition scheme.

```
# 'db' indicates the database handle name on the DolphinDB server.
s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath=WORK_DIR+"/valuedb")
# this is equivalent to executing 'db=database(=WORK_DIR+"/valuedb", VALUE, ["AMZN","NFLX", "NVDA"])' on DolphinDB server.
```

To create a partitioned database in DFS, just make the database path start with "dfs://". Before we execute the following script, we need to configure a DFS cluster. Please refer to the tutorial [multi_machine_cluster_deploy.md](https://github.com/dolphindb/Tutorials_EN/blob/master/multi_machine_cluster_deploy.md)

```
s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
```

In addition to VALUE partition, DolphinDB also supports SEQ, RANGE, LIST, COMBO, and HASH partitions.

#### 2.2.2 Create a partitioned table and append data to the table

After a partitioned database is created successfully, we can import text files to a partitioned table in the partitioned database with function `loadTextEx`. If the partitioned table does not exist, `loadTextEx` creates it and appends the imported data to it. Otherwise, the function appends the imported data to the partitioned table.

In function `loadTextEx`, parameter "dbPath" is the database path; "tableName" is the partitioned table name; "partitionColumns" is the partitioning columns; "filePath" is the absolute path of the text file; "delimiter" is the delimiter of the text file (comma by default).

In the following example, function `loadTextEx` creates a partitioned table "trade" on the DolphinDB server and then appends the data from "example.csv" to the table. 

```
if s.existsDatabase(WORK_DIR+"/valuedb"):
    s.dropDatabase(WORK_DIR+"/valuedb")
s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
trade = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")
print(trade.toDF())

# output
      TICKER        date       VOL        PRC        BID       ASK
0       AMZN  1997.05.16   6029815   23.50000   23.50000   23.6250
1       AMZN  1997.05.17   1232226   20.75000   20.50000   21.0000
2       AMZN  1997.05.20    512070   20.50000   20.50000   20.6250
3       AMZN  1997.05.21    456357   19.62500   19.62500   19.7500
4       AMZN  1997.05.22   1577414   17.12500   17.12500   17.2500
5       AMZN  1997.05.23    983855   16.75000   16.62500   16.7500
...
13134   NFLX  2016.12.29   3444729  125.33000  125.31000  125.3300
13135   NFLX  2016.12.30   4455012  123.80000  123.80000  123.8300

[13136 rows x 6 columns]

# show the number of rows in the table
print(trade.rows)
13136

# show the number of columns in the table
print(trade.cols)
6

# show the schema of the table
print(trade.schema)
     name typeString  typeInt
0  TICKER     SYMBOL       17
1    date       DATE        6
2     VOL        INT        4
3     PRC     DOUBLE       16
4     BID     DOUBLE       16
5     ASK     DOUBLE       16

```

To refer to the table later:

```
 trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")

```

#### 2.3 Import data as an in-memory partitioned table

#### 2.3.1 `loadTextEx`

We can import data as an in-memory partitioned table. Operations on an in-memory partitioned table are faster than those on a nonpartitioned in-memory table as the former utilizes parallel computing.

We can use function `loadTextEx` to create an in-memory partitioned database with an empty string for the parameter "dbPath".

```
s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="")

# "dbPath='db'" means that the system uses database handle 'db' to import data into in-memory partitioned table trade
trade=s.loadTextEx(dbPath="db", partitionColumns=["TICKER"], tableName='trade', filePath=WORK_DIR + "/example.csv")
```

#### 2.3.2 `ploadText`

Function `ploadText` loads a text file in parallel to generate an in-memory partitioned table. It runs much faster than `loadText`.

```
trade=s.ploadText(WORK_DIR+"/example.csv")
print(trade.rows)

# output
13136

````

#### 2.4 Upload data from Python to DolphinDB server

#### 2.4.1 With function `upload`

We can upload a Python object to the DolphinDB server with function `upload`. The input of function `upload` is a Python dictionary object. For this dictionary object, the key is the variable name in DolphinDB and the value is the Python object.

```
df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.value.avg()"))

# output
5.44
```

#### 2.4.2 With function `table`

A DolphinDB table object can be created with the `table` method of a session. The input of the `table` method can be a dictionary, a DataFrame, or a table name on the DolphinDB server. 

```
# save the table to DolphinDB server as table "test"
dt = s.table(data={'id': [1, 2, 2, 3],
                   'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                   'price': [22, 3.5, 21, 26]}).executeAs("test")

# load table "test" on DolphinDB server 
print(s.loadTable("test").toDF())

# output
   id  price   sym
0   1   22.0  AAPL
1   2    3.5  AMZN
2   2   21.0  AMZN
3   3   26.0     A

```

#### 2.4.3 Data type conversion

When we upload data to DolphinDB server, certain basic Python types such as bool, int64 and float64 are automatically converted into corresponding DolphinDB types BOOL, INT, DOUBLE. Temporal data types, however, need special treatment. DolphinDB provides 9 temporal data types: DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME and NANOTIMESTAMP. The temporal data type datetime64 in Python is converted into DolphinDB temporal data type NANOTIMESTAMP. To convert datetime64 into other DolphinDB temporal data types, please use `from_time`, `from_date` or `from_datetime` function. For more details, please refer to the following table.

```
# import DolphinDB data type package 
from dolphindb.type_util import *
```

|DolphinDB Temporal Data Type|Example|Result|
|--------|---------------|--------------|
|DATE|Date.from_date(date(2012,12,20))|2012.12.20|
|MONTH|Month.from_date(date(2012,12,26))|2012.12M|
|TIME|Time.from_time(time(12,30,30,8))|12:30:30.008|
|MINUTE|Minute.from_time(time(12,30))|12:30m|
|SECOND|Second.from_time(time(12,30,30))|12:30:30|
|DATETIME|Datetime.from_datetime(datetime(2012,12,30,15,12,30))|2012.12.30 15:12:30|
|TIMESTAMP|Timestamp.from_datetime(datetime(2012,12,30,15,12,30,8))|2012.12.30 15:12:30.008|
|NANOTIME|NanoTime.from_time(time(13,30,10,706))|13:30:10.000706000|
|NANOTIMESTAMP|NanoTimestamp.from_datetime(datetime(2012,12,24,13,30,10,80706))|2012.12.24 13:30:10.080706000|

As np.NaN is of float type in Python, when np.NaN is uploaded to DolphinDB server it will be converted into FLOAT. Python API provides special Null corresponding to DolphinDB data types. The following table shows how to create DolphinDB Null in Python:

|DolphinDB Data Type|Corresponding Null in Python|
|-------|--------|
|BOOL|boolNan|
|CHAR|byteBan|
|SHORT|shortNan|
|INT|intNan|
|DATE|Date.null()|
|MONTH|Month.null()|
|TIME|Time.null()|
|SECOND|Second.null()|
|DATETIME|Datetime.null()|
|TIMESTAMP|Timestamp.null()|
|NANOTIME|NanoTime.null()|
|NANOTIMESTAMP|NanoTimestamp.null()|

Please note that, Python data type and DolphinDB data type can't in the same column of dictionary or dataframe. For example, 'date':[date(2012,12,30),Date.from_date(date(2012,12,31)),Date.null()]. It will raise exception since 'date' contains Python datetime64 and DolphinDB DATE.


#### 3 Load DolphinDB database tables

#### 3.1 `loadTable`

To load a table from a database, use function `loadTable`. Parameter "tableName" indicates the partitioned table name; "dbPath" is the database location. If "dbPath" is not specified, `loadTable` will load a DolphinDB table in memory whose name is specified in argument "tableName".

For a partitioned table: if memoryMode=True and parameter "partition" is unspecified, load all data of the table into DolphinDB server memory as a partitioned table; if memoryMode=True and parameter "partition" is specified, load only the specified partitions of the table into DolphinDB server memory as a partitioned table; if memoryMode=false, only load the metadata of the table into DolphinDB server memory.

#### 3.1.1 Load an entire table

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
trade.toDF()

# output
      TICKER        date       VOL        PRC        BID       ASK
0       AMZN  1997.05.16   6029815   23.50000   23.50000   23.6250
1       AMZN  1997.05.17   1232226   20.75000   20.50000   21.0000
2       AMZN  1997.05.20    512070   20.50000   20.50000   20.6250
3       AMZN  1997.05.21    456357   19.62500   19.62500   19.7500
4       AMZN  1997.05.22   1577414   17.12500   17.12500   17.2500
5       AMZN  1997.05.23    983855   16.75000   16.62500   16.7500
...
13134   NFLX  2016.12.29   3444729  125.33000  125.31000  125.3300
13135   NFLX  2016.12.30   4455012  123.80000  123.80000  123.8300

[13136 rows x 6 columns]
```

#### 3.1.2 Load selected partitions

To load only the "AMZN" partition:

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", partitions="AMZN")
print(trade.rows)

# output
4941
```

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", partitions=["NFLX","NVDA"], memoryMode=True)
print(trade.rows)

# output
8195
```

#### 3.2. `loadTableBySQL`

Method `loadTableBySQL` imports only the rows of an on-disk partitioned table that satisfy the filtering conditions in a SQL query as an in-memory partitioned table.

```
import os
if s.existsDatabase(WORK_DIR+"/valuedb"  or os.path.exists(WORK_DIR+"/valuedb")):
    s.dropDatabase(WORK_DIR+"/valuedb")
s.database(dbName='db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
t = s.loadTextEx("db",  tableName='trade',partitionColumns=["TICKER"], filePath=WORK_DIR + "/example.csv")

trade = s.loadTableBySQL(tableName="trade", dbPath=WORK_DIR+"/valuedb", sql="select * from trade where date>2010.01.01")
print(trade.rows)

# output
5286

```

#### 3.3 Data Type Conversion

When we load data from DolphinDB, DolphinDB types BOOL and INT are converted into Python types bool and int64; CHAR, SHORT, INT and LONG are converted into int64. To convert integer into charater, use chr(). DOUBLE and FLOAT are converted into float64; SYMBOL and STRING are converted into object. Temporal data types are converted into datetime64. For example, 2012.06M is converted into 2012-06-01, 13:30m is converted into 1970-01-01 13:30:00.

|DolphinDB Data Type|Python Data Type|
|-------------|----------|
|BOOL|bool|
|CHAR, SHORT, INT, LONG|int64|
|DOUBLE, FLOAT|float64|
|SYMBOL, STRING|object|
|DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP|datetime64|

```
//DolphinDB script：
db=database(WORK_DIR+"/testPython")
t1 = table(10000:0,`cid`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])
insert into t1 values (1,true,'a',122h,21,22l,2012.06.12,2012.06M,13:10:10.008,13:30m,13:30:10,2012.06.13 13:30:10,2012.06.13 13:30:10.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,"ABC","abc")
insert into t1 values (2,,,,,,,,,,,,,,,,,"","")
insert into t1 values (3,bool(),char(),short(),int(),long(),date(),month(),time(),minute(),second(),datetime(),timestamp(),nanotime(),nanotimestamp(),float(),double(),"",string())
saveTable(db,t1,`t1)
```

```
t=s.loadTable(WORK_DIR+"/testPython",tableName="t1")
print(t.toDF())
```


|  |cid |cbool |cchar |cshort |cint |clong |cdate |cmonth |ctime |cminute |csecond |cdatetime |ctimestamp|cnanotime |cnanotimestamp |cfloat |cdouble |csymbol |cstring |
|--|----|------|------|-------|-----|------|------|-------|------|--------|--------|----------|----------|----------|---------------|-------|--------|--------|--------|
|0 |1 |True |97 |122 |21 |22 |2012-06-12 |2012-06-01 |1970-01-01 13:10:10.008 |1970-01-01 13:30:00 |1970-01-01 13:30:10 |2012-06-13 13:30:10 |2012-06-13 13:30:10.000008 |1970-01-01 13:30:10.008007006 |2012-06-13 13:30:10.008007006 |2.1 |2.1 |ABC |abc |
|1 |2 |nan  |nan|nan |nan|nan|NaT        |NaT        |NaT                     |NaT                 |NaT                 |NaT                 |NaT                        |NaT                          |NaT                           |nan |nan |    |    |
|2 |3 |nan  |nan|nan |nan|nan| NaT       |NaT        |NaT                     |NaT                 |NaT                 |NaT                 |NaT                        |NaT                          |NaT                           |nan |nan |    |    |

```
t.toDF().dtypes

cbool                       bool
cchar                      int64
cshort                     int64
cint                       int64
clong                      int64
cdate             datetime64[ns]
cmonth            datetime64[ns]
ctime             datetime64[ns]
cminute           datetime64[ns]
csecond           datetime64[ns]
cdatetime         datetime64[ns]
ctimestamp        datetime64[ns]
cnanotime         datetime64[ns]
cnanotimestamp    datetime64[ns]
cfloat                   float64
cdouble                  float64
csymbol                   object
cstring                   object
dtype: object
```

#### 4 Databases and Tables

#### 4.1 Database operations

#### 4.1.1 Create a database

To create a partitioned database, use session method `database`.

```
s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath=WORK_DIR+"/valuedb")
```

#### 4.1.2 Delete a database

To delete a database, use session method `dropDatabase`. The following statement will drop a database if it exists.

```
if s.existsDatabase(WORK_DIR+"/valuedb"):
    s.dropDatabase(WORK_DIR+"/valuedb")
```

#### 4.1.3 Drop a DFS database partition

To drop a DFS database partition, use session method `dropPartition`.

```
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database('db', partitionType=ddb.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
trade=s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.rows)

# output
13136

s.dropPartition("dfs://valuedb", partitionPaths=["/AMZN", "/NFLX"])
trade = s.loadTable(tableName="trade", dbPath="dfs://valuedb")
print(trade.rows)
# output
4516

print(trade.select("distinct TICKER").toDF())

  distinct_TICKER
0            NVDA
```

#### 4.2 Table operations

#### 4.2.1 Load a database table

Please see section 3.1. 

#### 4.2.2 Append to a table

The following example appends to a partitioned table on disk. To use the appended table, we need to reload the table after appending. 

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.rows)

# output
13136

# take the top 10 rows of table "trade" on the DolphinDB server
t = trade.top(10).executeAs("top10")

trade.append(t)

# table "trade" needs to be reloaded in order to see the appended records
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print (trade.rows)

# output
13146
```

The following example appends to an in-memory table. 

```
trade=s.loadText(WORK_DIR+"/example.csv")
t = trade.top(10).executeAs("top10")
t1=trade.append(t)

print(t1.rows)

# output
13146

```

#### 4.3 Update a table

Function `update` can only be used on in-memory tables and must be followed by function `execute`. 

```
trade = s.loadTable(tableName="trade", dbPath=WORK_DIR+"/valuedb", memoryMode=True)
trade = trade.update(["VOL"],["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
t1=trade.where("ticker=`AMZN").where("VOL=999999")
print(t1.toDF())

# output
     TICKER        date     VOL        PRC        BID        ASK
0      AMZN  1997.05.15  999999   23.50000   23.50000   23.62500
1      AMZN  1997.05.16  999999   20.75000   20.50000   21.00000
2      AMZN  1997.05.19  999999   20.50000   20.50000   20.62500
3      AMZN  1997.05.20  999999   19.62500   19.62500   19.75000
4      AMZN  1997.05.21  999999   17.12500   17.12500   17.25000
...
4948   AMZN  1997.05.27  999999   19.00000   19.00000   19.12500
4949   AMZN  1997.05.28  999999   18.37500   18.37500   18.62500
4950   AMZN  1997.05.29  999999   18.06250   18.00000   18.12500

[4951 rows x 6 columns]

```

#### 4.4 Delete records from a table

Function `delete` must be followed by function `execute` to delete records from a table.

```
trade = s.loadTable(tableName="trade", dbPath=WORK_DIR+"/valuedb", memoryMode=True)
trade.delete().where('date<2013.01.01').execute()
print(trade.rows)

# output
3024
```

#### 4.5 Delete table columns

```
trade = s.loadTable(tableName="trade", dbPath=WORK_DIR + "/valuedb", memoryMode=True)
t1=trade.drop(['ask', 'bid'])
print(t1.top(5).toDF())

# output
  TICKER        date      VOL     PRC
0   AMZN  1997.05.15  6029815  23.500
1   AMZN  1997.05.16  1232226  20.750
2   AMZN  1997.05.19   512070  20.500
3   AMZN  1997.05.20   456357  19.625
4   AMZN  1997.05.21  1577414  17.125
```

#### 4.6 Drop a table

```
s.dropTable(WORK_DIR + "/valuedb", "trade")
```

#### 5 SQL query

DolphinDB's table class supports method chaining to generate SQL statements.

#### 5.1 `select`

#### 5.1.1 A list of column names as input

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", memoryMode=True)
print(trade.select(['ticker','date','bid','ask','prc','vol']).toDF())

# output
  TICKER        date      VOL     PRC     BID     ASK
0   AMZN  1997.05.15  6029815  23.500  23.500  23.625
1   AMZN  1997.05.16  1232226  20.750  20.500  21.000
2   AMZN  1997.05.19   512070  20.500  20.500  20.625
3   AMZN  1997.05.20   456357  19.625  19.625  19.750
4   AMZN  1997.05.21  1577414  17.125  17.125  17.250
...

```

We can use the `showSQL` method to display the SQL statement.

```
print(trade.select(['ticker','date','bid','ask','prc','vol']).where("date=2012.09.06").where("vol<10000000").showSQL())

# output
select ticker,date,bid,ask,prc,vol from T64afd5a6 where date=2012.09.06 and vol<10000000
```

#### 5.1.2 String as input

```
print(trade.select("ticker,date,bid,ask,prc,vol").where("date=2012.09.06").where("vol<10000000").toDF())

# output
  ticker       date        bid     ask     prc      vol
0   AMZN 2012-09-06  251.42999  251.56  251.38  5657816
1   NFLX 2012-09-06   56.65000   56.66   56.65  5368963
...

```

#### 5.2 **top**

Get the top records in a table.

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
trade.top(5).toDF()

# output
      TICKER        date       VOL        PRC        BID       ASK
0       AMZN  1997.05.16   6029815   23.50000   23.50000   23.6250
1       AMZN  1997.05.17   1232226   20.75000   20.50000   21.0000
2       AMZN  1997.05.20    512070   20.50000   20.50000   20.6250
3       AMZN  1997.05.21    456357   19.62500   19.62500   19.7500
4       AMZN  1997.05.22   1577414   17.12500   17.12500   17.2500

```

#### 5.3 `where`

We can use `where` method to filter the selection.

#### 5.3.1 method chaining

We can use method chaining to apply multiple conditions.

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb", memoryMode=True)

# use chaining WHERE conditions and save result to DolphinDB server variable "t1" through function "executeAs"
t1=trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').executeAs("t1")
print(t1.toDF())
# output

         date    bid      ask     prc        vol
0  2007.04.25  56.80  56.8100  56.810  104463043
1  1999.09.29  80.75  80.8125  80.750   80380734
2  2006.07.26  26.17  26.1800  26.260   76996899
3  2007.04.26  62.77  62.8300  62.781   62451660
4  2005.02.03  35.74  35.7300  35.750   60580703
...
print(t1.rows)

765

```

We can use the `showSQL` method to display the SQL statement.

```
print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').showSQL())

# output
select date,bid,ask,prc,vol from Tff260d29 where TICKER=`AMZN and bid!=NULL and ask!=NULL and vol>10000000 order by vol desc

```

#### 5.3.2 Use string as input

We can pass a list of field names as a string to `select` method and conditions as string to `where` method.

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.select("ticker, date, vol").where("bid!=NULL, ask!=NULL, vol>50000000").toDF())

# output
   ticker        date        vol
0    AMZN  1999.09.29   80380734
1    AMZN  2000.06.23   52221978
2    AMZN  2001.11.26   51543686
3    AMZN  2002.01.22   57235489
4    AMZN  2005.02.03   60580703
...
38   NVDA  2016.11.11   54384267
39   NVDA  2016.12.28   57384116
40   NVDA  2016.12.29   54384676

```

#### 5.4 `groupby`

Method `groupby` must be followed by an aggregate function such as `count`, `sum`, `avg`, `std`, etc.

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.select('ticker').groupby(['ticker']).count().sort(bys=['ticker desc']).toDF())

# output
  ticker  count_ticker
0   NVDA          4516
1   NFLX          3679
2   AMZN          4941

```

Calculate the sum of column "vol" and the sum of column "prc" in each "ticker" group:

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.select(['vol','prc']).groupby(['ticker']).sum().toDF())

# output

  ticker      sum_vol       sum_prc
0   AMZN  33706396492  772503.81377
1   NFLX  14928048887  421568.81674
2   NVDA  46879603806  127139.51092
```

`groupby` can be used with `having`:

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb")
print(trade.select('count(ask)').groupby(['vol']).having('count(ask)>1').toDF())

# output
       vol  count_ask
0   579392          2
1  3683504          2
2  5732076          2
3  6299736          2
4  6438038          2
5  6946976          2
6  8160197          2
7  8924303          2
...

```

#### 5.5 `contextby`

`contextby` is similar to `groupby` except that for each group, `groupby` returns a scalar whereas `contextby` returns a vector of the same size as the group.

```
df= s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb").contextby('ticker').top(3).toDF()
print(df)

# output
  TICKER       date      VOL      PRC      BID      ASK
0   AMZN 1997-05-15  6029815  23.5000  23.5000  23.6250
1   AMZN 1997-05-16  1232226  20.7500  20.5000  21.0000
2   AMZN 1997-05-19   512070  20.5000  20.5000  20.6250
3   NFLX 2002-05-23  7507079  16.7500  16.7500  16.8500
4   NFLX 2002-05-24   797783  16.9400  16.9400  16.9500
5   NFLX 2002-05-28   474866  16.2000  16.2000  16.3700
6   NVDA 1999-01-22  5702636  19.6875  19.6250  19.6875
7   NVDA 1999-01-25  1074571  21.7500  21.7500  21.8750
8   NVDA 1999-01-26   719199  20.0625  20.0625  20.1250

```
```
df= s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb").select("TICKER, month(date) as month, cumsum(VOL)").contextBy("TICKER,month(date)").toDF()
print(df)

# output
      TICKER     month  cumsum_VOL
0       AMZN  1997.05M     6029815
1       AMZN  1997.05M     7262041
2       AMZN  1997.05M     7774111
3       AMZN  1997.05M     8230468
4       AMZN  1997.05M     9807882
...
13133   NVDA  2016.12M   367356016
13134   NVDA  2016.12M   421740692
13135   NVDA  2016.12M   452063951

```
```
df= s.loadTable(tableName="trade",dbPath=WORK_DIR+"/valuedb").select("TICKER, month(date) as month, sum(VOL").contextBy("TICKER,month(date)").toDF()
print(df)

# output
      TICKER     month    sum_VOL
0       AMZN  1997.05M   13736587
1       AMZN  1997.05M   13736587
2       AMZN  1997.05M   13736587
3       AMZN  1997.05M   13736587
4       AMZN  1997.05M   13736587
5       AMZN  1997.05M   13736587
...
13133   NVDA  2016.12M  452063951
13134   NVDA  2016.12M  452063951
13135   NVDA  2016.12M  452063951

```
```
df= s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade").contextby('ticker').having("sum(VOL)>40000000000").toDF()
print(df)

# output
     TICKER        date       VOL       PRC       BID       ASK
0      NVDA  1999.01.22   5702636   19.6875   19.6250   19.6875
1      NVDA  1999.01.25   1074571   21.7500   21.7500   21.8750
2      NVDA  1999.01.26    719199   20.0625   20.0625   20.1250
3      NVDA  1999.01.27    510637   20.0000   19.8750   20.0000
4      NVDA  1999.01.28    476094   19.9375   19.8750   20.0000
5      NVDA  1999.01.29    509718   19.0000   19.0000   19.3125
...
4512   NVDA  2016.12.27  29857132  117.3200  117.3100  117.3200
4513   NVDA  2016.12.28  57384116  109.2500  109.2500  109.2900
4514   NVDA  2016.12.29  54384676  111.4300  111.2600  111.4200
4515   NVDA  2016.12.30  30323259  106.7400  106.7300  106.7500

```

#### 5.6 Table join

DolphinDB table class has method `merge` for inner, left, and outer join; method `merge_asof` for asof join; method `merge_window` for window join.

#### 5.6.1 `merge`

Specify joining columns with parameter "on" if joining column names are identical in both tables; use parameters "left_on" and "right_on" when joining column names are different. The optional parameter "how" indicates table join type. The default table join mode is inner join. 

```
trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print(trade.merge(t1,on=["TICKER","date"]).toDF())

# output
  TICKER        date      VOL        PRC        BID        ASK  open
0   AMZN  2015.12.29  5734996  693.96997  693.96997  694.20001   674
1   AMZN  2015.12.30  3519303  689.07001  689.07001  689.09998   685
2   AMZN  2015.12.31  3749860  675.89001  675.85999  675.94000   695
```

We need to specify arguments "left_on" and "right_on" when joining column names are different. 

```
trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")
t1 = s.table(data={'TICKER1': ['AMZN', 'AMZN', 'AMZN'], 'date1': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print(trade.merge(t1,left_on=["TICKER","date"], right_on=["TICKER1","date1"]).toDF())

# output
  TICKER        date      VOL        PRC        BID        ASK  open
0   AMZN  2015.12.29  5734996  693.96997  693.96997  694.20001   674
1   AMZN  2015.12.30  3519303  689.07001  689.07001  689.09998   685
2   AMZN  2015.12.31  3749860  675.89001  675.85999  675.94000   695
```

To conduct left join, set how="left". 

```
trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print(trade.merge(t1,how="left", on=["TICKER","date"]).where('TICKER=`AMZN').where('2015.12.23<=date<=2015.12.31').toDF())

# output
  TICKER       date      VOL        PRC        BID        ASK   open
0   AMZN 2015-12-23  2722922  663.70001  663.48999  663.71002    NaN
1   AMZN 2015-12-24  1092980  662.78998  662.56000  662.79999    NaN
2   AMZN 2015-12-28  3783555  675.20001  675.00000  675.21002    NaN
3   AMZN 2015-12-29  5734996  693.96997  693.96997  694.20001  674.0
4   AMZN 2015-12-30  3519303  689.07001  689.07001  689.09998  685.0
5   AMZN 2015-12-31  3749860  675.89001  675.85999  675.94000  695.0

```

To conduct outer join, set how="outer". A partitioned table can only be outer joined with a partitioned table, and an in-memory table can only be outer joined with an in-memory table.

```
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'NFLX'], 'date': ['2015.12.29', '2015.12.30', '2015.12.31'], 'open': [674, 685, 942]})
t2 = s.table(data={'TICKER': ['AMZN', 'NFLX', 'NFLX'], 'date': ['2015.12.29', '2015.12.30', '2015.12.31'], 'close': [690, 936, 951]})
print(t1.merge(t2, how="outer", on=["TICKER","date"]).toDF())

# output
  TICKER        date   open TMP_TBL_ec03c3d2_TICKER TMP_TBL_ec03c3d2_date  \
0   AMZN  2015.12.29  674.0                    AMZN            2015.12.29   
1   AMZN  2015.12.30  685.0                                                 
2   NFLX  2015.12.31  942.0                    NFLX            2015.12.31   
3                       NaN                    NFLX            2015.12.30   

   close  
0  690.0  
1    NaN  
2  951.0  
3  936.0  

```

#### 5.6.2 `merge_asof`

The asof join function is a type of non-synchronous join. It is similar to the left join function witht the following differences:
1. The data type of the last matching column is usually temporal. For a row in the left table with time t, if there is not a match of left join in the right table, the row in the right table that corresponds to the most recent time before time t is taken, if all the other matching columns are matched; if there are more than one matching record in the right table, the last record is taken. 
2. If there is only 1 joining column, the asof join function assumes the right table is sorted on the joining column. If there are multiple joining columns, the asof join function assumes the right table is sorted on the last joining column within each group defined by the other joining columns. The right table does not need to be sorted by the other joining columns. If these conditions are not met, we may see unexpected results. The left table does not need to be sorted. 

For the examples in this and the next section, we use trades.csv and quotes.csv which have AAPL and FB trades and quotes data on 10/24/2016 taken from NYSE website. 

```
WORK_DIR = "C:/DolphinDB/Data"
if s.existsDatabase(WORK_DIR+"/tickDB"):
    s.dropDatabase(WORK_DIR+"/tickDB")
s.database('db', partitionType=ddb.VALUE, partitions=["AAPL","FB"], dbPath=WORK_DIR+"/tickDB")
trades = s.loadTextEx("db",  tableName='trades',partitionColumns=["Symbol"], filePath=WORK_DIR + "/trades.csv")
quotes = s.loadTextEx("db",  tableName='quotes',partitionColumns=["Symbol"], filePath=WORK_DIR + "/quotes.csv")

print(trades.top(5).toDF())

# output
                 Time Symbol  Trade_Volume  Trade_Price
0  09:30:00.087488712   AAPL        370466      117.100
1  09:30:00.087681843   AAPL        370466      117.100
2  09:30:00.103645440   AAPL           100      117.100
3  09:30:00.213850801   AAPL            20      117.100
4  09:30:00.264854448   AAPL            17      117.095

print(quotes.where("second(Time)>=09:29:59").top(5).toDF())

# output
                 Time Symbol  Bid_Price  Bid_Size  Offer_Price  Offer_Size
0  09:29:59.300399073   AAPL     117.07         1       117.09           1
1  09:29:59.300954263   AAPL     117.07         1       117.09           1
2  09:29:59.301594217   AAPL     117.05         1       117.19          10
3  09:30:00.499924044   AAPL     117.09        46       117.10           3
4  09:30:00.500005573   AAPL     116.86        53       117.37          64

print(trades.merge_asof(quotes,on=["Symbol","Time"]).select(["Symbol","Time","Trade_Volume","Trade_Price","Bid_Price", "Bid_Size","Offer_Price", "Offer_Size"]).top(5).toDF())

# output
  Symbol                Time  Trade_Volume  Trade_Price  Bid_Price  Bid_Size  \
0   AAPL  09:30:00.087488712        370466      117.100     117.05         1   
1   AAPL  09:30:00.087681843        370466      117.100     117.05         1   
2   AAPL  09:30:00.103645440           100      117.100     117.05         1   
3   AAPL  09:30:00.213850801            20      117.100     117.05         1   
4   AAPL  09:30:00.264854448            17      117.095     117.05         1   

   Offer_Price  Offer_Size  
0       117.19          10  
1       117.19          10  
2       117.19          10  
3       117.19          10  
4       117.19          10  
```
To calculate trading cost:

```
print(trades.merge_asof(quotes, on=["Symbol","Time"]).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2))/sum(Trade_Volume*Trade_Price)*10000 as cost").groupby("Symbol").toDF())

# output
  Symbol      cost
0   AAPL  0.899823
1     FB  2.722923

```
#### 5.6.3 `merge_window`

`merge_window` (window join) is a generalization of asof join. With a window defined by parameters "leftBound" (w1) and "rightBound" (w2), for each row in the left table with the value of the last joining column equal to t, find the rows in the right table with the value of the last joining column between (t+w1) and (t+w2) conditional on all other joining columns are matched, then apply "aggFunctions" to the selected rows in the right table. 

The only difference between window join and prevailing window join is that if the right table doesn't contain a matching value for t+w1 (the left boundary of the window), prevailing window join will fill it with the last value before t+w1 (conditional on all other joining columns are matched), and apply "aggFunctions". To use prevailing window join, set prevailing=True. 

```
print(trades.merge_window(quotes, -5000000000, 0, aggFunctions=["avg(Bid_Price)","avg(Offer_Price)"], on=["Symbol","Time"]).where("Time>=15:59:59").top(10).toDF())

# output
                 Time Symbol  Trade_Volume  Trade_Price  avg_Bid_Price  \
0  15:59:59.003095025   AAPL           250      117.620     117.603714   
1  15:59:59.003748103   AAPL           100      117.620     117.603714   
2  15:59:59.011092788   AAPL            95      117.620     117.603714   
3  15:59:59.011336471   AAPL           200      117.620     117.603714   
4  15:59:59.022841207   AAPL           144      117.610     117.603689   
5  15:59:59.028169703   AAPL           130      117.615     117.603544   
6  15:59:59.035357411   AAPL          1101      117.610     117.603544   
7  15:59:59.035360176   AAPL           799      117.610     117.603544   
8  15:59:59.035602676   AAPL           130      117.610     117.603544   
9  15:59:59.036929307   AAPL          2201      117.610     117.603544   

   avg_Offer_Price  
0       117.626816  
1       117.626816  
2       117.626816  
3       117.626816  
4       117.626803  
5       117.626962  
6       117.626962  
7       117.626962  
8       117.626962  
9       117.626962  

...

```

To calculate trading cost:

```
trades.merge_window(quotes,-1000000000, 0, aggFunctions="[wavg(Offer_Price, Offer_Size) as Offer_Price, wavg(Bid_Price, Bid_Size) as Bid_Price]", on=["Symbol","Time"], prevailing=True).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2))/sum(Trade_Volume*Trade_Price)*10000 as cost").groupby("Symbol").executeAs("tradingCost")

print(s.loadTable(tableName="tradingCost").toDF())

# output
  Symbol      cost
0   AAPL  0.953315
1     FB  1.077876

```

#### 5.7 `executeAs`

Function `executeAs` saves query result as a table on DolphinDB server. 

```
trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")
trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').executeAs("AMZN")
```

To use the table "AMZN" on DolphinDB server:

```
t1=s.loadTable(tableName="AMZN")
```


#### 6 Regression

Method `ols` conducts ordinary least squares regression. It returns a dictionary with regression results.

```
trade = s.loadTable(tableName="trade",dbPath=WORK_DIR + "/valuedb", memoryMode=True)
z=trade.select(['bid','ask','prc']).ols('PRC', ['BID', 'ASK'])

print(z["ANOVA"])

# output
    Breakdown     DF            SS            MS             F  Significance
0  Regression      2  2.689281e+08  1.344640e+08  1.214740e+10           0.0
1    Residual  13133  1.453740e+02  1.106937e-02           NaN           NaN
2       Total  13135  2.689282e+08           NaN           NaN           NaN

print(z["RegressionStat"])

# output
           item    statistics
0            R2      0.999999
1    AdjustedR2      0.999999
2      StdError      0.105211
3  Observations  13136.000000


print(z["Coefficient"])

# output
      factor      beta  stdError      tstat    pvalue
0  intercept  0.003710  0.001155   3.213150  0.001316
1        BID  0.605307  0.010517  57.552527  0.000000
2        ASK  0.394712  0.010515  37.537919  0.000000

print(z["Coefficient"].beta[1])

# output
0.6053065019659698
```

The following example conducts regression on a partitioned database. Note that the ratio operator between 2 integer columns in DolphinDB is "\", which happens to be the escape character in Python, so we need to use "VOL\\SHROUT" in function `select`.

```
result = s.loadTable(tableName="US",dbPath="dfs://US").select("select VOL\\SHROUT as turnover, abs(RET) as absRet, (ASK-BID)/(BID+ASK)*2 as spread, log(SHROUT*(BID+ASK)/2) as logMV").where("VOL>0").ols("turnover", ["absRet","logMV", "spread"], True)
print(result["ANOVA"])

# output
   Breakdown        DF            SS            MS            F  Significance
0  Regression         3  2.814908e+09  9.383025e+08  30884.26453           0.0
1    Residual  46701483  1.418849e+12  3.038125e+04          NaN           NaN
2       Total  46701486  1.421674e+12           NaN          NaN           NaN

```

#### 7 `run`

A session object has a method `run` that can execute any DolphinDB script. If the script returns an object in DolphinDB, method `run` converts the object to a corresponding object in Python.  

```
# Load table
trade = s.loadTable(dbPath=WORK_DIR+"/valuedb", tableName="trade")

# query the table and return a pandas DataFrame
t = s.run("select bid, ask, prc from trade where bid!=NULL, ask!=NULL, vol>1000")
print(t)
```

#### 8 More Examples

#### 8.1 Stock momentum strategy

In this section we give an example of a backtest on a stock momentum strategy. The momentum strategy is one of the best-known quantitative long short equity strategies. It has been studied in numerous academic and sell-side publications since Jegadeesh and Titman (1993). Investors in the momentum strategy believe among individual stocks, past winners will outperform past losers. The most commonly used momentum factor is stocks' past 12 months returns skipping the most recent month. In academic research, the momentum strategy is usually rebalanced once a month and the holding period is also one month. In this example, we rebalance 1/5 of our portfolio positions every day and hold the new tranche for 5 days. For simplicity, transaction costs are not considered.

**Create server session**

```
import dolphindb as ddb
s=ddb.session()
s.connect("localhost",8921, "admin", "123456")

```

**Step 1:** Load data, clean the data, and construct the momentum signal (past 12 months return skipping the most recent month) for each firm. Undefine the table "USstocks" to release the large amount of memory it occupies. Note that `executeAs` must be used to save the intermediate results on DolphinDB server. Dataset "US" contains US stock price data from 1990 to 2016.

```
US = s.loadTable(dbPath="dfs://US", tableName="US")
def loadPriceData(inData):
    s.loadTable(inData).select("PERMNO, date, abs(PRC) as PRC, VOL, RET, SHROUT*abs(PRC) as MV").where("weekday(date) between 1:5, isValid(PRC), isValid(VOL)").sort(bys=["PERMNO","date"]).executeAs("USstocks")
    s.loadTable("USstocks").select("PERMNO, date, PRC, VOL, RET, MV, cumprod(1+RET) as cumretIndex").contextby("PERMNO").executeAs("USstocks")
    return s.loadTable("USstocks").select("PERMNO, date, PRC, VOL, RET, MV, move(cumretIndex,21)/move(cumretIndex,252)-1 as signal").contextby("PERMNO").executeAs("priceData")

priceData = loadPriceData(US.tableName())
# US.tableName() returns the name of the table on the DolphinDB server that corresponds to the table object "US" in Python. 
```

**Step 2:** Generate the portfolios for the momentum strategy.

```
def genTradeTables(inData):
    return s.loadTable(inData).select(["date", "PERMNO", "MV", "signal"]).where("PRC>5, MV>100000, VOL>0, isValid(signal)").sort(bys=["date"]).executeAs("tradables")


def formPortfolio(startDate, endDate, tradables, holdingDays, groups, WtScheme):
    holdingDays = str(holdingDays)
    groups=str(groups)
    ports = tradables.select("date, PERMNO, MV, rank(signal,,"+groups+") as rank, count(PERMNO) as symCount, 0.0 as wt").where("date between "+startDate+":"+endDate).contextby("date").having("count(PERMNO)>=100").executeAs("ports")
    if WtScheme == 1:
        ports.where("rank=0").contextby("date").update(cols=["wt"], vals=["-1.0/count(PERMNO)/"+holdingDays]).execute()
        ports.where("rank="+groups+"-1").contextby("date").update(cols=["wt"], vals=["1.0/count(PERMNO)/"+holdingDays]).execute()
    elif WtScheme == 2:
        ports.where("rank=0").contextby("date").update(cols=["wt"], vals=["-MV/sum(MV)/"+holdingDays]).execute()
        ports.where("rank="+groups+"-1").contextby("date").update(cols=["wt"], vals=["MV/sum(MV)/"+holdingDays]).execute()
    else:
        raise Exception("Invalid WtScheme. valid values:1 or 2")
    return ports.select("PERMNO, date as tranche, wt").where("wt!=0").sort(bys=["PERMNO","date"]).executeAs("ports")

tradables=genTradeTables(priceData.tableName())
startDate="1996.01.01"
endDate="2017.01.01"
holdingDays=5
groups=10
ports=formPortfolio(startDate=startDate,endDate=endDate,tradables=tradables,holdingDays=holdingDays,groups=groups,WtScheme=2)
dailyRtn=priceData.select("date, PERMNO, RET as dailyRet").where("date between "+startDate+":"+endDate).executeAs("dailyRtn")

```

**Step 3:** Calculate the profit/loss for each stock in the portfolio in each of the days in the holding period. Close the positions at the end of the holding period.

```
def calcStockPnL(ports, dailyRtn, holdingDays, endDate):
    s.table(data={'age': list(range(1,holdingDays+1))}).executeAs("ages")
    ports.select("tranche").sort("tranche").executeAs("dates")
    s.run("dates = sort distinct dates.tranche")
    s.run("dictDateIndex=dict(dates,1..dates.size())")
    s.run("dictIndexDate=dict(1..dates.size(), dates)")
    ports.merge_cross(s.table(data="ages")).select("dictIndexDate[dictDateIndex[tranche]+age] as date, PERMNO, tranche, age, take(0.0,age.size()) as ret, wt as expr, take(0.0,age.size()) as pnl").where("isValid(dictIndexDate[dictDateIndex[tranche]+age]), dictIndexDate[dictDateIndex[tranche]+age]<=min(lastDays[PERMNO], "+endDate+")").executeAs("pos")
    t1= s.loadTable("pos")
    t1.merge(dailyRtn, on=["date","PERMNO"], merge_for_update=True).update(["ret"],["dailyRet"]).execute()
    t1.contextby(["PERMNO","tranche"]).update(["expr"], ["expr*cumprod(1+ret)"]).execute()
    t1.update(["pnl"],["expr*ret/(1+ret)"]).execute()
    return t1

lastDaysTable = priceData.select("max(date) as date").groupby("PERMNO").executeAs("lastDaysTable")
s.run("lastDays=dict(lastDaysTable.PERMNO,lastDaysTable.date)")
# undefine priceData to release memory
s.undef(priceData.tableName(), 'VAR')
stockPnL = calcStockPnL(ports=ports, dailyRtn=dailyRtn, holdingDays=holdingDays, endDate=endDate)

```

**Step 4:** Calculate portfolio profit/loss

```
portPnl = stockPnL.select("pnl").groupby("date").sum().sort(bys=["date"]).executeAs("portPnl")

print(portPnl.toDF())
```

#### 8.2 Time series operations

The example below shows how to calculate factor No. 98 in "101 Formulaic Alphas" by Kakushadze (2015) with daily data of US stocks.

```
def alpha98(t):
    t1 = s.table(data=t)
    # add two calcualted columns through function update
    t1.contextby(["date"]).update(cols=["rank_open","rank_adv15"], vals=["rank(open)","rank(adv15)"]).execute()
    # add two more calculated columns
    t1.contextby(["PERMNO"]).update(["decay7", "decay8"], ["mavg(mcorr(vwap, msum(adv5, 26), 5), 1..7)","mavg(mrank(9 - mimin(mcorr(rank_open, rank_adv15, 21), 9), true, 7), 1..8)"]).execute()
    # return the final results with three columns: PERMNO, date, and A98
    return t1.select("PERMNO, date, rank(decay7)-rank(decay8) as A98").contextby(["date"]).executeAs("alpha98")

US = s.loadTable(tableName="US", dbPath="dfs://US").select("PERMNO, date, PRC as vwap, PRC+rand(1.0, PRC.size()) as open, mavg(VOL, 5) as adv5, mavg(VOL,15) as adv15").where("2007.01.01<=date<=2016.12.31").contextby("PERMNO").executeAs("US")
result=alpha98(US.tableName()).where('date>2007.03.12').executeAs("result")
print(result.top(10).toDF())


```

