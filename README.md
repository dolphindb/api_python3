# Python API for DolphinDB

DolphinDB Python API supports Python 3.6 - 3.8.

Please install DolphinDB Python API with the following command:
```Console
$ pip install dolphindb
```

- [1. Execute DolphinDB script and call DolphinDB functions](#1-execute-dolphindb-script-and-dolphindb-functions)
    - [1.1 Establish DolphinDB connection](#11-establish-dolphindb-connection)
    - [1.2 Execute DolphinDB script](#12-execute-dolphindb-script)
    - [1.3 Call DolphinDB functions](#13-call-dolphindb-functions)
    - [1.4 undef](#14-undef)
    - [1.5 Automatically release variables after a query](#15-automatically-release-variables-after-a-query)
- [2. Upload Python objects to DolphinDB server](#2-upload-local-objects-to-dolphindb-server)
    - [2.1 Upload with function `upload`](#21-upload-with-session-method-upload)
    - [2.2 Upload with function `table`](#22-upload-with-method-table) 
    - [2.3 The life cycle of uploaded table](#23-the-life-cycle-of-uploaded-table) 
- [3. Create DolphinDB databases and tables](#3-create-dolphindb-databases-and-tables)
    - [3.1 Use DolphinDB Python API methods](#31-use-dolphindb-python-api-methods)
    - [3.2 Use run method](#32-use-run-method)
- [4. Import data to DolphinDB Database](#4-import-data-to-dolphindb-database)
    - [4.1 Import data as an in-memory table](#41-import-data-as-an-in-memory-table) 
    - [4.2 Import data into DFS database](#42-import-data-into-dfs-database) 
    - [4.3 Import data as an in-memory partitioned table](#43-import-data-as-an-in-memory-partitioned-table) 
- [5. Load data from DolphinDB database](#5-load-data-from-dolphindb-database)
    - [5.1 loadTable](#51-loadtable)
    - [5.2 loadTableBySQL](#52-loadtablebysql)
    - [5.3 Load tables in blocks](#53-load-tables-in-blocks)
    - [5.4 Data conversion when downloading data from DolphinDB to Python](#54-data-conversion-when-downloading-data-from-dolphindb-to-python)
- [6. Append to DolphinDB tables](#6-append-to-dolphindb-tables)
    - [6.1 Append to in-memory tables](#61-append-to-in-memory-tables) 
    - [6.2 Append to DFS tables](#62-append-to-dfs-tables)
- [7. Database and Table Operations](#7-database-and-table-operations)
    - [7.1 Summary](#71-summary)
    - [7.2 Database Operations](#72-database-operations)
    - [7.3 Table operations](#73-table-operations)
- [8. SQL queries](#8-sql-query)
    - [8.1 select](#81-select)
    - [8.2 top](#82-top)
    - [8.3 where](#83-where)
    - [8.4 groupby](#84-groupby)
    - [8.5 contextby](#85-contextby)
    - [8.6 Table joins](#86-表连接)
    - [8.7 executeAs](#87-executeas)
    - [8.8 Regression](#88-regression)
- [9. More Examples](#9-more-examples)
    - [9.1 Stock momentum strategy](#91-stock-momentum-strategy) 
    - [9.2 Time series operations](92-time-series-operations) 

DolphinDB Python API in essense encapsulates a subset of DolphinDB's scripting language. It converts Python script to DolphinDB script to be executed on the DolphinDB server. The result can either be saved on DolphinDB server or serialized to a Python client object. 

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

In the following script, we first create a session in Python, then connect the session to a DolphinDB server with specified domain name/IP address and port number. Please note that We need to start a DolphinDB server before running the following Python script.

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


#### SSL

Since server version 1.10.17 and 1.20.6, we can add the parameter 'enableSSL' when creating a session. The default value is False. 

Use the following script to enable SSL. Please also add the configuration parameter enableHTTPS=true at the server. 
```
s=ddb.session(enableSSL=True)
```

#### Asynchronous Communication

Since server version 1.10.17 and 1.20.6, we can add the parameter 'enableASYN' when creating a session. The default value is False. 

Use the following script to enable asynchronous communication. With asynchronous communication, communication with the server can only use the `session.run` method and no values are returned. This mode is ideal for writing data asynchronously. 

```
s=ddb.session(enableASYN=True)
```

### 1.2 Execute DolphinDB script

All DolphinDB script can be executed through the `run(script)` method. If the script returns an object in DolphinDB, it will be converted to an object in Python. If the script fails to run, there will be a corresponding error prompt.
```python
s = ddb.session()
s.connect("localhost", 8848)
a=s.run("`IBM`GOOG`YHOO")
repr(a)
# output
"array(['IBM', 'GOOG', 'YHOO'], dtype='<U4')"
```

User-defined functions can be generated with the `run` method:
```python
s.run("def getTypeStr(input){ \nreturn typestr(input)\n}")
```

For multiple lines of script, we can wrap them inside triple quotes for clarity. For example:
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

** Note: ** The maximum length of the text in the `run` method is 65,535 bytes. 

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

The following examples explain the use of various types of Python objects as parameters. 

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
   
    np.datetime64 is converted into corresponding DolphinDB temporal type. 
  
    | datetime64    |        DolphinDB Type            |
    | :------------- | :------------------------------ |
    | '2019-01-01'     |     DATE                      |
    | '2019-01'        |      MONTH                    |
    | '2019-01-01T20:01:01'      |     DATETIME        |
    | '2019-01-01T20:01:01.122'    |   TIMESTAMP       |
    | '2019-01-01T20:01:01.122346100'  | NANOTIMESTAMP |

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
    Please note that in the last step of the example above, when the NANOTIME type in DolphinDB is downloaded to Python, Python automatically adds 1970-01-01 as the date part.
    
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
    a = pd.DataFrame({'id': np.int32([1, 4, 3, 2, 3]),
        'date': np.array(['2019-02-03','2019-02-04','2019-02-05','2019-02-06','2019-02-07'], dtype='datetime64[D]'),
        'value': np.double([7.8, 4.6, 5.1, 9.6, 0.1]),},
        index=['one', 'two', 'three', 'four', 'five'])

    s.upload({'a':a})
    s.run("typestr",a)
    # output
    'IN-MEMORY TABLE'
    
    s.run('a')
    # output
       id date        value
    0  1  2019-02-03  7.8
    1  4  2019-02-04  4.6
    2  3  2019-02-05  5.1
    3  2  2019-02-06  9.6
    4  3  2019-02-07  0.1
    ```

### 1.4 `undef`

The session method `undef` releases specified objects in a session; method `undefAll` releases all objects in a session. `undef` can be used on the following objects: "VAR"(variable), "SHARED"(shared variable) and "DEF"(function definition). The default type is "VAR". "SHARED" refers to shares variables across sessions, such as a shared stream table. 
<!--- 
假设session中有一个DolphinDB的表对象t1, 可以通过session.undef("t1","VAR")将该表释放掉。释放后，并不一定能够看到内存马上释放。这与DolphinDB的内存管理机制有关。DolphinDB从操作系统申请的内存，释放后不会立即还给操作系统，因为这些释放的内存在DolphinDB中可以立即使用。申请内存首先从DolphinDB内部的池中申请内存，不足才会向操作系统去申请。配置文件(dolphindb.cfg)中参数maxMemSize设置的内存上限会尽量保证。譬如说设置为8GB，那么DolphinDB会尽可能利用8GB内存。所以如果用户需要反复undef内存中的一个变量以释放内存，为后面程序腾出更多内存空间，则需要将maxMemSize调整到一个合理的数值，否则当前内存没有释放，而后面需要的内存超过了系统的最大内存，DolphinDB的进程就有可能被操作系统杀掉或者出现out of memory的错误。

 --->

### 1.5 Automatically release variables after a query

Sometimes we would like to automatically release the variables created in a `run` statement after the execution is finished to reduce memory footprint. To do this, we can set the parameter clearMemory=True in Session or DBConnectionPool's `run` method. Please note that the default value of 'clearMemory' of Session's `run` method is False, whereas the default value of 'clearMemory' of DBConnectionPool's `run` method is True. 

```python
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456") 
s.run("t = 1", clearMemory = True) 
s.run("t")   
```
As the variable t is released after the execution of ```s.run("t = 1", clearMemory = True)```, the last statement will throw an exception:
```
<Exception> in run: Syntax Error: [line #1] Cannot recognize the token t 
```

### 2 Upload local objects to DolphinDB server

To use a Python object repeatedly in DolphinDB, we can upload the Python object to the DolphinDB server and specify the variable name in DolphinDB.

If a Python object is used only once at DolphinDB server, it is recommended to include it as a parameter in a function call instead of uploading it. Please refer to section 2.3 for details. 

### 2.1 Upload with Session method `upload`

The Python API provides method `upload` to upload Python objects to the DolphinDB server. The input of the method `upload` is a Python dictionary object. The key of the dictionary is the variable name in DolphinDB and the value is a Python object, which can be Numbers, Strings, Lists, DataFrame, etc.

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
Please note that a Python list with multiple data types such as a=[1,2,3.0] will be recognized as an ANY vector after being uploaded to DolphinDB. For such cases, it is recommended to use np.array instead of list. With np.array, we can specify a single data type through ```a=np.array([1,2,3.0],dtype=np.double)``` so that after uploading, "a" is a vector of DOUBLE type.

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

df = pd.DataFrame({'id': np.int32([1, 2, 3, 6, 8]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.x.avg()"))
# output
3.0
```

### 2.2 Upload with method `table`

In Python, we can use the method `table` to create a DolphinDB table object and upload it to the server. The input of the method `table` can be a dictionary, DataFrame or table name in DolphinDB.

* Upload dict

The script below defines a function `createDemoDict()` to create a dictionary. 

```python
import numpy as np

def createDemoDict():
    return {'id': [1, 2, 2, 3],
            'date': np.array(['2021.05.06', '2021.05.07', '2021.05.06', '2021.05.07'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AAPL', 'AMZN', 'AMZN'],
            'price': [129.74, 130.21, 3306.37, 3291.61]}
```

Upload the dictionary to the DolphinDB server with the method `table`, and name the table as "testDict", then we can read the table with the method `loadTable` provided by the API.

```python
import numpy as np

# save the table to DolphinDB server as table "testDict"
dt = s.table(data=createDemoDict(), tableAliasName="testDict")

# load table "testDict" on DolphinDB server 
print(s.loadTable("testDict").toDF())

# output
        date ticker    price
0 2021-05-06   AAPL   129.74
1 2021-05-07   AAPL   130.21
2 2021-05-06   AMZN  3306.37
3 2021-05-07   AMZN  3291.61
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

### 2.3 The life cycle of uploaded table

Functions `table` and `loadTable` return a local Python object. In the following example, table t1 at DolphinDB server corresponds to a local Python object t0: 
```python
t0=s.table(data=createDemoDict(), tableAliasName="t1")
```
Use the following 3 ways to release the variable at DolphinDB server t1 at DolphinDB server:

- `undef`
```python
s.undef("t1", "VAR")
```
- assign Null value to the variable at DolphinDB server
```python
s.run("t1=NULL")
```
- assign None to the local Python variable
```python
t0=None
```

After a variable is uploaded to DolphinDB server from Python with session.table function, the system creates a reference to the DolphinDB table for the Python variable. If the reference no longer exists, the DolphinDB table is automatically released. 

The following script uploads a table to DolphinDB server and then downloads data with `toDF()`. 

```python
t1=s.table(data=createDemoDict(), tableAliasName="t1")
print(t1.toDF())

#output
        date ticker    price
0 2021-05-06   AAPL   129.74
1 2021-05-07   AAPL   130.21
2 2021-05-06   AMZN  3306.37
3 2021-05-07   AMZN  3291.61
```
<!--- 

If the script above is executed again, an exception is thrown that t1 cannot be found. Python端对server端表t1的原有引用已经取消，在重新给Python端t1分配DolphinDB的表对象前，
DolphinDB要对session中的对应的表t1进行释放（通过函数`undef`取消它在session中的定义），所以会出现无法找到t1的异常。
```python
t1=s.table(data=createDemoDict(), tableAliasName="t1")
print(t1.toDF())

#output
<Server Exception> in run: Can't find the object with name t1
```

To avoid this, we can assign this table object to another local Python variable. Now, however, 2 copies of the same table object are created at DolphinDB server that correspond to t1 and t2 at Python side. 
```python
t2=s.table(data=createDemoDict(), tableAliasName="t1")
print(t2.toDF())

#output
        date ticker    price
0 2021-05-06   AAPL   129.74
1 2021-05-07   AAPL   130.21
2 2021-05-06   AMZN  3306.37
3 2021-05-07   AMZN  3291.61
```

If we would like to use the same local variable t1 to refer to the same or different uploaded tables, it is recommended not to specify the DolphinDB table name. A randomly generated temporary table name is generated for the user that can be obtained through t1.tableName(). As the same variable name t1 is used on the Python side, when re-uploading data, the previous DolphinDB table object is released and a new DolphinDB table object is used as reference to t1 on the Python side, so there is always only one corresponding DolphinDB table object.

```python
t1=s.table(data=createDemoDict())
print(t1.tableName())

#output
TMP_TBL_876e0ce5

print(t1.toDF())

#output
        date ticker    price
0 2021-05-06   AAPL   129.74
1 2021-05-07   AAPL   130.21
2 2021-05-06   AMZN  3306.37
3 2021-05-07   AMZN  3291.61

t1=s.table(data=createDemoDict())
print(t1.tableName())

#output
'TMP_TBL_4c5647af'

print(t1.toDF())

#output
        date ticker    price
0 2021-05-06   AAPL   129.74
1 2021-05-07   AAPL   130.21
2 2021-05-06   AMZN  3306.37
3 2021-05-07   AMZN  3291.61
```

 --->
In the same spirit, when loading a DFS table into memory with Python API, there is a correspondence between the local Python variable and the DolphinDB in-memory table.

Execute the following DolphinDB script:
```
db = database("dfs://testdb",RANGE, [1, 5 ,11])
t1=table(1..10 as id, 1..10 as v)
db.createPartitionedTable(t1,`t1,`id).append!(t1)
```

Then execute the following Python script:
```python
pt1=s.loadTable(tableName='t1',dbPath="dfs://testdb")
```

The script above creates a DFS table on DolphinDB server, then loads its metadata into memory with function `loadTable`and assigns it to the local Python object pt1. Please note t1 is the DFS table name instead of the DolphinDB table name corresponding to the local Python variable pt1. The corresponding DolphinDB table name can be obtained with pt1.tableName(). 
```python
print(pt1.tableName())
'TMP_TBL_4c5647af'
```

If a Python variable is used only once at DolphinDB server, it is recommended to include it as a parameter in a function call instead of uploading it. A function call does not cache data. After the function call is executed, all variables are released. Moreover, a function call is faster to execute as the network transmission only occurs once. 

## 3 Create DolphinDB databases and tables

Use DolphinDB Python API methods or "run" method to create DolphinDB databases and tables in Python API. 

### 3.1 Use DolphinDB Python API methods

```python
import numpy as np
import pandas as pd
import dolphindb.settings as keys
```

#### 3.1.1 Create partitioned databases and tables with VALUE domain

Each date is a partition:
```python
dbPath="dfs://db_value_date"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
dates=np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]")
db = s.database(dbName='mydb', partitionType=keys.VALUE, partitions=dates,dbPath=dbPath)
df = pd.DataFrame({'datetime':np.array(['2012-01-01T00:00:00', '2012-01-02T00:00:00'], dtype='datetime64'), 'sym':['AA', 'BB'], 'val':[1,2]})
t = s.table(data=df)
db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime').append(t)
re=s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```

Each month is a partition:
```python
dbPath="dfs://db_value_month"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
months=np.array(pd.date_range(start='2012-01', end='2012-10', freq="M"), dtype="datetime64[M]")
db = s.database(dbName='mydb', partitionType=keys.VALUE, partitions=months,dbPath=dbPath)
df = pd.DataFrame({'date': np.array(['2012-01-01', '2012-02-01', '2012-05-01', '2012-06-01'], dtype="datetime64"), 'val':[1,2,3,4]})
t = s.table(data=df)
db.createPartitionedTable(table=t, tableName='pt', partitionColumns='date').append(t)
re=s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```

#### 3.1.2 Create partitioned databases and tables with RANGE domain

Partitions are based on id ranges:
```python
dbPath="dfs://db_range_int"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
db = s.database(dbName='mydb', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dbPath)
df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
t = s.table(data=df, tableAliasName='t')
db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```

#### 3.1.3 Create partitioned databases and tables with LIST domain

Partitions are based on lists of stock tickers:
```python
dbPath="dfs://db_list_sym"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
db = s.database(dbName='mydb', partitionType=keys.LIST, partitions=[['IBM', 'ORCL', 'MSFT'], ['GOOG', 'FB']],dbPath=dbPath)
df = pd.DataFrame({'sym':['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'], 'val':[1,2,3,4,5]})
t = s.table(data=df)
db.createPartitionedTable(table=t, tableName='pt', partitionColumns='sym').append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```

#### 3.1.4 Create partitioned databases and tables with HASH domain

Partitions are based on hash values of id:
```python
dbPath="dfs://db_hash_int"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
db = s.database(dbName='mydb', partitionType=keys.HASH, partitions=[keys.DT_INT, 2], dbPath=dbPath)
df = pd.DataFrame({'id':[1,2,3,4,5], 'val':[10, 20, 30, 40, 50]})
t = s.table(data=df)
pt = db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id')
pt.append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```

#### 3.1.5 Create partitioned databases and tables with COMPO domain

The first level of partitions uses a VALUE domain and the second level of partitions uses a RANGE domain. 

Please note that when creating a DFS database with COMPO domain, the parameter 'dbPath' for each partition level must be either an empty string or unspecified.
```python
db1 = s.database('db1', partitionType=keys.VALUE,partitions=np.array(["2012-01-01", "2012-01-06"], dtype="datetime64[D]"), dbPath='')
db2 = s.database('db2', partitionType=keys.RANGE,partitions=[1, 6, 11], dbPath='')
dbPath="dfs://db_compo_test"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
db = s.database(dbName='mydb', partitionType=keys.COMPO, partitions=[db1, db2], dbPath=dbPath)
df = pd.DataFrame({'date':np.array(['2012-01-01', '2012-01-01', '2012-01-06', '2012-01-06'], dtype='datetime64'), 'val': [1, 6, 1, 6]})
t = s.table(data=df)
db.createPartitionedTable(table=t, tableName='pt', partitionColumns=['date', 'val']).append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```

### 3.2 Use `run` method

```python
dbPath="dfs://valuedb"
dstr = """
dbPath="dfs://valuedb"
if (existsDatabase(dbPath)){
    dropDatabase(dbPath)
}
mydb=database(dbPath, VALUE, ['AMZN','NFLX', 'NVDA'])
t=table(take(['AMZN','NFLX', 'NVDA'], 10) as sym, 1..10 as id)
mydb.createPartitionedTable(t,`pt,`sym).append!(t)

"""
t1=s.run(dstr)
t1=s.loadTable(tableName="pt",dbPath=dbPath)
t1.toDF()

# output

     sym	id
0	AMZN	1
1	AMZN	4
2	AMZN	7
3	AMZN	10
4	NFLX	2
5	NFLX	5
6	NFLX	8
7	NVDA	3
8	NVDA	6
9	NVDA	9
```

### 4 Import data to DolphinDB Database

There are 2 types of DolphinDB databases: in-memory database and DFS (Distributed File System) database. 

The examples below use a csv file [data_example.csv](data/example.csv). Please download it and save it under the directory as specified in "WORK_DIR" in the example below. 

#### 4.1 Import data as an in-memory table

To import text files into DolphinDB as an in-memory table, use session method `loadText`. It returns a DolphinDB table object in Python that corresponds to an in-memory table on the DolphinDB server. This DolphinDB table object in Python has a method `toDF` to convert it to a pandas DataFrame.

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

#### 4.2 Import data into DFS database

To load data files that are larger than available memory into DolphinDB, we can load data into a DFS database.

#### 4.2.1 Create a DFS database

The examples below use the database "valuedb". The following script deletes the database if it already exists. 

```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
```

Now create a value-based DFS database "valuedb" with a session method `database`. We use a VALUE partition with stock ticker as the partitioning column. The parameter "partitions" indicates the partitioning scheme.

```
import dolphindb.settings as keys

s.database(dbName='mydb', partitionType=keys.VALUE, partitions=['AMZN','NFLX', 'NVDA'], dbPath='dfs://valuedb')
# this is equivalent to:   s.run("db=database('dfs://valuedb', VALUE, ['AMZN','NFLX', 'NVDA'])") 
```

In addition to VALUE partition, DolphinDB also supports RANGE, LIST, COMBO, and HASH partitions.

Once a DFS database has been created, the partition domain cannot be changed. The partitioning scheme generally cannot be revised, but we can use functions `addValuePartitions` and `addRangePartitions` to add partitions for DFS databases with VALUE and RANGE partitions (or VALUE and RANGE partitions in a COMPO domain), respectively. 

#### 4.2.2 Create a partitioned table and append data to the table

After a DFS database is created successfully, we can import text files to a partitioned table in the DFS database with function `loadTextEx`. If the partitioned table does not exist, `loadTextEx` creates it and appends the imported data to it. Otherwise, the function appends the imported data to the partitioned table.

The parameters of function `loadTextEx`:
- "dbPath" is the database path
- "tableName" is the partitioned table name
- "partitionColumns" is the partitioning columns
- "remoteFilePath" is the absolute path of the text file on the DolphinDB server. 
- "delimiter" is the delimiter of the text file (comma by default).

In the following example, function `loadTextEx` creates a partitioned table "trade" on the DolphinDB server and then appends the data from "example.csv" to the table. 

```python
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")

trade = s.loadTextEx(dbPath="mydb", tableName='trade',partitionColumns=["TICKER"], remoteFilePath=WORK_DIR + "/data_example.csv")
print(trade.toDF())

# output
      TICKER       date       VOL      PRC      BID      ASK
0       AMZN 1997-05-15   6029815   23.500   23.500   23.625
1       AMZN 1997-05-16   1232226   20.750   20.500   21.000
2       AMZN 1997-05-19    512070   20.500   20.500   20.625
3       AMZN 1997-05-20    456357   19.625   19.625   19.750
4       AMZN 1997-05-21   1577414   17.125   17.125   17.250
...      ...        ...       ...      ...      ...      ...
13131   NVDA 2016-12-23  16193331  109.780  109.770  109.790
13132   NVDA 2016-12-27  29857132  117.320  117.310  117.320
13133   NVDA 2016-12-28  57384116  109.250  109.250  109.290
13134   NVDA 2016-12-29  54384676  111.430  111.260  111.420
13135   NVDA 2016-12-30  30323259  106.740  106.730  106.750

[13136 rows x 6 columns]

# the number of rows of the table
print(trade.rows)
# output
13136

# the number of columns of the table
print(trade.cols)
# output
6

print(trade.schema)
# output
     name typeString  typeInt
0  TICKER     SYMBOL       17
1    date       DATE        6
2     VOL        INT        4
3     PRC     DOUBLE       16
4     BID     DOUBLE       16
5     ASK     DOUBLE       16
```

```python
trade = s.table(dbPath="dfs://valuedb", data="trade")
```

#### 4.3 Import data as an in-memory partitioned table

#### 4.3.1 `loadTextEx`

Operations on an in-memory partitioned table are faster than those on a nonpartitioned in-memory table as the former utilizes parallel computing.

We can use function `loadTextEx` to create an in-memory partitioned database with an empty string for the parameter "dbPath".

```python
import dolphindb.settings as keys

s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="")

trade=s.loadTextEx(dbPath="mydb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/data_example.csv")
trade.toDF()
```

#### 4.3.2 `ploadText`

Function `ploadText` loads a text file in parallel to generate an in-memory partitioned table. It runs much faster than `loadText`.

```python
trade=s.ploadText(WORK_DIR+"/data_example.csv")
print(trade.rows)

# output
13136
```

### 4.3.3 Concurrent Writes to DFS tables

DFS tables in DolphinDB support concurrent writes.

Note that DolphinDB does not allow multiple writers to write to the same partition at the same time. Therefore, when multiple threads are writing to the same database concurrently, we need to make sure each of them writes to a different partition. Python API provides a convenient way for it. 

With DolphinDB server version 1.30 or above, we can write to DFS tables with the PartitionedTableAppender object in Python API. The user needs to first specify a connection pool. The system then obtains information about partitions before assigning the partitions to the connection pool for concurrent writing. A partition can only be written to by one connection at a time. 

With the latest DolphinDB version 1.30 and above, we can write to partitioned tables using the PartitionTableAppender object in the Python API. The rationale is to design a connection pool for multithreaded writing, and then use server's schema function to obtain partition information for distributed tables, classifying the data written by the user by the specified partition column, and handing it over to different connections for parallel writing.
  
```python
PartitionedTableAppender(dbPath, tableName, partitionColName, dbConnectionPool)
```
- dbPath: DFS database path
- tableName: name of a DFS table
- partitionColName: partitioning column name
- dbConnectionPool: connection pool

The following script creates database dfs://Rangedb and partitioned table pt, then creates a connection pool for PartitionedTableAppender, and use the `append` method to write data to pt concurrently.

```python
import pandas as pd
import dolphindb as ddb
import numpy as np
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
script='''
dbPath = "dfs://Rangedb"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`val1`val2,[INT,DOUBLE,SYMBOL])
        db=database(dbPath,RANGE,  1  100  200  300)
        pt = db.createPartitionedTable(t, `pt, `id)
'''
s.run(script)
s.close()

pool = ddb.DBConnectionPool ("localhost", 8848, 20, "admin", "123456")
appender = ddb.PartitionedTableAppender("dfs://Rangedb","pt", "id", pool)
v = []
for i in range(0,10000000):
    v.append("a"+str(i%100))
data = pd.DataFrame({"id":np.random.randint(1,300,10000000),"val1":np.random.rand(10000000),"val2":v})
re = appender.append(data)
print(re)
```

#### 5 Load data from DolphinDB database

#### 5.1 `loadTable`

Use function `loadTable` to load a table from a DolphinDB database. Parameter "tableName" indicates the partitioned table name; "dbPath" is the database location. 


```python
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")

print(trade.schema)
#output
     name typeString  typeInt comment
0  TICKER     SYMBOL       17
1    date       DATE        6
2     VOL        INT        4
3     PRC     DOUBLE       16
4     BID     DOUBLE       16
5     ASK     DOUBLE       16

print(trade.toDF())

# output
      TICKER       date       VOL      PRC      BID      ASK
0       AMZN 1997-05-15   6029815   23.500   23.500   23.625
1       AMZN 1997-05-16   1232226   20.750   20.500   21.000
2       AMZN 1997-05-19    512070   20.500   20.500   20.625
3       AMZN 1997-05-20    456357   19.625   19.625   19.750
4       AMZN 1997-05-21   1577414   17.125   17.125   17.250
...      ...        ...       ...      ...      ...      ...
13131   NVDA 2016-12-23  16193331  109.780  109.770  109.790
13132   NVDA 2016-12-27  29857132  117.320  117.310  117.320
13133   NVDA 2016-12-28  57384116  109.250  109.250  109.290
13134   NVDA 2016-12-29  54384676  111.430  111.260  111.420
13135   NVDA 2016-12-30  30323259  106.740  106.730  106.750
```

#### 5.2 `loadTableBySQL`

Method `loadTableBySQL` imports only the rows of a DFS table that satisfy the filtering conditions in a SQL query as an in-memory partitioned table.

```python
import os
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"  or os.path.exists("dfs://valuedb")):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
t = s.loadTextEx(dbPath="mydb",  tableName='trade',partitionColumns=["TICKER"], remoteFilePath=WORK_DIR + "/data_example.csv")

trade = s.loadTableBySQL(tableName="trade", dbPath="dfs://valuedb", sql="select * from trade where date>2010.01.01")
print(trade.rows)

# output
5286
```

### 5.3 Load tables in blocks

For tables with large amounts of data, Python API provides a way to load them in blocks (for DolphinDB 1.20.5 or above, and DolphinDB Python API 1.30.0.6 or above). 

Execute the following script in Python to create a large table:
```python
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
script='''
    rows=100000;
    testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);
'''
s.run(script)
```

Use the parameter 'fetchSize' of the `run` method to specify the size of a block. A BlockReader object is returned. We can use the `read` method of the BlockReader object to read data in blocks. Please note that the value of 'fetchSize' cannot be smaller than 8192. 

```python
script1='''
select * from testblock
'''
block= s.run(script1, fetchSize = 8192)
total = 0
while block.hasNext():
    tem = block.read() 
    total+=len(tem)
                
print("total=", total)
```

<!--- 

///待处理部分 1 

使用上述分段读取的方法时，若数据未读取完毕，需要调用skipAll方法来放弃读取后续数据，才能继续执行后续代码。否则会导致套接字缓冲区滞留数据，引发后续数据的反序列化失败。

```python
script='''
    rows=100000;
    testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);
'''
s.run(script)

script1='''
select * from testblock
'''
block= s.run(script1, fetchSize = 8192)
re = block.read()
block.skipAll()
s.run("1 + 1") //若没有调用skipAll，执行此代码会抛出异常。
```

 --->


### 5.4 Data conversion when downloading data from DolphinDB to Python

#### 5.4.1 Data Form Conversion

DolphinDB Python API saves data downloaded from DolphinDB server as native Python objects.

|DolphinDB|Python|DolphinDB data|Python data|
|-------------|----------|-------------|-----------|
|scalar|Numbers, Strings, NumPy.datetime64|see section 6.3.2|see section 6.3.2
|vector|NumPy.array|1..3|[1 2 3]
|pair|Lists|1:5|[1, 5]
|matrix|Lists|1..6$2:3|[array([[1, 3, 5],[2, 4, 6]], dtype=int32), None, None]
|set|Sets|set(3 5 4 6)|{3, 4, 5, 6}|
|dictionary|Dictionaries|dict(['IBM','MS','ORCL'], 170.5 56.2 49.5)|{'MS': 56.2, 'IBM': 170.5, 'ORCL': 49.5}|
|table|pandas.DataFame|see section 6.1|see section 6.1|

#### 5.4.2 Data Type Conversion

The table below explains data type conversion when data is downloaded from DolphinDB database and converted into a Python DataFrame with function `toDF()`. 
- DolphinDB CHAR types are converted into Python int64 type. Use Python function `chr` to convert CHAR type into a character. 
- As all temporal types in Python pandas are datetime64, all DolphinDB temporal types [are converted into datetime64 type](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803). MONTH type such as 2012.06M is converted into 2012-06-01 (the first day of the month). 
- TIME, MINUTE, SECOND and NANOTIME types do not include information about date. 1970-01-01 is automatically added during conversion. For example, 13:30m is converted into 1970-01-01 13:30:00. 

| DolphinDB type | Python type | DolphinDB data                                  | Python data                             |
| ------------- | ---------- | ----------------------------------------------- | -------------------------------------- |
| BOOL          | bool       | [true,00b]                                      | [True, nan]                            |
| CHAR          | int64      | [12c,00c]                                       | [12, nan]                              |
| SHORT         | int64      | [12,00h]                                        | [12, nan]                              |
| INT           | int64      | [12,00i]                                        | [12, nan]                              |
| LONG          | int64      | [12l,00l]                                       | [12, nan]                              |
| DOUBLE        | float64    | [3.5,00F]                                       | [3.5,nan]                              |
| FLOAT         | float64    | [3.5,00f]                                       | [3.5, nan]                             |
| SYMBOL        | object     | symbol(["AAPL",NULL])                           | ["AAPL",""]                            |
| STRING        | object     | ["AAPL",string()]                               | ["AAPL", ""]                           |
| DATE          | datetime64 | [2012.6.12,date()]                              | [2012-06-12, NaT]                      |
| MONTH         | datetime64 | [2012.06M, month()]                             | [2012-06-01, NaT]                      |
| TIME          | datetime64 | [13:10:10.008,time()]                           | [1970-01-01 13:10:10.008, NaT]         |
| MINUTE        | datetime64 | [13:30,minute()]                                | [1970-01-01 13:30:00, NaT]             |
| SECOND        | datetime64 | [13:30:10,second()]                             | [1970-01-01 13:30:10, NaT]             |
| DATETIME      | datetime64 | [2012.06.13 13:30:10,datetime()]                | [2012-06-13 13:30:10,NaT]              |
| TIMESTAMP     | datetime64 | [2012.06.13 13:30:10.008,timestamp()]           | [2012-06-13 13:30:10.008,NaT]          |
| NANOTIME      | datetime64 | [13:30:10.008007006, nanotime()]                | [1970-01-01 13:30:10.008007006,NaT]    |
| NANOTIMESTAMP | datetime64 | [2012.06.13 13:30:10.008007006,nanotimestamp()] | [2012-06-13 13:30:10.008007006,NaT]    |
| UUID          | object     | 5d212a78-cc48-e3b1-4235-b4d91473ee87            | "5d212a78-cc48-e3b1-4235-b4d91473ee87" |
| IPADDR        | object     | 192.168.1.13                                    | "192.168.1.13"                         |
| INT128        | object     | e1671797c52e15f763380b45e841ec32                | "e1671797c52e15f763380b45e841ec32"     |

#### 5.4.3 Missing value processing

When data is downloaded from DolphinDB database and converted into a Python DataFrame with function `toDF()`, NULLs of logical, temporal and numeric types are converted into NaN or NaT; NULLs of string types are converted into empty string. 



 

## 6 Append to DolphinDB tables

This section introduces how to use Python API to upload data and append it to DolphinDB tables. 

### 6.1 Append to in-memory tables

- Use function `tableInsert` to append data or a table 
- Use `insert into` statement to append data

Execute the following script in Python to generate an empty in-memory table to be used in the examples later:
```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

script = """t = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)
```

#### 6.1.1 Use `tableInsert` function to append a List to an in-memory table

<!--- 
///待处理部分
若Python程序获取的数据可以组织成List方式，且保证数据类型正确的情况下，可以直接使用`tableInsert`函数来批量保存多条数据。这个函数可以接受多个数组作为参数，将数组追加到数据表中。这样做的好处是，可以在一次访问服务器请求中将上传数据对象和追加数据这两个步骤一次性完成，相比`INSERT INTO`做法减少了一次访问DolphinDB服务器的请求。
 --->

```python
ids = [1,2,3]
dates = np.array(['2019-03-03','2019-03-04','2019-03-05' ], dtype="datetime64[D]")
tickers=['AAPL','GOOG','AAPL']
prices = [302.5, 295.6, 297.5]
args = [ids, dates, tickers, prices]
s.run("tableInsert{tglobal}", args)
#output
3

s.run("tglobal")
#output
   id       date ticker  price
0   1 2019-03-03   AAPL  302.5
1   2 2019-03-04   GOOG  295.6
2   3 2019-03-05   AAPL  297.5
```

#### 6.1.2 Use `tableInsert` function to append a DataFrame to an in-memory table

- If there is no temporal type column in the table

We can upload a DataFrame to the server and append it to an in-memory table with [partial application](https://www.dolphindb.com/help/PartialApplication.html). 

```python
script = """t = table(1000:0,`id`ticker`price, [INT,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)

tb=pd.DataFrame({'id': [1, 2, 2, 3],
                 'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                 'price': [22, 3.5, 21, 26]})
s.run("tableInsert{tglobal}",tb)

#output
4

s.run("tglobal")
#output
   id	ticker	price
0	1	AAPL	22.0
1	2	AMZN	3.5
2	2	AMZN	21.0
3	3	A	26.0
```

- If there is a temporal type column in the table

As [the only temporal data type in Python pandas is datetime64](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803), all temporal columns of a DataFrame are converted into nanotimestamp type after uploaded to DolphinDB. Therefore, if the DataFrame contains a temporal column, we need to convert its data type before appending the DataFrame to an in-memory table. In the following example, we convert the nanotimestamp type into date type. 
```python
script = """t = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)

import pandas as pd
tb=pd.DataFrame(createDemoDict())
s.upload({'tb':tb})
s.run("tableInsert(tglobal,(select id, date(date) as date, ticker, price from tb))")
s.run("tglobal")

#output
   id	      date ticker	price
0	1	2019-02-04	AAPL	22.0
1	2	2019-02-05	AMZN	3.5
2	2	2019-02-09	AMZN	21.0
3	3	2019-02-13	A	26.0
```

#### 6.1.3 `insert into`

To insert a single row of data:
```python
import numpy as np

script = "insert into tglobal values(%s, date(%s), %s, %s)" % (1, np.datetime64("2019-01-01").astype(np.int64), '`AAPL', 5.6)
s.run(script)
```
As introduced in 6.1.2, we also need to convert the temporal column data type. 

To insert multiple rows of data:
```python
import numpy as np
import random

rowNum = 5
ids = np.arange(1, rowNum+1, 1, dtype=np.int32)
dates = np.array(pd.date_range('4/1/2019', periods=rowNum), dtype='datetime64[D]')
tickers = np.repeat("AA", rowNum)
prices = np.arange(1, 0.6*(rowNum+1), 0.6, dtype=np.float64)
s.upload({'ids':ids, "dates":dates, "tickers":tickers, "prices":prices})
script = "insert into tglobal values(ids,dates,tickers,prices);"
s.run(script)
```
<!--- 
**Please note that **，从性能方面考虑，不建议使用`INSERT INTO`来插入数据，因为服务器端要对INSERT语句进行解析会造成额外开销。
 --->
#### 6.1.4 Use `tableAppender` object for automatic temporal type conversion when appending

As [the only temporal data type in Python pandas is datetime64](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803), all temporal columns of a DataFrame are converted into nanotimestamp type after uploaded to DolphinDB. Each time we use `tableInsert` or `insert into` to append a DataFrame with a temporal column to an in-memory table or DFS table, we need to conduct a data type conversion for the temporal column. For automatic data type conversion in these situations, Python API offers tableAppender object. 


```
tableAppender(dbPath="", tableName="", ddbSession=None, action="fitColumnType")
```
- dbPath: The path of a DFS database. Leave it unspecified for in-memory tables. 
- tableName: The name of a table. 
- ddbSession: A session connected to DolphinDB server. 
- action: What to do when appending. Now only supports "fitColumnType", which means convert temporal column types. 

The example below appends to a shared in-memory table t with tableAppender:
```python
import pandas as pd
import dolphindb as ddb 
import numpy as np
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

s.run("share table(1000:0, `sym`timestamp`qty, [SYMBOL, TIMESTAMP, INT]) as t")
appender = ddb.tableAppender(tableName="t", ddbSession=s)
sym = ['A1', 'A2', 'A3', 'A4', 'A5']
timestamp = np.array(['2012-06-13 13:30:10.008', 'NaT','2012-06-13 13:30:10.008', '2012-06-13 15:30:10.008', 'NaT'], dtype="datetime64")
qty = np.arange(1, 6)
data = pd.DataFrame({'sym': sym, 'timestamp': timestamp, 'qty': qty})
num = appender.append(data)
print(num)  
t = s.run("t")
print(t)
```

The example below appends to a DFS table pt with tableAppender:
```python
import pandas as pd
import dolphindb as ddb
import numpy as np
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
script='''
dbPath = "dfs://tableAppender"
if(existsDatabase(dbPath))
    dropDatabase(dbPath)
t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
pt = db.createPartitionedTable(t, `pt, `qty)
'''
s.run(script)
appender = ddb.tableAppender("dfs://tableAppender","pt", s)
sym = list(map(str, np.arange(100000, 600000)))
date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],50000), dtype="datetime64[D]")
month = np.array(np.tile(['1965-08', 'NaT','2012-02', '2012-03', 'NaT'],100000), dtype="datetime64")
time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],100000), dtype="datetime64")
second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],100000), dtype="datetime64")
nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT', '2015-06-09T23:59:59.999008007'],100000), dtype="datetime64")
qty = np.arange(100000, 600000)
data = pd.DataFrame({'sym': sym, 'date': date, 'month':month, 'time':time, 'minute':time, 'second':second, 'datetime':second, 'timestamp':time, 'nanotimestamp':nanotime, 'qty': qty})
num = appender.append(data)
print(num)
print(s.run("select * from pt"))
```

### 6.2 Append to DFS tables

Use the following script to create a DFS table in DolphinDB:

```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

dbPath="dfs://testPython"
tableName='t1'
script = """
dbPath='{db}'
if(existsDatabase(dbPath))
	dropDatabase(dbPath)
db = database(dbPath, VALUE, 0..100)
t1 = table(10000:0,`id`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])
insert into t1 values (0,true,'a',122h,21,22l,2012.06.12,2012.06M,13:10:10.008,13:30m,13:30:10,2012.06.13 13:30:10,2012.06.13 13:30:10.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,'','')
t = db.createPartitionedTable(t1, `{tb}, `id)
t.append!(t1)""".format(db=dbPath,tb=tableName)
s.run(script)
```

Use `tableInsert` to append data to a DFS table. In the following example, we use the user-defined function `createDemoDataFrame()` to create a DataFrame, then append it to a DFS table. Please note that when appending to a DFS table, the temporal data types are automatically converted.
```python
tb = createDemoDataFrame()
s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb=tableName), tb)
```

<!--- 

///待处理部分 2

### 6.3 异步追加数据

在高吞吐率的场景下，尤其是典型的高速小数据写入时，使用API的异步调用可以有效提高客户端的任务吞吐量。异步方式提交有如下几个特点：

- API客户端提交任务后，服务端接到任务后客户端即认为任务已完成。
- API客户端无法得知任务在服务端执行的情况和结果。
- API客户端的异步任务提交时间取决于提交参数的序列化及其网络传输时间。

**Please note**: 异步方式不适用前后任务之间有依赖的场景。比如两个任务，一个任务向分布式数据库写入数据，后一个任务将新写入的数据结合历史数据做分析。这样后一个任务对前一任务有依赖的场景，不能使用异步的方式。

Python API打开ASYN（异步）模式可以参照1.1节建立DolphinDB连接的部分，即设置session的`enableASYN`参数为`True`.
```python
s=ddb.session(enableASYN=True)
```

通过这种方式异步写入数据可以节省API端检测返回值的时间，在DolphinDB中可以参考如下脚本使用异步方式追加数据(以追加数据到分布式表为例)。

```python
import dolphindb as ddb
import numpy as np
import dolphindb.settings as keys
import pandas as pd

s = ddb.session(enableASYN=True) # 打开异步模式
s.connect("localhost", 8848, "admin", "123456")
dbPath = "dfs://testDB"
tableName = "tb1"

script = """
dbPath="dfs://testDB"

tableName=`tb1

if(existsDatabase(dbPath))
    dropDatabase(dbPath)
db=database(dbPath, VALUE, ["AAPL", "AMZN", "A"])

testDictSchema=table(5:0, `id`ticker`price, [INT,STRING,DOUBLE])

tb1=db.createPartitionedTable(testDictSchema, tableName, `ticker)
"""
s.run(script) #此处脚本可以在服务器端运行

tb = pd.DataFrame({'id': [1, 2, 2, 3],
                   'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                   'price': [22, 3.5, 21, 26]})

s.run("append!{{loadTable('{db}', `{tb})}}".format(db=dbPath, tb=tableName), tb)
```

**Please note**：异步通讯的条件下，与服务端的通讯只能通过`session.run()`方法，**并无返回值**。

由于异步在数据吞吐量较高的情况下使用效果更佳，下面给出一个Python API写入流数据表的案例，具体流表的使用请参考第9章 Python Streaming API的内容。

```python
import dolphindb as ddb
import numpy as np
import pandas as pd
import random
import datetime

s = ddb.session(enableASYN=True)
s.connect("localhost", 8848, "admin", "123456")

n = 100

script = """trades = streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT])"""
s.run(script) # 此处的脚本可以在服务端直接运行

# 随机生成一个dataframe
sym_list = ['IBN', 'GTYU', 'FHU', 'DGT', 'FHU', 'YUG', 'EE', 'ZD', 'FYU']
price_list = []
time_list = []
for i in range(n):
    price_list.append(round(np.random.uniform(1, 100), 1))
    time_list.append(np.datetime64(datetime.date(2020, random.randint(1, 12), random.randint(1, 20))))
    
tb = pd.DataFrame({'time': time_list,
                   'sym': np.random.choice(sym_list, n),
                   'price': price_list,
                   'id': np.random.choice([1, 2, 3, 4, 5], n)})

s.run("append!{trades}", tb)
```

由于异步模式的特殊性，对于需要进行类型转换的时间类型的数据，注意不要在异步追加数据时先用upload提交数据到服务端然后再在服务端用SQL脚本再进行类型转换，因为异步可能导致数据提交还未完成却已经开始执行SQL的情况。为了解决这个问题，我们可以再服务端定义函数视图，这样在客户端只需要做上传工作即可。

首先，在服务端我们先定义一个视图函数`appendStreamingData`：

```txt
login("admin","123456")
trades = streamTable(10000:0,`time`sym`price`id, [DATE,SYMBOL,DOUBLE,INT])
share trades as tglobal
def appendStreamingData(mutable data){
tableInsert(tglobal, data.replaceColumn!(`time, date(data.time)))
}
addFunctionView(appendStreamingData)
```

然后在客户端异步追加数据：

```python
import dolphindb as ddb
import numpy as np
import pandas as pd
import random
import datetime

s = ddb.session(enableASYN=True)
s.connect("localhost", 8848, "admin", "123456")

n = 100

# 随机生成一个dataframe
sym_list = ['IBN', 'GTYU', 'FHU', 'DGT', 'FHU', 'YUG', 'EE', 'ZD', 'FYU']
price_list = []
time_list = []
for i in range(n):
    price_list.append(round(np.random.uniform(1, 100), 1))
    time_list.append(np.datetime64(datetime.date(2020, random.randint(1, 12), random.randint(1, 20))))
    
tb = pd.DataFrame({'time': time_list,
                   'sym': np.random.choice(sym_list, n),
                   'price': price_list,
                   'id': np.random.choice([1, 2, 3, 4, 5], n)})

s.upload({'tb': tb})
s.run("trades.append!(tb)")
```
 --->

## 7 Database and Table Operations

### 7.1 Summary

A Session object has methods with the same purpose as certain DolphinDB built-in functions to work with databases and tables.

* For databases/partitions

| **method**                                       | **details**          |
| :----------------------------------------------- | :------------------- |
| database                                         | Create a database    |
| dropDatabase(dbPath)                             | Delete a database    |
| dropPartition(dbPath, partitionPaths, tableName) | Delete a database partition  |
| existsDatabase                                   | Determine if a database exists  |

* For tables

| **method**                   | **details**                      |
| :--------------------------- | :------------------------------- |
| dropTable(dbPath, tableName) | Delete a table                   |
| existsTable                  | Determine if a table exists      |
| loadTable                    | Load a table into memory         |
| table                        | Create a table                   |

Can all the following methods for a table object in Python，这些方法是Table类方法。

| **method**           | **details**                                  |
| :------------------- | :------------------------------------------- |
| append               | Append to a table                            |
| drop(colNameList)    | Delete columns of a table                    |
| executeAs(tableName) | Save result as an in-memory table with the specified name |
| execute()            | Execute script. Used with `update` or `delete`|
| toDF()               | Convert DolphinDB table object into pandas DataFrame |

The tables above only lists most commonly used methods. Please refer to session.py and table.py文件关于Session类和Table类提供的所有方法。

<!--- 
**请注意**，Python API实质上封装了DolphinDB的脚本语言。Python代码被转换成DolphinDB脚本在DolphinDB服务器执行，执行结果保存到DolphinDB服务器或者序列化到Python客户端。例如，在Python客户端创建一个数据表时，有如下几种方式：

1.调用Session类提供的`table`方法：

```python
tdata = {'id': [1, 2, 2, 3],
         'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
         'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
         'price': [22, 3.5, 21, 26]}
s.table(data=tdata).executeAs('tb')
```

2.调用Session类提供的`upload`方法：

```python
tdata = pd.DataFrame({'id': [1, 2, 2, 3], 
                      'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
                      'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'], 
                      'price': [22, 3.5, 21, 26]})
s.upload({'tb': tdata})
```

3.调用Session类提供的`run`方法：
```python
s.run("tb=table([1, 2, 2, 3] as id, [2019.02.04,2019.02.05,2019.02.09,2019.02.13] as date, ['AAPL','AMZN','AMZN','A'] as ticker, [22, 3.5, 21, 26] as price)")
```

以上3种方式都等价于在DolphinDB服务端调用`table`方法创建一个名为'tb'的内存数据表：

```
tb=table([1, 2, 2, 3] as id, [2019.02.04,2019.02.05,2019.02.09,2019.02.13] as date, ['AAPL','AMZN','AMZN','A'] as ticker, [22, 3.5, 21, 26] as price)
```

下面，我们在Python环境中调用Session类提供的各种方法创建分布式数据库和表，并向表中追加数据。

```python
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
dbPath="dfs://testDB"
tableName='tb'
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath)
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AAPL", "AMZN", "A"], dbPath=dbPath)
tdata=s.table(data=createDemoDict()).executeAs("testDict")
s.run("mydb.createPartitionedTable(testDict, `{tb}, `ticker)".format(tb=tableName))
tb=s.loadTable(tableName, dbPath)
tb.append(tdata)
tb.toDF()

# output
    id       date ticker  price
 0   3 2019-02-13      A   26.0
 1   1 2019-02-04   AAPL   22.0
 2   2 2019-02-05   AMZN    3.5
 3   2 2019-02-09   AMZN   21.0
```

类似地，我们也可以在Python环境中直接调用Session类提供的`run`方法来创建数据库和表，再调用DolphinDB的内置函数`append！`来追加数据。需要注意的是，在Python客户端远程调用DolphinDB内置函数`append！`时，服务端会向客户端返回一个表结构，增加通信量。因此，我们建议通过`tableInsert`函数来追加数据。

```python
import dolphindb as ddb
import numpy as np

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
dbPath="dfs://testDB"
tableName='tb'
testDict=pd.DataFrame(createDemoDict())
script="""
dbPath='{db}'
if(existsDatabase(dbPath))
    dropDatabase(dbPath)
db=database(dbPath, VALUE, ["AAPL", "AMZN", "A"])
testDictSchema=table(5:0, `id`date`ticker`price, [INT,DATE,STRING,DOUBLE])
db.createPartitionedTable(testDictSchema, `{tb}, `ticker)""".format(db=dbPath,tb=tableName)
s.run(script)
# s.run("append!{{loadTable({db}, `{tb})}}".format(db=dbPath,tb=tableName),testDict)
s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb=tableName),testDict)
s.run("select * from loadTable('{db}', `{tb})".format(db=dbPath,tb=tableName))

# output
    id	date	ticker	price
0	3	2019-02-13	A	26.0
1	1	2019-02-04	AAPL	22.0
2	2	2019-02-05	AMZN	3.5
3	2	2019-02-09	AMZN	21.0
```

上述两个例子等价于在DolphinDB服务端执行以下脚本，创建分布式数据库和表，并向表中追加数据。

```
db_script="""
login("admin","123456")
dbPath="dfs://testDB"
tableName=`tb
if(existsDatabase(dbPath))
    dropDatabase(dbPath)
db=database(dbPath, VALUE, ["AAPL", "AMZN", "A"])
testDictSchema=table(5:0, `id`date`ticker`price, [INT,DATE,STRING,DOUBLE])
tb=db.createPartitionedTable(testDictSchema, tableName, `ticker)
testDict=table([2021.05.07,2021.05.06,2021.05.07,2021.05.06] as date, ['AAPL','AMZN','AMZN','AAPL'] as ticker, [130.21, 3306.37, 3291.61, 129.74] as price)
tb.append!(testDict)
select * from tb
"""
s.run(db_script)

# output
    date	ticker	price
0	2021-05-06	AAPL	129.74
1	2021-05-07	AAPL	130.21
2	2021-05-06	AMZN	3306.37
3	2021-05-07	AMZN	3291.61
```
 --->

### 7.2 Database Operations

#### 7.2.1 Create databases

Use function `database` to create DFS databases:
```python
import dolphindb.settings as keys
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
``` 

#### 7.2.2 Delete databases

Use `dropDatabase` to delete databases:
```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
```

#### 7.2.3 Delete database partitions

Use `dropPartition` to delete database partitions. Please note that if the name of a partition to be deleted is quoted in DolphinDB's `dropPartition` command, then we need to add another level of quotes to the partition name in Python API's `dropPartition` method. For example, if the parameter of 'partitions' in DolphinDB's `dropPartition` command is ["AMZN","NFLX"], then in Python API's `dropPartition` method the parameter 'partitions' should be ["'AMZN'","'NFLX'"]. Similarly, in Python API for range partitions: partitionPaths=["'/0_50'","'/50_100'"]; for list partitions: partitionPaths=["'/List0'","'/List1'"], etc. 

```python
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
trade=s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/data_example.csv")
print(trade.rows)
# output
13136

s.dropPartition("dfs://valuedb", partitionPaths=["'AMZN'","'NFLX'"]) # or s.dropPartition("dfs://valuedb", partitionPaths=["`AMZN`NFLX"])
trade = s.loadTable(tableName="trade", dbPath="dfs://valuedb")
print(trade.rows)
# output
4516

print(trade.select("distinct TICKER").toDF())
# output
  distinct_TICKER
0            NVDA
```

### 7.3 Table operations

#### 7.3.1 Load table from database

Please refer to [section 5. Load data from DolphinDB database](#5-Load data from DolphinDB database). 

#### 7.3.2 Append to tables

Please refer to [section 6.1](#6.1-Append to in-memory tables) about how to append to in-memory tables. 

Please refer to [section 6.2](#6.2-Append to DFS tables) about how to append to DFS tables. 

### 7.3.3 Update tables

`update` can only be used on in-memory tables and must be used with `execute` together. 

```python
trade=s.loadText(WORK_DIR+"/data_example.csv")
trade = trade.update(["VOL"],["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
t1=trade.where("ticker=`AMZN").where("VOL=999999")
print(t1.toDF())

# output
           TICKER       date     VOL             PRC           BID              ASK
0      AMZN 1997-05-15  999999   23.50000   23.50000   23.62500
1      AMZN 1997-05-16  999999   20.75000   20.50000   21.00000
2      AMZN 1997-05-19  999999   20.50000   20.50000   20.62500
3      AMZN 1997-05-20  999999   19.62500   19.62500   19.75000
4      AMZN 1997-05-21  999999   17.12500   17.12500   17.25000
...     
4936   AMZN 2016-12-23  999999  760.59003  760.33002  760.59003
4937   AMZN 2016-12-27  999999  771.40002  771.40002  771.76001
4938   AMZN 2016-12-28  999999  772.13000  771.92999  772.15997
4939   AMZN 2016-12-29  999999  765.15002  764.66998  765.15997
4940   AMZN 2016-12-30  999999  749.87000  750.02002  750.40002

[4941 rows x 6 columns]
```

#### 7.3.4 Delete records from a table

`delete` must be used with `execute`. 

```python
trade=s.loadText(WORK_DIR+"/data_example.csv")
trade.delete().where('date<2013.01.01').execute()
print(trade.rows)

# output
3024
```

#### 7.3.5 Delete columns from a table

We can only delete columns from an in-memory table. 

```python
trade=s.loadText(WORK_DIR+"/data_example.csv")
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

#### 7.3.6 Delete a table

```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/data_example.csv")
s.dropTable(dbPath="dfs://valuedb", tableName="trade")
```

## 8 SQL query

### 8.1 `select`

#### 8.1.1 A list of column names as input

```python
trade=s.loadText(WORK_DIR+"/data_example.csv")
print(trade.select(['ticker','date','bid','ask','prc','vol']).toDF())

# output
      ticker       date        bid      ask        prc      vol
0       AMZN 1997-05-15   23.50000   23.625   23.50000  6029815
1       AMZN 1997-05-16   20.50000   21.000   20.75000  1232226
2       AMZN 1997-05-19   20.50000   20.625   20.50000   512070
3       AMZN 1997-05-20   19.62500   19.750   19.62500   456357
4       AMZN 1997-05-21   17.12500   17.250   17.12500  1577414
...
```

We can use the `showSQL` method to display the SQL statement. 
```python
print(trade.select(['ticker','date','bid','ask','prc','vol']).showSQL())

# output
select ticker,date,bid,ask,prc,vol from T64afd5a6
```

#### 8.1.2 String as input

```python
print(trade.select("ticker,date,bid,ask,prc,vol").where("date=2012.09.06").where("vol<10000000").toDF())

# output
  ticker       date        bid     ask     prc      vol
0   AMZN 2012-09-06  251.42999  251.56  251.38  5657816
1   NFLX 2012-09-06   56.65000   56.66   56.65  5368963
...
```

### 8.2 **top**

Get the top records in a table.

```python
trade=s.loadText(WORK_DIR+"/data_example.csv")
trade.top(5).toDF()

# output
      TICKER        date       VOL        PRC        BID       ASK
0       AMZN  1997.05.16   6029815   23.50000   23.50000   23.6250
1       AMZN  1997.05.17   1232226   20.75000   20.50000   21.0000
2       AMZN  1997.05.20    512070   20.50000   20.50000   20.6250
3       AMZN  1997.05.21    456357   19.62500   19.62500   19.7500
4       AMZN  1997.05.22   1577414   17.12500   17.12500   17.2500
```

### 8.3 `where`

#### 8.3.1 multiple where conditions

```python
trade=s.loadText(WORK_DIR+"/data_example.csv")

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
# output
765
```

We can use the `showSQL` method to display the SQL statement.
```python
print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').showSQL())

# output
select date,bid,ask,prc,vol from Tff260d29 where TICKER=`AMZN and bid!=NULL and ask!=NULL and vol>10000000 order by vol desc
```

#### 8.3.2 Use string as input

We can pass a list of field names as a string to `select` method and a list of conditions as string to `where` method.

```python
trade=s.loadText(WORK_DIR+"/data_example.csv")
print(trade.select("ticker, date, vol").where("bid!=NULL, ask!=NULL, vol>50000000").toDF())

# output
   ticker       date        vol
0    AMZN 1999-09-29   80380734
1    AMZN 2000-06-23   52221978
2    AMZN 2001-11-26   51543686
3    AMZN 2002-01-22   57235489
4    AMZN 2005-02-03   60580703
...
38   NFLX 2016-01-20   53009419
39   NFLX 2016-04-19   55728765
40   NFLX 2016-07-19   55685209
```


### 8.4 `groupby`

Method `groupby` must be followed by an aggregate function such as `count`, `sum`, `avg`, `std`, etc.

```
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/data_example.csv")
```

```python
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")
print(trade.select('count(*)').groupby(['ticker']).sort(bys=['ticker desc']).toDF())

# output
  ticker  count_ticker
0   NVDA          4516
1   NFLX          3679
2   AMZN          4941
```

Calculate the sum of column "vol" and the sum of column "prc" in each "ticker" group:

```python
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")
print(trade.select(['vol','prc']).groupby(['ticker']).sum().toDF())

# output
  ticker      sum_vol       sum_prc
0   AMZN  33706396492  772503.81377
1   NFLX  14928048887  421568.81674
2   NVDA  46879603806  127139.51092
```

`groupby` can be used with `having`:
```python
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")
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

### 8.5 `contextby`

`contextby` is similar to `groupby` except that for each group, `groupby` returns a scalar whereas `contextby` returns a vector of the same size as the group.

```python
df= s.loadTable(tableName="trade",dbPath="dfs://valuedb").contextby('ticker').top(3).toDF()
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

```python
df= s.loadTable(tableName="trade",dbPath="dfs://valuedb").select("TICKER, month(date) as month, cumsum(VOL)").contextby("TICKER,month(date)").toDF()
print(df)

# output
         TICKER     month     cumsum_VOL
0       AMZN 1997-05-01     6029815
1       AMZN 1997-05-01     7262041
2       AMZN 1997-05-01     7774111
3       AMZN 1997-05-01     8230468
4       AMZN 1997-05-01     9807882
...      
13131   NVDA 2016-12-01   280114768
13132   NVDA 2016-12-01   309971900
13133   NVDA 2016-12-01   367356016
13134   NVDA 2016-12-01   421740692
13135   NVDA 2016-12-01   452063951
```

```python
df= s.loadTable(tableName="trade",dbPath="dfs://valuedb").select("TICKER, month(date) as month, sum(VOL)").contextby("TICKER,month(date)").toDF()
print(df)

# output
      TICKER     month    sum_VOL
0       AMZN 1997-05-01   13736587
1       AMZN 1997-05-01   13736587
2       AMZN 1997-05-01   13736587
3       AMZN 1997-05-01   13736587
4       AMZN 1997-05-01   13736587
...      
13131   NVDA 2016-12-01  452063951
13132   NVDA 2016-12-01  452063951
13133   NVDA 2016-12-01  452063951
13134   NVDA 2016-12-01  452063951
13135   NVDA 2016-12-01  452063951
```

```python
df= s.loadTable(dbPath="dfs://valuedb", tableName="trade").contextby('ticker').having("sum(VOL)>40000000000").toDF()
print(df)

# output
          TICKER        date         VOL          PRC          BID             ASK
0      NVDA 1999-01-22   5702636   19.6875   19.6250   19.6875
1      NVDA 1999-01-25   1074571   21.7500   21.7500   21.8750
2      NVDA 1999-01-26    719199   20.0625   20.0625   20.1250
3      NVDA 1999-01-27    510637   20.0000   19.8750   20.0000
4      NVDA 1999-01-28    476094   19.9375   19.8750   20.0000
...     
4511   NVDA 2016-12-23  16193331  109.7800  109.7700  109.7900
4512   NVDA 2016-12-27  29857132  117.3200  117.3100  117.3200
4513   NVDA 2016-12-28  57384116  109.2500  109.2500  109.2900
4514   NVDA 2016-12-29  54384676  111.4300  111.2600  111.4200
4515   NVDA 2016-12-30  30323259  106.7400  106.7300  106.7500
```

### 8.6 Table join

DolphinDB table class has method `merge` for inner, left, and outer join; method `merge_asof` for asof join; method `merge_window` for window join.

#### 8.6.1 `merge`

Specify joining columns with parameter "on" if joining column names are identical in both tables; use parameters "left_on" and "right_on" when joining column names are different. The optional parameter "how" indicates table join type. The default table join mode is inner join. 

```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': np.array(['2015-12-31', '2015-12-30', '2015-12-29'], dtype='datetime64[D]'), 'open': [695, 685, 674]}, tableAliasName="t1")
s.run("""t1 = select TICKER,date(date) as date,open from t1""")
print(trade.merge(t1,on=["TICKER","date"]).toDF())

# output
  TICKER        date                 VOL        PRC                     BID        ASK          open
0   AMZN  2015.12.29  5734996  693.96997  693.96997  694.20001   674
1   AMZN  2015.12.30  3519303  689.07001  689.07001  689.09998   685
2   AMZN  2015.12.31  3749860  675.89001  675.85999  675.94000   695
```

We need to specify arguments "left_on" and "right_on" when joining column names are different. 
```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = s.table(data={'TICKER1': ['AMZN', 'AMZN', 'AMZN'], 'date1': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]}, tableAliasName="t1")
s.run("""t1 = select TICKER1,date(date1) as date1,open from t1""")
print(trade.merge(t1,left_on=["TICKER","date"], right_on=["TICKER1","date1"]).toDF())

# output
  TICKER        date               VOL          PRC                   BID            ASK          open
0   AMZN  2015.12.29  5734996  693.96997  693.96997  694.20001   674
1   AMZN  2015.12.30  3519303  689.07001  689.07001  689.09998   685
2   AMZN  2015.12.31  3749860  675.89001  675.85999  675.94000   695
```

To conduct left join, set how="left". 

```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]}, tableAliasName="t1")
s.run("""t1 = select TICKER,date(date) as date,open from t1""")
print(trade.merge(t1,how="left", on=["TICKER","date"]).where('TICKER=`AMZN').where('2015.12.23<=date<=2015.12.31').toDF())

# output
  TICKER       date               VOL             PRC               BID               ASK          open
0   AMZN 2015-12-23  2722922  663.70001  663.48999  663.71002    NaN
1   AMZN 2015-12-24  1092980  662.78998  662.56000  662.79999    NaN
2   AMZN 2015-12-28  3783555  675.20001  675.00000  675.21002    NaN
3   AMZN 2015-12-29  5734996  693.96997  693.96997  694.20001  674.0
4   AMZN 2015-12-30  3519303  689.07001  689.07001  689.09998  685.0
5   AMZN 2015-12-31  3749860  675.89001  675.85999  675.94000  695.0
```

To conduct outer join, set how="outer". A partitioned table can only be outer joined with a partitioned table, and an in-memory table can only be outer joined with an in-memory table.

```python
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'NFLX'], 'date': ['2015.12.29', '2015.12.30', '2015.12.31'], 'open': [674, 685, 942]})
t2 = s.table(data={'TICKER': ['AMZN', 'NFLX', 'NFLX'], 'date': ['2015.12.29', '2015.12.30', '2015.12.31'], 'close': [690, 936, 951]})
print(t1.merge(t2, how="outer", on=["TICKER","date"]).toDF())

# output
     TICKER     date           open TMP_TBL_1b831e46_TICKER TMP_TBL_1b831e46_date  close
0   AMZN  2015.12.29    674.0                    AMZN                                              2015.12.29                690.0
1   AMZN  2015.12.30    685.0                                                                                                                    NaN
2   NFLX  2015.12.31     942.0                      NFLX                                               2015.12.31               951.0
3                                           NaN                        NFLX                                               2015.12.30               936.0  
```

#### 8.6.2 `merge_asof`

The asof join function is a type of non-synchronous join. It is similar to the left join function witht the following differences:
- 1. The data type of the last matching column is usually temporal. For a row in the left table with time t, if there is not a match of left join in the right table, the row in the right table that corresponds to the most recent time before time t is taken, if all the other matching columns are matched; if there are more than one matching record in the right table, the last record is taken. 
- 2. If there is only 1 joining column, the asof join function assumes the right table is sorted on the joining column. If there are multiple joining columns, the asof join function assumes the right table is sorted on the last joining column within each group defined by the other joining columns. The right table does not need to be sorted by the other joining columns. If these conditions are not met, we may see unexpected results. The left table does not need to be sorted. 

For the examples in this and the next section, we use [trades.csv](data/trades.csv) and [quotes.csv](data/quotes.csv) which have AAPL and FB trades and quotes data on 10/24/2016 taken from NYSE website. 


```python
import dolphindb.settings as keys

WORK_DIR = "C:/DolphinDB/Data"
if s.existsDatabase(WORK_DIR+"/tickDB"):
    s.dropDatabase(WORK_DIR+"/tickDB")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AAPL","FB"], dbPath=WORK_DIR+"/tickDB")
trades = s.loadTextEx("mydb",  tableName='trades',partitionColumns=["Symbol"], remoteFilePath=WORK_DIR + "/trades.csv")
quotes = s.loadTextEx("mydb",  tableName='quotes',partitionColumns=["Symbol"], remoteFilePath=WORK_DIR + "/quotes.csv")

print(trades.top(5).toDF())

# output
                        Time  Exchange  Symbol  Trade_Volume  Trade_Price
0 1970-01-01 08:00:00.022239        75    AAPL           300        27.00
1 1970-01-01 08:00:00.022287        75    AAPL           500        27.25
2 1970-01-01 08:00:00.022317        75    AAPL           335        27.26
3 1970-01-01 08:00:00.022341        75    AAPL           100        27.27
4 1970-01-01 08:00:00.022368        75    AAPL            31        27.40

print(quotes.where("second(Time)>=09:29:59").top(5).toDF())

# output
                         Time  Exchange  Symbol  Bid_Price  Bid_Size  Offer_Price  Offer_Size
0  1970-01-01 09:30:00.005868        90    AAPL      26.89         1        27.10           6
1  1970-01-01 09:30:00.011058        90    AAPL      26.89        11        27.10           6
2  1970-01-01 09:30:00.031523        90    AAPL      26.89        13        27.10           6
3  1970-01-01 09:30:00.284623        80    AAPL      26.89         8        26.98           8
4  1970-01-01 09:30:00.454066        80    AAPL      26.89         8        26.98           1

print(trades.merge_asof(quotes,on=["Symbol","Time"]).select(["Symbol","Time","Trade_Volume","Trade_Price","Bid_Price", "Bid_Size","Offer_Price", "Offer_Size"]).top(5).toDF())

# output
  Symbol                        Time          Trade_Volume  Trade_Price  Bid_Price  Bid_Size  \
0   AAPL  1970-01-01 08:00:00.022239                   300        27.00       26.9         1   
1   AAPL  1970-01-01 08:00:00.022287                   500        27.25       26.9         1   
2   AAPL  1970-01-01 08:00:00.022317                   335        27.26       26.9         1   
3   AAPL  1970-01-01 08:00:00.022341                   100        27.27       26.9         1   
4   AAPL  1970-01-01 08:00:00.022368                    31        27.40       26.9         1   

  Offer_Price   Offer_Size  
0       27.49           10  
1       27.49           10  
2       27.49           10  
3       27.49           10  
4       27.49           10  
[5 rows x 8 columns]
```

To calculate trading cost with asof join:

```python
print(trades.merge_asof(quotes, on=["Symbol","Time"]).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2))/sum(Trade_Volume*Trade_Price)*10000 as cost").groupby("Symbol").toDF())

# output
  Symbol       cost
0   AAPL   6.486813
1     FB  35.751041
```

#### 8.6.3 `merge_window`

`merge_window` (window join) is a generalization of asof join. With a window defined by parameters "leftBound" (w1) and "rightBound" (w2), for each row in the left table with the value of the last joining column equal to t, find the rows in the right table with the value of the last joining column between (t+w1) and (t+w2) conditional on all other joining columns are matched, then apply "aggFunctions" to the selected rows in the right table. 

The only difference between window join and prevailing window join is that if the right table doesn't contain a matching value for t+w1 (the left boundary of the window), prevailing window join will fill it with the last value before t+w1 (conditional on all other joining columns are matched), and apply "aggFunctions". To use prevailing window join, set prevailing=True. 
```python
print(trades.merge_window(quotes, -5000000000, 0, aggFunctions=["avg(Bid_Price)","avg(Offer_Price)"], on=["Symbol","Time"]).where("Time>=07:59:59").top(10).toDF())

# output
                        Time  Exchange Symbol  Trade_Volume \
0 1970-01-01 08:00:00.022239        75   AAPL           300
1 1970-01-01 08:00:00.022287        75   AAPL           500
2 1970-01-01 08:00:00.022317        75   AAPL           335
3 1970-01-01 08:00:00.022341        75   AAPL           100
4 1970-01-01 08:00:00.022368        75   AAPL            31
5 1970-01-01 08:00:02.668076        68   AAPL          2434
6 1970-01-01 08:02:20.116025        68   AAPL            66
7 1970-01-01 08:06:31.149930        75   AAPL           100
8 1970-01-01 08:06:32.826399        75   AAPL           100
9 1970-01-01 08:06:33.168833        75   AAPL            74

   avg_Bid_Price  avg_Offer_Price
0          26.90            27.49
1          26.90            27.49
2          26.90            27.49
3          26.90            27.49
4          26.90            27.49
5          26.75            27.36
6            NaN              NaN
7            NaN              NaN
8            NaN              NaN
9            NaN              NaN

[10 rows x 6 columns]
```


To calculate trading cost with window join:
```python
trades.merge_window(quotes,-1000000000, 0, aggFunctions="[wavg(Offer_Price, Offer_Size) as Offer_Price, wavg(Bid_Price, Bid_Size) as Bid_Price]", on=["Symbol","Time"], prevailing=True).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2))/sum(Trade_Volume*Trade_Price)*10000 as cost").groupby("Symbol").executeAs("tradingCost")

print(s.loadTable(tableName="tradingCost").toDF())

# output
  Symbol       cost
0   AAPL   6.367864
1     FB  35.751041
```

### 8.7 `executeAs`

Function `executeAs` saves query result as a table on DolphinDB server. 
```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').executeAs("AMZN")
```

To use the table "AMZN" on DolphinDB server:
```python
t1=s.loadTable(tableName="AMZN")
```


### 8.8 Regression

Function `ols` conducts an OLS regression and returns a dictionary. 

```python
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")
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
0.6053065014691369
```

For the example below, please note that the ratio operator between 2 integers in DolphinDB is "\", which happens to be an escape character in Python. Therefore we need to use ```VOL\\SHROUT``` in the `select` statement. 

```python
result = s.loadTable(tableName="US",dbPath="dfs://US").select("select VOL\\SHROUT as turnover, abs(RET) as absRet, (ASK-BID)/(BID+ASK)*2 as spread, log(SHROUT*(BID+ASK)/2) as logMV").where("VOL>0").ols("turnover", ["absRet","logMV", "spread"], True)
```


## 9 More Examples

### 9.1 Stock momentum strategy

In this section we give an example of a backtest on a stock momentum strategy. The momentum strategy is one of the best-known quantitative long short equity strategies. It has been studied in numerous academic and sell-side publications since Jegadeesh and Titman (1993). Investors in the momentum strategy believe among individual stocks, past winners will outperform past losers. The most commonly used momentum factor is stocks' past 12 months returns skipping the most recent month. In academic research, the momentum strategy is usually rebalanced once a month and the holding period is also one month. In this example, we rebalance 1/5 of our portfolio positions every day and hold the new tranche for 5 days. For simplicity, transaction costs are not considered.

**Create server session**

```python
import dolphindb as ddb
s=ddb.session()
s.connect("localhost",8921, "admin", "123456")
```

**Step 1:** Load data, clean the data, and construct the momentum signal (past 12 months return skipping the most recent month) for each firm. Undefine the table "USstocks" to release the large amount of memory it occupies. Note that `executeAs` must be used to save the intermediate results on DolphinDB server. Dataset "US" contains US stock price data from 1990 to 2016.
```python
if s.existsDatabase("dfs://US"):
	s.dropDatabase("dfs://US")
s.database(dbName='USdb', partitionType=keys.VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="dfs://US")
US=s.loadTextEx(dbPath="dfs://US", partitionColumns=["TICKER"], tableName='US', remoteFilePath=WORK_DIR + "/USPrices_FIRST.csv") 
US = s.loadTable(dbPath="dfs://US", tableName="US")
def loadPriceData(inData):
    s.loadTable(inData).select("PERMNO, date, abs(PRC) as PRC, VOL, RET, SHROUT*abs(PRC) as MV").where("weekday(date) between 1:5, isValid(PRC), isValid(VOL)").sort(bys=["PERMNO","date"]).executeAs("USstocks")
    s.loadTable("USstocks").select("PERMNO, date, PRC, VOL, RET, MV, cumprod(1+RET) as cumretIndex").contextby("PERMNO").executeAs("USstocks")
    return s.loadTable("USstocks").select("PERMNO, date, PRC, VOL, RET, MV, move(cumretIndex,21)/move(cumretIndex,252)-1 as signal").contextby("PERMNO").executeAs("priceData")

priceData = loadPriceData(US.tableName())
# US.tableName() returns the name of the table on the DolphinDB server that corresponds to the table object "US" in Python. 
```
**Step 2:** Generate the portfolios for the momentum strategy.

```python
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

```python
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
stockPnL = chuzhaoalcStockPnL(ports=ports, dailyRtn=dailyRtn, holdingDays=holdingDays, endDate=endDate)
```

**Step 4:** Calculate portfolio profit/loss

```python
portPnl = stockPnL.select("pnl").groupby("date").sum().sort(bys=["date"]).executeAs("portPnl")

print(portPnl.toDF())
```

### 9.2 Time series operations

The example below shows how to calculate factor No. 98 in "101 Formulaic Alphas" by Kakushadze (2015) with daily data of US stocks.

```python
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


<!--- 
## 7 多线程调用线程池对象

DolphinDB Python API's `Session.run` method only conducts serial execution. We can use the DBConnectionPool object for parallel execution.  法执行脚本时只能串行执行，如果需要并发地执行脚本，可以使用`DBConnectionPool`来提高任务运行的效率。`DBConnectionPool`创建了多个线程（由threadNum参数指定）用于执行任务。

```Python
pool = ddb.DBConnectionPool(host, port, threadNum, [userid], [password])
```

为了提高效率，我们将`DBConnectionPool`中的`run`方法包装成了协程函数，通过`run`方法将脚本传入线程池中调用线程运行，因此在python中调用时需要使用协程来进行使用。以一个简单的固定任务为例说明：

```python
import dolphindb as ddb
import datetime
import time
import asyncio
import threading
import sys
import numpy
import pandas

pool = ddb.DBConnectionPool("localhost", 8848, 20)

# 创建一个任务函数，用sleep模拟一段运行的时间
async def test_run():
    try:
        return await pool.run("sleep(1000);1+2")
    except Exception as e:
        print(e)

# 定义任务列表
tasks = [
    asyncio.ensure_future(test_run()),
    asyncio.ensure_future(test_run()),
    asyncio.ensure_future(test_run()),
    asyncio.ensure_future(test_run()),
]

# 创建一个事件循环对象，运行任务列表直到全部任务完成
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.wait(tasks))
except Exception as e:
    print("catch e:")
    print(e)

for i in tasks:
    print(i)

pool.shutDown() # 关闭进程池对象
```

上述的例子给出的是已固定脚本任务的调用线程池的用法，在Python中只有一个主线程，但是使用了协程创建子任务并调用线程池去运行，但注意在DolphinDB底层实现中这些任务的运行仍然是多线程运行的。当然我们也可以自己定义传入脚本的对象。以下例子定义了一个可以传入自定义脚本作为参数的类，并配合Python的多线程机制动态添加子任务。

```python
import dolphindb as ddb
import datetime
import time
import asyncio
import threading
import sys
import numpy
import pandas

# 在该例子中主线程负责创建协程对象传入自定义脚本并调用自定义的对象去运行，并新起子线程运行事件循环防止阻塞主线程。
class Dolphindb(object):

    pool = ddb.DBConnectionPool ("localhost", 8848, 20)

    @classmethod
    async def test_run1(cls,script):
        print("test_run1")
        return await cls.pool.run(script)

    @classmethod
    async def runTest(cls,script):
        start = time.time()
        task = loop.create_task(cls.test_run1(script))
        result = await asyncio.gather(task)
        print(time.time()-start)
        print(result)
        print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        return result 

#定义一个跑事件循环的线程函数    
def start_thread_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

    
if __name__=="__main__":
    start = time.time()
    print("In main thread ",threading.current_thread())
    loop = asyncio.get_event_loop()
    # 在子线程中运行事件循环,让它run_forever
    t = threading.Thread(target= start_thread_loop, args=(loop,))
    t.start()
    task1 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(1000);1+1"),loop)
    task2 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(3000);1+2"),loop)
    task3 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(5000);1+3"),loop)
    task4 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(1000);1+4"),loop)
    
    print('主线程不会阻塞')
    end = time.time()
    print(end - start)
```

 --->






<!--- 

#### 4.4.3 Data type conversion when uploading data

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


 --->
