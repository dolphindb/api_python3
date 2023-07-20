# Python API for DolphinDB

> **Note:** This README file is no longer maintained starting in DolphinDB Python API version *1.30.22.1*. Please refer to the [new DolphinDB Python API manual](https://www.dolphindb.com/pydoc/webhelp/) going forward for up-to-date documentation.

DolphinDB Python API runs on the following operating systems:

| Operating System | Supported Python Versions                      |
| :--------------- | :--------------------------------------------- |
| Windows          | Python 3.6-3.10                                 |
| Linux            | Python 3.6-3.10                                 |
| Linux(aarch64)   | Python 3.7-3.10 in conda environment            |
| Mac(x86-64)      | Python 3.6-3.10 in conda environment            |
| Mac(arm64)       | Python 3.8-3.10 in conda environment            |

DolphinDB Python API has these library dependencies: 

- future 
- NumPy 1.18 - 1.23.4 
- pandas 1.0.0 or higher (version 1.3.0 is not supported)

Install DolphinDB Python API with the following command:

```Console
$ pip install dolphindb
```

If it cannot be installed or imported, try the following steps:

1. Search for the *dolphindb* wheel that runs on your current operating system (e.g., Linux ARM, Mac M1, etc.) on [PyPI](https://pypi.org/project/dolphindb/#files). Download the wheel (*.whl* file) to your local system.
2. Enter the following command in the terminal:

```
pip debug --verbose
```

The `Compatible tags` section indicates which distributions are compatible with your system.

3. Rename the downloaded *dolphindb* wheel according to the compatibility tags. For example, the file name for Mac(x86_64) is “dolphindb-1.30.19.2-cp37-cp37m-macosx_10_16_x86_64.whl“. If the compatibility tags show that the system version supported by pip is 10.13, then replace the “10_16“ in the original filename with “10_13“.

4. Install the renamed wheel.

If the installation or import still fails, please post your question on [StackOverflow ](https://stackoverflow.com/questions/tagged/dolphindb)with the “dolphindb“ tag. We will get back to you soon.

- [Python API for DolphinDB](#python-api-for-dolphindb)
  - [1 Execute DolphinDB Scripts and Functions](#1-execute-dolphindb-scripts-and-functions)
    - [1.1 Establish Connection](#11-establish-connection)
    - [1.2 Execute DolphinDB Scripts](#12-execute-dolphindb-scripts)
    - [1.3 Execute DolphinDB Functions](#13-execute-dolphindb-functions)
    - [1.4 `undef`](#14-undef)
    - [1.5 Automatically Release Variables after Query Execution](#15-automatically-release-variables-after-query-execution)
  - [2 Upload Python Objects to DolphinDB](#2-upload-python-objects-to-dolphindb)
    - [2.1 Upload with `upload`](#21-upload-with-upload)
    - [2.2 Upload with `table`](#22-upload-with-table)
    - [2.3 Life Cycle of Uploaded Tables](#23-life-cycle-of-uploaded-tables)
  - [3 Create DolphinDB Databases and Tables](#3-create-dolphindb-databases-and-tables)
    - [3.1 DolphinDB Python API Methods](#31-dolphindb-python-api-methods)
    - [3.2 `run`](#32-run)
  - [4 Import Data to DolphinDB Databases](#4-import-data-to-dolphindb-databases)
    - [4.1 Import Data as Standard In-Memory Table](#41-import-data-as-standard-in-memory-table)
    - [4.2 Import Data as DFS Partitioned Table](#42-import-data-as-dfs-partitioned-table)
    - [4.3 Import Data as Partitioned In-Memory Table](#43-import-data-as-partitioned-in-memory-table)
  - [5 Load Data from DolphinDB](#5-load-data-from-dolphindb)
    - [5.1 `loadTable`](#51-loadtable)
    - [5.2 `loadTableBySQL`](#52-loadtablebysql)
    - [5.3 Load Tables in Blocks](#53-load-tables-in-blocks)
    - [5.4 Data Conversion](#54-data-conversion)
    - [5.5 Data Format Protocols](#55-data-format-protocols)
  - [6 Append to DolphinDB Tables](#6-append-to-dolphindb-tables)
    - [6.1 Append Lists to In-memory Tables with the `tableInsert` Function](#61-append-lists-to-in-memory-tables-with-the-tableinsert-function)
    - [6.2 Append DataFrame to In-memory Tables with the `tableInsert` Function](#62-append-dataframe-to-in-memory-tables-with-the-tableinsert-function)
    - [6.3 Append Data with the `insert into` Statement](#63-append-data-with-the-insert-into-statement)
    - [6.4 Automatic Temporal Data Type Conversion with the `tableAppender` Object](#64-automatic-temporal-data-type-conversion-with-the-tableappender-object)
    - [6.5 Append Data with the `tableUpsert` Object](#65-append-data-with-the-tableupsert-object)
    - [6.6 Append to DFS Tables](#66-append-to-dfs-tables)
    - [6.7 Append Data Asynchronously](#67-append-data-asynchronously)
    - [6.8 Append Data in Batch Asynchronously with the `MultithreadedTableWriter` Object](#68-append-data-in-batch-asynchronously-with-the-multithreadedtablewriter-object)
    - [6.9 Data Conversion](#69-data-conversion)
  - [7 Connection Pooling in Multi-Threaded Applications](#7-connection-pooling-in-multi-threaded-applications)
  - [8 Database and Table Operations](#8-database-and-table-operations)
    - [8.1 Summary](#81-summary)
    - [8.2 Database Operations](#82-database-operations)
    - [8.3 Table Operations](#83-table-operations)
  - [9 SQL Queries](#9-sql-queries)
    - [9.1 `select`](#91-select)
    - [9.2 `exec`](#92-exec)
    - [9.3 `top` \& `limit`](#93-top--limit)
    - [9.4 `where`](#94-where)
    - [9.5 `groupby`](#95-groupby)
    - [9.6 `contextby`](#96-contextby)
    - [9.7 `pivotby`](#97-pivotby)
    - [9.8 Table Join](#98-table-join)
    - [9.9 `executeAs`](#99-executeas)
    - [9.10 Regression](#910-regression)
  - [10 Python Streaming API](#10-python-streaming-api)
    - [10.1 `enableStreaming`](#101-enablestreaming)
    - [10.2 Subscribe and Unsubscribe](#102-subscribe-and-unsubscribe)
    - [10.3   Subscribe to Heterogeneous Stream Table](#103---subscribe-to-heterogeneous-stream-table)
    - [10.4 Streaming Applications](#104-streaming-applications)
  - [11 More Examples](#11-more-examples)
    - [11.1 Stock Momentum Strategy](#111-stock-momentum-strategy)
    - [11.2 Time-Series Operations](#112-time-series-operations)
  - [12 FAQ](#12-faq)
  - [13 Null Values Handling](#13-null-values-handling)
  - [14 Other Features](#14-other-features)
    - [14.1 Forced Termination of Processes](#141-forced-termination-of-processes)
    - [14.2 Setting TCP Timeout](#142-setting-tcp-timeout)




DolphinDB Python API in essence encapsulates a subset of DolphinDB's scripting language. It converts Python script to DolphinDB script to be executed on the DolphinDB server. The result can either be saved on the DolphinDB server or serialized to a Python client object. 

## 1 Execute DolphinDB Scripts and Functions

### 1.1 Establish Connection

Python interacts with DolphinDB through a `session` object:

```
session(host=None, port=None, userid="", password="", enableSSL=False, enableASYNC=False, keepAliveTime=30, enableChunkGranularityConfig=False, compress=False, enablePickle=True, python=False)
```

The most commonly used `Session` class methods are as follows:

| Method                                                       | Explanation                                                  |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| connect(host,port,[username,password, startup, highAvailability, highAvailabilitySites, keepAliveTime, reconnect]) | Connect a session to DolphinDB server                        |
| login(username,password,[enableEncryption=True])                   | Log in DolphinDB server                                      |
| run(DolphinDBScript)                                         | Execute scripts on DolphinDB server                          |
| run(DolphinDBFunctionName,args)                              | Call functions on DolphinDB server                           |
| runFile(filePath)                                            | Run a DolphinDB script file on the server. Please note that the file must be in UTF-8 encoding on Linux and ASCII encoding on Windows. |
| upload(DictionaryOfPythonObjects)                            | Upload Python objects to DolphinDB server                    |
| undef(objName,objType)                                       | Undefine an object in DolphinDB to release memory            |
| undefAll()                                                   | Undefine all objects in DolphinDB to release memory          |
| getSessionId()                                               | Get the current session ID                                   |
| close()                                                      | Close the session                                            |

The following script first imports Python API, then creates a session in Python to connect to a DolphinDB server with the specified domain name/IP address and port number. 

Note:

- Start a DolphinDB server before running the following Python script.
- It may take a while for an inactive session to be closed automatically. You can explicitly close the session once you are done with it by calling `close()` to release the connection.

```python
import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848)
# output
True

s.close()  #close session
```

#### connect

```
connect(host,port,[userid=None,password=None, startup=None, highAvailability=False, highAvailabilitySites=None, keepAliveTime=None, reconnect=False])
```

* **host/port**: IP address and port number of the host
* **username/password**: username and password
* **startup**: the startup script to execute the preloaded tasks. It can be used to load plugins and DFS tables, define and load stream tables, etc.
* **highAvailability / highAvailabilitySites**: High-availability parameters. To enable high availability for DolphinDB Python API, set *highAvailability* = true and specify `ip:port` of all available nodes for *highAvailabilitySites*.  
* **keepAliveTime**: the duration between two keepalive transmissions to detect the TCP connection status. The default value is 30 (seconds). Set the parameter to release half-open TCP connections timely when the network is unstable.

In high-availability mode, when a single thread is used to create multiple sessions, load balancing is implemented across all available nodes. However, when the sessions are created by multiple threads, load balancing is not guaranteed.

Use the following script to connect to DolphinDB server with your username and password. The default is 'admin' and '123456'. 

```python
s.connect("localhost", 8848, "admin", "123456")
```

or

```python
s.connect("localhost", 8848)
s.login("admin","123456")
```

To enable high availability for DolphinDB Python API, specify the IP addresses of all data nodes in the high availbility group. For example:

```python
import dolphindb as ddb

s = ddb.session()
sites=["192.168.1.2:24120", "192.168.1.3:24120", "192.168.1.4:24120"]
s.connect(host="192.168.1.2", port=24120, userid="admin", password="123456", highAvailability=True, highAvailabilitySites=sites)
```

For sessions that are expired or initialized without username and password, use the method `login` to log in DolphinDB server. By default, the username and password are encrypted during connection.

After the session is connected, call function `getSessionId` to obtain the current session ID:

```python
import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848)
print(s.getSessionId())
```

- **SSL**

Since server version 1.10.17 and 1.20.6, you can specify the parameter *enableSSL* to enable SSL when creating a session. The default value is False. 

Please also specify the configuration parameter *enableHTTPS* = true. 

```
s=ddb.session(enableSSL=True)
```

- **Asynchronous Communication**

Since server version 1.10.17 and 1.20.6, you can specify the parameter *enableASYNC* to enable asynchronous communication when creating a session. The default value is False. 

The asynchronous mode only supports the `session.run` method to connect to the server and no values are returned. This mode is ideal for writing data asynchronously as it saves time on the API to detect the return values.

```
s=ddb.session(enableASYNC=True)
```

- **Compressed Communication** 

Since server version 1.30.6, you can specify the compression parameter *compress* to enable compressed communication when creating a session. The default value is False. 

This mode is ideal for large writes or queries as it saves network bandwidth. However, it increases the computational complexity on the server and API client.

Note: Please disable pickle when you enable compressed communication.

```
s=ddb.session(compress=True, enablePickle=False)
```

### 1.2 Execute DolphinDB Scripts

DolphinDB script can be executed with the `run(script)` method. If the script returns an object in DolphinDB, it will be converted to a Python object. If the script fails to run, there will be an error prompt.

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
s.run("getTypeStr", 1)

# output
'LONG'
```

### 1.3 Execute DolphinDB Functions

In addition to executing scripts, the `run` method can directly call DolphinDB built-in or user-defined functions on a remote DolphinDB server. The first parameter of the `run` method is the function name and the subsequent parameters are the parameters of the function. For example, `session.run(“func”,”params”)`.

#### 1.3.1 Parameter Passing

The following example shows a Python program calling DolphinDB built-in function `add` through method `run`. The `add` function has 2 parameters: *x* and *y*. Depending on whether the values of the parameters have been assigned on the DolphinDB server, there are 3 ways to call the function:

(1) Both parameters have been assigned value on DolphinDB server:

If both *x* and *y* have been assigned value on DolphinDB server in the Python program,

```python
s.run("x = [1,3,5];y = [2,4,6]")
```

then just use `run(script)`

```python
a=s.run("add(x,y)")
repr(a)

# output
'array([3, 7, 11], dtype=int32)'
```

(2) Only one parameter has been assigned value at DolphinDB server:

If only *x* has been assigned value on DolphinDB server in the Python program,

```python
s.run("x = [1,3,5]")
```

and *y* is to be assigned value when calling `add`, we need to use [Partial Application](https://www.dolphindb.com/help/Functionalprogramming/PartialApplication.html) to fix parameter x to function `add`. 

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

(3) Both parameters are to be assigned value:

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

#### 1.3.2 Data Types and Forms of Parameters

When calling DolphinDB built-in functions through `run`, the parameters uploaded can be scalar, list, dict, NumPy objects, pandas DataFrame and Series, etc.

> Note:
> 
> 1. NumPy arrays can only be 1D or 2D. 
> 2. If a pandas DataFrame or Series has an index, the index will be lost after the object is uploaded to DolphinDB. To keep the index, use the pandas DataFrame function `reset_index`.
> 3. If a parameter of a DolphinDB function is of temporal type, convert it to numpy.datetime64 type before uploading. 



The following examples explain how to use various types of Python objects as parameters. 

(1) list

Add 2 Python lists with DolphinDB function `add`:

```python
s.run("add",[1,2,3,4],[1,2,1,1])

# output
array([2, 4, 4, 5])
```

(2) NumPy objects

- **np.int**

  ```python
  import numpy as np
  s.run("add{1,}",np.int(4))
  # output
  5
  ```

- **np.datetime64**

  np.datetime64 is converted into corresponding DolphinDB temporal type. 

  | datetime64                      | DolphinDB Type |
  | :------------------------------ | :------------- |
  | '2019-01-01'                    | DATE           |
  | '2019-01'                       | MONTH          |
  | '2019-01-01T20:01:01'           | DATETIME       |
  | '2019-01-01T20:01:01.122'       | TIMESTAMP      |
  | '2019-01-01T20:01:01.122346100' | NANOTIMESTAMP  |

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
  
  As TIME, MINUTE, SECOND and NANOTIME types in DolphinDB don't contain date information, datetime64 type cannot be converted into these types directly in Python API. To generate these data types in DolphinDB from Python, we can upload the datetime64 type to DolphinDB server and discard the date information. See [2. Upload Python objects to DolphinDB server](#2-upload-local-objects-to-dolphindb-server).

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
  
- **list of np.datetime64 objects**

  ```python
  import numpy as np
  a=[np.datetime64('2019-01-01T20:00:00.000000001'), np.datetime64('2019-01-01T20:00:00.000000001')]
  s.run("add{1,}",a)
  # output
  array(['2019-01-01T20:00:00.000000002', '2019-01-01T20:00:00.000000002'], dtype='datetime64[ns]')
  ```

(3) pandas objects

If a pandas DataFrame or Series object has an index, the index will be lost after the object is uploaded to DolphinDB. 

- **Series**

  ```python
  import pandas as pd
  import numpy as np
  a = pd.Series([1,2,3,1,5],index=np.arange(1,6,1))
  s.run("add{1,}",a)
  # output
  array([2, 3, 4, 2, 6])
  ```

- **DataFrame**

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

The session method `undef` releases specified objects in a session and the method `undefAll` releases all objects in a session. `undef` can be used on the following objects: "VAR"(variable), "SHARED"(shared variable) and "DEF"(function definition). The default type is "VAR". "SHARED" refers to shared variables across sessions, such as a shared stream table. 


### 1.5 Automatically Release Variables after Query Execution

To automatically release the variables created in a `run` statement after the execution is finished, set the parameter *clearMemory* = True in Session or use DBConnectionPool's `run` method. 

Please note that the default value of *clearMemory* of Session's `run` method is False, whereas the default value of *clearMemory* of DBConnectionPool's `run` method is True. 

```python
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456") 
s.run("t = 1", clearMemory = True) 
s.run("t")   
```

As the variable t is released in the above example after the execution of `s.run("t = 1", clearMemory = True)`, the last statement will throw an exception:

```
<Exception> in run: Syntax Error: [line #1] Cannot recognize the token t 
```

## 2 Upload Python Objects to DolphinDB

To reuse a Python object in DolphinDB, we can upload it to the DolphinDB server and specify the variable name in DolphinDB.

### 2.1 Upload with `upload`

You can use method `upload` to upload Python objects to the DolphinDB server. The input of the method `upload` is a Python dictionary object. The keys of the dictionary are the variable names in DolphinDB and the values are Python objects, which can be numbers, strings, lists, DataFrame, etc.

(1) upload Python list

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

Please note that a Python list with multiple data types such as a=[1,2,3.0] will be recognized as an ANY VECTOR after being uploaded to DolphinDB. For such cases, it is recommended to upload an np.array instead of list. With np.array, we can specify the data type through `a=np.array([1,2,3.0],dtype=np.double)` , then "a" is recognized as a vector of DOUBLE type.

(2) upload NumPy array

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

(3) upload pandas DataFrame

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({'id': np.int32([1, 2, 3, 6, 8]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.x.avg()"))
# output
3.0
```

Note: When uploading DataFrame to DolphinDB, the elements in each column must have the same data type.

### 2.2 Upload with `table`

In Python, you can use the method `table` to create a DolphinDB table object and upload it to the server. The input can be a dictionary, DataFrame or table name in DolphinDB.

(1) upload dictionary

The script below defines a function `createDemoDict()` to create a dictionary. 

```python
import numpy as np

def createDemoDict():
    return {'id': [1, 2, 2, 3],
            'date': np.array(['2021.05.06', '2021.05.07', '2021.05.06', '2021.05.07'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AAPL', 'AMZN', 'AMZN'],
            'price': [129.74, 130.21, 3306.37, 3291.61]}
```

Upload the dictionary to the DolphinDB server with the method `table`, and name the table as "testDict". You can read the table with method `loadTable` provided by API.

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

(2) upload pandas DataFrame

Example 1:

The script below defines function `createDemoDataFrame()` to create a pandas DataFrame. 

```python
import pandas as pd
import numpy as np

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

Upload the DataFrame to DolphinDB server with method `table`, name it as "testDataFrame". You can read the table with method `loadTable` provided by API.

```python
import pandas as pd

# save the table to DolphinDB server as table "testDataFrame"
dt = s.table(data=createDemoDataFrame(), tableAliasName="testDataFrame")

# load table "testDataFrame" on DolphinDB server 
print(s.loadTable("testDataFrame").toDF())

# output
   cid  cbool  cchar  cshort  cint  ...             cnanotimestamp    cfloat    cdouble csymbol cstring
0    1   True      1       1     1  ... 2019-01-01 15:00:00.807060  2.100000   0.000000       A     abc
1    2  False      2       2     2  ... 2019-01-01 15:30:00.807060  2.658956  47.456213       B     def
2    3   True      3       3     3  ...                        NaT       NaN        NaN
```

Example 2:

Use `table` to upload a DataFrame with arrays to DolphinDB as a table with array vectors. 

```python
import numpy as np
import pandas as pd
import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
df = pd.DataFrame({
                'value': [np.array([1,2,3,4,5,6,7,8,9],dtype=np.int64),np.array([11,12,13,14],dtype=np.int64),np.array([22,13,11,12,13,14],dtype=np.int64)]
        })
tmp = s.table(data=df, tableAliasName="testArrayVector")

print(s.loadTable("testArrayVector").toDF())

# output
                         value
0  [1, 2, 3, 4, 5, 6, 7, 8, 9]
1             [11, 12, 13, 14]
2     [22, 13, 11, 12, 13, 14]
```


### 2.3 Life Cycle of Uploaded Tables

Functions `table` or `loadTable` returns a Python object. In the following example, table t1 of DolphinDB corresponds to a local Python object t0: 

```python
t0=s.table(data=createDemoDict(), tableAliasName="t1")
```

Use the following 3 ways to release the variable t1 at DolphinDB server:

- `undef`

```python
s.undef("t1", "VAR")
```

- assign NULL value to the variable at DolphinDB server

```python
s.run("t1=NULL")
```

- assign None to the local Python variable

```python
t0=None
```

After a variable is uploaded to DolphinDB from Python with `session.table` function, the system creates a reference to the DolphinDB table for the Python variable. If the reference no longer exists, the DolphinDB table is automatically released. 

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
Likewise, when loading a DFS table to memory with Python API, there is a correspondence between the local Python variable and the DolphinDB in-memory table.

Execute DolphinDB script:

```
db = database("dfs://testdb",RANGE, [1, 5 ,11])
t1=table(1..10 as id, 1..10 as v)
db.createPartitionedTable(t1,`t1,`id).append!(t1)
```

Then execute Python script:

```python
pt1=s.loadTable(tableName='t1',dbPath="dfs://testdb")
```

The scripts above create a DFS table on DolphinDB server, then load its metadata into memory with function `loadTable` and assign it to the local Python object pt1. Please note t1 is the DFS table name, not the DolphinDB table name. The corresponding DolphinDB table name can be obtained with pt1.tableName(). 

```python
print(pt1.tableName())
'TMP_TBL_4c5647af'
```

If a Python variable is used only once at DolphinDB server, it is recommended to include it as a parameter in a function call instead of uploading it. A function call does not cache data. After the function call is executed, all variables are released. Moreover, a function call is faster to execute as the network transmission only occurs once. 

## 3 Create DolphinDB Databases and Tables

There are two ways to create DolphinDB databases and tables through Python API: 

- Using the native API method `s.database`, which returns a Database object
- Using the `run` method

To create a DFS table in the database, use the following methods of the Database class:

| **Method**                                                   | **Description**                                              |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| createTable(table, tableName, sortColumns=None)              | Create a dimension (non-partitioned) table in a distributed database. Return a table object. A dimension table is used to store small datasets with infrequent updates. |
| createPartitionedTable(table, tableName, partitionColumns, compressMethods={}, sortColumns=None, keepDuplicates=None, sortKeyMappingFunction=None) | Create a partitioned table in a distributed database. Return a table object. |

### 3.1 DolphinDB Python API Methods

Import packages:

```python
import numpy as np
import pandas as pd
import dolphindb.settings as keys
```

#### 3.1.1 Create OLAP Databases

(1) Create Partitioned Databases and Tables with VALUE Domain

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

(2) Create Partitioned Databases and Tables with RANGE Domain

Partitions are based on ID ranges:

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

(3) Create Partitioned Databases and Tables with LIST Domain

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

(4) Create Partitioned Databases and Tables with HASH Domain

Partitions are based on hash values of ID:

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

(5) Create Partitioned Databases and Tables with COMPO Domain

The first level of partitions uses a VALUE domain and the second level of partitions uses a RANGE domain. 

Please note that when creating a DFS database with COMPO domain, the parameter *dbPath* for each partition level must be either an empty string or unspecified.

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

#### 3.1.2 Create TSDB Databases

Creating databases in the TSDB engine is similar to that in the OLAP engine. You can set *engine* = "TSDB" in the function `database` and specify the parameter *sortColumns* in function `createTable` or `createPartitionedTable`. For the function usage, see [database](https://www.dolphindb.com/help/FunctionsandCommands/FunctionReferences/d/database.html), [createPartitionedTable](https://www.dolphindb.com/help/FunctionsandCommands/FunctionReferences/c/createPartitionedTable.html?highlight=createpartitionedtable), and [createTable](https://www.dolphindb.com/help/FunctionsandCommands/FunctionReferences/c/createTable.html).

Example 1:

```python
import dolphindb.settings as keys
import numpy as np
import pandas as pd

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

dates = np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]")

dbPath = "dfs://tsdb"
if s.existsDatabase(dbPath): s.dropDatabase(dbPath)
db = s.database(dbName='mydb_tsdb', partitionType=keys.VALUE, partitions=dates, dbPath=dbPath, engine="TSDB")

df = pd.DataFrame({'datetime': np.array(
    ['2012-01-01T00:00:00', '2012-01-02T00:00:00', '2012-01-04T00:00:00', '2012-01-05T00:00:00', '2012-01-08T00:00:00'],
    dtype='datetime64'),
    'sym': ['AA', 'BB', 'BB', 'AA', 'BB'], 'val': [1, 2, 3, 4, 5]})
t = s.table(data=df)

db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime', sortColumns=["sym", "datetime"]).append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
print(re)

# output

    datetime sym  val
0 2012-01-01  AA    1
1 2012-01-02  BB    2
2 2012-01-04  BB    3
3 2012-01-05  AA    4
4 2012-01-08  BB    5
```

Example 2:

Create a DFS table with array vectors

```python
import dolphindb.settings as keys
import numpy as np
import pandas as pd

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

dates = np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]")
values = np.array([np.array([11,12,13,14],dtype=np.int64),
    np.array([15,16,17,18],dtype=np.int64),
    np.array([19,10,11,12],dtype=np.int64),
    np.array([13,14,15],dtype=np.int64),
    np.array([11,14,17,12,15],dtype=np.int64),
],dtype=object)

dbPath = "dfs://tsdb"
if s.existsDatabase(dbPath): s.dropDatabase(dbPath)
db = s.database(dbName='mydb_tsdb', partitionType=keys.VALUE, partitions=dates, dbPath=dbPath, engine="TSDB")

df = pd.DataFrame({'datetime': np.array(
    ['2012-01-01T00:00:00', '2012-01-02T00:00:00', '2012-01-04T00:00:00', '2012-01-05T00:00:00', '2012-01-08T00:00:00'],
    dtype='datetime64'),
    'sym': ['AA', 'BB', 'BB', 'AA', 'BB'], 'val': values})
t = s.table(data=df)

db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime', sortColumns=["sym", "datetime"]).append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
print(re)
```

### 3.2 `run`

Pass the DolphinDB script of creating databases and tables to the `run` method as a string. For example:

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

## 4 Import Data to DolphinDB Databases

The examples below use a csv file [example.csv](data/example.csv). Please download it and save it under the directory of WORK_DIR ("C:/DolphinDB/Data" in the following examples). 

### 4.1 Import Data as Standard In-Memory Table

To import text files into DolphinDB as an in-memory table, use session method `loadText`. It returns a DolphinDB table object in Python that corresponds to a DolphinDB in-memory table. You can convert the table object in Python to a pandas DataFrame with `toDF`.

Please note that when loading a text file as an in-memory table with `loadText`, the table size must be smaller than the available memory.

```python
WORK_DIR = "C:/DolphinDB/Data"

# Return a DolphinDB table object in Python
trade=s.loadText(WORK_DIR+"/example.csv")

# Convert it to pandas DataFrame. Data transfer of the table occurs at this step.
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

The default delimiter of function `loadText` is comma "," and you can also specify other delimiters. For example, to import a tabular text file:

```python
t1=s.loadText(WORK_DIR+"/t1.tsv", '\t')
```

### 4.2 Import Data as DFS Partitioned Table

To persist the imported data, or to load data files that are larger than available memory into DolphinDB, you can load data into a DFS database.

#### 4.2.1 Create a DFS Database

The following examples use the database "valuedb". The script below deletes the database if it already exists. 

```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
```

Now create a value-based DFS database "valuedb" with a session method `database`. We use a VALUE partition with stock ticker as the partitioning column. The parameter *partitions* indicates the partitioning scheme.

```python
import dolphindb.settings as keys

s.database(dbName='mydb', partitionType=keys.VALUE, partitions=['AMZN','NFLX', 'NVDA'], dbPath='dfs://valuedb')
# this is equivalent to: s.run("db=database('dfs://valuedb', VALUE, ['AMZN','NFLX', 'NVDA'])") 
```

In addition to VALUE partition, DolphinDB also supports RANGE, LIST, COMBO, and HASH partitions. Please refer to function [database](https://www.dolphindb.com/help/FunctionsandCommands/FunctionReferences/d/database.html?highlight=database).

Once a DFS database has been created, the partition domain cannot be changed.  Generally, the partitioning scheme cannot be revised, but we can use functions `addValuePartitions` and `addRangePartitions` to add partitions for DFS databases with VALUE and RANGE partitions (or VALUE and RANGE partitions in a COMPO domain), respectively. 

#### 4.2.2 `loadTextEx`

After a DFS database is created successfully, we can import text files to a partitioned table in the DFS database with function `loadTextEx`. If the partitioned table does not exist, `loadTextEx` creates it and appends data to it. 

Parameters of function `loadTextEx`:
- **dbPath:** the database path
- **tableName:** the partitioned table name
- **partitionColumns:**  the partitioning columns
- **remoteFilePath:** the absolute path of the text file on the DolphinDB server. 
- **delimiter:** the delimiter of the text file (comma by default).

In the following example, function `loadTextEx` creates a partitioned table "trade" on the DolphinDB server and then appends the data from "example.csv" to the table. 

```python
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")

trade = s.loadTextEx(dbPath="mydb", tableName='trade',partitionColumns=["TICKER"], remoteFilePath=WORK_DIR + "/example.csv")
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

Access the table:

```python
trade = s.table(dbPath="dfs://valuedb", data="trade")
```

#### 4.2.3 `PartitionedTableAppender`

DolphinDB supports concurrent writes to a DFS table. This section introduces how to write data concurrently to DolphinDB DFS tables in Python.

<!---

Note that DolphinDB does not allow multiple writers to write to the same partition at the same time. Therefore, when multiple threads are writing to the same database concurrently, we need to make sure each of them writes to a different partition. Python API provides a convenient way for it.

--->

With DolphinDB server version 1.30 or above, we can write to DFS tables with the `PartitionedTableAppender` object in Python API. The user needs to first specify a connection pool. The system then obtains information about partitions before assigning the partitions to the connection pool for concurrent writes. A partition can only be written to by one thread at a time. 

```python
PartitionedTableAppender(dbPath=None, tableName=None, partitionColName=None, dbConnectionPool=None)
```

- **dbPath:** DFS database path
- **tableName:** name of a DFS table
- **partitionColName:** partitioning column name
- **dbConnectionPool:** connection pool

The following script first creates database dfs://valuedb and partitioned table pt. With a connection pool specified for `PartitionedTableAppender`, it uses the `append` method to write data to pt concurrently. Similar to `tableAppender`, `PartitionedTableAppender` supports automatic conversion of data of temporal types.

```python
import pandas as pd
import dolphindb as ddb
import numpy as np
import random

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
script = '''
dbPath = "dfs://valuedb"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:100,`id`time`vol,[SYMBOL,DATE, INT])
        db=database(dbPath,VALUE, `APPL`IBM`AMZN)
        pt = db.createPartitionedTable(t, `pt, `id)
'''
s.run(script)


pool = ddb.DBConnectionPool("localhost", 8848, 20, "admin", "123456")
appender = ddb.PartitionedTableAppender("dfs://valuedb", "pt", "id", pool)
n = 100

date = []
for i in range(n):
    date.append(np.datetime64(
        "201{:d}-0{:1d}-{:2d}".format(random.randint(0, 9), random.randint(1, 9), random.randint(10, 28))))

data = pd.DataFrame({"id": np.random.choice(['AMZN', 'IBM', 'APPL'], n), "time": date,
                     "vol": np.random.randint(100, size=n)})
re = appender.append(data)

print(re)
print(s.run("pt = loadTable('dfs://valuedb', 'pt'); select * from pt;"))

# output

100
      id       time  vol
0   AMZN 2011-07-13   69
1   AMZN 2016-04-11   40
2   AMZN 2014-04-14   56
3   AMZN 2015-09-14   68
4   AMZN 2016-03-10   99
..   ...        ...  ...
95   IBM 2012-01-19   29
96   IBM 2010-05-10    5
97   IBM 2014-09-27   90
98   IBM 2010-01-25   33
99   IBM 2014-01-12   48

[100 rows x 3 columns]
```

### 4.3 Import Data as Partitioned In-Memory Table

#### 4.3.1 `loadTextEx`

Operations on partitioned in-memory tables are faster than those on non-partitioned in-memory tables as the former utilize parallel computing. We can use function `loadTextEx` to create a partitioned in-memory table with an empty string for the parameter "dbPath".

```python
import dolphindb.settings as keys

s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="")

trade=s.loadTextEx(dbPath="mydb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
trade.toDF()
```

#### 4.3.2 `ploadText`

Function `ploadText` loads a text file in parallel to generate a partitioned in-memory table. It runs much faster than `loadText`.

```python
trade=s.ploadText(WORK_DIR+"/example.csv")
print(trade.rows)

# output
13136
```

## 5 Load Data from DolphinDB

### 5.1 `loadTable`

Use `loadTable` to load a DolphinDB table. Parameter *tableName* is the partitioned table name; *dbPath* is the database path. 

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

### 5.2 `loadTableBySQL`

Method `loadTableBySQL` loads only the rows that satisfy the filtering conditions in a SQL query as a partitioned in-memory table.

```python
import os
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"  or os.path.exists("dfs://valuedb")):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
t = s.loadTextEx(dbPath="mydb",  tableName='trade',partitionColumns=["TICKER"], remoteFilePath=WORK_DIR + "/example.csv")

trade = s.loadTableBySQL(tableName="trade", dbPath="dfs://valuedb", sql="select * from trade where date>2010.01.01")
print(trade.rows)

# output
5286
```

### 5.3 Load Tables in Blocks

Starting from DolphinDB server 1.20.5 and DolphinDB Python API 1.30.0.6, to query a large table, Python API provides a way to load them in blocks. 

Create a large table in Python:

```python
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
script='''
    rows=100000;
    testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);
'''
s.run(script)
```

Set the parameter *fetchSize* of `run` method to specify the size of a block, and the method returns a BlockReader object. Use the `read` method to read data in blocks. **Please note that** the value of *fetchSize* cannot be smaller than 8192. 

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

When using the above method to read data in blocks, if not all blocks are read, please call the `skipAll` method to abort the reading before executing the subsequent code. Otherwise, data will be stuck in the socket buffer and the deserialization of the subsequent data will fail.

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
s.run("1 + 1") # The script will throw an error if skipAll is not called
```


### 5.4 Data Conversion

#### 5.4.1 Data Form Conversion

DolphinDB Python API saves data downloaded from DolphinDB server as native Python objects.

|DolphinDB|Python|DolphinDB data|Python data|
|-------------|----------|-------------|-----------|
|scalar|Numbers, Strings, NumPy.datetime64|see section 6.3.2|see section 6.3.2
|vector|c.array|1..3|[1 2 3]
|array vector|Numpy.Ndarray|[[1, 2, 3], [4, 5], [6]]|[np.array([1, 2, 3]), np.array([4, 5]), np.array([6])]|
|pair|Lists|1:5|[1, 5]
|matrix|Lists|1..6$2:3|[array([[1, 3, 5],[2, 4, 6]], dtype=int32), None, None]
|set|Sets|set(3 5 4 6)|{3, 4, 5, 6}|
|dictionary|Dictionaries|dict(['IBM','MS','ORCL'], 170.5 56.2 49.5)|{'MS': 56.2, 'IBM': 170.5, 'ORCL': 49.5}|
|table|pandas.DataFame|see section 6.1|see section 6.1|

#### 5.4.2 Data Type Conversion

The table below explains the data type conversion when data is downloaded from DolphinDB and converted into a Python DataFrame with function `toDF()`. 

- DolphinDB CHAR types are converted into Python int64 type. Use Python method `chr` to convert CHAR type into a character. 
- As the temporal types in Python pandas are datetime64, all DolphinDB temporal types [are converted into datetime64 type](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803). MONTH type such as 2012.06M is converted into 2012-06-01 (the first day of the month). 
- TIME, MINUTE, SECOND and NANOTIME types do not include information about date. 1970-01-01 is automatically added during conversion. For example, 13:30m is converted into 1970-01-01 13:30:00. 
- If you are uploading a table that includes columns containing Python `decimal.Decimal` objects, ensure that all the values in a particular column have the same number of decimal places. The following script aligns the decimal digits of values:

```
b = decimal.Decimal("1.23")
b = b.quantize(decimal.Decimal("0.000"))
```

| DolphinDB type | Python type | DolphinDB data                                  | Python data                             |
| -------------- | ----------- | ----------------------------------------------- | --------------------------------------- |
| BOOL           | bool        | [true,00b]                                      | [True, nan]                             |
| CHAR           | int64       | [12c,00c]                                       | [12, nan]                               |
| SHORT          | int64       | [12,00h]                                        | [12, nan]                               |
| INT            | int64       | [12,00i]                                        | [12, nan]                               |
| LONG           | int64       | [12l,00l]                                       | [12, nan]                               |
| DOUBLE         | float64     | [3.5,00F]                                       | [3.5,nan]                               |
| FLOAT          | float32     | [3.5,00f]                                       | [3.5, nan]                              |
| SYMBOL         | object      | symbol(["AAPL",NULL])                           | ["AAPL",""]                             |
| STRING         | object      | ["AAPL",string()]                               | ["AAPL", ""]                            |
| DATE           | datetime64  | [2012.6.12,date()]                              | [2012-06-12, NaT]                       |
| MONTH          | datetime64  | [2012.06M, month()]                             | [2012-06-01, NaT]                       |
| TIME           | datetime64  | [13:10:10.008,time()]                           | [1970-01-01 13:10:10.008, NaT]          |
| MINUTE         | datetime64  | [13:30,minute()]                                | [1970-01-01 13:30:00, NaT]              |
| SECOND         | datetime64  | [13:30:10,second()]                             | [1970-01-01 13:30:10, NaT]              |
| DATETIME       | datetime64  | [2012.06.13 13:30:10,datetime()]                | [2012-06-13 13:30:10,NaT]               |
| TIMESTAMP      | datetime64  | [2012.06.13 13:30:10.008,timestamp()]           | [2012-06-13 13:30:10.008,NaT]           |
| NANOTIME       | datetime64  | [13:30:10.008007006, nanotime()]                | [1970-01-01 13:30:10.008007006,NaT]     |
| NANOTIMESTAMP  | datetime64  | [2012.06.13 13:30:10.008007006,nanotimestamp()] | [2012-06-13 13:30:10.008007006,NaT]     |
| UUID           | object      | 5d212a78-cc48-e3b1-4235-b4d91473ee87            | "5d212a78-cc48-e3b1-4235-b4d91473ee87"  |
| IPADDR         | object      | 192.168.1.13                                    | "192.168.1.13"                          |
| INT128         | object      | e1671797c52e15f763380b45e841ec32                | "e1671797c52e15f763380b45e841ec32"      |

#### 5.4.3 Missing Value Processing

When data is downloaded from DolphinDB and converted into a Python DataFrame with function `toDF()`, NULLs of logical, temporal and numeric types are converted into NaN or NaT; NULLs of string types are converted into empty strings. 

### 5.5 Data Format Protocols

Before API version 1.30.21.1, DolphinDB Python API supported transferring data using the Pickle protocol and the DolphinDB serialization protocol, which you can specify through the *enablePickle* parameter of the session object. 

Starting from API version 1.30.21.1, a new parameter *protocol* has been added to the session and DBConnectionPool classes. It can be PROTOCOL_DDB, PROTOCOL_PICKLE (default), or PROTOCOL_ARROW.

**Example**

```
import dolphindb as ddb
import dolphindb.settings as keys
s = ddb.session(protocol=keys.PROTOCOL_DDB)
s = ddb.session(protocol=keys.PROTOCOL_PICKLE)
s = ddb.session(protocol=keys.PROTOCOL_ARROW)
```

#### 5.5.1 PROTOCOL_DDB

PROTOCOL_DDB is the same data serialization protocol as used by the DolphinDB C++ API, C# API, and Java API. 

For data form and data type mappings, see section 5.4.1 and 5.4.2.

**Example**

```
import dolphindb as ddb
import dolphindb.settings as keys
s = ddb.session(protocol=keys.PROTOCOL_DDB)
s.connect("localhost", 8848, "admin", "123456")

re = s.run("table(1..10 as a)")   # pandas.DataFrame
```

#### 5.5.2 PROTOCOL_PICKLE

PROTOCOL_PICKLE is adapted from the Python pickle protocol. Refer to the following table when transferring data:

| **DolphinDB Data form** | **DolphinDB->Python**                       | **Python->DolphinDB** |
| :---------------------- | :------------------------------------------ | :-------------------- |
| Matrix                  | Matrix -> [numpy.ndarray, colName, rowName] | use PROTOCOL_DDB      |
| Table                   | Table -> pandas.DataFrame                   | use PROTOCOL_DDB      |
| Other forms             | use PROTOCOL_DDB                            | use PROTOCOL_DDB      |

In `session.run`, if the parameter *pickleTableToList* is set to True, refer to the following table when transferring data:


| **DolphinDB Data Form** | **DolphinDB->Python**     | **Python->DolphinDB** |
| :---------------------- | :------------------------ | :-------------------- |
| Matrix                  | use PROTOCOL_PICKLE       | use PROTOCOL_DDB      |
| Table                   | Table ->  [np.ndarray, …] | use PROTOCOL_DDB      |
| Other forms             | use PROTOCOL_DDB          | use PROTOCOL_DDB      |


```
import dolphindb as ddb
import dolphindb.settings as keys
s = ddb.session(protocol=keys.PROTOCOL_PICKLE)
s.connect("localhost", 8848, "admin", "123456")

# pickleTableToList = False (default)
re1 = s.run("m=matrix(1 2, 3 4, 5 6);m.rename!(1 2, `a`b`x);m")
re2 = s.run("table(1..3 as a)")
print(re1)
print(re2)
-----------------------------
[array([[1, 3, 5],
       [2, 4, 6]], dtype=int32), 
 array([1, 2], dtype=int32), 
 array(['a', 'b', 'x'], dtype=object)]
   a
0  1
1  2
2  3

# pickleTableToList = True
re1 = s.run("m=matrix(1 2, 3 4, 5 6);m.rename!(1 2, `a`b`x);m", pickleTableToList=True)
re2 = s.run("table(1..3 as a)", pickleTableToList=True)
print(re1)
print(re2)
-----------------------------
[array([[1, 3, 5],
       [2, 4, 6]], dtype=int32), 
 array([1, 2], dtype=int32), 
 array(['a', 'b', 'x'], dtype=object)]
[array([1, 2, 3], dtype=int32)]
```

#### 5.5.3 PROTOCOL_ARROW

PROTOCOL_ARROW is adapted from the Apache Arrow protocol. Refer to the following table when transferring data:

| **DolphinDB Data Form** | **DolphinDB->Python**  | **Python->DolphinDB** |
| :---------------------- | :--------------------- | :-------------------- |
| Table                   | Table -> pyarrow.Table | use PROTOCOL_DDB      |
| Other forms             | use PROTOCOL_DDB       | use PROTOCOL_DDB      |

Note: *PROTOCOL_ARROW* is only supported on Linux x86_64 with PyArrow 9.0.0 or later versions installed.

**Example**

```
import dolphindb as ddb
import dolphindb.settings as keys
s = ddb.session(protocol=keys.PROTOCOL_ARROW)
s.connect("localhost", 8848, "admin", "123456")

re = s.run("table(1..3 as a)")
print(re)
-----------------------------
pyarrow.Table
a: int32
----
a: [[1,2,3]]
```

For more information, see [DolphinDB formatArrow Plugin](https://github.com/dolphindb/DolphinDBPlugin/blob/release200/formatArrow/README.md).

## 6 Append to DolphinDB Tables

This section introduces how to use Python API to upload data and append it to DolphinDB tables. 

<!--- 

There are 2 types of DolphinDB tables based on the storage method:

- In-memory table: data is stored only in memory with optimal query performance.
- DFS tables: data is distributed in different nodes' disks and managed by DolphinDB's distributed file system.

---->

Use the following 2 methods to append data to DolphinDB in-memory tables:

- Use function `tableInsert` to append data or a table 
- Use `insert into` statement to append data

Execute the Python script to generate an empty in-memory table to be used in the later examples:

```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

script = """t = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)
```

The example above specifies the initial size, column names, and data types when creating an in-memory table on the DolphinDB server. For a standard in-memory table, data in a session is isolated and only visible to the current session. For an in-memory table to be accessed from multiple servers at the the same time, `share` it across sessions.

### 6.1 Append Lists to In-memory Tables with the `tableInsert` Function

You can organize your data in Python into a list and append it to a table with the function `tableInsert`. This way, you can upload data objects and append the data in just one request to the DolphinDB server (which is 1 step less than using `insert into`).

```python
import dolphindb as ddb
import numpy as np

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
ids = [1,2,3]
dates = np.array(['2019-03-03','2019-03-04','2019-03-05'], dtype="datetime64[D]")
tickers=['AAPL','GOOG','AAPL']
prices = [302.5, 295.6, 297.5]
// Insert the "args" list to the "tglobal" table with tableInsert
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

### 6.2 Append DataFrame to In-memory Tables with the `tableInsert` Function

- If there is no time column in the table:

Upload a DataFrame to the server and append it to an in-memory table with [partial application](https://www.dolphindb.com/help/PartialApplication.html). 

```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

import pandas as pd
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

- If there is a time column in the table:

As [the only temporal data type in Python pandas is datetime64](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803), all temporal columns of a DataFrame are converted into nanotimestamp type after uploaded to DolphinDB. Each time we use `tableInsert` or `insert into` to append a DataFrame with a temporal column to an in-memory table or DFS table, we need to conduct a data type conversion for the time column.

```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")
script = """t = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)

import pandas as pd
import numpy as np
def createDemoDict():
		return {'id': [1, 2, 2, 3],
            'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
            'price': [22.0, 3.5, 21.0, 26.0]}


tb=pd.DataFrame(createDemoDict())
s.upload({'tb':tb})
s.run("tableInsert(tglobal,(select id, date(date) as date, ticker, price from tb))")
print(s.run("tglobal"))

#output
   id	      date ticker	price
0	1	2019-02-04	AAPL	22.0
1	2	2019-02-05	AMZN	3.5
2	2	2019-02-09	AMZN	21.0
3	3	2019-02-13	A	26.0
```


### 6.3 Append Data with the `insert into` Statement

To insert a single row of data:

```python
import numpy as np

script = "insert into tglobal values(%s, date(%s), %s, %s)" % (1, np.datetime64("2019-01-01").astype(np.int64), '`AAPL', 5.6)
s.run(script)
```

**Note:** Starting from DolphinDB server version 1.30.16, the in-memory table supports automatic data type conversion.

<!---

**Please note** that DolphinDB in-memory table does not provide automatic data type conversion. Therefore, when appending data to an in-memory table, it is required to call the time conversion function on the server to convert the time column.

Before appending data, please make sure the inserted data types are consistent with those of the table columns.

In the above example, the time type of NumPy is forcibly converted to int64, and the `date` function is called in `insert` to convert the data type of the time column to the corresponding type on the server.

--->

To insert multiple rows of data:

```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

import numpy as np
import random
import pandas as pd
rowNum = 5
ids = np.arange(1, rowNum+1, 1, dtype=np.int32)
dates = np.array(pd.date_range('4/1/2019', periods=rowNum), dtype='datetime64[D]')
tickers = np.repeat("AA", rowNum)
prices = np.arange(1, 0.6*(rowNum+1), 0.6, dtype=np.float64)
s.upload({'ids':ids, "dates":dates, "tickers":tickers, "prices":prices})
script = "insert into tglobal values(ids,dates,tickers,prices);"
s.run(script)
```

The above example specifies the *dtype* parameter of the `date_range()` function as datetime64[D] to generate a time column containing only date information. The data type is the same as the DATE type of DolphinDB, so it can be inserted directly with `insert` statement without data conversion. If the data type is datetime64, please append data to in-memory tables in the following way:  

```python
script = "insert into tglobal values(ids,date(dates),tickers,prices);"
s.run(script)
```

**Please note**: For performance reasons, it is not recommended to use `insert into` to insert data as parsing the insert statement causes extra overhead.

### 6.4 Automatic Temporal Data Type Conversion with the `tableAppender` Object

As [the only temporal data type in Python pandas is datetime64](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803), all temporal columns of a DataFrame are converted into nanotimestamp type after uploaded to DolphinDB. Each time we use `tableInsert` or `insert into` to append a DataFrame with a temporal column to an in-memory table or DFS table, we need to conduct a data type conversion for the time column.  With the `tableAppender` object, you will no longer have to do the conversion manually when you `append` local DataFrames to an in-memory table or a DFS table.

```
tableAppender(dbPath=None, tableName=None, ddbSession=None, action="fitColumnType")
```

- **dbPath:** the path of a DFS database. Leave it unspecified for in-memory tables. 
- **tableName:** the table name. 
- **ddbSession:** a session connected to DolphinDB server. 
- **action:** What to do when appending. Now only supports "fitColumnType", which means convert temporal column types. 

The example below appends data to the shared in-memory table t with `tableAppender`:

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

### 6.5 Append Data with the `tableUpsert` Object

Use the `tableUpsert` object to update data in indexed in-memory tables, keyed in-memory tables and DFS tables. Like the `tableAppender` object, `tableUpsert` automatically converts the temporal data when writing local DataFrames to the target table.

```
tableUpsert(dbPath=None, tableName=None, ddbSession=None, ignoreNull=False, keyColNames=[], sortColumns=[])
```

- **dbPath**: the path of a DFS database. Leave it unspecified for in-memory tables.
- **tableName**: a STRING indicating the name of a DFS table, indexed in-memory table or keyed in-memory table.
- **ddbSession**: a session connected to DolphinDB server.
- **ignoreNull**: a Boolean value. If set to true, for the NULL values (if any) in the data to be written, the corresponding elements in the table are not updated. The default value is false.
- **keyColNames**: a STRING scalar/vector. When updating a DFS table, *keyColNames* are considered as the key columns.
- **sortColumns**: a STRING scalar or vector. When updating a DFS table, the updated partitions will be sorted on sortColumns (only within each partition, not across partitions).

The following example creates and shares the keyed in-memory table `ttable` and use `tableUpsert` to insert data into the table:


```python
import dolphindb as ddb
import numpy as np
import pandas as pd
import time
import random
import dolphindb.settings as keys

import threading

HOST = "192.168.1.193"
PORT = 8848

s = ddb.session()
s.connect(HOST, PORT, "admin", "123456")
script_DFS_HASH = """
    testtable=keyedTable(`id,1000:0,`date`text`id,[DATETIME,STRING,LONG])
    share testtable as ttable
    """
s.run(script_DFS_HASH)

upsert=ddb.tableUpsert("","ttable",s)
dates=[]
texts=[]
ids=[]
print(np.datetime64('now'))
for i in range(1000):
    dates.append(np.datetime64('2012-06-13 13:30:10.008'))
    texts.append(str(time.time()))
    ids.append(i%20)
df = pd.DataFrame({'date': dates,'text': texts,'id': np.array(ids,np.int64)})
upsert.upsert(df)
print(s.run("ttable"))
```

### 6.6 Append to DFS Tables

DFS table is recommended by DolphinDB in production environment. It supports snapshot isolation and ensures data consistency. With data replication, DFS tables offers fault tolerance and load balancing. The following example appends data to a DFS table via the Python API.

Please note that the DFS tables can only be used in clustered environments with the configuration parameter *enableDFS* = 1.

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

You can load a DFS table with `loadTable` and then append data with `tableInsert`. In the following example, we use the user-defined function `createDemoDataFrame()` to create a DataFrame, then append it to a DFS table. Please note that when appending to a DFS table, the temporal data types are automatically converted.

```python
tb = createDemoDataFrame()
s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb=tableName), tb)
```

You can also use function `append!` to append a table to another. However, it is not recommended to call `append!` because it will return a schema, which increases the volume of data to be transferred.


```python
tb = createDemoDataFrame()
s.run("append!{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb=tableName),tb)
```

The example below appends to a DFS table pt with `tableAppender`:

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

### 6.7 Append Data Asynchronously

For high throughput data processing, especially data writes in high frequency, you can enable the asynchronous mode to effectively increase the data throughput of tasks on the client. It has the following characteristics:

- After submitting a task, the client considers the task is complete as soon as the server receives it.
- The client cannot obtain the status or result of the task executed on the server.
- The total time it takes to submit asynchronous tasks depends on the serialization time of the submitted parameters and the network transmission.

**Note**: The asynchronous mode is not suitable if there's dependency between 2 consecutive tasks. For example, a task writes data to a DFS database and the next task analyzes the written data and historical data. In this case, the asynchronous mode is not suitable. 

To enable the asynchronous mode for DolphinDB Python API, set *enableASYNC* = True in the session. See [Section 1.1](#11-establish-connection) for more information.

```python
s=ddb.session(enableASYNC=True)
```

By writing data asynchronously, you can save the time of detecting the returned value. Refer to the following script in DolphinDB to append data asynchronously to a DFS table.

```python
import dolphindb as ddb
import numpy as np
import dolphindb.settings as keys
import pandas as pd

s = ddb.session(enableASYNC=True) # enable asynchronous mode
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
s.run(script) # The script above can be executed on the server

tb = pd.DataFrame({'id': [1, 2, 2, 3],
                   'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                   'price': [22, 3.5, 21, 26]})

s.run("append!{{loadTable('{db}', `{tb})}}".format(db=dbPath, tb=tableName), tb)
```

**Note**: In asynchronous mode, only the method `session.run()` is supported to communicate with the server and no value is returned.

The asynchronous mode shows better performance with higher data throughput. The following example writes to a stream table. For details on Python Streaming API, see [Chap 10](#10-python-streaming-api).


```python
import dolphindb as ddb
import numpy as np
import pandas as pd
import random
import datetime

s = ddb.session(enableASYNC=True)
s.connect("localhost", 8848, "admin", "123456")

n = 100

script = """trades = streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT])"""
s.run(script) # The script above can be executed on the server

# Randomly generate a DataFrame
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

For data of temporal types that need to be converted, please do not submit the two tasks of `uploading` data to the server and converting data types with SQL script in asynchronous mode. It may lead to the problem that the SQL script is already executed though data loading is not finished. To solve this problem, you can first define a view function on the server, then the client just needs to upload the data.

First, define a function view `appendStreamingData` on the server:


```txt
login("admin","123456")
trades = streamTable(10000:0,`time`sym`price`id, [DATE,SYMBOL,DOUBLE,INT])
share trades as tglobal
def appendStreamingData(mutable data){
tableInsert(tglobal, data.replaceColumn!(`time, date(data.time)))
}
addFunctionView(appendStreamingData)
```

Then append data asynchronously:

```python
import dolphindb as ddb
import numpy as np
import pandas as pd
import random
import datetime

s = ddb.session(enableASYNC=True)
s.connect("localhost", 8848, "admin", "123456")

n = 100

# Randomly generate a DataFrame
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
s.run("appendStreamingData(tb)")
```

### 6.8 Append Data in Batch Asynchronously with the `MultithreadedTableWriter` Object

To insert single record frequently, you can use methods of `MultithreadedTableWriter` class for asynchronous writes via DolphinDB Python API. The class maintains a buffer queue in Python. Even when the server is fully occupied with network I/O operations, the writing threads of the API client will not be blocked. You can use the method `getStatus` to check the status of the `MultithreadedTableWriter` object.

**`MultithreadedTableWriter`**

```Python
MultithreadedTableWriter(host, port, userId, password, dbPath, tableName, useSSL, enableHighAvailability, highAvailabilitySites, batchSize, throttle, threadCount, partitionCol, compressMethods, mode, modeOption)
```

**Parameters:**

* **host**: host name
* **port**: port number
* **userId** / **password**: username and password
* **dbPath**:  a STRING indicating the DFS database path. Leave it unspecified for an in-memory table.
* **tableName**: a STRING indicating the in-memory or DFS table name.

**For API 1.30.17 or lower versions, when writing to an in-memory table,  please specify the in-memory table name for *dbPath* and leave *tableName* empty.**

* **useSSL**: a Boolean value indicating whether to enable SSL. The default value is False.
* **enableHighAvailability**: a Boolean value indicating whether to enable high availability. The default value is False.
* **highAvailabilitySites**: a list of ip:port of all available nodes
* **batchSize**: an integer indicating the number of messages in batch processing. The default value is 1, meaning the server processes the data as soon as they are written. If it is greater than 1, only when the number of data reaches *batchSize*, the client will send the data to the server.
* **throttle**: a floating point number greater than 0 indicating the waiting time (in seconds) before the server processes the incoming data if the number of data written from the client does not reach *batchSize*.
* **threadCount**: an integer indicating the number of working threads to be created. The default value is 1, indicating single-threaded process. It must be 1 for a dimension table.
* **partitionCol**: a STRING indicating the partitioning column. It is None by default, and only works when *threadCount* is greater than 1. For a partitioned table, it must be the partitioning column; for a stream table, it must be a column name; for a dimension table, the parameter does not work.
* **compressMethods** a list of the compression methods used for each column. If unspecified, the columns are not compressed. The compression methods include:
    * "LZ4": LZ4 algorithm
    * "DELTA": Delta-of-delta encoding
* - **mode**: a STRING indicating the write mode. It can be: "upsert" (to [upsert!](https://dolphindb.com/help/FunctionsandCommands/FunctionReferences/u/upsert!.html) the data) or "append" (to [append!](https://dolphindb.com/help/FunctionsandCommands/FunctionReferences/a/append!.html) the data).
- **modeOption**: a list of strings indicating the optional parameters of [upsert!](https://dolphindb.com/help/FunctionsandCommands/FunctionReferences/u/upsert!.html). This parameter only takes effect when *mode* = "upsert". 

The following part introduces methods of `MultithreadedTableWriter` class.

(1) `insert`

```Python
insert(*args)
```

**Details:**
Insert a single record. Return a class `ErrorCodeInfo` containing *errorCode* and *errorInfo*. If *errorCode* is not None, `MultithreadedTableWriter` has failed to insert the data, and *errorInfo* displays the error message.

The class `ErrorCodeInfo` provides methods `hasError()` and `succeed()` to check whether the data is written properly. `hasError()` returns True if an error occurred, False otherwise. `succeed()` returns True if the data is written successfully, False otherwise.

**Parameters:**

* **args**: a variable-length argument indicating the record to be inserted.

**Examples:**

```python
import numpy as np
import pandas as pd
import dolphindb as ddb
import time
import random

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

script = """t=table(1000:0, `date`ticker`price, [DATE,SYMBOL,LONG])
share t as tglobal"""
s.run(script)

writer = ddb.MultithreadedTableWriter("localhost", 8848, "admin", "123456","","tglobal",False,False,[],10,1,5,"date")
for i in range(10):
  res = writer.insert(np.datetime64('2022-03-23'),"AAAAAAAB", random.randint(1,10000))
writer.waitForThreadCompletion()
print(writer.getStatus())
```

Output:

```
errorCode     : None
 errorInfo     : 
 isExiting     : True
 sentRows      : 10
 unsentRows    : 0
 sendFailedRows: 0
 threadStatus  : 
 	threadId	sentRows	unsentRows	sendFailedRows
	     508	       0	         0	             0
	   16124	       0	         0	             0
	   24020	       0	         0	             0
	    4636	       0	         0	             0
	    4092	      10	         0	             0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x000001E3FCF02808>
```



(2) `getUnwrittenData`

```Python
getUnwrittenData()
```

**Details:**

Return a nested list of data that has not been written to the server,  including both data failed to be sent and data to be sent.

**Note:** Data obtained by this method will be released by `MultithreadedTableWriter`.


(3) `insertUnwrittenData`

```Python
insertUnwrittenData(unwrittenData)
```

**Details:**
Insert unwritten data. The result is in the same format as `insert`. The difference is that `insertUnwrittenData` can insert multiple records at a time.

**Parameters:**

* **unwrittenData**: the data that has not been written to the server. You can obtain the object with method `getUnwrittenData`.


(4) `getStatus`


```Python
getStatus()
```

**Details:**
Get the current status of the object. It returns a class with the following attributes and methods:

* **isExiting:** whether the threads are exiting
* **errorCode:** error code
* **errorInfo:** error message
* **sentRows:** number of sent rows
* **unsentRows:** number of rows to be sent
* **sendFailedRows:** number of rows failed to be sent
* **threadStatus:** a list of the thread status
    * threadId: thread ID
    * sentRows: number of rows sent by the thread
    * unsentRows: number of rows to be sent by the thread
    * sendFailedRows: number of rows failed to be sent by the thread

Methods:

* `hasError()`

Return True if an error occurred, False otherwise.

* `succeed()`

Return True if the data is written successfully, False otherwise.

(5) `waitForThreadCompletion`

```Python
waitForThreadCompletion()
```

**Details:** 
After calling the method, `MultithreadedTableWriter` will wait until all working threads complete their tasks. If you call `insert` or `insertUnwrittenData` after the execution of `waitForThreadCompletion`, an error "thread is exiting" will be raised.

The methods of `MultithreadedTableWriter` are usually used in the following order:

```python
# insert data
writer.insert(data)
...
writer.waitForThreadCompletion()
# Check whether the tasks have been completed
writeStatus=writer.getStatus()
if writeStatus.hasError():
    # obtain unwritten data and insert again
    unwrittendata = writer.getUnwrittenData()
    unwrittendata = revise(unwrittendata)
    newwriter.insertUnwrittenData(unwrittendata)
else
    print("Write successfully!")
```

The following example shows how to use `MultithreadedTableWriter` to insert data.

- Create a DolphinDB DFS table

```python
import numpy as np
import pandas as pd
import dolphindb as ddb
import time
import random
import threading

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

script = """
    dbName = 'dfs://valuedb3';
    if(exists(dbName)){
        dropDatabase(dbName);
    }
    datetest=table(1000:0,`date`symbol`id,[DATE,SYMBOL,LONG]);
    db = database(directory=dbName, partitionType=HASH, partitionScheme=[INT, 10]);
    pt=db.createPartitionedTable(datetest,'pdatetest','id');
"""
s.run(script)
```

- Create a `MultithreadedTableWriter` object

```python
writer = ddb.MultithreadedTableWriter("localhost", 8848, "admin", "123456","dfs://valuedb3","pdatetest",False,False,[],10000,1,5,"id",["LZ4","LZ4","DELTA"])
```

- Execute `writer.insert()` to insert data and obtain the status with `writer.getStatus()`.

```python
try:
    # insert 100 rows of data 
    for i in range(100):
        res = writer.insert(random.randint(1,10000),"AAAAAAAB", random.randint(1,10000))
except Exception as ex:
    print("MTW exit with exception %s" % ex)
writer.waitForThreadCompletion()
writeStatus=writer.getStatus()
if writeStatus.succeed():
    print("Write successfully!")
print("writeStatus: \n", writeStatus)
print(s.run("select count(*) from pt"))
```
Output:

```
Write successfully!
writeStatus: 
 errorCode     : None
 errorInfo     : 
 isExiting     : True
 sentRows      : 100
 unsentRows    : 0
 sendFailedRows: 0
 threadStatus  : 
 	threadId	sentRows	unsentRows	sendFailedRows
	       0	       0	         0	             0
	    9252	      17	         0	             0
	    8104	      26	         0	             0
	   14376	      18	         0	             0
	   20780	      21	         0	             0
	   19536	      18	         0	             0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x000002557E4D1488>
   count
0    100
```

As shown in the output, `MultithreadedTableWriter` has written data to the DFS table successfully and *errorCode* is None. 

In `MultithreadedTableWriter`, writing to the MTW queue, converting data types and sending data to the server are processed asynchronously. The queue simply performs basic validation on the incoming data, such as checking whether there is a column count mismatch. If an error occurs, the queue returns an error message without terminating the working threads. If the data types cannot be converted properly in the working threads before writes, all threads are terminated immediately. 

The following example inserts data in batches to the `MultithreadedTableWriter` object. 

```python
writer = ddb.MultithreadedTableWriter("localhost", 8848, "admin", "123456","dfs://valuedb3","pdatetest",False,False,[],10000,1,5,"id",["LZ4","LZ4","DELTA"])
try:
        # insert 100 rows of records with correct data types and column count
        for i in range(100):
        res = writer.insert(np.datetime64('2022-03-23'),"AAAAAAAB", random.randint(1,10000))
        
    # insert 10 rows of records with incorrect data types
    for i in range(10):
        res = writer.insert(np.datetime64('2022-03-23'),222, random.randint(1,10000))
        if res.hasError():
            print("Insert wrong format data:\n", res)
    # Insert a row of record with incorrect column count
    res = writer.insert(np.datetime64('2022-03-23'),"AAAAAAAB")
    if res.hasError():
        print("Column counts don't match:\n", res)
    
    # Wait 1 sec for the working threads to process the data until it detects the incorrect data types for the second insert. All working threads terminate and the status turns to error.
    time.sleep(1)

    # Insert another row of data.
    res = writer.insert(np.datetime64('2022-03-23'),"AAAAAAAB", random.randint(1,10000))
    print("MTW has exited")
except Exception as ex:
    print("MTW exit with exception %s" % ex)
writer.waitForThreadCompletion()
writeStatus=writer.getStatus()
if writeStatus.hasError():
    print("Error in writing:")
print(writeStatus)
print(s.run("select count(*) from pt"))
```

Output:

```
Column counts don't match:
 errorCode: A2
 errorInfo: Column counts don't match 3
<dolphindb.session.ErrorCodeInfo object at 0x000002557CCCDF48>
MTW exit with exception <Exception> in insert: thread is exiting.
Error in writing:
errorCode     : A1
 errorInfo     : Data conversion error: Cannot convert long to SYMBOL
 isExiting     : True
 sentRows      : 0
 unsentRows    : 100
 sendFailedRows: 10
 threadStatus  : 
 	threadId	sentRows	unsentRows	sendFailedRows
	       0	       0	         0	            10
	   19708	       0	        24	             0
	   13480	       0	        23	             0
	    5820	       0	        15	             0
	   23432	       0	        19	             0
	   18756	       0	        19	             0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x000002557E52D908>
   count
0    100
```

If error occurs when `MultithreadedTableWriter` is writing data, all working threads exit. You can use `writer.getUnwrittenData()` to obtain the unwritten data and then rewrite it with `insertUnwrittenData(unwriterdata)`. Please note that a new MTW object must be created to write the unwritten data.

```python
if writeStatus.hasError():
    print("Error in writing:")
    unwriterdata = writer.getUnwrittenData()
    print("Unwriterdata: %d" % len(unwriterdata))
    # Creater a new MTW object
    newwriter = ddb.MultithreadedTableWriter("localhost", 8848, "admin", "123456","dfs://valuedb3","pdatetest",False,False,[],10000,1,5,"id",["LZ4","LZ4","DELTA"])
    try:
        for row in unwriterdata:
            row[1]="aaaaa"
        res = newwriter.insertUnwrittenData(unwriterdata)
        if res.succeed():
            newwriter.waitForThreadCompletion()
            writeStatus=newwriter.getStatus()
            print("Write again:\n", writeStatus)
        else:
            print("Failed to write data again: \n",res) 
    except Exception as ex:
        print("MTW exit with exception %s" % ex)
    finally:
        newwriter.waitForThreadCompletion()
else:
    print("Write successfully:\n", writeStatus)

print(s.run("select count(*) from pt"))
```

Output:

```
Error in writing:
Unwriterdata: 110
Write again:
 errorCode     : None
 errorInfo     : 
 isExiting     : True
 sentRows      : 110
 unsentRows    : 0
 sendFailedRows: 0
 threadStatus  : 
 	threadId	sentRows	unsentRows	sendFailedRows
	       0	       0	         0	             0
	   26056	      25	         0	             0
	     960	      25	         0	             0
	   22072	      19	         0	             0
	    1536	      21	         0	             0
	   26232	      20	         0	             0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x000002557CCCDF48>
   count
0    210
```


Please note that the method `writer.waitForThreadCompletion()` will wait for `MultithreadedTableWriter` to finish the data writes, and then terminate all working threads with the last status retained. A new MTW object must be created to write data again.

As shown in the above example, `MultithreadedTableWriter` applies multiple threads to data conversion and writes. The API client also use multiple threads to call `MultithreadedTableWriter`, and the implementation is thread-safe.


```python
# Create a MTW object
writer = ddb.MultithreadedTableWriter("localhost", 8848, "admin", "123456","dfs://valuedb3","pdatetest",False,False,[],10000,1,5,"id",["LZ4","LZ4","DELTA"])

def insert_MTW(writer):
    try:
        # insert 100 rows of records
        for i in range(100):
            res = writer.insert(random.randint(1,10000),"AAAAAAAB", random.randint(1,10000))
    except Exception as ex:
        print("MTW exit with exception %s" % ex)

# create a thread to write data to MTW
thread=threading.Thread(target=insert_MTW, args=(writer,))
thread.setDaemon(True)
thread.start()

time.sleep(1)

thread.join()
writer.waitForThreadCompletion()
writeStatus=writer.getStatus()
print("writeStatus:\n", writeStatus)
print(s.run("select count(*) from pt"))
```

Output:

```
writeStatus:
 errorCode     : None
 errorInfo     : 
 isExiting     : True
 sentRows      : 100
 unsentRows    : 0
 sendFailedRows: 0
 threadStatus  : 
 	threadId	sentRows	unsentRows	sendFailedRows
	       0	       0	         0	             0
	   22388	      16	         0	             0
	   10440	      20	         0	             0
	   22832	      16	         0	             0
	    5268	      30	         0	             0
	   23488	      18	         0	             0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x000002557E4C0548>
   count
0    310
```

- Set the *mode* parameter of `MultithreadedTableWriter` to "UPSERT" to update table.

```python
import dolphindb as ddb
import numpy as np
import pandas as pd
import time
import random
import dolphindb.settings as keys
import threading
HOST = "192.168.1.193"
PORT = 8848
s = ddb.session()
s.connect(HOST, PORT, "admin", "123456")
script_DFS_HASH = """
    testtable=keyedTable(`id,1000:0,`text`id,[STRING,LONG])
    share testtable as ttable
    """
s.run(script_DFS_HASH)
def insert_mtw(writer, id):
    try:
        print("thread",id,"start.")
        for i in range(1000):
            text=str(time.time())
            id=random.randint(1, 10)
            print(text,id)
            res=writer.insert(text, id)
        print("thread",id,"exit.")
    except Exception as e:
        print(e)
print("test start.")
writer = ddb.MultithreadedTableWriter(HOST, PORT,"admin","123456","","ttable",False,False,[], 1, 0.1, 1,"id",mode="UPSERT",
                                      modeOption=["ignoreNull=false","keyColNames=`id"])
threads=[]
for i in range(2):
    threads.append(threading.Thread(target=insert_mtw, args=(writer,i,)))
for t in threads:
    t.setDaemon(True)
    t.start()
for t in threads:
    t.join()
writer.waitForThreadCompletion()
status=writer.getStatus()
print("test exit",status)
```

### 6.9 Data Conversion

It is recommended to use `MultithreadedTableWriter`  to upload data from Python to DolphinDB as it supports conversion of more data types and forms.

| No.  | DolphinDB Data                                               | Python Data          |
| ---- | ------------------------------------------------------------ | -------------------- |
| 1    | Vector                                                       | tuple                |
| 2    | Vector                                                       | list                 |
| 3    | Vector, Matrix                                               | Numpy.array          |
| 4    | Vector, Matrix                                               | Pandas.series        |
| 5    | Table                                                        | Pandas.dataframe     |
| 6    | Set                                                          | Set                  |
| 7    | Dictionary                                                   | Dict                 |
| 8    | VOID                                                         | None                 |
| 9    | BOOL                                                         | Bool                 |
| 10   | NANOTIME, NANOTIMESTAMP, TIMESTAMP, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, LONG, INT, SHORT, CHAR | Int                  |
| 11   | FLOAT, DOUBLE                                                | Float                |
| 12   | INT128, UUID, IP, SYMBOL, STRING, BLOB                       | Str                  |
| 13   | INT128, UUID, IP, SYMBOL, STRING, BLOB                       | Bytes                |
| 14   | NANOTIMESTAMP                                                | Numpy.datetime64[ns] |
| 15   | DATE                                                         | Numpy.datetime64[D]  |
| 16   | MONTH                                                        | Numpy.datetime64[M]  |
| 17   | DATETIME                                                     | Numpy.datetime64[m]  |
| 18   | DATETIME                                                     | Numpy.datetime64[s]  |
| 19   | DATEHOUR                                                     | Numpy.datetime64[h]  |
| 20   | TIMESTAMP                                                    | Numpy.datetime64[ms] |
| 21   | NANOTIME                                                     | Numpy.datetime64[us] |
| 22   | DATETIME                                                     | Numpy.datetime64     |
| 23   | BOOL                                                         | Numpy.bool           |
| 24   | LONG, INT, SHORT, CHAR                                       | Numpy.int8           |
| 25   | LONG, INT, SHORT, CHAR                                       | Numpy.int16          |
| 26   | LONG, INT, SHORT, CHAR                                       | Numpy.int32          |
| 27   | NANOTIME, NANOTIMESTAMP, TIMESTAMP, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, LONG, INT, SHORT, CHAR | Numpy.int64          |
| 28   | FLOAT, DOUBLE                                                | Numpy.float32        |
| 29   | FLOAT, DOUBLE                                                | Numpy.float64        |

Note: When uploading an array vector with the data type INT128, UUID or IP, it must be written using `MultithreadedTableWriter` and the session must not use pickle (```session(enablePickle=False)```)

## 7 Connection Pooling in Multi-Threaded Applications 

When calling method `session.run` in DolphinDB Python API, the scripts can only be executed serially. To execute the scripts concurrently, you can use `DBConnectionPool` which creates multiple threads (specified by the *threadNum* parameter) to execute the tasks. You can obtain the session ID of all the threads with `getSessionId()` of the `DBConnectionPool` object. Note that it may take a while before an inactive `DBConnectionPool` is closed automatically. You can explicitly close a `DBConnectionPool` by calling `shutDown()` to release the connection resources upon the completion of thread tasks. 

```Python
pool = ddb.DBConnectionPool(host, port, threadNum=10, userid=None, password=None, loadBalance=False, highAvailability=False, compress=False,reConnectFlag=False, python=False)
```

The `run` method in `DBConnectionPool` is wrapped in a coroutine for efficiency. The scripts are passed to the connection pool via the `run` method and executed by the thread. For example:

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

# define a task function and simulate the runtime with function sleep
async def test_run():
    try:
        return await pool.run("sleep(1000);1+2")
    except Exception as e:
        print(e)

# define the tasks
tasks = [
    asyncio.ensure_future(test_run()),
    asyncio.ensure_future(test_run()),
    asyncio.ensure_future(test_run()),
    asyncio.ensure_future(test_run()),
]

# create an event loop to run the tasks until all tasks are completed
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.wait(tasks))
except Exception as e:
    print("catch e:")
    print(e)

for i in tasks:
    print(i)

# shut down the pool
pool.shutDown() 
```

The above example shows how to run tasks with given scripts using the connection pool. There is only one main thread in Python and a coroutine is used to create subtasks and execute them in the pool. You can also define an object to pass in scripts. Please note that the tasks in DolphinDB are executed in a multi-threaded process. The following example defines a class that can pass in user-defined scripts and add subtasks dynamically.

```python
import dolphindb as ddb
import datetime
import time
import asyncio
import threading
import sys
import numpy
import pandas

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

#Define an event loop

def start_thread_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__=="__main__":
    start = time.time()
    print("In main thread",threading.current_thread())
    loop = asyncio.get_event_loop()   
    # create an event loop and run_forever in subthread
    
    t = threading.Thread(target= start_thread_loop, args=(loop,))
    t.start()
    task1 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(1000);1+1"),loop)
    task2 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(3000);1+2"),loop)
    task3 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(5000);1+3"),loop)
    task4 = asyncio.run_coroutine_threadsafe(Dolphindb.runTest("sleep(1000);1+4"),loop)

    print('the main thread is not blocked')
    end = time.time()
    print(end - start)
```

You can execute DolphinDB scripts with method `run` or `runTaskAsync` of class `DBConnectionPool`. The method `run` runs tasks synchronously (See [1.3 Execute DolphinDB Functions](#13-execute-dolphindb-functions)). The method `runTaskAsync` calls asynchronous tasks concurrently. You can add a task with `runTaskAsync` and it returns an object of concurrent.futures.Future. The result can be obtained by calling `result(timeout=None)` (*timeout* is in seconds) on the object. Specify a period of time for the parameter *timeout* in method `result()` to wait for the task to complete. If the task completes within the period, the method returns the result, otherwise a timeoutErr is thrown.

```python
import dolphindb as ddb
import time
pool = ddb.DBConnectionPool("localhost", 8848, 10)

t1 = time.time()
task1 = pool.runTaskAsync("sleep(1000); 1+0");
task2 = pool.runTaskAsync("sleep(2000); 1+1");
task3 = pool.runTaskAsync("sleep(4000); 1+2");
task4 = pool.runTaskAsync("sleep(1000); 1+3");
t2 = time.time()
print(task1.result())
t3 = time.time()
print(task2.result())
t4 = time.time()
print(task4.result())
t5 = time.time()
print(task3.result())
t6 = time.time()
print(t2-t1)
print(t3-t1)
print(t4-t1)
print(t5-t1)
print(t6-t1)
pool.shutDown()
```
You can also pass parameters synchronously to method `runTaskAsync`, see [Parameter Passing](#131-parameter-passing).


## 8 Database and Table Operations

### 8.1 Summary

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

After obtaining a table object in Python, you can call the following methods for table operations.

| **method**           | **details**                                  |
| :------------------- | :------------------------------------------- |
| append               | Append to a table                            |
| drop(colNameList)    | Delete columns of a table                    |
| executeAs(tableName) | Save result as an in-memory table with the specified name |
| execute()            | Execute script. Used with `update` or `delete`|
| toDF()               | Convert DolphinDB table object into pandas DataFrame |
| toList()             | Convert DolphinDB table object into list of numpy.ndarrary. The order of the objects in the list is consistent with the order of the table columns. |

Note: 

1. You can use the method `toList()` to convert a DolphinDB array vector to a 2d numpy.ndarray for optimal performance. This method only applies to an [array vector](https://dolphindb.com/help200/DataTypesandStructures/DataForms/Vector/arrayVector.html) with arrays of the same length, otherwise, an error will be raised.
2. When using `toList` to read array vectors with data type INT128, UUID or IP, do not use pickle in your session (```session(enablePickle=False)```).

The tables above only lists most commonly used methods. Please refer to files [session.py](sample.py) and [table.py]() on all the methods provided by the class `session` and `table`.

### 8.2 Database Operations

#### 8.2.1 Create Databases

Use function `database` to create a DFS database:

```python
import dolphindb.settings as keys
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
```

#### 8.2.2 Delete Databases

Use `dropDatabase` to delete a database:
```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
```

#### 8.2.3 Delete Database Partitions

If the partition name is a string (or displayed as a string), like the VALUE partitions "AMZN" and "NFLX" in this example, we must wrap it with an extra pair of quotation marks ("") in `dropPartition` via python API. For example, if the parameter of *partitions* in DolphinDB's `dropPartition` command is ["AMZN","NFLX"], then in Python API's `dropPartition` method the parameter *partitions* should be ["'AMZN'","'NFLX'"]. Similarly, in Python API for range partitions: partitionPaths=["'/0_50'","'/50_100'"]; for list partitions: partitionPaths=["'/List0'","'/List1'"], etc. 

```python
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
trade=s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
print(trade.rows)
# output
13136

s.dropPartition("dfs://valuedb", partitionPaths=["'AMZN'","'NFLX'"]) 
# or s.dropPartition("dfs://valuedb", partitionPaths=["`AMZN`NFLX"]), tableName="trade")
trade = s.loadTable(tableName="trade", dbPath="dfs://valuedb")
print(trade.rows)
# output
4516

print(trade.select("distinct TICKER").toDF())
# output
  distinct_TICKER
0            NVDA
```

### 8.3 Table Operations

#### 8.3.1 Load Table from Database

Please refer to [Chap 5. Load data from DolphinDB database](#5-load-data-from-dolphindb-database). 

#### 8.3.2 Append to Tables

Please refer to [section 6.1](#61-append-to-in-memory-tables) about how to append to in-memory tables. 

Please refer to [section 6.2](#62-append-to-dfs-tables) about how to append to DFS tables. 

#### 8.3.3 Update Tables

You can use `update` with `execute` to update an in-memory table or a DFS table.

```python
trade=s.loadText(WORK_DIR+"/example.csv")
trade = trade.update(["VOL"],["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
t1=trade.where("ticker=`AMZN").where("VOL=999999")
print(t1.toDF())

# output
  TICKER       date     VOL        PRC        BID        ASK
0   AMZN 2015-12-16  999999  675.77002  675.76001  675.83002
```

```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

dbPath="dfs://valuedb"
dstr = """
dbPath="dfs://valuedb"
if (existsDatabase(dbPath)){
    dropDatabase(dbPath)
}
mydb=database(dbPath, VALUE, ['AMZN','NFLX', 'NVDA'])
t=table(take(['AMZN','NFLX', 'NVDA'], 10) as sym, 1..10 as id, rand(10,10) as price)
mydb.createPartitionedTable(t,`pt,`sym).append!(t)

"""
t1=s.run(dstr)
t1=s.loadTable(tableName="pt",dbPath=dbPath)
t1.update(["price"],["11"]).where("sym=`AMZN").execute()

t1.toDF()
print(t1.toDF())

# output
    sym  id  price
0  AMZN   1     11
1  AMZN   4     11
2  AMZN   7     11
3  AMZN  10     11
4  NFLX   2      3
5  NFLX   5      5
6  NFLX   8      5
7  NVDA   3      1
8  NVDA   6      1
9  NVDA   9      5
```


#### 8.3.4 Delete Records

`delete` must be used with `execute`. 

```python
trade=s.loadText(WORK_DIR+"/example.csv")
trade.delete().where('date<2013.01.01').execute()
print(trade.rows)

# output
3024
```

```python
import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

dbPath="dfs://valuedb"
dstr = """
dbPath="dfs://valuedb"
if (existsDatabase(dbPath)){
    dropDatabase(dbPath)
}
mydb=database(dbPath, VALUE, ['AMZN','NFLX', 'NVDA'])
t=table(take(['AMZN','NFLX', 'NVDA'], 10) as sym, 1..10 as id, rand(10,10) as price)
mydb.createPartitionedTable(t,`pt,`sym).append!(t)

"""
t1=s.run(dstr)
t1=s.loadTable(tableName="pt",dbPath=dbPath)
t1.delete().where("sym=`AMZN").execute()

t1.toDF()
print(t1.toDF())

# output
    sym  id  price
0  NFLX   2      1
1  NFLX   5      1
2  NFLX   8      3
3  NVDA   3      5
4  NVDA   6      7
5  NVDA   9      2
```


#### 8.3.5 Delete Columns

```python
trade=s.loadText(WORK_DIR+"/example.csv")
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


#### 8.3.6 Delete Tables

```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
s.dropTable(dbPath="dfs://valuedb", tableName="trade")
```

The partitioned table Trade has been deleted, so the following script throws an exception:

```
s.loadTable(dbPath="dfs://valuedb", tableName="trade")

Exception:
getFileBlocksMeta on path '/valuedb/trade.tbl' failed, reason: path does not exist
```

## 9 SQL Queries

### 9.1 `select`

#### 9.1.1 List of Column Names as Input

```python
trade=s.loadText(WORK_DIR+"/example.csv")
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

#### 9.1.2 String as Input

```python
print(trade.select("ticker,date,bid,ask,prc,vol").where("date=2012.09.06").where("vol<10000000").toDF())

# output
  ticker       date        bid     ask     prc      vol
0   AMZN 2012-09-06  251.42999  251.56  251.38  5657816
1   NFLX 2012-09-06   56.65000   56.66   56.65  5368963
...
```

### 9.2 `exec`

The `select` clause always generates a table, even when only one column is selected. To generate a scalar or vector, you can use `exec` clause.

If only one column is selected, `exec` generates a DolphinDB vector. Download the object with `toDF()` in Python and you can obtain an array object.

```sql
trade = s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
print(trade.exec('ticker').toDF())

# output
['AMZN' 'AMZN' 'AMZN' ... 'NVDA' 'NVDA' 'NVDA']
```

If multiple columns are selected, same as `select`, the `exec` clause generates a DolphinDB table. Download the object with `toDF()` in Python and you can obtain a DataFrame object.

```sql
trade = s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
print(trade.exec(['ticker','date','bid','ask','prc','vol']).toDF())

# output
      ticker       date      bid      ask      prc       vol
0       AMZN 1997-05-15   23.500   23.625   23.500   6029815
1       AMZN 1997-05-16   20.500   21.000   20.750   1232226
2       AMZN 1997-05-19   20.500   20.625   20.500    512070
3       AMZN 1997-05-20   19.625   19.750   19.625    456357
4       AMZN 1997-05-21   17.125   17.250   17.125   1577414
...      ...        ...      ...      ...      ...       ...
13131   NVDA 2016-12-23  109.770  109.790  109.780  16193331
13132   NVDA 2016-12-27  117.310  117.320  117.320  29857132
13133   NVDA 2016-12-28  109.250  109.290  109.250  57384116
13134   NVDA 2016-12-29  111.260  111.420  111.430  54384676
13135   NVDA 2016-12-30  106.730  106.750  106.740  30323259

[13136 rows x 6 columns]
```

### 9.3 `top` & `limit`

`top` is used to get a top N number of records in a table.

```python
trade=s.loadText(WORK_DIR+"/example.csv")
trade.top(5).toDF()

# output
      TICKER        date       VOL        PRC        BID       ASK
0       AMZN  1997.05.16   6029815   23.50000   23.50000   23.6250
1       AMZN  1997.05.17   1232226   20.75000   20.50000   21.0000
2       AMZN  1997.05.20    512070   20.50000   20.50000   20.6250
3       AMZN  1997.05.21    456357   19.62500   19.62500   19.7500
4       AMZN  1997.05.22   1577414   17.12500   17.12500   17.2500
```

The `limit` clause is similar to the `top` clause with the following differences:

* The `top` clause cannot use negative integers. When used with the `context by` clause, the `limit` clause can use a negative integer to select a limited number of records from the end of each group. In all other cases, the `limit` clause can only use non-negative integers.

* The `limit` clause can select a limited number of rows starting from a specified row.

```python
tb = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = tb.select("*").contextby('ticker').limit(-2)

# output
  TICKER       date       VOL        PRC        BID        ASK
0   AMZN 2016-12-29   3158299  765.15002  764.66998  765.15997
1   AMZN 2016-12-30   4139451  749.87000  750.02002  750.40002
2   NFLX 2016-12-29   3444729  125.33000  125.31000  125.33000
3   NFLX 2016-12-30   4455012  123.80000  123.80000  123.83000
4   NVDA 2016-12-29  54384676  111.43000  111.26000  111.42000
5   NVDA 2016-12-30  30323259  106.74000  106.73000  106.75000
```

```python
tb = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = tb.select("*").limit([2, 5])
print(t1.toDF())

# output
  TICKER       date      VOL     PRC     BID     ASK
0   AMZN 1997-05-19   512070  20.500  20.500  20.625
1   AMZN 1997-05-20   456357  19.625  19.625  19.750
2   AMZN 1997-05-21  1577414  17.125  17.125  17.250
3   AMZN 1997-05-22   983855  16.750  16.625  16.750
4   AMZN 1997-05-23  1330026  18.000  18.000  18.125
```


### 9.4 `where`

The `where` clause is used to extract only the records that satisfy the specified condition or conditions.

#### 9.4.1 Multiple `where` Conditions

```python
trade=s.loadText(WORK_DIR+"/example.csv")

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

You can use the `showSQL` method to display the SQL statement.

```python
print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').showSQL())

# output
select date,bid,ask,prc,vol from Tff260d29 where TICKER=`AMZN and bid!=NULL and ask!=NULL and vol>10000000 order by vol desc
```

#### 9.4.2 String as Input

We can pass a list of column names as a string to `select` method and a list of conditions as a string to `where` method.

```python
trade=s.loadText(WORK_DIR+"/example.csv")
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

### 9.5 `groupby`

Method `groupby` must be followed by an aggregate function such as `count`, `sum`, `avg`, `std`, etc.

Create a database

```
import dolphindb.settings as keys
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")

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

### 9.6 `contextby`

`contextby` is similar to `groupby` except that for each group, `groupby` returns a scalar whereas `contextby` returns a vector of the same size as the number of rows in the group.

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

The `csort` keyword can be used to sort the data in each group after `contextby` and before the `select` clause is executed.

```python
df = s.loadTable(dbPath="dfs://valuedb", tableName="trade").contextby('ticker').csort('date desc').toDF()
print(df)

# output
      TICKER       date      VOL        PRC        BID        ASK
0       AMZN 2016-12-30  4139451  749.87000  750.02002  750.40002
1       AMZN 2016-12-29  3158299  765.15002  764.66998  765.15997
2       AMZN 2016-12-28  3301025  772.13000  771.92999  772.15997
3       AMZN 2016-12-27  2638725  771.40002  771.40002  771.76001
4       AMZN 2016-12-23  1981616  760.59003  760.33002  760.59003
...      ...        ...      ...        ...        ...        ...
13131   NVDA 1999-01-28   476094   19.93750   19.87500   20.00000
13132   NVDA 1999-01-27   510637   20.00000   19.87500   20.00000
13133   NVDA 1999-01-26   719199   20.06250   20.06250   20.12500
13134   NVDA 1999-01-25  1074571   21.75000   21.75000   21.87500
13135   NVDA 1999-01-22  5702636   19.68750   19.62500   19.68750
```

In addition to specifying the keywords *asc* and *desc* in the sort functions `sort` and `csort` to indicate the sort order, you can also do it by passing parameters.


```python
sort(by, ascending=True)
csort(by, ascending=True)
```

The parameter *ascending* indicates whether to sort the data in an ascending order or not. The default value is True. You can specify different sorting methods for multiple columns by passing a list to *ascending*.

```python
tb = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = tb.select("*").contextby('ticker').csort(["TICKER", "VOL"], True).limit(5)

# output
   TICKER       date    VOL      PRC      BID     ASK
0    AMZN 1997-12-26  40721  54.2500  53.8750  54.625
1    AMZN 1997-08-12  47939  26.3750  26.3750  26.750
2    AMZN 1997-07-21  48325  26.1875  26.1250  26.250
3    AMZN 1997-08-13  49690  26.3750  26.0000  26.625
4    AMZN 1997-06-02  49764  18.1250  18.1250  18.375
5    NFLX 2002-09-05  20725  12.8500  12.8500  12.950
6    NFLX 2002-11-11  26824   8.4100   8.3000   8.400
7    NFLX 2002-09-04  27319  13.0000  12.8200  13.000
8    NFLX 2002-06-10  35421  16.1910  16.1900  16.300
9    NFLX 2002-09-06  54951  12.8000  12.7900  12.800
10   NVDA 1999-05-10  41250  17.5000  17.5000  17.750
11   NVDA 1999-05-07  52310  17.5000  17.3750  17.625
12   NVDA 1999-05-14  59807  18.0000  17.7500  18.000
13   NVDA 1999-04-01  63997  20.5000  20.1875  20.500
14   NVDA 1999-04-19  65940  19.0000  19.0000  19.125

tb = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = tb.select("*").contextby('ticker').csort(["TICKER", "VOL"], [True, False]).limit(5)

# output
   TICKER       date        VOL       PRC     BID       ASK
0    AMZN 2007-04-25  104463043   56.8100   56.80   56.8100
1    AMZN 1999-09-29   80380734   80.7500   80.75   80.8125
2    AMZN 2006-07-26   76996899   26.2600   26.17   26.1800
3    AMZN 2007-04-26   62451660   62.7810   62.77   62.8300
4    AMZN 2005-02-03   60580703   35.7500   35.74   35.7300
5    NFLX 2015-07-16   63461015  115.8100  115.85  115.8600
6    NFLX 2015-08-24   59952448   96.8800   96.85   96.8800
7    NFLX 2016-04-19   55728765   94.3400   94.30   94.3100
8    NFLX 2016-07-19   55685209   85.8400   85.81   85.8300
9    NFLX 2016-01-20   53009419  107.7400  107.73  107.7800
10   NVDA 2011-01-06   87693472   19.3300   19.33   19.3400
11   NVDA 2011-02-17   87117555   25.6800   25.68   25.7000
12   NVDA 2011-01-12   86197484   23.3525   23.34   23.3600
13   NVDA 2011-08-12   80488616   12.8800   12.86   12.8700
14   NVDA 2003-05-09   77604776   21.3700   21.39   21.3700
```

### 9.7 `pivotby`

`pivot by` is a unique feature in DolphinDB and an extension to the standard SQL. It rearranges a column (or multiple columns) of a table (with or without a data transformation function) on two dimensions. 

When used with `select`, `pivotby` returns a table.

```python
df = s.loadTable(tableName="trade", dbPath="dfs://valuedb")
print(df.select("VOL").pivotby("TICKER", "date").toDF())

# output
  TICKER  1997.05.15  1997.05.16  ...  2016.12.28  2016.12.29  2016.12.30
0   AMZN   6029815.0   1232226.0  ...     3301025     3158299     4139451
1   NFLX         NaN         NaN  ...     4388956     3444729     4455012
2   NVDA         NaN         NaN  ...    57384116    54384676    30323259
```

When used with `select`, `pivotby` returns a DolphinDB matrix.


```python
df = s.loadTable(tableName="trade", dbPath="dfs://valuedb")
print(df.exec("VOL").pivotby("TICKER", "date").toDF())

# output
[array([[ 6029815.,  1232226.,   512070., ...,  3301025.,  3158299.,
         4139451.],
       [      nan,       nan,       nan, ...,  4388956.,  3444729.,
         4455012.],
       [      nan,       nan,       nan, ..., 57384116., 54384676.,
        30323259.]]), array(['AMZN', 'NFLX', 'NVDA'], dtype=object), array(['1997-05-15T00:00:00.000000000', '1997-05-16T00:00:00.000000000',
       '1997-05-19T00:00:00.000000000', ...,
       '2016-12-28T00:00:00.000000000', '2016-12-29T00:00:00.000000000',
       '2016-12-30T00:00:00.000000000'], dtype='datetime64[ns]')]

```


### 9.8 Table Join

DolphinDB table class has method `merge` for inner, left, left semi, and outer join; method `merge_asof` for asof join; method `merge_window` for window join.

#### 9.8.1 `merge`

Specify join columns with parameter *on* if join column names are identical in both tables; use parameters *left_on* and *right_on* when join column names are different. The optional parameter *how* indicates the table join type. The default table join mode is inner join. 

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

We need to specify parameters *left_on* and *right_on* when the join column names are different. 

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

To conduct left join, set *how* = "left". 

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

To conduct outer join, set *how* = "outer". A partitioned table can only be outer joined with a partitioned table, and an in-memory table can only be outer joined with an in-memory table.

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

#### 9.8.2 `merge_asof`

The `merge_asof` method is a type of non-synchronous join. It is similar to the `left join` function with the following differences:

- The data type of the last matching column is usually temporal. For a row in the left table with time t, if there is not a match of left join in the right table, the row in the right table that corresponds to the most recent time before time t is taken, if all the other matching columns are matched; if there are more than one matching record in the right table, the last record is taken. 
- If there is only 1 join column, the asof join function assumes the right table is sorted on the join column. If there are multiple join columns, the asof join function assumes the right table is sorted on the last join column within each group defined by the other join columns. The right table does not need to be sorted by the other join columns. If these conditions are not met, we may see unexpected results. The left table does not need to be sorted. 

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

#### 9.8.3 `merge_window`

`merge_window` (`window join`) is a generalization of asof join. With a window defined by parameters *leftBound* (w1) and *rightBound* (w2), for each row in the left table with the value of the last join column equal to t, find the rows in the right table with the value of the last join column between (t+w1) and (t+w2) conditional on all other join columns are matched, then apply *aggFunctions* to the selected rows in the right table. 

The only difference between `window join` and `prevailing window join` is that if the right table doesn't contain a matching value for t+w1 (the left boundary of the window), prevailing window join will fill it with the last value before t+w1 (conditional on all other join columns are matched), and apply *aggFunctions*. To use `prevailing window join`, set *prevailing* = True. 

```python
print(trades.merge_window(quotes, -5000000000, 0, aggFunctions=["avg(Bid_Price)","avg(Offer_Price)"], on=["Symbol","Time"]).where("Time>=07:59:59").top(10).toDF())

# output
 Time                          Exchange Symbol  Trade_Volume  Trade_Price  avg_Bid_Price  avg_Offer_Price
0 1970-01-01 08:00:00.022239        75   AAPL           300        27.00          26.90            27.49
1 1970-01-01 08:00:00.022287        75   AAPL           500        27.25          26.90            27.49
2 1970-01-01 08:00:00.022317        75   AAPL           335        27.26          26.90            27.49
3 1970-01-01 08:00:00.022341        75   AAPL           100        27.27          26.90            27.49
4 1970-01-01 08:00:00.022368        75   AAPL            31        27.40          26.90            27.49
5 1970-01-01 08:00:02.668076        68   AAPL          2434        27.42          26.75            27.36
6 1970-01-01 08:02:20.116025        68   AAPL            66        27.00            NaN              NaN
7 1970-01-01 08:06:31.149930        75   AAPL           100        27.25            NaN              NaN
8 1970-01-01 08:06:32.826399        75   AAPL           100        27.25            NaN              NaN
9 1970-01-01 08:06:33.168833        75   AAPL            74        27.25            NaN              NaN

[10 rows x 6 columns]
```

To calculate trading cost with window join:

```python
tb = trades.merge_window(quotes,-1000000000, 0, aggFunctions="[wavg(Offer_Price, Offer_Size) as Offer_Price, wavg(Bid_Price, Bid_Size) as Bid_Price]",
                         on=["Symbol","Time"], prevailing=True)\
                         .select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2))/sum(Trade_Volume*Trade_Price)*10000 as cost")\
                         .groupby("Symbol")
print(tb.toDF())

# output
  Symbol       cost
0   AAPL   6.367864
1     FB  35.751041
```

### 9.9 `executeAs`

Function `executeAs` saves query result as a DolphinDB table. The table name is specified by parameter *newTableName*.

**Note**: A table variable must be created in Python to refer to the table object created by `executeAs`, otherwise the table will be released. See [Section 2.3](#23-life-cycle-of-uploaded-tables) 


```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")

t = trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').executeAs("AMZN")

print(s.run('AMZN'))
```

### 9.10 Regression

Function `ols` conducts an OLS regression and returns a dictionary. 

Please download the file [US.csv](data/US.csv) and decompress it under the directory of  WORK_DIR.

```python
import dolphindb.settings as keys
if s.existsDatabase("dfs://US"):
	s.dropDatabase("dfs://US")
s.database(dbName='USdb', partitionType=keys.VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="dfs://US")
US=s.loadTextEx(dbPath="dfs://US", partitionColumns=["TICKER"], tableName='US', remoteFilePath=WORK_DIR + "/US.csv")

result = s.loadTable(tableName="US",dbPath="dfs://US")\
         .select("select VOL\\SHROUT as turnover, abs(RET) as absRet, (ASK-BID)/(BID+ASK)*2 as spread, log(SHROUT*(BID+ASK)/2) as logMV")\
         .where("VOL>0").ols("turnover", ["absRet","logMV", "spread"], True)

print(result)

#output
{'Coefficient':       factor       beta  stdError      tstat        pvalue
0  intercept -14.107396  1.044444 -13.507088  0.000000e+00
1     absRet  75.524082  3.113285  24.258651  0.000000e+00
2      logMV   1.473178  0.097839  15.057195  0.000000e+00
3     spread -15.644738  1.888641  -8.283596  2.220446e-16, 'ANOVA':     Breakdown    DF             SS           MS           F  Significance
0  Regression     3   15775.710620  5258.570207  258.814918           0.0
1    Residual  5462  110976.255372    20.317879         NaN           NaN
2       Total  5465  126751.965992          NaN         NaN           NaN, 'RegressionStat':            item   statistics
0            R2     0.124461
1    AdjustedR2     0.123980
2      StdError     4.507536
3  Observations  5466.000000}
```
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


## 10 Python Streaming API

This section introduces the methods for streaming subscription in DolphinDB Python API.

### 10.1 `enableStreaming` 

To enable streaming subscription, call method `enableStreaming` in DolphinDB Python API.

```
s.enableStreaming(port=0)
```

**Parameter:**

- **port:** the unique port for each session that specifies the subscription port to ingest data. Specify the subscription port on the client to subscribe to the data sent from the server.

the port for data subscription. It is used to subscribe to data sent from the DolphinDB server.

**Note:**

- DolphinDB server versions prior to 1.30.21/2.00.9 require the publisher to re-initiate a TCP connection for data transfer after the subscription request is submitted by the subscriber.
- DolphinDB server version 1.30.21/2.00.9 and later support the publisher to push data through the requested connection on the subscriber side. Therefore, the subscriber does not need to specify a port (default value 0). If the parameter is specified, it will be ignored by the API.

| DolphinDB Server        | Python API              | Port Number  |
| ----------------------- | ----------------------- | ------------ |
| Before 1.30.21/2.00.9   | Before 1.30.21/2.00.9   | Required     |
| 1.30.21/2.00.9 or later | 1.30.21/2.00.9 or later | Not Required |

*Compatibility note: Before upgrading the server or the API to version 1.30.21/2.00.9 or later, cancel your current subscriptions. Re-establish the subscriptions after the upgrade is complete.* 

```
import dolphindb as ddb
import numpy as np
s = ddb.session()
# before 1.30.21/2.00.9, port number must be specified
s.enableStreaming(8000)   
# 1.30.21/2.00.9 or later, port number is not required
s.enableStreaming() 
```


### 10.2 Subscribe and Unsubscribe

#### 10.2.1 `subscribe`

Use function `subscribe` to subscribe to a DolphinDB stream table.

**Syntax**

```python
s.subscribe(host, port, handler, tableName, actionName=None, offset=-1, resub=False, filter=None, msgAsTable=False, [batchSize=0], [throttle=1], [userName=None],[password=None], [streamDeserializer=None])
```

**Parameters:**

- **host:** the IP address of the publisher node.
- **port:** the port number of the publisher node.
- **handler:** a user-defined function to process the subscribed data.
- **tableName:** a string indicating the name of the publishing stream table.
- **actionName:** a string indicating the name of the subscription task.
- **offset:** an integer indicating the position of the first message where the subscription begins. A message is a row of the stream table. If *offset* is unspecified, negative or exceeding the number of rows in the stream table, the subscription starts with the next new message. *offset* is relative to the first row of the stream table when it is created. If some rows were cleared from memory due to cache size limit, they are still considered in determining where the subscription starts.
- **resub:** a Boolean value indicating whether to resubscribe after network disconnection.
- **filter:** a vector indicating the filtering conditions. Only the rows with values of the filtering column in the vector specified by the parameter *filter* are published to the subscriber.
- **msgAsTable:** a Boolean value. If *msgAsTable* = true, the subscribed data is ingested into *handler* as a DataFrame. The default value is false, which means the subscribed data is ingested into *handler* as a List of nparrays. This optional parameter has no effect if *batchSize* is not specified. If *streamDeserializer* is specified,  this parameter must be set to False. 
- **batchSize:** an integer indicating the number of unprocessed messages to trigger the *handler*. 
    - If *batchSize* is positive: 
        - and *msgAsTable* = false:  The handler does not process messages until the number of unprocessed messages reaches batchSize. The handler processes *batchSize* messages at a time.
        - and *msgAsTable* = true:  the messages will be processed by block ([maxMsgNumPerBlock](https://dolphindb.com/help/DatabaseandDistributedComputing/Configuration/StandaloneMode.html) = 1024, by default). For example, there are a total of 1524 messages from the publisher side. By default, the messages will be sent in two blocks, the first contains 1024 messages and the second contains 500. Suppose *batchSize* is set to 1500, when the first batch arrives, the 1024 messages will not be processed as they haven't reached the batchSize. When the second block arrives, the handler processes the 2 blocks (totaling 1524 records) all at once.
    - If it is unspecified or non-positive, the *handler* processes incoming messages one by one as soon as they come in.
- **throttle:** a floating point number indicating the maximum waiting time (in seconds) before the *handler* processes the incoming messages. The default value is 1. This optional parameter has no effect if *batchSize* is not specified.
- **userName:** a string indicating the username used to connect to the server
- **password:** a string indicating the password used to connect to the server
- **streamDeserializer:** the deserializer for the subscribed heterogeneous stream table

Please specify the configuration parameter *maxPubConnections* for the publisher node first. See [Streaming Tutorial](https://github.com/dolphindb/Tutorials_EN/blob/master/streaming_tutorial.md#2-core-functionalities)

**Examples:**

(1) Share a stream table in DolphinDB and specify the filtering column as "sym". 
Then insert 2 records to each of the 5 symbols:

```
share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades
setStreamTableFilterColumn(trades, `sym)
insert into trades values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0, 1..10)
```

(2) Subscribe to table trades in Python and obtain the records with symbol 000905.

```python
from threading import Event 

import dolphindb as ddb
import numpy as np
s = ddb.session()
s.enableStreaming(10020)


def handler(lst):
    print(lst)

s.subscribe("192.168.1.92",8848,handler,"trades","action",0,False,np.array(['000905']),)


Event().wait() 

# output
[numpy.datetime64('2020-10-29T10:23:31.411'), '000905', 94.3, 1]
[numpy.datetime64('2020-10-29T10:23:31.411'), '000905', 35.0, 6]
```

Please keep the main thread from exiting because the subscription task is executed asynchronously. Otherwise, the subscription thread will terminate immediately after the main thread exits, leading to no subscription messages received. For example:

```python
from threading import Event     # Write in the first line of the script
Event().wait()                  # Write in the last line of the script
```

#### 10.2.2 Get Subscription Topics

You can obtain all the subscription topics with function `getSubscriptionTopics`. A subscription topic is in the format of "host/port/tableName/actionName". Topics are different across sessions.

```python
s.getSubscriptionTopics()
# output
['192.168.1.103/8921/trades/action']
```

#### 10.2.3 `unsubscribe`

Unsubscribe to tables with `unsubscribe`.

**Syntax:**

```python
s.unsubscribe(host,port,tableName,actionName=None)
```

The following code unsubscribes to the table trades in the above example:

```python
s.unsubscribe("192.168.1.103", 8921,"trades","action")
```

#### 10.2.4 Streaming Subscription Cases

The following example calculates OHLC bars with streaming subscription:


The process of calculating real-time K-lines in DolphinDB database is shown in the following diagram:

avatar(images/K-line.png)

Data vendors usually provide subscription services based on APIs in Python, Java or other languages. In this example, trading data is written into a stream table through DolphinDB Python API. DolphinDB's time-series engine conducts real-time OHLC calculations at specified frequencies in moving windows.

This example uses the file [trades.csv](data/k_line/trades.csv) to simulate real-time data. The following table shows its column names and one row of sample data:

| Symbol | Datetime            | Price | Volume |
| ------ | ------------------- | ----- | ------ |
| 000001 | 2018.09.03T09:30:06 | 10.13 | 4500   |


The output table for calculation results contains the following 7 columns:

| datetime            | symbol | open  | close | high  | low   | volume |      |
| ------------------- | ------ | ----- | ----- | ----- | ----- | ------ | ---- |
| 2018.09.03T09:30:07 | 000001 | 10.13 | 10.13 | 10.12 | 10.12 | 468060 |      |

### 10.3   Subscribe to Heterogeneous Stream Table

Since DolphinDB server version 1.30.17/2.00.5, the [replay](https://dolphindb.com/help/FunctionsandCommands/FunctionReferences/r/replay.html) function supports replaying (serializing) multiple stream tables with different schemata into a single stream table (known as “heterogeneous stream table“). Starting from DolphinDB Python API version 1.30.19, a new class `streamDeserializer` has been introduced for the subscription and deserialization of heterogeneous stream table.

#### 10.3.1 Construct Deserializer for Heterogeneous Stream Table

  Construct a deserializer for heterogeneous table with `streamDeserializer` .

```
1sd = streamDeserializer(sym2table, session=None)
```

- **sym2table**: a dictionary object. Each key indicates the name of an input table of `replay`, and the corresponding value indicates the same schema as this input table. `streamDeserializer` will deserialize the ingested data based on the schemata specified in *sym2table*.
- **session**: session object connected to the DolphinDB server. The default value is None, indicating the current session.

The deserialized data is returned in the form of a list. 

<!--For more information about how to construct heterogeneous stream table, see .-->

#### 10.3.2 Subscribe to a Heterogeneous Stream Table

Here’s an example:

(1) Create a heterogeneous stream table

```
try{dropStreamTable(`outTables)}catch(ex){}
// Create a heterogeneous table
share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables
n = 10;
dbName = 'dfs://test_StreamDeserializer_pair'
if(existsDatabase(dbName)){
    dropDB(dbName)}
// Create database and tables
db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
table3 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
tableInsert(table3, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
// Create three types of tables (partitioned table, stream table and in-memory table) 
pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
share streamTable(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]).append!(table2) as pt2
share table3 as pt3
// Create the heterogeneous stream table
d = dict(['msg1', 'msg2', 'msg3'], [table1, table2, table3])
replay(inputTables=d, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)
```

(2) Subscribe to the heterogeneous stream table in Python.

```
from threading import Event

def streamDeserializer_handler(lst): # the last element of the list returned by the deserializer is a key specified in sym2table
    if lst[-1]=="msg1":
        print("Msg1: ", lst)
    elif lst[-1]=='msg2':
        print("Msg2: ", lst)
    else:
        print("Msg3: ", lst)

s = ddb.session("192.168.1.103", 8921, "admin", "123456")
s.enableStreaming(10020)

# Construct the deserializer
sd = ddb.streamDeserializer({
    'msg1': ["dfs://test_StreamDeserializer_pair", "pt1"],	# specify a list of the DFS database path and table name
    'msg2': "pt2",		 # Specify the stream table name
    'msg3': "pt3",		 # Specify the in-memory table name
}, session=s)			 # If session is not specified, get the current session during subscription
s.subscribe(host="192.168.1.103", port=8921, handler=streamDeserializer_handler, tableName="outTables", actionName="action", offset=0, resub=False,
            msgAsTable=False, streamDeserializer=sd, userName="admin", password="123456")
Event().wait()
```

### 10.4 Streaming Applications

This section describes the 3 steps to conduct real-time OHLC calculations.

#### 10.4.1 Receive Real-Time Data and Write to DolphinDB Stream Table

* Create a DolphinDB stream table

```
share streamTable(100:0, `Symbol`Datetime`Price`Volume,[SYMBOL,DATETIME,DOUBLE,INT]) as Trade

```

* Insert simulated data of trades.csv from Python to DolphinDB.

As the unit of column 'Datetime' is second and DataFrame in pandas can only use DateTime[64] which corresponds to data type NANOTIMESTAMP in DolphinDB, we need to convert the data type of column 'Datetime' before inserting the data to the stream table.


```python
import dolphindb as ddb
import pandas as pd
import numpy as np
csv_file = "trades.csv"
csv_data = pd.read_csv(csv_file,parse_dates=['Datetime'], dtype={'Symbol':str})
csv_df = pd.DataFrame(csv_data)
s = ddb.session();
s.connect("192.168.1.103", 8921,"admin","123456")

# Upload DataFrame to DolphinDB and convert the data type of column 'Datetime'

s.upload({"tmpData":csv_df})
s.run("data = select Symbol, datetime(Datetime) as Datetime, Price, Volume from tmpData;tableInsert(Trade,data)")
```

Please note that the methods `s.upload` and `s.run` transfer data twice and there may be network delays. It's recommended to filter the data in Python first, and then call `tableInsert` to insert the data.

```
csv_df=csv_df['Symbol', 'Datetime', 'Price', 'Volume']
s.run("tableInsert{Trade}", csv_df)
```

#### 10.4.2 Calculate OHLC Bars in Real-Time

The following case uses DolphinDB time-series engine to calculate OHLC bars in real-time. The result is output to the stream table OHLC.

OHLC bars can be calculated in moving windows in real time in DolphinDB. Generally there are the following 2 scenarios: Calculations are conducted in non-overlapping windows, for example, calculate OHLC bars for the previous 5 minutes every 5 minutes; Or in partially overlapping windows, for example, calculate OHLC bars for the previous 5 minutes every 1 minute.

You can specify the parameters *windowSize* and *step* of function `createTimeSeriesEngine` for the above 2 scenarios. For non-overlapping windows, set the same value for parameter *windowSize* and *step*; For partially overlapping windows, *windowSize* is a multiple of *step*.

Create an output table:

```
share streamTable(100:0, `datetime`symbol`open`high`low`close`volume,[DATETIME, SYMBOL, DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG]) as OHLC
```

Define the time-series engine:

- Non-overlapping windows:

```
tsAggrKline = createTimeSeriesAggregator(name="aggr_kline", windowSize=300, step=300, metrics=<[first(Price),max(Price),min(Price),last(Price),sum(volume)]>, dummyTable=Trade, outputTable=OHLC, timeColumn=`Datetime, keyColumn=`Symbol)
```

- Overlapping windows:

```
tsAggrKline = createTimeSeriesAggregator(name="aggr_kline", windowSize=300, step=60, metrics=<[first(Price),max(Price),min(Price),last(Price),sum(volume)]>, dummyTable=Trade, outputTable=OHLC, timeColumn=`Datetime, keyColumn=`Symbol)
```

Last, subscribe to the table. If data has already been written to the stream table Trade at this time, it will be immediately subscribed and ingested to the streaming engine:

```
subscribeTable(tableName="Trade", actionName="act_tsaggr", offset=0, handler=append!{tsAggrKline}, msgAsTable=true)
```

#### 10.4.3 Display OHLC Bars in Python

In this example, the output table of the time-series engine is also defined as a stream table. The client can subscribe to the output table through Python API and display the calculation results to Python.

The following script uses Python API to subscribe to the output table OHLC of the real-time calculation, and print the result.

```python
from threading import Event
import dolphindb as ddb
import pandas as pd
import numpy as np
s=ddb.session()
# set local port 20001 for subscribed streaming data
s.enableStreaming(20001)
def handler(lst):
    print(lst)
# subscribe to the stream table OHLC (local port 8848)
s.subscribe("192.168.1.103", 8921, handler, "OHLC")
Event().wait()

# output
[numpy.datetime64('2018-09-03T09:31:00'), '000001', 10.13, 10.15, 10.1, 10.14, 586160]
[numpy.datetime64('2018-09-03T09:32:00'), '000001', 10.13, 10.16, 10.1, 10.15, 1217060]
[numpy.datetime64('2018-09-03T09:33:00'), '000001', 10.13, 10.16, 10.1, 10.13, 1715460]
[numpy.datetime64('2018-09-03T09:34:00'), '000001', 10.13, 10.16, 10.1, 10.14, 2268260]
[numpy.datetime64('2018-09-03T09:35:00'), '000001', 10.13, 10.21, 10.1, 10.2, 3783660]
...
```

You can also connect to DolphinDB database through a visualization system such as [Grafana](https://github.com/dolphindb/grafana-datasource/blob/master/README.md) to query the output table and display the results.

## 11 More Examples

### 11.1 Stock Momentum Strategy

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
US=s.loadTextEx(dbPath="dfs://US", partitionColumns=["TICKER"], tableName='US', remoteFilePath=WORK_DIR + "/US.csv")
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
        ports.contextby("date").update(cols=["wt"], vals=["-MV/sum(MV)/"+holdingDays]).where("rank=0").execute()
        ports.contextby("date").update(cols=["wt"], vals=["MV/sum(MV)/"+holdingDays]).where("rank="+groups+"-1").execute()
    else:
        raise Exception("Invalid WtScheme. valid values:1 or 2")
    return ports.select("PERMNO, date as tranche, wt").where("wt!=0.0").sort(bys=["PERMNO","date"]).executeAs("ports")

tradables=genTradeTables(priceData.tableName())
startDate="2016.01.01"
endDate="2017.01.01"
holdingDays=5
groups=10
ports=formPortfolio(startDate=startDate,endDate=endDate,tradables=tradables,holdingDays=holdingDays,groups=groups,WtScheme=2)
dailyRtn=priceData.select("date, PERMNO, RET as dailyRet").where("date between "+startDate+":"+endDate).executeAs("dailyRtn")
```

**Step 3:** Calculate the profit/loss for each stock in the portfolio in each of the days in the holding period. Close the positions at the end of the holding period.

```python
def calcStockPnL(ports,inData, dailyRtn, holdingDays, endDate):
    s.table(data={'age': list(range(1,holdingDays+1))}).executeAs("ages")
    ports.select("tranche").sort("tranche").executeAs("dates")
    s.run("dates = sort distinct dates.tranche")
    s.run("dictDateIndex=dict(dates,1..dates.size())")
    s.run("dictIndexDate=dict(1..dates.size(), dates)")
    inData.select("max(date) as date").groupby("PERMNO").executeAs("lastDaysTable")
    s.run("lastDays=dict(lastDaysTable.PERMNO,lastDaysTable.date)")
    ports.merge_cross(s.table(data="ages")).select("dictIndexDate[dictDateIndex[tranche]+age] as date, PERMNO, tranche, age, take(0.0,age.size()) as ret, wt as expr, take(0.0,age.size()) as pnl").where("isValid(dictIndexDate[dictDateIndex[tranche]+age]), dictIndexDate[dictDateIndex[tranche]+age]<=min(lastDays[PERMNO],"+endDate+")").executeAs("pos")
    t1= s.loadTable("pos")
    # t1.merge(dailyRtn, on=["date","PERMNO"], merge_for_update=True).update(["ret"],["dailyRet"]).execute()
    t1.merge(dailyRtn, on=["date","PERMNO"]).update(["ret"],["dailyRet"]).execute()

    t1.contextby(["PERMNO","tranche"]).update(["expr"], ["expr*cumprod(1+ret)"]).execute()
    t1.update(["pnl"],["expr*ret/(1+ret)"]).execute()
    return t1


stockPnL = calcStockPnL(ports=ports,inData=priceData, dailyRtn=dailyRtn, holdingDays=holdingDays, endDate=endDate)
```

**Step 4:** Calculate portfolio profit/loss

```python
portPnl = stockPnL.select("sum(pnl)").groupby("date").sort(bys=["date"]).executeAs("portPnl")
print(portPnl.toDF())
```

### 11.2 Time-Series Operations

The example below shows how to calculate factor No. 98 in "101 Formulaic Alphas" by Kakushadze (2015) with daily data of US stocks.

```python
def alpha98(t):
    t1 = s.table(data=t)
    t1.contextby(["date"]).update(cols=["rank_open","rank_adv15"], vals=["rank(open)","rank(adv15)"]).execute()
    t1.contextby(["PERMNO"]).update(["decay7", "decay8"], ["mavg(mcorr(vwap, msum(adv5, 26), 5), 1..7)",\
               "mavg(mrank(9 - mimin(mcorr(rank_open, rank_adv15, 21), 9), true, 7), 1..8)"]).execute()
    # from previous update the server's schema is changed, so you may reload it
    t1 = s.table(data=t)
    return t1.select("PERMNO, date,decay7, decay8, rank(decay7)-rank(decay8) as A98")\
        .contextby(["date"])\
        .executeAs("alpha98")

US = s.loadTable(tableName="US", dbPath="dfs://US")\
    .select("PERMNO, date, PRC as vwap, PRC+rand(1.0, PRC.size()) as open, mavg(VOL, 5) as adv5\
            , mavg(VOL,15) as adv15")\
    .where("2007.01.01<=date<=2016.12.31")\
    .contextby("PERMNO")\
    .executeAs("US")
result=alpha98(US.tableName()).where('date>2007.03.12').executeAs("result")
print(result.toDF())
s.close()

```

## 12 FAQ

For DolphinDB server version before 1.30.3, the following errors may be raised:

```
<Server Exception> in run: Received invalid serialized data during deserialization!
```
```
<Server Exception> in run: Failed to read response header from the socket with IO error type
```
```
<Server Exception> in run: Error when Unpickle socket data!
```

Solution: Please upgrade your server version to 1.30.3 and above.

## 13 Null Values Handling

*None*, *pd.NaT* and *np.nan* can all represent null values in Python. When a list or a NumPy.array containing null values is uploaded to the DolphinDB server, versions prior to 1.30.19.1 of the Python API determine the column type with numpy.dtype, which may cause errors when dtype is “object“. Therefore, since the 1.30.19.1 version, when uploading a list or a NumPy.array containing null values, Python API determines the column type with the following rules:

| **Types of Null Values in the Array / List**     | **Python Data Type** | **DolphinDB Column Type**            |
| ------------------------------------------------ | -------------------- | ------------------------------------ |
| *None*                                           | object               | STRING                               |
| *np.NaN* and *None*                              | float64              | DOUBLE                               |
| *pd.NaT* and *None*                              | datetime64           | NANOTIMESTAMP                        |
| *np.NaN* and *pd.NaT*                            | datetime64           | NANOTIMESTAMP                        |
| *None*, *np.NaN* and *pd.NaT*                    | datetime64           | NANOTIMESTAMP                        |
| *None* / *pd.NaT* / *np.nan* and non-null values | -                    | the data type of the non-null values |


## 14 Other Features
### 14.1 Forced Termination of Processes

The `session` object provides a static method `enableJobCancellation()` to enable forced termination of processes. It is disabled by default. When the feature is enabled, all running jobs submitted by the session in the API process can be terminated with "Ctrl+C". Currently, this feature is only available on Linux.

For example:

```
ddb.session.enableJobCancellation()
```

### 14.2 Setting TCP Timeout

The static method `setTimeout` specifies the TCP connection timeout period (in seconds) before the peer acknowledges receiving the data packets. After the timeout period expires, the TCP connection is closed. It is equivalent to the TCP_USER_TIMEOUT option. The default value is 30. (If the value is specified as 0, TCP will use the system default.)

This method is only available on Linux.

Example

```
ddb.session.setTimeout(60)
```
