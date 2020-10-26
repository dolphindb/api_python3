# Python API for DolphinDB

DolphinDB Python API 支持Python 3.6 - 3.8版本。通过执行如下指令进行安装：

```Console
$ pip install dolphindb
```

本教程目录如下：

- [1 运行DolphinDB脚本及调用函数](#1-运行dolphindb脚本及调用函数)
    - [1.1 建立DolphinDB连接](#11-建立dolphindb连接)
    - [1.2 运行DolphinDB脚本](#12-运行dolphindb脚本)
    - [1.3 运行DolphinDB函数](#13-运行dolphindb函数)
    - [1.4 session函数undef和内存释放的关系](#14-session函数undef和内存释放的关系)
- [2 上传本地对象到DolphinDB服务器](#2-上传本地对象到dolphindb服务器)
    - [2.1 使用session的upload方法上传](#21-使用session的upload方法上传)
    - [2.2 使用table函数上传](#22-使用table函数上传) 
    - [2.3 上传的数据表的的生命周期](#23-上传的数据表的的生命周期) 
- [3 创建DolphinDB数据库以及分区表](#3-创建dolphindb数据库以及分区表)
    - [3.1 使用DolphinDB Python API的原生方法](#31-使用dolphindb-python-api的原生方法)
    - [3.2 使用run方法来创建](#32-使用run方法来创建) 
    - [3.3 使用其它DolphinDB客户端软件来创建](#33-使用其它dolphindb客户端软件来创建) 
- [4 导入数据到DolphinDB数据库](#4-导入数据到dolphindb数据库)
    - [4.1 导入到内存表](#41-导入到内存表)
    - [4.2 导入到磁盘分区表](#42-导入到磁盘分区表)
    - [4.3 导入到内存分区表](#43-导入到内存分区表)
- [5 从DolphinDB数据库中加载数据](#5-从dolphindb数据库中加载数据)
    - [5.1 使用loadTable函数](#51-使用loadtable函数) 
    - [5.2 使用loadTableBySQL函数](#52-使用loadtablebysql函数) 
    - [5.3 从DolphinDB下载数据到Python时的数据转换](#53-从dolphindb下载数据到python时的数据转换) 
- [6 追加数据到DolphinDB数据表](#6-追加数据到dolphindb数据表)
    - [6.1 追加数据到内存表](#61-追加数据到内存表)
    - [6.2 追加数据到分布式表](#62-追加数据到分布式表)
- [7 数据库和表操作](#7-数据库和表操作)
    - [7.1 数据库和表的操作方法说明](#71-数据库和表的操作方法说明) 
    - [7.2 数据库操作](#72-数据库操作)
    - [7.3 表操作](#73-表操作)
- [8 SQL查询](#8-sql查询)
    - [8.1 select](#81-select)
    - [8.2 top](#82-top)
    - [8.3 where](#83-where)
    - [8.4 groupby](#84-groupby)
    - [8.5 contextby](#85-contextby)
    - [8.6 表连接](#86-表连接)
    - [8.7 executeAs](#87-executeas)
    - [8.8 回归运算](#88-回归运算)
- [9 Python Streaming API](#9-python-streaming-api)
    - [9.1 指定订阅端口号](#91-指定订阅端口号)  
    - [9.2 订阅和反订阅](#92-订阅和反订阅)
    - [9.3 流数据应用](#93-流数据应用)
- [10 更多实例](#10-更多实例)
    - [10.1 动量交易策略](#101-动量交易策略) 
    - [10.2 时间序列操作](#102-时间序列操作) 


## 1 运行DolphinDB脚本及调用函数

### 1.1 建立DolphinDB连接

Python应用通过会话（Session）在DolphinDB服务器上执行脚本和函数以及在两者之间双向传递数据。最为常用的Session类的函数如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host,port,[username,password])|将会话连接到DolphinDB服务器|
|login(username,password,[enableEncryption])|登录服务器|
|run(DolphinDBScript)|将脚本在DolphinDB服务器运行|
|run(DolphinDBFunctionName,args)|调用DolphinDB服务器上的函数|
|upload(DictionaryOfPythonObjects)|将本地数据对象上传到DolphinDB服务器|
|undef(objName,objType)|取消指定对象在DolphinDB内存中定义以释放内存|
|undefAll()|取消所有对象在DolphinDB内存中的定义以释放内存|
|close()|关闭当前会话|

以下脚本中，通过import语句导入API以后，在Python中创建一个会话，然后使用指定的域名或IP地址和端口号把该会话连接到DolphinDB服务器。请注意，在执行以下Python脚本前，需要先启动DolphinDB服务器。
```python
import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848)
# output
True
```

如果需要使用用户名和密码连接, 可使用以下脚本。其中admin为用户名， "123456"为密码。
```python
s.connect("localhost", 8848, "admin", "123456")
```
或者
```python
s.connect("localhost", 8848)
s.login("admin","123456")
```
若会话过期，或者初始化会话时没有指定登录信息（用户名与密码），可使用`login`函数来登录服务器。DolphinDB默认的管理员用户名为'admin'，密码为'123456'，并且默认会在连接时对用户名与密码进行加密传输。


#### SSL(加密）模式

Session初始化的时候,从server 1.10.17,1.20.6之后开始支持加密通讯 参数enableSSL，默认值为False. 如果需要启动SSL通讯，可以通过以下脚本，同时server端需要添加enableHTTPS=true配置项。

```
s=ddb.Session(enableSSL=True)
```

#### ASYN(异步)模式

Session初始化的时候,从server 1.10.17,1.20.6之后开始支持加密通讯 参数enableASYN，默认值为False.如果需要启动异步通讯，可以通过以下脚本。异步通讯的情况下，与server端的通讯只能通过
`session.run`方法，并且无返回值，因为异步通讯情况下之前的操作并不一定能确保执行完毕。这种模式非常适用于异步写入数据，节省了API端检测返回值的时间。 

```
s=ddb.Session(enableASYN=True)
```


### 1.2 运行DolphinDB脚本

凡是可在DolphinDB上运行的脚本都可以通过`run(script)`方法来运行。如果脚本在DolphinDB中返回对象，会转换成Python中对象。脚本运行失败的话，会有相应的错误提示。
```python
a=s.run("`IBM`GOOG`YHOO")
repr(a)
# output
"array(['IBM', 'GOOG', 'YHOO'], dtype='<U4')"
```

使用`run`方法可生成自定义函数：
```python
s.run("def getTypeStr(input){ \nreturn typestr(input)\n}")
```

对多行脚本，可以采用三引号的方式将其格式化，这样更易于维护，例如：
```
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

**注意**：`run`方法可接受的脚本最大长度为65,535字节。

### 1.3 运行DolphinDB函数

除了运行脚本之外，`run`命令可以直接在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。对这种用法，`run`方法的第一个参数是DolphinDB中的函数名，之后的参数是该函数的参数。

#### 1.3.1 传参

下面的示例展示Python程序通过`run`调用DolphinDB内置的`add`函数。`add`函数有x和y两个参数。根据参数是否已在DolphinDB server端被赋值，有以下三种调用方式：

- 所有参数均已在DolphinDB server端被赋值

若变量x和y已经通过Python程序在DolphinDB server端被赋值，
```python
s.run("x = [1,3,5];y = [2,4,6]")
```

那么在Python端要对这两个向量做加法运算，只需直接使用`run(script)`即可：
```python
a=s.run("add(x,y)")
repr(a)
# output
'array([3, 7, 11])'
```

- 仅有一个参数在DolphinDB server端被赋值

若仅变量x已通过Python程序在服务器端被赋值：
```python
s.run("x = [1,3,5]")

# output
array([1, 3, 5])
```

而参数y要在调用`add`函数时一并赋值，需要使用“部分应用”方式把参数x固化在`add`函数内。具体请参考[部分应用文档](https://www.dolphindb.cn/cn/help/index.html?PartialApplication.html)。
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

- 两个参数都待由Python客户端赋值

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

#### 1.3.2 参数支持的数据类型与数据结构

通过`run`调用DolphinDB的内置函数时，客户端上传参数的数据结构可以是标量(scalar)，列表(list)，字典(dict)，numpy的对象，pandas的DataFrame和Series等等。

> 需要注意：
> 1. NumPy array的维度不能超过2。
> 2. pandas的DataFrame和Series若有index，在上传到DolphinDB以后会丢失。如果需要保留index列，则需要使用pandas的DataFrame函数reset_index。
> 3. 如果DolphinDB函数的参数是时间或日期类型，Python客户端上传的时候参数应该先转换为numpy.datetime64类型。

下面具体介绍不同的Python对象作为参数参与运算的例子。

- 将list对象作为参数

  使用DolphinDB的`add`函数对两个Python的list进行相加：
```python
  s.run("add",[1,2,3,4],[1,2,1,1])
  # output
  array([2, 4, 4, 5])
  ```

- 将NumPy对象作为参数

  除了NumPy的array对象之外，NumPy的数值型标量也可以作为参数参与运算，例如，将np.int、np.datetime64等对象上传到DolphinDB作为函数参数。

  - np.int作为参数
    ```python
    import numpy as np
    s.run("add{1,}",np.int(4))
    # output
    5
    ```

  - np.datetime64作为参数
   
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
    
    由于DolphinDB中的TIME, MINUTE, SECOND, NANOTIME等类型没有日期信息，datetime64类型无法由Python API直接转换为这些类型。若需要根据Python中数据在DolphinDB中产生这些数据类型，可先将datetime64类型数据上传到DolphinDB Server，然后去除日期信息。上传数据方法可参见[上传本地对象到DolphinDB服务器](#2-上传本地对象到dolphindb服务器)。
    
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
    请注意，在上例最后一步中，将DolphinDB中的NANOTIME类型返回Python时，Python会自动添加1970-01-01作为日期部分。
    
  - np.datetime64对象的list作为参数

    ```python
    import numpy as np
    a=[np.datetime64('2019-01-01T20:00:00.000000001'), np.datetime64('2019-01-01T20:00:00.000000001')]
    s.run("add{1,}",a)
    # output
    array(['2019-01-01T20:00:00.000000002', '2019-01-01T20:00:00.000000002'], dtype='datetime64[ns]')
    ```

- 将pandas的对象作为参数

  pandas的DataFrame和Series若有index，在上传到DolphinDB后会丢失。

  - Series作为参数：
    ```python
    import pandas as pd
    import numpy as np
    a = pd.Series([1,2,3,1,5],index=np.arange(1,6,1))
    s.run("add{1,}",a)
    # output
    array([2, 3, 4, 2, 6])
    ```

  - DataFrame作为参数
    ```python
    import pandas as pd
    import numpy as np
    a = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]),
	  'value': np.double([7.8, 4.6, 5.1, 9.6, 0.1]),
	  'x': np.int32([5, 4, 3, 2, 1]),
	  'date': np.array(['2019-02-03','2019-02-04','2019-02-05','2019-02-06','2019-02-07'],
			   dtype='datetime64[D]')},
	 index=[1, 2, 3, 4, 5])

    s.upload({'a':a})
    s.run("typestr",a)
    # output
    'IN-MEMORY TABLE'
    
    s.run('a')
    # output
	   id  value  x       date
	1   1    7.8  5 2019-02-03
	2   2    4.6  4 2019-02-04
	3   3    5.1  3 2019-02-05
	4   4    9.6  2 2019-02-06
	5   3    0.1  1 2019-02-07
    ```
    
### 1.4 Session函数`undef`与内存释放的关系

函数`undef`或者`undefAll`用于将session中的指定对象或者全部对象释放掉。`undef`支持的对象类型包括:"VAR"(变量)、"SHARED"(共享变量)与"DEF"(函数定义)。默认类型为最常见的变量"VAR"。
"SHARED"指内存中跨session的共享变量，例如流数据表。

假设session中有一个DolphinDB的表对象t1, 可以通过session.undef("t1","VAR")将该表释放掉。释放后，并不一定能够看到内存马上释放。这与DolphinDB的内存管理机制有关。DolphinDB从操作系统申请的内存，释放后不会立即还给操作系统，因为这些释放的内存在DolphinDB中可以立即使用。申请内存首先从DolphinDB内部的池中申请内存，不足才会向操作系统去申请。配置文件(dolphindb.cfg)中参数maxMemSize设置的内存上限是需要保证的。譬如说设置为8G，那么DolphinDB会充分的去利用这个8G内存。所以如果用户需要反复undef内存中的一个变量来达到释放内存以为后面程序腾出更多内存空间，则需要将maxMemSize调整到一个合理的数值，否则当前内存没有释放，而后面需要的内存超过了系统的最大内存，DolphinDB的进程就有可能被操作系统杀掉或者出现out of memory的错误。


## 2 上传本地对象到DolphinDB服务器

若需要在DolphinDB中重复调用一个本地对象变量，可将本地对象上传到DolphinDB服务器，上传时需要指定变量名，以用于之后重复调用。

### 2.1 使用Session的upload方法上传

Python API提供upload方法将Python对象上传到DolphinDB服务器。upload方法输入一个Python的字典对象，它的key对应的是DolphinDB中的变量名，value对应的是Python对象，可为Numbers，Strings，Lists，DataFrame等数据对象。

- 上传 Python list

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

注意，Python中像a=[1,2,3.0]这样含有不同数据类型的list，上传到DolphinDB后，会被识别为any vector。这种情况下，建议使用numpy.array代替list，即通过a=numpy.array([1,2,3.0],dtype=numpy.double)指定统一的数据类型，这样上传a以后，a会被识别为double类型的向量。

- 上传 NumPy array

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

- 上传pandas DataFrame

```python
import pandas as pd
import numpy as np

df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.value.avg()"))
# output
5.44
```

### 2.2 使用`table`方法上传

在Python中可使用`table`方法创建DolphinDB表对象，并上传到server端。table方法的输入可以是字典、DataFrame或DolphinDB中的表名。

* 上传dict

下面的程序定义了一个函数`createDemoDict()`以创建一个字典。

```python
import numpy as np

def createDemoDict():
    return {'id': [1, 2, 2, 3],
            'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
            'price': [22, 3.5, 21, 26]}
```

调用`table`方法将该字典上传到DolphinDB server端，并将该表命名为"testDict"，再通过API提供的`loadTable`函数读取和查看表内数据。

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

* 上传pandas DataFrame

以下程序定义函数`createDemoDataFrame()`，以创建一个pandas的DataFrame对象。

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

调用`table`方法将该DataFrame上传到DolphinDB server端，命名为"testDataFrame"，再通过API提供的`loadTable`函数读取和查看表内数据。

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

`table`和`loadTable`函数必须返回一个Python本地变量，即Python端的变量和server端的变量是一一对应的。假设server端表对象为t0, 
```
t0=s.table(data=createDemoDict(), tableAliasName="t1")
```
释放server端对象有三种方法：

- 取消server端定义(undef)
```
s.undef("t0", "VAR")
```
- 将server端对象置空
```
s.run("t0=NULL")
```
- 取消本地变量对server端对象的引用
```
t0=None
```

当Python端通过session.table函数将数据上传到server之后，DolphinDB会为Python端的变量建立一个变量对server端table变量的引用。当Python端对server端table变量引用消失后，server端的table会自动释放。

以下代码将一个表上传到server，然后通过toDF()加载数据。
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

如果重复下面这个语句，会发生找到不到t1的异常。原因是Python端对Server端表t1的原有引用已经取消，在重新给Python端t1分配DolphinDB的表对象前，
DolphinDB要对session中的对应的表t1进行释放（通过函数`undef`取消它在session中的定义），所以会出现无法找到t1的异常。

```
t1=s.table(data=createDemoDict(), tableAliasName="t1")
print(t1.toDF())

#output
<Server Exception> in run: Can't find the object with name t1
```

如何避免这种情况呢？可将这个table对象赋值给另一个Python本地变量，但代价是server端保存了两份同样的table对象，因为Python端有两个引用：t1和t2。
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

如果需要反复通过同一个本地变量指向相同的或者不同的上传表，更合理的方法是不指定表名。此时会为用户随机产生一个临时表名。这个表名可以通过t1.tableName()来获取。这里可能会产生一个疑惑，那么server端是不是会产生很多表对象，造成内存溢出。由于python端使用了同一个变量名，所以在重新上传数据的时候，系统会将上一个表对象释放掉(TMP_TBL_876e0ce5)，而用一个新的table对象TMP_TBL_4c5647af来对应Python端的t1，所以Server端始终只有一个对应的表对象。
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

同理，通过`loadTable`来加载一个磁盘分区表到内存的原理也是必须赋值给一个Python本地变量，建立起Python本地变量和server端一一对应的关系。 首先运行以下DolphinDB脚本：

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


## 3 创建DolphinDB数据库以及分区表

创建DolphinDB数据库可以有如下三种方式:

### 3.1 使用DolphinDB Python API的原生方法

准备环境

```
import numpy as np
import pandas as pd
import dolphindb.settings as keys
```

#### 3.1.1 创建基于VALUE的DolphinDB数据库以及分区表


按date分区

```
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

按month分区

```
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

#### 3.1.2 创建基于RANGE的DolphinDB数据库以及分区表

按int类型ID分区

```
dbPath="dfs://db_range_int"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
db = s.database(dbName='mydb', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dbPath)
df = pd.DataFrame({'id': np.arange(1, 21), 'val': np.repeat(1, 20)})
t = s.table(data=df, tableAliasName='t')
db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```


#### 3.1.3 创建基于LIST的DolphinDB数据库以及分区表

按Symbol类型的股票代码来分区

```
dbPath="dfs://db_list_sym"
if s.existsDatabase(dbPath):
    s.dropDatabase(dbPath) 
db = s.database(dbName='mydb', partitionType=keys.LIST, partitions=[['IBM', 'ORCL', 'MSFT'], ['GOOG', 'FB']],dbPath=dbPath)
df = pd.DataFrame({'sym':['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'], 'val':[1,2,3,4,5]})
t = s.table(data=df)
db.createPartitionedTable(table=t, tableName='pt', partitionColumns='sym').append(t)
re = s.loadTable(tableName='pt', dbPath=dbPath).toDF()
```  

#### 3.1.4 创建基于HASH的DolphinDB数据库以及分区表

按int类型ID来分区

```
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

#### 3.1.5 创建基于COMPO的DolphinDB数据库以及分区表

以下脚本创建基于COMPO的数据库以及分区表：第一层是基于VALUE的date类型分区，第二层是基于RANGE的int类型分区。

注意： 创建COMPO的子分区数据库的dbPath参数必须设置为空字符串。
```
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

### 3.2 使用run方法来创建

这种方法就是将用DolphinDB脚本语言编写的创建数据库及分区表的脚本，通过字符串的方式传给run方法来实现，例如：




```
dstr = """
dbPath="dfs://valuedb"
if (existsDatabase(dbPath)){
    dropDatabase(dbPath)
}
mydb=database(dbPath, VALUE, ['AMZN','NFLX', 'NVDA'])
t=table(take(['AMZN','NFLX', 'NVDA'], 10) as sym, 1..10 as id)
db.createPartitionedTable(t,`pt,`sym).append!(t)

"""
t1=s.run(dstr)
t1=s.loadTable(tableName="pt",dbPath=dbPath)
t1.toDF()

# output
     sym   id
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

### 3.3 使用其它DolphinDB客户端软件来创建

用户可以通过 DolphinDB GUI, VS Code Extension, DolphinDB Notebook, DolphinDB Jupyter Notebook Extension, DolphinDB终端，以及DolphinDB命令行本地或者远程脚本执行来创建DolphinDB数据库以及分区。

例如可以将下述创建数据库脚本放到以上工具中执行。

```
dbPath="dfs://valuedb"
if (existsDatabase(dbPath)){
    dropDatabase(dbPath)
}
db=database(dbPath, VALUE, ['AMZN','NFLX', 'NVDA'];
t=table(take(['AMZN','NFLX', 'NVDA'], 10) as sym, 1..10 as id);
db.createPartitionedTable(t,`pt,`sym).append!(t)
```

     
## 4 导入数据到DolphinDB数据库

DolphinDB数据库根据存储方式可以分为3种类型：内存数据库、本地文件系统的数据库和分布式文件系统（DFS）中的数据库。DFS数据库能够自动管理数据存储和备份，并且性能最优。因此，推荐用户使用分布式文件系统，部署方式请参考[多服务器集群部署](https://github.com/dolphindb/Tutorials_CN/blob/master/multi_machine_cluster_deploy.md)。

下面的例子中，我们使用了一个csv文件：[example.csv](data/example.csv)。

### 4.1 导入到内存表

可使用`loadText`方法把文本文件导入到DolphinDB的内存表中。该方法会在Python中返回一个DolphinDB内存表对象。可使用`toDF`方法把Python中的DolphinDB的Table对象转换成pandas的DataFrame。

```python
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

`loadText`函数导入文件时的默认分隔符是','。用户也可指定其他符号作为分隔符。例如，导入表格形式的文本文件：
```python
t1=s.loadText(WORK_DIR+"/t1.tsv", '\t')
```

### 4.2 导入到磁盘分区表

如果需要导入的文件超过可用内存，可导入分区数据库。

### 4.2.1 创建分区数据库

创建了分区数据库后，一般不能修改分区方案。唯一的例外是值分区（或者复合分区中的值分区）创建后，可以添加分区。

本节例子中会用到数据库valuedb。首先检查该数据库是否存在，如果存在，将其删除：
```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
```

使用`database`方法创建值分区（VALUE）的数据库。由于example.csv文件中只有3个股票代码，使用股票代码作为分区字段。参数partitions表示分区方案。下例中，我们先导入DolphinDB的关键字，再创建数据库。
```python
import dolphindb.settings as keys

# 'db' indicates the database handle name on the DolphinDB server.
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=['AMZN','NFLX', 'NVDA'], dbPath='dfs://valuedb')
#equals to s.run("db=database('dfs://valuedb', VALUE, ['AMZN','NFLX', 'NVDA'])")
```

除了值分区（VALUE），DolphinDB还支持顺序分区（SEQ）、哈希分区（HASH）、范围分区（RANGE）、列表分区（LIST）与组合分区（COMPO），具体请参见[database函数](https://www.dolphindb.cn/cn/help/database1.html)。

### 4.2.2 创建分区表，并导入数据到表中

创建数据库后，可使用函数`loadTextEx`把文本文件导入到分区数据库的分区表中。如果分区表不存在，函数会自动生成该分区表并把数据追加到表中。如果分区表已经存在，则直接把数据追加到分区表中。

函数`loadTextEx`的各个参数如下：
- dbPath表示数据库路径
- tableName表示分区表的名称
- partitionColumns表示分区列
- remoteFilePath表示文本文件的绝对路径; 如果终端和DolphinDB服务器不在一台机器上，remoteFilePath指远程文件在DolphinDB服务器上的绝对路径。
- delimiter表示文本文件的分隔符（默认分隔符是逗号）

下面的例子使用函数`loadTextEx`创建了分区表trade，并把example.csv中的数据加载到表中。

```python
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")

trade = s.loadTextEx(dbPath="mydb",  tableName='trade',partitionColumns=["TICKER"], remoteFilePath=WORK_DIR + "/example.csv")
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

#返回表中的行数：
print(trade.rows)
# output
13136

#返回表中的列数：
print(trade.cols)
# output
6

#展示表的结构：
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

访问表：
```python
trade = s.table(dbPath="dfs://valuedb", data="trade")
```

### 4.3 导入到内存分区表

### 4.3.1 使用`loadTextEx`

`database`函数中，若将dbPath参数设为空字符串，可创建内存分区数据库。由于内存分区表可进行并行计算，因此对它进行操作比对内存未分区表进行操作要快。

使用`loadTextEx`可把数据导入到分区内存表中。

```python
import dolphindb.settings as keys

s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="")

trade=s.loadTextEx(dbPath="mydb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
trade.toDF()
```

### 4.3.2 使用`ploadText`

`ploadText`函数可以并行加载文本文件到内存分区表中。它的加载速度要比`loadText`函数快。

```python
trade=s.ploadText(WORK_DIR+"/example.csv")
print(trade.rows)

# output
13136
```

## 5 从DolphinDB数据库中加载数据

### 5.1 使用`loadTable`函数

参数tableName表示分区表的名称，dbPath表示数据库的路径。如果没有指定dbPath，`loadTable`函数会加载内存中的表。

对分区表，若参数memoryMode=false，只把元数据加载到内存；若参数memoryMode=true，把表中的所有数据加载到内存分区表中。

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

### 5.2 使用`loadTableBySQL`函数

`loadTableBySQL`函数把磁盘上的分区表中满足SQL语句过滤条件的数据加载到内存分区表中。

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

### 5.3 从DolphinDB下载数据到Python时的数据转换

#### 5.3.1 数据形式的转换

DolphinDB Python API使用Python原生的各种形式的数据对象来存放DolphinDB服务端返回的数据。下面给出从DolphinDB的数据对象到Python的数据对象的映射关系。

|DolphinDB|Python|DolphinDB生成数据|Python数据|
|-------------|----------|-------------|-----------|
|scalar|Numbers, Strings, NumPy.datetime64|见6.3.2小节|见6.3.2小节
|vector|NumPy.array|1..3|[1 2 3]
|pair|Lists|1:5|[1, 5]
|matrix|Lists|1..6$2:3|[array([[1, 3, 5],[2, 4, 6]], dtype=int32), None, None]
|set|Sets|set(3 5 4 6)|{3, 4, 5, 6}|
|dictionary|Dictionaries|dict(['IBM','MS','ORCL'], 170.5 56.2 49.5)|{'MS': 56.2, 'IBM': 170.5, 'ORCL': 49.5}|
|table|pandas.DataFame|见[第6.1小节](#61-使用loadtable函数)|见[第6.1小节](#61-使用loadtable函数)

#### 5.3.2 数据类型的转换

下表展示了从DolphinDB数据库中通过`toDF()`函数下载数据到Python时，数据类型的转换。需要指出的是：
- DolphinDB CHAR类型会被转换成Python int64类型。对此结果，用户可以使用Python的`chr`函数使之转换为字符。
- 由于Python pandas中所有有关时间的数据类型均为datetime64，DolphinDB中的所有时间类型数据[均会被转换为datetime64类型](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803)。MONTH类型，如2012.06M，会被转换为2012-06-01（即当月的第一天）。
- TIME, MINUTE, SECOND与NANOTIME类型不包含日期信息，转换时会自动添加1970-01-01，例如13:30m会被转换为1970-01-01 13:30:00。

|DolphinDB类型|Python类型|DolphinDB数据|Python数据|
|-------------|----------|-------------|-----------|
|BOOL|bool|[true,00b]|[True, nan]|
|CHAR|int64|[12c,00c]|[12, nan]|
|SHORT|int64|[12,00h]|[12, nan]|
|INT|int64|[12,00i]|[12, nan]|
|LONG|int64|[12l,00l]|[12, nan]|
|DOUBLE|float64|[3.5,00F]|[3.5,nan]|
|FLOAT|float64|[3.5,00f]|[3.5, nan]|
|SYMBOL|object|symbol(["AAPL",NULL])|["AAPL",""]|
|STRING|object|["AAPL",string()]|["AAPL", ""]|
|DATE|datetime64|[2012.6.12,date()]|[2012-06-12, NaT]|
|MONTH|datetime64|[2012.06M, month()]|[2012-06-01, NaT]|
|TIME|datetime64|[13:10:10.008,time()]|[1970-01-01 13:10:10.008, NaT]|
|MINUTE|datetime64|[13:30,minute()]|[1970-01-01 13:30:00, NaT]|
|SECOND|datetime64|[13:30:10,second()]|[1970-01-01 13:30:10, NaT]|
|DATETIME|datetime64|[2012.06.13 13:30:10,datetime()]|[2012-06-13 13:30:10,NaT]|
|TIMESTAMP|datetime64|[2012.06.13 13:30:10.008,timestamp()]|[2012-06-13 13:30:10.008,NaT]|
|NANOTIME|datetime64|[13:30:10.008007006, nanotime()]|[1970-01-01 13:30:10.008007006,NaT]|
|NANOTIMESTAMP|datetime64|[2012.06.13 13:30:10.008007006,nanotimestamp()]|[2012-06-13 13:30:10.008007006,NaT]|
|UUID|object|5d212a78-cc48-e3b1-4235-b4d91473ee87|"5d212a78-cc48-e3b1-4235-b4d91473ee87"|
|IPADDR|object|	192.168.1.13|"192.168.1.13"|
|INT128|object|e1671797c52e15f763380b45e841ec32|"e1671797c52e15f763380b45e841ec32"|

#### 5.3.3 缺失值处理

从DolphinDB下载数据到Python，并使用`toDF()`方法把DolphinDB数据转换为Python的DataFrame，DolphinDB中的逻辑型、数值型和时序类型的NULL值默认情况下是NaN、NaT，字符串的NULL值为空字符串。

## 6 追加数据到DolphinDB数据表

实际工作中，用户经常从其他数据库系统或第三方Web API中取得数据后存入DolphinDB数据表中。本节将介绍如何通过Python API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

- 内存表：数据仅保存在内存中，存取速度最快。
- 本地磁盘表：数据保存在本地磁盘上，可以从磁盘加载到内存。不建议在生产环境中使用。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。建议在生产环境中使用。

### 6.1 追加数据到内存表

DolphinDB提供以下方式来追加数据到内存表：

- 通过`tableInsert`函数追加数据或一个表
- 通过`insert into`语句追加数据

在本例中使用的数据表有4列，分别是INT, DATE, STRINR, DOUBLE类型，列名分别为id, date, ticker和price。

在Python中执行以下脚本：
```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

# 生成内存表
script = """t = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)
```

上面的例子通过`table`函数在DolphinDB server端来创建内存表，指定了初始内存分配和初始长度、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。
若需要多个客户端可以同时访问内存表，可使用`share`在会话间共享内存表。


#### 6.1.1 使用`tableInsert`函数批量追加

若Python程序获取的数据可以组织成List方式，且保证数据类型正确的情况下，可以直接使用`tableInsert`函数来批量保存多条数据。这个函数可以接受多个数组作为参数，将数组追加到数据表中。这样做的好处是，可以在一次访问服务器请求中将上传数据对象和追加数据这两个步骤一次性完成，相比5.1.3小节中的`INSERT INTO`做法减少了一次访问DolphinDB服务器的请求。

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

#### 6.1.2 使用`tableInsert`函数追加表

可通过`tableInsert`函数直接向内存表追加一个表。

- 若表中没有时间列

可直接通过部分应用的方式，将一个DataFrame直接上传到服务器并追加到内存表。

```python
script = """t = table(1000:0,`id`ticker`price, [INT,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)

# 生成要追加的DataFrame
tb=pd.DataFrame({'id': [1, 2, 2, 3],
                 'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                 'price': [22, 3.5, 21, 26]})
s.run("tableInsert{tglobal}",tb)

#output
4

s.run("tglobal")


id	ticker	price
0	1	AAPL	22.0
1	2	AMZN	3.5
2	2	AMZN	21.0
3	3	A	26.0

```

- 若表中有时间列

由于Python pandas中所有[有关时间的数据类型均为datetime64](https://github.com/pandas-dev/pandas/issues/6741#issuecomment-39026803)，上传一个DataFrame到DolphinDB以后所有时间类型的列均为nanotimestamp类型，因此在追加一个带有时间列的DataFrame时，我们需要在DolphinDB服务端对时间列进行数据类型转换：先将该DataFrame上传到服务端，通过select语句将表内的每一列都选出来，并进行时间类型转换（该例子将nanotimestamp类型转换为date类型），再追加到内存表中，具体如下：
```python
script = """t = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE])
share t as tglobal"""
s.run(script)

import pandas as pd
tb=pd.DataFrame(createDemoDict())
s.upload({'tb':tb})
s.run("tableInsert(tglobal,(select id, date(date) as date, ticker, price from tb))")
s.run("tglobal")


id	date	ticker	price
0	1	2019-02-04	AAPL	22.0
1	2	2019-02-05	AMZN	3.5
2	2	2019-02-09	AMZN	21.0
3	3	2019-02-13	A	26.0
```

把数据保存到内存表，还可以使用`append!`函数，它可以把一张表追加到另一张表。但是，一般不建议通过`append!`函数保存数据，因为`append!`函数会返回一个表的schema，增加通信量。

- 若表中没有时间列

```python
import pandas as pd

# 生成内存表
script = """t = table(1:0,`id`ticker`price, [INT,SYMBOL,DOUBLE])
share t as tdglobal"""
s.run(script)

# 生成要追加的DataFrame
tb=pd.DataFrame({'id': [1, 2, 2, 3],
                 'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                 'price': [22, 3.5, 21, 26]})
s.run("append!{tdglobal}",tb)
```

- 若表中有时间列

```python
import pandas as pd
tb=pd.DataFrame(createDemoDict())
s.upload({'tb':tb})
s.run("append!(tglobal, (select id, date(date) as date, ticker, price from tb))")
```

#### 6.1.3 使用`INSERT INTO`语句追加数据

可以采用如下方式保存单条数据：
```python
import numpy as np

script = "insert into tglobal values(%s, date(%s), %s, %s)" % (1, np.datetime64("2019-01-01").astype(np.int64), '`AAPL', 5.6)
s.run(script)
```

**请注意**，由于DolphinDB的内存表并不提供数据类型自动转换的功能，因此在向内存表追加数据时，需要在服务端调用时间转换函数对时间类型的列进行转换，首先要确保插入的数据类型与内存表的数据类型一致，再追加数据。

上例中，将numpy的时间类型强制转换成64位整型，并且在insert语句中调用`date`函数，在服务端将时间列的整型数据转换成对应的类型。

也可使用`INSERT INTO`语句一次性插入多条数据:
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

上例中，通过指定`date_range()`函数的dtype参数为datetime64[D]，生成了只含有日期的时间列，这与DolphinDB的date类型一致，因此可直接通过insert语句插入数据，无需显示转换。若这里时间数据类型为datetime64，则需要这样追加数据到内存表：

```python
script = "insert into tglobal values(ids,date(dates),tickers,prices);" 
s.run(script)
```

**请注意**，从性能方面考虑，不建议使用`INSERT INTO`来插入数据，因为服务器端要对INSERT语句进行解析会造成额外开销。


### 6.2 追加数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过Python API把数据保存至分布式表。

请注意只有启用enableDFS=1的集群环境才能使用分布式表。

在DolphinDB中使用以下脚本创建分布式表，脚本中，`database`函数用于创建数据库，`createPartitionedTable`函数用于创建分区表。

```python
import dolphindb as ddb

s = ddb.session()
s.connect("localhost", 8848, "admin", "123456")

# 生成分布式表
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

DolphinDB提供`loadTable`方法来加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下。脚本中，我们通过自定义的函数`createDemoDataFrame()`创建一个DataFrame，再追加数据到DolphinDB数据表中。与内存表和磁盘表不同的是，分布式表在追加表的时候提供时间类型自动转换的功能，因此无需显示地进行类型转换。

```python
tb = createDemoDataFrame()
s.run("tableInsert{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb=tableName), tb)
```

把数据保存到分布式表，还可以使用`append!`函数，它可以把一张表追加到另一张表。但是，一般不建议通过append!函数保存数据，因为`append!`函数会返回一个表结构，增加通信量。

```python
tb = createDemoDataFrame()
s.run("append!{{loadTable('{db}', `{tb})}}".format(db=dbPath,tb=tableName),tb)
```

## 7 数据库和表操作

### 7.1 数据库和表的操作方法说明

除了第1节列出的常用方法之外，Session类还提供了一些与DolphinDB内置函数作用相同的方法，用于操作数据库和表，具体如下：

* 数据库相关

| 方法名        | 详情          |
|:------------- |:-------------|
|database|创建数据库|
|dropDatabase(dbPath)|删除数据库|
|dropPartition(dbPath, partitionPaths, tableName)|删除数据库的某个分区|
|existsDatabase|判断是否存在数据库|

* 表和分区相关

| 方法名        | 详情          |
|:------------- |:-------------|
|dropTable(dbPath, tableName)|删除数据库中的表|
|existsTable|判断是否存在表|
|loadTable|加载本地磁盘表或者分布式表到内存|
|table|创建表|

我们在Python中得到一个表对象以后，可以对这个对象调用如下的方法，这些方法是Table类方法。

| 方法名        | 详情          |
|:------------- |:-------------|
|append|向表中追加数据|
|drop(colNameList)|删除表中的某列|
|executeAs(tableName)|执行结果保存为指定表名的内存表|
|execute()|执行脚本。与`update`和`delete`一起使用|
|toDF()|把DolphinDB表对象转换成pandas的DataFrame对象|

以上只是列出其中最为常用的方法，关于Session类和Table类提供的所有方法请参见session.py和table.py文件。

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
testDict=table([1, 2, 2, 3] as id, [2019.02.04,2019.02.05,2019.02.09,2019.02.13] as date, ['AAPL','AMZN','AMZN','A'] as ticker, [22, 3.5, 21, 26] as price)
tb.append!(testDict)
select * from tb
"""
s.run(db_script)


    id	date	ticker	price
0	3	2019-02-13	A	26.0
1	1	2019-02-04	AAPL	22.0
2	2	2019-02-05	AMZN	3.5
3	2	2019-02-09	AMZN	21.0

```

### 7.2 数据库操作

#### 7.2.1 创建数据库

使用`database`创建分区数据库：
```python
import dolphindb.settings as keys
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX", "NVDA"], dbPath="dfs://valuedb")
```

#### 7.2.2 删除数据库

使用`dropDatabase`删除数据库：
```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
```

#### 7.2.3 删除DFS数据库的分区

使用`dropPartition`删除DFS数据库的分区。需要注意的是，若要删除的分区名称在DolphinDB中需要通过字符串的形式表示，例如本例中按照TICKER进行值分区：partitions=["AMZN","NFLX","NVDA"]，则在删除这类分区时，需要为分区名称加上引号： partitionPaths=["'AMZN'","'NFLX'"]。类似情况还有有范围分区：partitionPaths=["'/0_50'","'/50_100'"]，列表分区：partitionPaths=["'/List0'","'/List1'"]等等。

```python
import dolphindb.settings as keys

if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
trade=s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
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

### 7.3 表操作

#### 7.3.1 加载数据库中的表

请参考[从dolphindb数据库中加载数据](#5-从dolphindb数据库中加载数据)。

#### 7.3.2 数据表添加数据

我们可以通过`append`方法追加数据。

下面的例子把数据追加到磁盘上的分区表。如果需要使用追加数据后的表，需要重新把它加载到内存中。

```python
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")
print(trade.rows)

# output
13136

# take the top 10 rows of table "trade" on the DolphinDB server
t = trade.top(10).executeAs("top10")

trade.append(t)

# table "trade" needs to be reloaded in order to see the appended records
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")
print (trade.rows)

# output
13146
```

下面的例子把数据追加到内存表中。

```python
trade=s.loadText(WORK_DIR+"/example.csv")
t = trade.top(10).executeAs("top10")
t1=trade.append(t)

print(t1.rows)

# output
13146
```

关于追加表的具体介绍请参考[追加数据到DolphinDB数据表](#5-追加数据到dolphindb数据表)。

### 7.3.3 更新表

`update`只能用于更新内存表，并且必须和`execute`一起使用。

```python
trade=s.loadText(WORK_DIR+"/example.csv")
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

#### 7.3.4 删除表中的记录

`delete`必须与`execute`一起使用来删除表中的记录。

```python
trade=s.loadText(WORK_DIR+"/example.csv")
trade.delete().where('date<2013.01.01').execute()
print(trade.rows)

# output
3024
```

#### 7.3.5 删除表中的列

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

#### 7.3.6 删除表

```python
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
s.dropTable(dbPath="dfs://valuedb", tableName="trade")
```

因为分区表trade已经被删除，所以执行下面加载trade的脚本会抛出异常
```
s.loadTable(dbPath="dfs://valuedb", tableName="trade")

Exeption:
getFileBlocksMeta on path '/valuedb/trade.tbl' failed, reason: path does not exist
```

## 8 SQL查询

DolphinDB提供了灵活的方法来生成SQL语句。

### 8.1 `select`

#### 8.1.1 使用一系列的列名作为输入内容

```python
trade=s.loadText(WORK_DIR+"/example.csv")
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
可以使用`showSQL`来展示SQL语句：
```python
print(trade.select(['ticker','date','bid','ask','prc','vol']).showSQL())

# output
select ticker,date,bid,ask,prc,vol from T64afd5a6
```

#### 8.1.2 使用字符串作为输入内容

```python
print(trade.select("ticker,date,bid,ask,prc,vol").where("date=2012.09.06").where("vol<10000000").toDF())

# output
  ticker       date        bid     ask     prc      vol
0   AMZN 2012-09-06  251.42999  251.56  251.38  5657816
1   NFLX 2012-09-06   56.65000   56.66   56.65  5368963
...
```

### 8.2 `top`

`top`用于取表中的前n条记录。

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

### 8.3 `where`

`where`用于过滤数据。

#### 8.3.1 多个条件过滤

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

使用`showSQL`来查看SQL语句：
```python
print(trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').showSQL())

# output
select date,bid,ask,prc,vol from Tff260d29 where TICKER=`AMZN and bid!=NULL and ask!=NULL and vol>10000000 order by vol desc
```

#### 8.3.2 使用字符串作为输入内容

`select`的输入内容可以是包含多个列名的字符串，`where`的输入内容可以是包含多个条件的字符串。

```python
trade=s.loadText(WORK_DIR+"/example.csv")
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

### 8.4 `groupby`

`groupby`后面需要使用聚合函数，如`count`, `sum`, `agg`和`agg2`。

准备数据库
```
if s.existsDatabase("dfs://valuedb"):
    s.dropDatabase("dfs://valuedb")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN","NFLX","NVDA"], dbPath="dfs://valuedb")
s.loadTextEx(dbPath="dfs://valuedb", partitionColumns=["TICKER"], tableName='trade', remoteFilePath=WORK_DIR + "/example.csv")
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

分别计算每个股票的vol总和与prc总和：
```python
trade = s.loadTable(tableName="trade",dbPath="dfs://valuedb")
print(trade.select(['vol','prc']).groupby(['ticker']).sum().toDF())

# output
  ticker      sum_vol       sum_prc
0   AMZN  33706396492  772503.81377
1   NFLX  14928048887  421568.81674
2   NVDA  46879603806  127139.51092
```

`groupby`与`having`一起使用：
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

`contextby`与`groupby`相似，区别在于`groupby`为每个组返回一个标量，但是`contextby`为每个组返回一个向量，向量的长度与该组的行数相同。

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

```python
df= s.loadTable(tableName="trade",dbPath="dfs://valuedb").select("TICKER, month(date) as month, sum(VOL)").contextby("TICKER,month(date)").toDF()
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

```python
df= s.loadTable(dbPath="dfs://valuedb", tableName="trade").contextby('ticker').having("sum(VOL)>40000000000").toDF()
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

### 8.6 表连接

`merge`用于内部连接、左连接和外部连接，`merge_asof`为asof join，`merge_window`为窗口连接。

#### 8.6.1 `merge`

如果连接列名称相同，使用on参数指定连接列，如果连接列名称不同，使用left_on和right_on参数指定连接列。可选参数how表示表连接的类型。默认的连接类型为内部连接。
```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'AMZN'], 'date': np.array(['2015-12-31', '2015-12-30', '2015-12-29'], dtype='datetime64[D]'), 'open': [695, 685, 674]})
print(trade.merge(t1,on=["TICKER","date"]).toDF())

# output
  TICKER        date      VOL        PRC        BID        ASK  open
0   AMZN  2015.12.29  5734996  693.96997  693.96997  694.20001   674
1   AMZN  2015.12.30  3519303  689.07001  689.07001  689.09998   685
2   AMZN  2015.12.31  3749860  675.89001  675.85999  675.94000   695
```

当连接列名称不相同时，需要指定left_on参数和right_on参数。
```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
t1 = s.table(data={'TICKER1': ['AMZN', 'AMZN', 'AMZN'], 'date1': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [695, 685, 674]})
print(trade.merge(t1,left_on=["TICKER","date"], right_on=["TICKER1","date1"]).toDF())

# output
  TICKER        date      VOL        PRC        BID        ASK  open
0   AMZN  2015.12.29  5734996  693.96997  693.96997  694.20001   674
1   AMZN  2015.12.30  3519303  689.07001  689.07001  689.09998   685
2   AMZN  2015.12.31  3749860  675.89001  675.85999  675.94000   695
```

左连接时，把how参数设置为'left'。
```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
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

外部连接时，把how参数设置为'outer'。分区表只能与分区表进行外部链接，内存表只能与内存表进行外部链接。
```python
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

#### 8.6.2 `merge_asof`

`merge_asof`对应DolphinDB中的asof join (`aj`)。asof join为非同时连接，它与left join非常相似，主要有以下区别：

- 1. asof join的最后一个连接列通常是时间类型。对于左表中某行的时间t，在右表最后一个连接列之外的其它连接列一致的记录中，如果右表没有与t对应的时间，asof join会取右表中t之前的最近时间对应的记录；如果有多个相同的时间，会取最后一个时间对应的记录。

- 2. 如果只有一个连接列，右表必须按照连接列排好序。如果有多个连接列，右表必须在其它连接列决定的每个组内根据最后一个连接列排好序。如果右表不满足这些条件，计算结果将会不符合预期。右表不需要按照其他连接列排序，左表不需要排序。

本节与下节的例子使用了[trades.csv](data/trades.csv)和[quotes.csv](data/quotes.csv)，它们含有NYSE网站下载的AAPL和FB的2016年10月24日的交易与报价数据。

```python
import dolphindb.settings as keys

WORK_DIR = "C:/DolphinDB/Data"
if s.existsDatabase(WORK_DIR+"/tickDB"):
    s.dropDatabase(WORK_DIR+"/tickDB")
s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AAPL","FB"], dbPath=WORK_DIR+"/tickDB")
trades = s.loadTextEx("db",  tableName='trades',partitionColumns=["Symbol"], remoteFilePath=WORK_DIR + "/trades.csv")
quotes = s.loadTextEx("db",  tableName='quotes',partitionColumns=["Symbol"], remoteFilePath=WORK_DIR + "/quotes.csv")

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

使用asof join计算交易成本：

```python
print(trades.merge_asof(quotes, on=["Symbol","Time"]).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2))/sum(Trade_Volume*Trade_Price)*10000 as cost").groupby("Symbol").toDF())

# output
  Symbol       cost
0   AAPL   6.486813
1     FB  35.751041
```

#### 8.6.3 `merge_window`

`merge_window`对应DolphinDB中的window join，它是asof join的扩展。leftBound参数和rightBound参数用于指定窗口的边界w1和w2，对左表中最后一个连接列对应的时间为t的记录，在右表中选择(t+w1)到(t+w2)的时间并且其他连接列匹配的记录，然后对这些记录使用指定的聚合函数。

window join和prevailing window join的唯一区别是，如果右表中没有与窗口左边界时间（即t+w1）匹配的值，prevailing window join会选择右表中(t+w1)之前的最近时间的记录作为t+w1时的记录。如果要使用prevailing window join，需将prevailing参数设置为True。

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

使用window join计算交易成本：

```python
trades.merge_window(quotes,-1000000000, 0, aggFunctions="[wavg(Offer_Price, Offer_Size) as Offer_Price, wavg(Bid_Price, Bid_Size) as Bid_Price]", on=["Symbol","Time"], prevailing=True).select("sum(Trade_Volume*abs(Trade_Price-(Bid_Price+Offer_Price)/2))/sum(Trade_Volume*Trade_Price)*10000 as cost").groupby("Symbol").executeAs("tradingCost")

print(s.loadTable(tableName="tradingCost").toDF())

# output
  Symbol       cost
0   AAPL   6.367864
1     FB  35.751041
```

### 8.7 `executeAs`

`executeAs`可以把结果保存为DolphinDB中的表对象。
```python
trade = s.loadTable(dbPath="dfs://valuedb", tableName="trade")
trade.select(['date','bid','ask','prc','vol']).where('TICKER=`AMZN').where('bid!=NULL').where('ask!=NULL').where('vol>10000000').sort('vol desc').executeAs("AMZN")
```

使用生成的表：
```python
t1=s.loadTable(tableName="AMZN")
```

### 8.8 回归运算

`ols`用于计算最小二乘回归系数。返回的结果是一个字典。

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

下面的例子在分区数据库中执行回归运算。请注意，在DolphinDB中，两个整数整除的运算符为“/”，恰好是Python的转移字符，因此在`select`中使用VOL\SHROUT。

```python
result = s.loadTable(tableName="US",dbPath="dfs://US").select("select VOL\\SHROUT as turnover, abs(RET) as absRet, (ASK-BID)/(BID+ASK)*2 as spread, log(SHROUT*(BID+ASK)/2) as logMV").where("VOL>0").ols("turnover", ["absRet","logMV", "spread"], True)

```

## 9 Python Streaming API

Python API支持流数据订阅的功能，以下介绍流数据订阅的相关方法与使用示例。

### 9.1 指定订阅端口号

使用Python API提供的`enableStreaming`函数启用流数据功能：
```python
s.enableStreaming(port)
```

- port是指定传入数据的订阅端口，每个session具备唯一的端口。在客户端指定订阅端口号的目的是用于订阅服务器端发送的数据。

示例：

在Python客户端中，导入 DolphinDB Python API，并启用流数据功能，指定订阅端口为8000：
```python
import dolphindb as ddb
import numpy as np
s = ddb.session()
s.enableStreaming(8000)
```

### 9.2 订阅与反订阅

#### 9.2.1 使用订阅函数
使用`subscribe`函数来订阅DolphinDB中的流数据表，语法如下：

```python
s.subscribe(host, port, handler, tableName, actionName="", offset=-1, resub=False, filter=None)
```

- host是发布端节点的IP地址。
- port是发布端节点的端口号。
- handler是用户自定义的回调函数，用于处理每次流入的数据。
- tableName是发布表的名称。
- actionName是订阅任务的名称。
- offset是整数，表示订阅任务开始后的第一条消息所在的位置。消息是流数据表中的行。如果没有指定offset，或它为负数或超过了流数据表的记录行数，订阅将会从流数据表的当前行开始。offset与流数据表创建时的第一行对应。如果某些行因为内存限制被删除，在决定订阅开始的位置时，这些行仍然考虑在内。
- resub是布尔值，表示订阅中断后，是否会自动重订阅。
- filter是一个向量，表示过滤条件。流数据表过滤列在filter中的数据才会发布到订阅端，不在filter中的数据不会发布。

示例：

请注意，发布节点需要配置maxPubConnections参数，具体请参照[DolphinDB流数据教程](https://github.com/dolphindb/Tutorials_CN/blob/master/streaming_tutorial.md)。

在DolphinDB中创建共享的流数据表，指定进行过滤的列为sym，并为5个symbol各插入2条记录共10条记录：
```
share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades
setStreamTableFilterColumn(trades, `sym)
insert into trades values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0, 1..10)
```

在Python中订阅trades表,设置filter为只接收symbol为000905的数据：
```python
def handler(lst):         
    print(lst)

s.subscribe("192.168.1.103",8921,handler,"trades","action",0,False,np.array(['000905']))

# output
[numpy.datetime64('2020-09-24T12:05:53.029'), '000905', 48.3, 1]
[numpy.datetime64('2020-09-24T12:05:53.029'), '000905', 75.4, 6]
```

#### 9.2.2 获取订阅主题

通过`getSubscriptionTopics`函数可以获取所有订阅主题，主题的构成方式是：host/port/tableName/actionName，每个session的所有主题互不相同。

```python
s.getSubscriptionTopics()
# output
['192.168.1.103/8921/trades/action']
```

#### 9.2.3 取消订阅

使用`unsubscribe`取消订阅，语法如下：
```python
s.unsubscribe(host,port,tableName,actionName="")
```

例如，取消示例中的订阅：
```python
s.unsubscribe("192.168.1.103", 8921,"trades","action")
```

**请注意:**，因为订阅是异步执行的，所以订阅完成后需要保持主线程不退出，例如：
```python
from threading import Event     # 加在第一行
Event().wait()                  # 加在最后一行
```
否则订阅线程会在主线程退出前立刻终止，导致无法收到订阅消息。

## 9.2.4 流数据订阅实例

下面的例子通过流数据订阅的方式计算实时K线。

DolphinDB database 中计算实时K线的流程如下图所示：

![avatar](images/K-line.png)

实时数据供应商一般会提供基于Python、Java或其他常用语言的API的数据订阅服务。本例中使用Python来模拟接收市场数据，通过DolphinDB Python API写入流数据表中。DolphinDB的流数据时序聚合引擎(TimeSeriesAggregator)可以对实时数据按照指定的频率与移动窗口计算K线。

本例使用的模拟实时数据源为[文本文件trades.csv](data/k_line/trades.csv)。该文件包含以下4列（一同给出一行样本数据）：

Symbol| Datetime | Price| Volume
---|---|---|---
000001	|2018.09.03T09:30:06	|10.13	|4500


最终输出的K线数据表包含以下7列（一同给出一行样本数据）：

datetime| symbol | open | close | high | low | volume | 
---|---|---|---|---|---|---
2018.09.03T09:30:07|	000001	|10.13|	10.13	|10.12	|10.12	| 468060

### 9.3 流数据应用

本节介绍实时K线计算的三个步骤。

#### 9.3.1 使用 Python 接收实时数据，并写入DolphinDB流数据表

* DolphinDB 中建立流数据表
```
share streamTable(100:0, `Symbol`Datetime`Price`Volume,[SYMBOL,DATETIME,DOUBLE,INT]) as Trade
```

* Python程序从数据源 trades.csv 文件中读取数据写入DolphinDB。

实时数据中Datetime的数据精度是秒，由于pandas DataFrame中仅能使用DateTime[64]即nanatimestamp类型，所以下列代码在写入前有一个数据类型转换的过程。这个过程也适用于大多数数据需要清洗和转换的场景。

```python
import dolphindb as ddb
import pandas as pd
import numpy as np
csv_file = "trades.csv"
csv_data = pd.read_csv(csv_file, dtype={'Symbol':str} )
csv_df = pd.DataFrame(csv_data)
s = ddb.session();
s.connect("192.168.1.103", 8921,"admin","123456")
#上传DataFrame到DolphinDB，并对Datetime字段做类型转换
s.upload({"tmpData":csv_df})
s.run("data = select Symbol, datetime(Datetime) as Datetime, Price, Volume from tmpData;tableInsert(Trade,data) ")
```
这个方法的缺点是，s.upload和s.run涉及两次网络数据传输，有可能会出现网络延迟。可以考虑先在Python端中过滤数据，然后再单步`tableInsert`到服务器端。

```
csv_df=csv_df['Symbol', 'Datetime', 'Price', 'Volume']
s.run("tableInsert{Trade}", csv_df)
```

#### 9.3.2 实时计算K线

本例中使用时序聚合引擎实时计算K线数据，并将计算结果输出到流数据表 OHLC 中。

计算K线数据，按照计算时间窗口是否存在重合分为两种计算场景：一是时间窗口不重合，比如每隔5分钟计算一次过去5分钟的K线数据；二是时间窗口部分重合，比如每隔1分钟计算过去5分钟的K线数据。

可通过设定 `createTimeSeriesAggregator` 函数的 windowSize 和 step 参数以实现这两个场景。场景一 windowSize 与 step 相等；场景二 windowSize 是 step 的倍数。

首先定义输出表:
```
share streamTable(100:0, `datetime`symbol`open`high`low`close`volume,[DATETIME, SYMBOL, DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG]) as OHLC
```
根据应用场景的不同，在以下两行代码中选择一行，以定义时序聚合引擎：

场景一：
```
tsAggrKline = createTimeSeriesAggregator(name="aggr_kline", windowSize=300, step=300, metrics=<[first(Price),max(Price),min(Price),last(Price),sum(volume)]>, dummyTable=Trade, outputTable=OHLC, timeColumn=`Datetime, keyColumn=`Symbol)
```
场景二：
```
tsAggrKline = createTimeSeriesAggregator(name="aggr_kline", windowSize=300, step=60, metrics=<[first(Price),max(Price),min(Price),last(Price),sum(volume)]>, dummyTable=Trade, outputTable=OHLC, timeColumn=`Datetime, keyColumn=`Symbol)
```
最后，定义流数据订阅。若此时流数据表Trade中已经有实时数据写入，那么实时数据会马上被订阅并注入聚合引擎：
```
subscribeTable(tableName="Trade", actionName="act_tsaggr", offset=0, handler=append!{tsAggrKline}, msgAsTable=true)
```

#### 9.3.3 在Python中展示K线数据

在本例中，聚合引擎的输出表也定义为流数据表，客户端可以通过Python API订阅输出表，并将计算结果展现到Python终端。

以下代码使用Python API订阅实时聚合计算的输出结果表OHLC，并将结果通过print函数打印出来。

```python
from threading import Event
import dolphindb as ddb
import pandas as pd
import numpy as np
s=ddb.session()
#设定本地端口20001用于订阅流数据
s.enableStreaming(20001)
def handler(lst):         
    print(lst)
# 订阅DolphinDB(本机8848端口)上的OHLC流数据表
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

也可通过[Grafana](https://github.com/dolphindb/grafana-datasource/blob/master/README_CN.md)等可视化系统来连接DolphinDB database，对输出表进行查询并将结果以图表方式展现。


## 10 更多实例

### 10.1 动量交易策略

下面的例子使用动量交易策略进行回测。最常用的动量因子是过去一年扣除最近一个月的收益率。本例中，每天调整1/5的投资组合，并持有新的投资组合5天。为了简化起见，不考虑交易成本。

**Create server session**

```python
import dolphindb as ddb
s=ddb.session()
s.connect("localhost",8921, "admin", "123456")
```

步骤1：加载股票交易数据，对数据进行清洗和过滤，然后为每只股票构建过去一年扣除最近一个月收益率的动量信号。注意，必须使用`executeAs`把中间结果保存到DolphinDB服务器上。数据集“US”包含了美国股票1990到2016年的交易数据。

```python
US = s.loadTable(dbPath="dfs://US", tableName="US")
def loadPriceData(inData):
    s.loadTable(inData).select("PERMNO, date, abs(PRC) as PRC, VOL, RET, SHROUT*abs(PRC) as MV").where("weekday(date) between 1:5, isValid(PRC), isValid(VOL)").sort(bys=["PERMNO","date"]).executeAs("USstocks")
    s.loadTable("USstocks").select("PERMNO, date, PRC, VOL, RET, MV, cumprod(1+RET) as cumretIndex").contextby("PERMNO").executeAs("USstocks")
    return s.loadTable("USstocks").select("PERMNO, date, PRC, VOL, RET, MV, move(cumretIndex,21)/move(cumretIndex,252)-1 as signal").contextby("PERMNO").executeAs("priceData")

priceData = loadPriceData(US.tableName())
# US.tableName() returns the name of the table on the DolphinDB server that corresponds to the table object "US" in Python. 
```

步骤2：为动量策略生成投资组合

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

步骤3：计算投资组合中每只股票接下来5天的利润或损失。在投资组合形成后的5天后关停投资组合。

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

步骤4：计算投资组合的利润或损失。

```python
portPnl = stockPnL.select("pnl").groupby("date").sum().sort(bys=["date"]).executeAs("portPnl")

print(portPnl.toDF())
```

### 10.2 时间序列计算

下面的例子计算"101 Formulaic Alphas" by Kakushadze (2015)中的98号因子。

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




