## Python API with C++ Implementation (Experimental)

DolphinDB Python API C++ Version底层基于DolphinDB C++ API实现，目前支持64位Linux/Windows的Python 3.4~3.7版本，通过执行如下指令进行安装。

```Console
$ pip install dolphindb
```
需要注意的是，由于实现原因，暂时无法与Linux平台Jupyter Notebook共同使用，将在后续解决这个问题。

**版本**

v0.1.13.8

**特性**

- 读写性能大幅提升，批量上传数据的情况下，性能提升10-30倍；批量下载数据的情况下，性能提升5-10倍，性能受具体用户数据影响

- 更准确的类型映射，原API会把`char`, `short`, `int`, `long`均处理成`numpy.int64`，现在会映射成更准确的numpy类型

- 新增流数据订阅功能，流数据订阅支持过滤

以下原API的功能或模块在Python API C++ Version中**暂不支持**：

- `session.run`因网络中断时的重新尝试，对应原API`session._reconnect()`函数

- `session.run`因网络中断时的重新尝试，并在连接成功时优先运行初始化脚本，对应原API `session.(set|get)InitScript()`函数

- 指定DolpdhinDB null值在Python里的转换结果，对应原API `session.(set|get)NullMap()`函数，目前所有null值都会转换为`numpy.nan`

- `session.signon()`函数

- `session.rpc()`函数

-  目前不支持导入dolphindb.type_util包：`from dolphindb.type_util import *`，因而也不支持date_util.py里定义的各时间类型，现只支持numpy.datetime64作为上传下载的时间类型，例如`session.upload({'time':np.datetime64('2019-01-02', dtype='datetime64[D]')})`

- pair.py里定义的`Pair`类型，现server端的pair会转成Python里的list

- `Matrix`类型，现server端的matrix会转成Python里的list

下面主要介绍以下内容：

- 建立DolphinDB连接
- 运行DolphinDB脚本
- 运行DolphinDB函数
- 数据的映射关系
- 上传本地对象到DolphinDB服务器
- 读取数据示例
- 读写DolphinDB数据表
- 补充方法说明

### 1. 建立DolphinDB连接

Python API C++ Version 提供的最核心的对象是session。Python 应用通过会话与DolphinDB服务器上执行脚本和函数，并在两者之间双向传递数据。session类提供如下主要方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|connect(host, port, [username, password])|将会话连接到DolphinDB服务器|
|login(username,password,enableEncryption)|登陆服务器|
|run(script)|将脚本在DolphinDB服务器运行|
|run(functionName,args)|调用DolphinDB服务器上的函数|
|upload(variableObjectMap)|将本地数据对象上传到DolphinDB服务器|
|close()|关闭当前会话|

在下面的例子中，通过import语句导入API以后，在Python中创建一个session，然后使用指定的域名或IP地址和端口号把该会话连接到DolphinDB服务器。在执行以下Python脚本前，需要先启动DolphinDB服务器。

```Python
import dolphindb as ddb
s = ddb.session()
s.connect("localhost", 8848)
```

如果需要使用用户名和密码连接DolphinDB，使用以下脚本：

```Python
s.connect("localhost", 8848, YOUR_USER_NAME, YOUR_PASS_WORD)
```

DolphinDB默认的管理员用户名为“admin”，密码为“123456”，并且默认会在连接时对YOUR_USER_NAME和YOUR_PASS_WORD进行加密传输。

### 2. 运行DolphinDB脚本

通过`run(script)`方法运行DolphinDB脚本,如果脚本在DolphinDB中返回对象，`run`会把DolphinDB对象转换成Python中的对象。

```Python
a=s.run("`IBM`GOOG`YHOO")
repr(a)

# output
>>> "array(['IBM', 'GOOG', 'YHOO'], dtype='<U4')"
```

需要注意的是，脚本的最大长度为65,535字节。

### 3. 运行DolphinDB函数

除了运行脚本之外，`run`命令可以直接在远程DolphinDB服务器上执行DolphinDB内置或用户自定义函数。`run`方法的第一个参数DolphinDB中的函数名，第二个参数是要在DolphinDB中调用的函数的参数。

下面的示例展示Python程序通过`run`调用DolphinDB内置的`add`函数。`add`函数有两个参数x和y。参数的存储位置不同，也会导致调用方式的不同。可能有以下三种情况：

* 所有参数都在DolphinDB Server端

若变量x和y已经通过Python程序在服务器端生成，

```Python
s.run("x = [1,3,5];y = [2,4,6]")
```

那么在Python端要对这两个向量做加法运算，只需直接使用`run(script)`即可。

```Python
a=s.run("add(x,y)")
repr(a)

# output
> 'array([ 3,  7, 11], dtype=int32)'
```

* 仅有一个参数在DolphinDB Server端存在

若变量x已经通过Python程序在服务器端生成，

```Python
s.run("x = [1,3,5]")
```

而参数y要在Python客户端生成，这时就需要使用“部分应用”方式，把参数x固化在`add`函数内。具体请参考[部分应用文档](https://www.dolphindb.com/cn/help/PartialApplication.html)。

```Python
import numpy as np

y=np.array([1,2,3])
result=s.run("add{x,}", y)
repr(result)
result.dtype

# output
>>> 'array([2, 5, 8])'
>>> dtype('int64')
```

* 两个参数都待由Python客户端赋值

```Python
import numpy as np

x=np.array([1.5,2.5,7])
y=np.array([8.5,7.5,3])
result=s.run("add", x, y)
repr(result)
result.dtype

# output
>>> 'array([10., 10., 10.])'
>>> dtype('float64')
```

### 4. 数据的映射关系

#### 4.1 数据形式的映射关系

DolphinDB Python API C++ Version 使用Python原生的各种形式的数据对象来存放DolphinDB服务端返回的各种形式的数据，下面给出从DolphinDB的数据对象到Python的数据对象的映射关系。

|DolphinDB|Python|
|:--------|:-----|
|scalar|Numbers, Strings, NumPy.datetime64|
|vector|NumPy.array|
|pair|Lists|
|matrix|Lists|
|set|Sets|
|dictionary|Dictionaries|
|table|Pandas.DataFame|

#### 4.2 数据类型的映射关系

当DolphinDB服务端的返回值形式为一个scalar时，在Python应用程序中也需要针对这个scalar的数据类型进行映射，以下为numpy支持的数据类型与DolphinDB支持的数据类型的对应映射关系。

|Python|DolphinDB|
|:-----|:--------|
|bool|BOOL|
|int8|CHAR|
|int16|SHORT|
|int32|INT|
|int64|LONG|
|float32|FLOAT|
|float64|DOUBLE|
|object|STRING or ANY (based on type inference)|
|datetime64[D]|DATE|
|datetime64[M]|MONTH|
|datetime64[ms]|TIME, TIMESTAMP|
|datetime64[m]|MINUTE|
|datetime64[s]|SECOND, DATETIME|
|datetime64[ns]|NANOTIME, NANOTIMESTAMP|

### 5. 上传本地对象到DolphinDB服务器

#### 5.1 使用`upload`函数上传

Python API C++ Version 提供`upload`函数将Python对象上传到DolphinDB服务器。`upload`函数的输入是Python的字典对象，它的key对应的是DolphinDB中的变量名，value对应的是Python对象，可以是Numbers，Strings，Lists，DataFrame等数据对象。

* 上传 Python list

```Python
a = [1,2,3.0]
s.upload({'a':a})
a_new = s.run("a")
a_type = s.run("typestr(a)")
print(a_new)
print(a_type)

# output
>>> [1. 2. 3.]
>>> ANY VECTOR
```

注意，Python中像a=[1,2,3.0]这种类型的内置list，上传到DolphinDB后，会被识别为any vector。这种情况下，建议使用numpy.array代替内置list，即通过a=numpy.array([1,2,3.0],dtype=numpy.double)指定统一的数据类型，这样上传a以后，a会被识别为double类型的向量。

* 上传 NumPy array

```Python
import numpy as np

arr = np.array([1,2,3.0],dtype=np.double)
s.upload({'arr':arr})
arr_new = s.run("arr")
arr_type = s.run("typestr(arr)")
print(arr_new)
print(arr_type)

# output
>>> [1. 2. 3.]
>>> FAST DOUBLE VECTOR
```

* 上传Pandas DataFrame

```Python
import pandas as pd
import numpy as np

df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.value.avg()"))

# output
>>> 5.44
```
#### 5.2 使用`table`函数上传

在Python中使用`table`函数创建DolphinDB表对象，并上传到server端，`table`函数的输入可以是字典、Dataframe或DolphinDB中的表名。

* 上传dict

下面的例子定义了一个函数`createDemoDict()`，该函数创建并返回一个字典。

```Python
import numpy as np

def createDemoDict():
    return {'id': [1, 2, 2, 3],
            'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
            'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
            'price': [22, 3.5, 21, 26]}
```

通过自定义的函数创建一个字典之后，调用`Table`函数将该字典上传到DolphinDB server端，命名为"testDict"，再通过API提供的`loadTable`函数读取和查看表内数据。

```Python
import numpy as np

# save the table to DolphinDB server as table "testDict"
dt = s.table(data=createDemoDict()).executeAs("testDict")

# load table "testDict" on DolphinDB server 
print(s.loadTable("testDict").toDF())

# output
   id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0
```

* 上传Pandas DataFrame

下面的例子定义了一个函数`createDemoDataFrame()`，该函数创建并返回一个pandas的DataFrame对象，该对象覆盖了DolphinDB提供的所有数据类型。

```Python
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

通过自定义的函数创建一个字典之后，调用`Table`函数将该字典上传到DolphinDB server端，命名为"testDataFrame"，再通过API提供的`loadTable`函数读取和查看表内数据。

```Python
import pandas as pd

# save the table to DolphinDB server as table "testDataFrame"
dt = s.table(data=createDemoDataFrame()).executeAs("testDataFrame")

# load table "testDataFrame" on DolphinDB server 
print(s.loadTable("testDataFrame").toDF())

# output
   cid  cbool  cchar  cshort  ...    cfloat    cdouble csymbol cstring
0    1   True      1       1  ...  2.100000   0.000000       A     abc
1    2  False      2       2  ...  2.658956  47.456213       B     def
2    3   True      3       3  ...       NaN        NaN                
[3 rows x 19 columns]
```

### 6. 读取数据示例

根据4小节的数据映射关系，下面给出读取数据的简单例子。

* scalar

返回BOOL类型的对象

```Python
a = s.run("1b")
type(a)

# output
>>> <class 'bool'>
```

返回INT类型的对象

```Python
a = s.run("5")
type(a)

# output
>>> <class 'int'>
```

返回FLOAT类型的对象

```Python
a = s.run("5f")
type(a)

# output
>>> <class 'float'>
```

返回STRING类型的对象

```Python
a = s.run("`AAA")
type(a)

# output
>>> <class 'str'>
```

返回DATETIME类型的对象

```Python
a = s.run("2019.01.01 08:00:00")
type(a)

# output
>>> <class 'numpy.datetime64'>
```

* vector

```Python
v = s.run("1 2 3")
vType = s.run("typestr(1 2 3)")
print(v)
print(vType)
type(v)

# output
>>> [1 2 3]
>>> FAST INT VECTOR
>>> <class 'numpy.ndarray'>
```

* pair

```Python
p = s.run("1:5")
pType = s.run("typestr(1:5)")
print(p)
print(pType)
type(p)

# output
>>> [1, 5]
>>> INT PAIR
>>> <class 'list'>
```

* matrix

```Python
m = s.run("1..6$2:3")
mType = s.run("typestr(1..6$2:3)")
print(m)
print(mType)
type(m)

# output
>>> [array([[1, 3, 5],
            [2, 4, 6]], dtype=int32), None, None]
>>> FAST INT MATRIX
>>> <class 'list'>
```

* set

```Python
sets = s.run("set(3 5 4 6)")
setsType = s.run("typestr(set(3 5 4 6))")
print(sets)
print(setsType)
type(sets)

# output
>>> {3, 4, 5, 6}
>>> INT SET
>>> <class 'set'>
```

* dictionary

```Python
dic = s.run("dict(`IBM`MS`ORCL, 170.5 56.2 49.5)")
dicType = s.run("typestr(dict(`IBM`MS`ORCL, 170.5 56.2 49.5))")
print(dic)
print(dicType)
type(dic)

# output
>>> {'MS': 56.2, 'IBM': 170.5, 'ORCL': 49.5}
>>> STRING->DOUBLE DICTIONARY
>>> <class 'dict'>
```

* table

```Python
tb = s.run("table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price)")
tbType = s.run("typestr(table(`IBM`MS`ORCL as sym, 170.5 56.2 49.5 as price))")
print(tb)
print(tbType)
type(tb)

# output
>>> sym  price
0   IBM  170.5
1    MS   56.2
2  ORCL   49.5
>>> IN-MEMORY TABLE
>>> <class 'pandas.core.frame.DataFrame'>
```

### 7. 读写DolphinDB数据表

使用Python API C++ Version的一个重要场景是，用户从其他数据库系统或是第三方Web API中取得数据后存入DolphinDB数据库中。本节将介绍通过Python API将取到的数据上传并保存到DolphinDB的数据表中。

DolphinDB数据表按存储方式分为三种:

- 内存表：数据仅保存在内存中，存取速度最快，但是节点关闭后数据就不存在了。
- 本地磁盘表：数据保存在本地磁盘上。可以从磁盘加载到内存。
- 分布式表：数据分布在不同的节点，通过DolphinDB的分布式计算引擎，仍然可以像本地表一样做统一查询。

#### 7.1 保存数据到DolphinDB内存表

DolphinDB提供多种方式来保存数据到内存表：

- 通过`insert into`语句保存数据
- 通过`tableInsert`函数批量保存多条数据

下面分别介绍三种方式保存数据的实例，在例子中使用到的数据表有4个列，分别是INT,DATE，STRINR，DOUBLE类型，列名分别为id, date，ticker和price。
在Python中执行以下脚本，该脚本通过`run`函数在DolphinDB服务器上创建内存表：

```Python
import dolphindb as ddb

s = ddb.session()
s.connect(host, port, "admin", "123456")

# 生成内存表
script = """t = table(1:0,`id`date`ticker`price, [INT,DATE,STRING,DOUBLE])
share t as tglobal"""
s.run(script)
```

上面的例子中，我们通过`table`函数在DolphinDB server端来创建表，指定了表的容量和初始大小、列名和数据类型。由于内存表是会话隔离的，所以普通内存表只有当前会话可见。为了让多个客户端可以同时访问t，我们使用`share`在会话间共享内存表。

#### 7.1.1 使用`INSERT INTO`语句保存数据

我们可以采用如下方式保存单条数据。

```Python
import numpy as np

script = "insert into tglobal values(%s, date(%s), %s, %s);tglobal"% (1, np.datetime64("2019-01-01").astype(np.int64), '`AAPL', 5.6)
s.run(script)
```

也可以使用`INSERT INTO`语句保存多条数据，实现如下:

```Python
import numpy as np
import random

rowNum = 5
ids = np.arange(1, rowNum+1, 1, dtype=np.int32)
dates = np.array(pd.date_range('4/1/2019', periods=rowNum), dtype='datetime64[D]')
tickers = np.repeat("AA", rowNum)
prices = np.arange(1, 0.6*(rowNum+1), 0.6, dtype=np.float64)
s.upload({'ids':ids, "dates":dates, "tickers":tickers, "prices":prices})
script = "insert into tglobal values(ids,dates,tickers,prices);"; 
s.run(script)
```

#### 7.1.2 使用`tableInsert`函数批量保存多条数据

若Python程序获取的数据可以组织成List方式，我们可以直接使用`tableInsert`函数来批量保存多条数据。这个函数可以接受多个数组作为参数，将数组追加到数据表中。这样做的好处是，可以在一次访问服务器请求中将上传数据对象和追加数据这两个步骤一次性完成，相比7.1.1小节中的做法减少了一次访问DolphinDB服务器的请求。

```Python
args = [ids, dates, tickers, prices]
s.run("tableInsert{tglobal}", args)
s.run("tglobal")
```

#### 7.2 保存数据到本地磁盘表

本地磁盘表通用用于静态数据集的计算分析，既可以用于数据的输入，也可以作为计算的输出。它不支持事务，也不持支并发读写。

在Python中执行以下脚本，在DolphinDB server端创建一个本地磁盘表，使用`database`函数创建数据库，调用`saveTable`函数将内存表保存到磁盘中：

```Python
import dolphindb as ddb

s = ddb.session()
s.connect(host, port, "admin", "123456")

# 生成磁盘表
script = """t = table(100:0, `id`date`ticker`price, [INT,DATE,STRING,DOUBLE]); 
db = database('/home/dolphindb/testPython'); 
saveTable(db, t, `dt); 
share t as tDiskGlobal;"""
s.run(script)
```

使用`tableInsert`函数是向本地磁盘表追加数据最为常用的方式。这个例子中，我们使用`tableInsert`向共享的内存表tDiskGlobal中插入数据，接着调用`saveTable`使插入的数据保存到磁盘上。请注意，对于本地磁盘表，`tableInsert`函数只把数据追加到内存，如果要保存到磁盘上，必须再次执行`saveTable`函数。

```Python
args = [ids, dates, tickers, prices]
s.run("tableInsert{tDiskGlobal}", args)
s.run("saveTable(db,tDiskGlobal,`dt);")
```

#### 7.3 保存数据到分布式表

分布式表是DolphinDB推荐在生产环境下使用的数据存储方式，它支持快照级别的事务隔离，保证数据一致性。分布式表支持多副本机制，既提供了数据容错能力，又能作为数据访问的负载均衡。下面的例子通过Python API C++ Version把数据保存至分布式表。

请注意只有启用enableDFS=1的集群环境才能使用分布式表。

在DolphinDB中使用以下脚本创建分布式表，脚本中，`database`函数用于创建数据库，对于分布式数据库，路径必须以“dfs”开头。`createPartitionedTable`函数用于创建分区表。

```Python
import dolphindb as ddb

s = ddb.session()
s.connect(host, port, "admin", "123456")

# 生成分布式表
script = """if(existsDatabase('dfs://testPython')){
	dropDatabase('dfs://testPython')
	}
db = database('dfs://testPython', VALUE, 0..100)
t1 = table(10000:0,`id`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring,[INT,BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING])
insert into t1 values (0,true,'a',122h,21,22l,2012.06.12,2012.06M,13:10:10.008,13:30m,13:30:10,2012.06.13 13:30:10,2012.06.13 13:30:10.008,13:30:10.008007006,2012.06.13 13:30:10.008007006,2.1f,2.1,'','')
t = db.createPartitionedTable(t1, `t1, `id)
t.append!(t1)"""
s.run(script)
```

DolphinDB提供`loadTable`方法来加载分布式表，通过`tableInsert`方式追加数据，具体的脚本示例如下：

```Python
tb = createDemoDataFrame()
s.run("tableInsert{loadTable('dfs://testPython', `t1)}", tb)
```

把数据保存到分布式表，还可以使用`append!`函数，它可以把一张表追加到另一张表。但是，一般不建议通过append!函数保存数据，因为`append!`函数会返回一个表结构，增加通信量。

```Python
tb = createDemoDataFrame()
s.upload({"tb": tb})
s.run("loadTable('dfs://testPython', `t1).append!(tb);")
```

### 8. 补充方法说明

#### 8.1 `Session`类方法

除了以上介绍过的常用方法之外，`Session`类还提供了一些与DolphinDB内置函数作用等同的方法，具体如下：

| 方法名        | 详情          |
|:------------- |:-------------|
|database|创建数据库|
|table|创建表|
|loadTable|加载本地磁盘表或者分布式表到内存|
|dropDatabase(dbPath)|删除数据库|
|dropPartition(dbPath, partitionPaths)|删除数据库的某个分区|
|dropTable(dbPath, tableName)|删除数据库中的表|
|existsTable|判断是否存在表|
|existsDatabase|判断是否存在数据库|

**请注意**，Python API实质上封装了DolphinDB的脚本语言。Python代码被转换成DolphinDB脚本在DolphinDB服务器执行，执行结果保存到DolphinDB服务器或者序列化到Python客户端。例如，在Python客户端创建一个数据表时，有如下几种方式：

1.调用`Session`类提供的`table`方法：

```Python
tdata = {'id': [1, 2, 2, 3],
         'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
         'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
         'price': [22, 3.5, 21, 26]}
s.table(data=tdata).executeAs('tb')
```

2.调用`Session`类提供的`upload`方法：

```Python
tdata = pd.DataFrame({'id': [1, 2, 2, 3], 
                      'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
                      'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'], 
                      'price': [22, 3.5, 21, 26]})
s.upload({'tb': tdata})
```

3.调用`Session`类提供的`run`方法：
```Python
s.run("tb=table([1, 2, 2, 3] as id, [2019.02.04,2019.02.05,2019.02.09,2019.02.13] as date, ['AAPL','AMZN','AMZN','A'] as ticker, [22, 3.5, 21, 26] as price)")
```

以上3种方式都等价于在DolphinDB服务端调用`table`方法创建一个名为'tb'的内存数据表：

```
tb=table([1, 2, 2, 3] as id, [2019.02.04,2019.02.05,2019.02.09,2019.02.13] as date, ['AAPL','AMZN','AMZN','A'] as ticker, [22, 3.5, 21, 26] as price)
```

下面，我们在Python环境中调用`Session`类提供的各种方法创建分布式数据库和表，并向表中追加数据。

```Python
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np

s = ddb.session()
s.connect(HOST, PORT, "admin", "123456")

if s.existsDatabase("dfs://testDB"):
    s.dropDatabase("dfs://testDB")
s.database('db', keys.VALUE, ["AAPL", "AMZN", "A"], "dfs://testDB")
tdata=s.table(data=createDemoDict()).executeAs("testDict")
s.run("db.createPartitionedTable(testDict, `tb, `ticker)")
tb=s.loadTable("tb", "dfs://testDB")
tb.append(tdata)
tb.toDF()

# output
>>> id       date ticker  price
 0   3 2019-02-13      A   26.0
 1   1 2019-02-04   AAPL   22.0
 2   2 2019-02-05   AMZN    3.5
 3   2 2019-02-09   AMZN   21.0
```

类似地，我们也可以在Python环境中直接调用`Session`类提供的`run`方法来创建数据库和表：先通过`table`函数上传表对象，再调用`run`方法执行DolphinDB脚本。

```Python
import dolphindb as ddb
import numpy as np

s = ddb.session()
s.connect(HOST, PORT, "admin", "123456")

s.table(data=createDemoDict()).executeAs("testDict")
script="""if(existsDatabase("dfs://testDB"))
              dropDatabase("dfs://testDB")
          db=database("dfs://testDB", VALUE, ["AAPL", "AMZN", "A"])
          tb=db.createPartitionedTable(testDict, `tb, `ticker)
          tb.append!(testDict)
          select * from loadTable("dfs://testDB", `tb)"""
s.run(script)
```

上述两个例子等价于在DolphinDB服务端执行以下脚本，创建分布式数据库和表，并向表中追加数据。

```
login("admin","123456")
testDict=table([1, 2, 2, 3] as id, [2019.02.04,2019.02.05,2019.02.09,2019.02.13] as date, ['AAPL','AMZN','AMZN','A'] as ticker, [22, 3.5, 21, 26] as price)
if(existsDatabase("dfs://testDB"))
    dropDatabase("dfs://testDB")
db=database("dfs://testDB", VALUE, ["AAPL", "AMZN", "A"])
tb=db.createPartitionedTable(testDict, `tb, `ticker)
tb.append!(testDict)
```

关于`Session`类提供的所有方法，请参照session.py文件。

#### 8.2 `Table`类方法

`Table`类方法即Python应用程序中的DolphinDB表对象的方法，我们在Python中得到一个表对象以后，可以调用如下方法：

| 方法名        | 详情          |
|:------------- |:-------------|
|toDF()|把DolphinDB表对象转换成pandas的DataFrame对象。|
|executeAs(tableName)|执行结果保存为指定表名的内存表。|
|execute()|执行脚本。与`update`和`delete`一起使用。|
|drop(colNameList)|删除表中的某列。|
|append|向表中追加数据|
|ols(Y, X, intercept)|计算普通最小二乘回归，返回结果是一个字典。|

下面给出几个具体的例子：

* `toDF()`

如下所示，调用`toDF`函数将DolphinDB表对象转换成pandas的DataFrame对象。

```Python
tdata={'id': [1, 2, 2, 3],
       'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
       'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
       'price': [22, 3.5, 21, 26]}
tb=s.table(data=tdata)
tb.toDF()

# output
>>>id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0
```

* `executeAs()`

如下所示，对一个DolphinDB表对象执行`executeAs()`，相当于在Dolphindb Server端定义了一个内存表tb。

```Python
tdata={'id': [1, 2, 2, 3],
       'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
       'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
       'price': [22, 3.5, 21, 26]}
tb=s.table(data=tdata)
tb.executeAs('tb')
s.run("tb")

# output
>>>id       date ticker  price
0   1 2019-02-04   AAPL   22.0
1   2 2019-02-05   AMZN    3.5
2   2 2019-02-09   AMZN   21.0
3   3 2019-02-13      A   26.0
```

`Table`类的其它方法本质上也是对DolphinDB脚本语言进行的封装。以上只是列出其中几个方法，关于`Table`类提供的所有方法请参见table.py文件。

Python Streaming API
---

Python API支持流数据订阅的功能，下面简单介绍一下流数据订阅的功能。

```Python
# 启用流数据功能，订阅的数据通过port端口传入，每个session有唯一的port
session.enableStreaming(port)

# 订阅流表
# host和port代表发布流数据的DolphinDB Server
# handler代表处理流数据的Python函数，流数据每一行构成一个list传给handler
# tableName代表被订阅的流表名字，actionName代表订阅动作，offset代表订阅起始行数
# resub代表若因网络异常导致流数据中断，是否自动尝试从上一条收到的数据开始重新订阅
# filter代表过滤条件，符合filter的数据才会发布到handler
session.subscribe(host, port, handler, tableName, actionName, offset, resub, filter)
session.unsubscribe(host, port, tableName, actionName)

# 获取所有订阅主题，主题的构成方式是：host/port/tableName/actionName，每个session的所有主题互不相同
session.getSubscriptionTopics()
```

**流数据使用示例**：

在DolphinDB Server端定义流表和过滤列，并插入一些测试数据，

```
share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades
setStreamTableFilterColumn(trades, `sym)
insert into trades values(take(now(), 10), rand(`ab`cd`ef`gh`ij, 10), rand(1000,10)/10.0, 1..10)
```

在Python应用程序中中运行如下脚本进行订阅，

```Python
>>> import dolphindb as ddb
>>> import numpy as np
>>> s = ddb.session()
>>> s.enableStreaming(9000)   # 每个session都可以指定一个端口用于接收流数据
>>> def handler(lst):         # 定义流数据的处理函数，每一行新的流数据都会调用处理函数，传入Python的list对象
...     print(lst)
...
>>> s.subscribe("192.168.1.109",8848,handler,"trades","action",0,False,np.array(['ab']))
[numpy.datetime64('2019-04-15T15:19:51.403'), 'ab', 53.6, 1]       # 流数据处理函数会在另一个线程中被调用
[numpy.datetime64('2019-04-15T15:19:51.403'), 'ab', 50.7, 9]
>>> s.getSubscriptionTopics()
['192.168.1.109/8848/trades/action']
>>> s.unsubscribe("192.168.1.109",8848,"trades","action")          # 停止订阅
```

**请注意:**，因为订阅是异步执行的，所以订阅完成后需要保持主线程不退出，比如：

```py
from threading import Event     # 加在第一行
Event().wait()                  # 加在最后一行
```
否则订阅线程会在主线程退出前立刻终止，导致无法收到订阅消息。