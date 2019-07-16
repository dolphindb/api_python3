## Python API with C++ Implementation (Experimental)

目前支持64位Linux/Windows的Python 3.4~3.7版本，可以使用`pip install dolphindb`来安装，目前是0.1.12版本

*注意*：底层基于DolphinDB C++ API，由于实现原因，暂时无法与Linux平台Jupyter Notebook共同使用，将在后续解决这个问题。

### 特性

1.提供`pip install dolphindb`进行安装

2.读写性能大幅提升，批量上传数据的情况下，性能提升10-30倍；批量下载数据的情况下，性能提升5-10倍，性能受具体用户数据影响

3.更准确的类型映射，原API会把`char short int long`均处理成`numpy.int64`，现在会映射成更准确的numpy类型

4.新增流数据订阅功能，流数据订阅支持过滤

```
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

流数据使用示例：

在Server端定义流表和过滤列，并插入一些测试数据

```
share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades
setStreamTableFilterColumn(trades, `sym)
insert into trades values(take(now(), 10), rand(`ab`cd`ef`gh`ij, 10), rand(1000,10)/10.0, 1..10)
```

在Python API中运行如下脚本进行订阅

```
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

### 以下原API的功能或模块在这个C++版本中**暂不支持**：

1.session.run因网络中断时的重新尝试，对应原API `session._reconnect()`函数

2.session.run因网络中断时的重新尝试，并在连接成功时优先运行初始化脚本，对应原API `session.(set|get)InitScript()`函数

3.指定DolpdhinDB null值在Python里的转换结果，对应原API `session.(set|get)NullMap()`函数，目前所有null值都会转换为`numpy.nan`

4.`session.signon()`函数

5.`session.rpc()`函数

6.date_util.py里定义的各时间类型，现只支持numpy.datetime64作为上传下载的时间类型，例如`session.upload({'time':np.datetime64('2019-01-02', dtype='datetime64[D]')})`

7.pair.py里定义的`Pair`类型，现server端的pair会转成Python里的list

### 其他说明

1.建议大批量上传下载数据，性能更优

2.类型的映射关系（基于numpy）

```
bool    -> BOOL
int8    -> CHAR
int16   -> SHORT
int32   -> INT
int64   -> LONG
float32 -> FLOAT
float64 -> DOUBLE
object  -> STRING or ANY (based on type inference)
datetime64[D]   -> DATE
datetime64[M]   -> MONTH
datetime64[ms]  -> TIME, TIMESTAMP
datetime64[m]   -> MINUTE
datetime64[s]   -> SECOND, DATETIME
datetime64[ns]  -> NANOTIME, NANOTIMESTAMP
```

例如上传一个`INT VECTOR`，原来可以用`conn.upload({'vec': [1, 2, 3, 4]})`并且如果再`conn.run('vec')`会下载回来一个`Python list`

现在建议使用`conn.upload({'vec': np.array([1, 2, 3, 4], dtype='int32')})`并且如果再`conn.run('vec')`会下载回来一个`numpy.array`
