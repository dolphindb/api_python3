## Python API with C++ Implementation (Experimental)

目前提供Python 3.6和3.7的版本，可以使用`pip install dolphindb`来安装

### 特性

1.提供`pip install dolphindb`进行安装，目前提供Python 3.6和3.7的Windows和Linux版本

由于使用了C++扩展等一些原因，目前Windows平台需要在Anaconda下使用新插件，在conda prompt中使用`pip install dolphindb`进行安装

2.python API底层用C++改写，读写性能大幅提升，批量上传数据的情况下，性能提升10-30倍；批量下载数据的情况下，性能提升5-10倍；性能改善取决于用户数据

3.更准确的类型映射，原API会把`char short int long`均处理成`numpy.int64`，现在会映射成更准确的numpy类型

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
