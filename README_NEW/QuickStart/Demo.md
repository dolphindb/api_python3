# Getting Started

This section provides a short demo to help you get started with the DolphinDB Python API. 

By the end of this section, you will be able to connect to a standalone DolphinDB server and interact with it using the DolphinDB Python API to perform database operations.

## Prerequisites

### Python Environment

Make sure you have a Python Environment set up. For more information on Python, see [Python Tutorial](https://docs.python.org/3/tutorial/).

### Standalone DolphinDB Server 

Download the DolphinDB server from

https://dolphindb.com/index.php. Deploy the DolphinDB service by following the [DolphinDB Standalone Deployment](https://github.com/dolphindb/Tutorials_EN/blob/master/standalone_deployment.md) tutorial.

For a quick start guide on DolphinDB server deployment, see [DolphinDB Deployment on a New Server](https://github.com/dolphindb/Tutorials_EN/blob/master/deploy_dolphindb_on_new_server.md).

For more deployment options, see [DolphinDB User Guide](https://github.com/dolphindb/Tutorials_EN/blob/master/dolphindb_user_guide.md).

## Connecting to Server

Sessions facilitate communication between the client and the DolphinDB server. Through sessions, you can execute scripts and functions on the DolphinDB server and transfer data in both directions.

**Note:** Starting from version 1.30.22.1 of the DolphinDB Python API, the class previously known as "session" has been renamed to "Session". To ensure backward compatibility, the old class name can still be used as an alias.

**Example**

In this example, we first import *dolphindb*, then create a Session object in Python. By specifying the domain (or IP address) and port number, the session connects to a DolphinDB server. We then run a simple script on DolphinDB using this connection.

```
>>> import dolphindb as ddb
>>> s = ddb.Session()
>>> s.connect("localhost", 8848)
True
>>> s.run("1+1;")
2
>>> s.close()
```

Note:

- Before initiating the connection, make sure the DolphinDB server has been started.
- It is recommended to explicitly close the session once you are done with it by calling `close()`. Otherwise other sessions may fail to connect due to too many open connections.

## Interacting with Server

The DolphinDB Python API supports various ways for the client to interact with the DolphinDB server. This section briefly introduces how to upload and download data with the `run` and `upload` methods. For more information, see [Session Methods](../BasicOperations/Session/OtherParams.md) 和 [DBConnectionPool Methods](../BasicOperations/DBConnectionPool/AsyncMethodsAndOthers.md).

DolphinDB supports various data forms, which are mapped to Python objects as follows:

| DolphinDB Data Form | Python                                        |
| :------------------ | :-------------------------------------------- |
| Scalar              | int/str/float/...                             |
| Vector              | numpy.ndarray                                 |
| Pair                | list                                          |
| Matrix              | \[numpy.ndarray, numpy.ndarray, numpy.ndarray] |
| Set                 | set                                           |
| Dictionary          | dict                                          |
| Table               | pandas.DataFrame                              |

**Note**

- DolphinDB matrices can have row and column names. Therefore, matrices downloaded from DolphinDB are converted into Python list objects comprising a 2D array containing data values, 1D array of row names, and 1D array of column names. 
- Depending on how DolphinDB objects are retrieved by the client, they can be mapped to various Python data types. For more information, see [Data Type Conversion](../AdvancedOperations/DataTypeCasting/TypeCasting.md).

First import *dolphindb* in Python. Then create a Session object and connect it to a DolphinDB server.

```
>>> import dolphindb as ddb
>>> s = ddb.Session()
>>> s.connect("localhost", 8848)
True
```

### Downloading Data to Client

The following examples demonstrate creating and downloading various DolphinDB data forms from the server using the `s.run()` method. The downloaded values are returned once the operation completes successfully. 

**Scalar**

```
>>> s.run("1;")
1
```

**Vector**

```
>>> s.run("1..10;") 
array([ 1,  2,  3,  4,  5,  6,  7,  8,  9, 10], dtype=int32)
```

**Pair**

```
>>> s.run("1:5;") 
[1, 5]
```

**Matrix**

```
>>> s.run("1..6$2:3;")
[array([[1, 3, 5],
       [2, 4, 6]], dtype=int32), None, None]
```

**Set**

```>>> s.run("set([1,5,9]);")
>>> s.run("set([1,5,9]);")
{1, 5, 9}
```

**Dictionary**

```>>> s.run("dict(1 2 3, 4.5 7.8 4.3);")
>>> s.run("dict(1 2 3, 4.5 7.8 4.3);")
{2: 7.8, 1: 4.5, 3: 4.3}
```

**Table**

```
>>> s.run("table(`XOM`GS`AAPL as id, 102.1 33.4 73.6 as x);")
     id      x
0   XOM  102.1
1    GS   33.4
2  AAPL   73.6
```

### Uploading Data to Server

Import the *numpy* and *pandas* libraries:

```
>>> import numpy as np
>>> import pandas as pd
```

The following examples show how to upload Python objects to DolphinDB as various data forms using `s.upload`. Arguments are passed to `s.upload` in the format `{variableName}:{data}`. The variable's address assigned by the server is returned upon successful completion. Use the `typestr` server function to verify its data form in DolphinDB, and `s.run` to retrieve it back to Python. 

Note that uploading pairs through Python API is not currently supported.

**Scalar**

```
>>> s.upload({'scalar_sample': 1})
62776640
>>> s.run("typestr(scalar_sample);")
'LONG'
>>> s.run("scalar_sample;")
1
```

**Vector**

```
>>> s.upload({'vector_sample': np.array([1, 3])})
65583680
>>> s.run("typestr(vector_sample);")
'FAST LONG VECTOR'
>>> s.run("vector_sample;")
array([1, 3])
```

**Matrix**

```
>>> s.upload({'matrix_sample': np.array([[1, 2, 3], [4, 5, 6]])})
65484832
>>> s.run("typestr(matrix_sample);")
'FAST LONG MATRIX'
>>> s.run("matrix_sample;")
[array([[1, 2, 3],
       [4, 5, 6]]), None, None]
```

**Set**

```
>>> s.upload({'set_sample': {1, 4, 7}})
65578432
>>> s.run("typestr(set_sample);")
'LONG SET'
>>> s.run("set_sample;")
{1, 4, 7}
```

**Dictionary**

```
>>> s.upload({'dict_sample': {'a': 1}})
58318576
>>> s.run("typestr(dict_sample);")
'STRING->LONG DICTIONARY'
>>> s.run("dict_sample;")
{'a': 1}
```

**Table**

```
>>> df = pd.DataFrame({'a': [1, 2, 3], 'b': ['a', 'b', 'c']})
>>> s.upload({'table_sample': df})
63409760
>>> s.run("typestr(table_sample);")
'IN-MEMORY TABLE'
>>> s.run("table_sample;")
   a  b
0  1  a
1  2  b
2  3  c
```

## Database Operations

You can use the following options to operate on the DolphinDB server using DolphinDB Python API:

- Execute DolphinDB scripts by calling `Session.run()`
- Call methods provided by the API 

**Option 1: Execute DolphinDB Scripts with** `Session.run()`

In this example, when connecting to the DolphinDB server, we need to log in as a DolphinDB admin (or a user with sufficient privileges) for database operations.

Call `s.run()` to create variables, a database and a partitioned table on the server, then append data to the partitioned table.

Then call `s.run()` again to execute a SQL query that returns the record count of the partitioned table following the insertion.

```
import dolphindb as ddb
s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")
s.run("""
    n=1000000
    ID=rand(10, n)
    x=rand(1.0, n)
    t=table(ID, x)
    db=database(directory="dfs://hashdb", partitionType=HASH, partitionScheme=[INT, 2])
    pt = db.createPartitionedTable(t, `pt, `ID)
    pt.append!(t);
""")
re = s.run("select count(x) from pt;")
print(re)

# output
   count_x
0  1000000
```

**Option 2: Calling API Methods**

In this example, when connecting to the DolphinDB server, we need to log in as a DolphinDB admin (or a user with sufficient privileges) for database operations. By calling `Session.table`, `Session.database`, and `Database.createPArtitionedTable`, we create an in-memory table, a DFS database “db“ with the predefined schema “schema_t“ and a partitioned table “pt“. Then append the uploaded data to the DFS table with `pt.append()`. Finally, download the partitioned table to Python using `Table.toDF()`.

```
import pandas as pd
import numpy as np
import dolphindb as ddb
import dolphindb.settings as keys

s = ddb.Session("192.168.1.113", 8848, "admin", "123456")
n = 1000000
df = pd.DataFrame({
    'ID':   np.random.randint(0, 10, n),
    'x':    np.random.rand(n),
})
s.run("schema_t = table(100000:0, `ID`x,[INT, DOUBLE])")
schema_t = s.table(data="schema_t")
if s.existsDatabase("dfs://hashdb"):
    s.dropDatabase("dfs://hashdb")
db = s.database(dbPath="dfs://hashdb", partitionType=keys.HASH, partitions=[keys.DT_INT, 2])
pt: ddb.Table = db.createPartitionedTable(table=schema_t, tableName="pt", partitionColumns=["ID"])
data = s.table(data=df)
pt.append(data)
print(pt.toDF())

# output
        ID         x
0        4  0.320935
1        8  0.426056
2        8  0.505221
3        4  0.692984
4        4  0.709175
...     ..       ...
999995   5  0.479531
999996   3  0.805629
999997   5  0.873164
999998   7  0.158090
999999   5  0.530824

[1000000 rows x 2 columns]
```



