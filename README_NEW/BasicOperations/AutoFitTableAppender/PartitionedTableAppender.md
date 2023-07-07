# PartitionedTableAppender

Similar to the TableAppender class, when you use a PartitionedTableAppender to append data to a DolphinDB table, time values are automatically converted to match the target schema. The PartitionedTableAppender class accepts a connection pool as an argument, and assigns each connection to concurrently append to a partition of the target table by calling the `append()` method. At any given time, only a single connection can write to a specific partition.

## Methods

The following script creates a PartitionedTableAppender object with default parameter values:

```
PartitionedTableAppender(dbPath=None, tableName=None, partitionColName=None, dbConnectionPool=None)
```

- **dbPath:** The address to a DFS database. If the partitioned table is an in-memory table, you can leave this parameter unspecified.
- **tableName:** Name of the partitioned table to append data to. 
- **partitionColName:** *str.* The partitioning column of the target table. Note: If the partitioned table has multiple partitioning columns, any one of the partitioning columns can be used. The specified partitioning column is used to determine connection allocation across partitions.
- **dbConnectionPool:** *DBConnectionPool*. A DBConnectionPool object.

PartitionedTableAppender has only one method, `append`:

```
append(table)
```

- **table:** the data to be appended to the DolphinDB table. It is usually a local pandas.DataFrame.

## Example

In this script, we create a DFS database named *dfs://valuedb* and construct a partitioned table *pt* within it. Then instantiate a DBConnectionPool, *pool*, with 3 connections. Passing this connection pool to the PartitionedTableAppender enables us to concurrently append the locally generated DataFrame across the partitions of `pt`. Finally, print the result of the `append()` call.

```
import dolphindb as ddb
import pandas as pd
import numpy as np
import random

s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")
script = """
    dbPath = "dfs://valuedb"
    if(existsDatabase(dbPath)){
        dropDatabase(dbPath)
    }
    t = table(100:0, `id`date`vol, [SYMBOL, DATE, LONG])
    db = database(dbPath, VALUE, `APPL`IBM`AMZN)
    pt = db.createPartitionedTable(t, `pt, `id)
"""
s.run(script)

pool = ddb.DBConnectionPool("localhost", 8848, 3, "admin", "123456")
appender = ddb.PartitionedTableAppender(dbPath="dfs://valuedb", tableName="pt", partitionColName="id", dbConnectionPool=pool)
n = 100

dates = []
for i in range(n):
    dates.append(np.datetime64(
        "201{:d}-0{:1d}-{:2d}".format(random.randint(0, 9), random.randint(1, 9), random.randint(10, 28))))

data = pd.DataFrame({
    "id": np.random.choice(['AMZN', 'IBM', 'APPL'], n), 
    "time": dates,
    "vol": np.random.randint(100, size=n)
})
re = appender.append(data)

print(re)
print(s.run("pt = loadTable('dfs://valuedb', 'pt'); select * from pt;"))
```

Output:

```
100
      id       date  vol
0   AMZN 2017-01-22   60
1   AMZN 2014-08-12   37
2   AMZN 2012-09-10   68
3   AMZN 2012-03-14   48
4   AMZN 2016-07-12    1
..   ...        ...  ...
95   IBM 2016-05-15   25
96   IBM 2012-06-19    6
97   IBM 2010-05-10   96
98   IBM 2017-07-10   32
99   IBM 2012-09-23   68

[100 rows x 3 columns]
```

