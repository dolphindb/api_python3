# TableUpserter

The TableUpserter class enables you to upsert (i.e., update existing or insert new) data to indexed in-memory tables, keyed in-memory tables and DFS tables. Similar to the TableAppender class, when upserting local DataFrames to a table, time values are automatically converted to match the target schema.

**Note:**  Since DolphinDB Python API 1.30.19.4, TableUpserter also supports auto conversion of the special DolphinDB data types such as UUID, INT128, IPADDR, and BLOB.

## Methods

The following script creates a TableUpserter object with default parameter values:

```
TableUpserter(dbPath=None, tableName=None, ddbSession=None, ignoreNull=False, keyColNames=[], sortColumns=[])
```

- **dbPath:** The address to the DFS database. For an in-memory table, leave this parameter unspecified.
- **tableName:** Name of the DFS table, indexed in-memory table, or keyed in-memory table to upsert data to.
- **ddbSession:** *session object.* The session connecting to a DolphinDB server.
- **ignoreNull:** *bool, default False.* If set to true, for the NULL values in the data to be upserted, the corresponding elements in the target table are not updated.
- **keyColNames:** *a list of strings, optional.* When *obj* is a DFS table, *keyColNames* are treated as the key columns.
- **sortColumns:** *a list of strings, optional.* The updated partitions will be sorted on *sortColumns* (only within each partition, not across partitions).

TableUpserter has only one method, `upsert`:

```
upsert(table)
```

- **table:** the data to be upserted into the DolphinDB table. It is usually a local pandas.DataFrame.

## Example 1

In this example, we create a shared in-memory table, *keyed_t*, with the “id“ column as its key. Then construct a TableAppender object to `upsert` test data to *keyed_t*. In the generated test data, the “id“ values repeat 0 through 9 and the “text“ values increment from 0 to 999. Finally, print the updated *keyed_t*.

```
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")


script_KEYEDTABLE = """
    testtable=keyedTable(`id,1000:0,`date`text`id,[DATETIME,STRING,LONG])
    share testtable as keyed_t
    """
s.run(script_KEYEDTABLE)
upserter = ddb.TableUpserter(tableName="keyed_t", ddbSession=s)
dates=[]
texts=[]
ids=[]
for i in range(1000):
    dates.append(np.datetime64('2012-06-13 13:30:10.008'))
    texts.append(f"test_i_{i}")
    ids.append(i%10)
df = pd.DataFrame({
    'date': dates,
    'text': texts,
    'id': ids,
})
upserter.upsert(df)
keyed_t = s.run("keyed_t")
print(keyed_t)
```

Output:

```
                 date        text  id
0 2012-06-13 13:30:10  test_i_990   0
1 2012-06-13 13:30:10  test_i_991   1
2 2012-06-13 13:30:10  test_i_992   2
3 2012-06-13 13:30:10  test_i_993   3
4 2012-06-13 13:30:10  test_i_994   4
5 2012-06-13 13:30:10  test_i_995   5
6 2012-06-13 13:30:10  test_i_996   6
7 2012-06-13 13:30:10  test_i_997   7
8 2012-06-13 13:30:10  test_i_998   8
9 2012-06-13 13:30:10  test_i_999   9
```

For each unique `id` value, only the latest record is kept in *keyed_t*.

## Example 2

To upsert data to a DFS table or a non-indexed in-memory table, the key column(s) must be specified when TableUpserter is constructed.

In this example, we define a DFS table *p_table* partitioned on values of the “flag“ column. Construct a TableUpserter object with the “id“ column as the key column. Then upsert test data to *p_table* using `upsert`. Finally, print the updated *p_table*.

```
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import datetime

s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")
script_DFS_VALUE = """
    if(existsDatabase("dfs://valuedb")){
        dropDatabase("dfs://valuedb")
    }
    db = database("dfs://valuedb", VALUE, 0..9)
    t = table(1000:0, `date`text`id`flag, [DATETIME, STRING, LONG, INT])
    p_table = db.createPartitionedTable(t, `pt, `flag)
"""
s.run(script_DFS_VALUE)
upserter = ddb.TableUpserter(dbPath="dfs://valuedb", tableName="pt", ddbSession=s, keyColNames=["id"])

for i in range(10):
    dates = [np.datetime64(datetime.datetime.now()) for _ in range(100)]
    texts = [f"test_{i}_{_}" for _ in range(100)]
    ids = np.array([ _ % 10 for _ in range(100)], dtype="int32")
    flags = [ _ % 10 for _ in range(100)]
    df = pd.DataFrame({
        'date': dates,
        'text': texts,
        'id': ids,
        'flag': flags,
    })
    upserter.upsert(df)

p_table = s.run("select * from p_table")
print(p_table)
```

Output:

```
                  date       text  id  flag
0  2023-03-16 10:09:33  test_9_90   0     0
1  2023-03-16 10:09:26  test_0_10   0     0
2  2023-03-16 10:09:26  test_0_20   0     0
3  2023-03-16 10:09:26  test_0_30   0     0
4  2023-03-16 10:09:26  test_0_40   0     0
..                 ...        ...  ..   ...
95 2023-03-16 10:09:26  test_0_59   9     9
96 2023-03-16 10:09:26  test_0_69   9     9
97 2023-03-16 10:09:26  test_0_79   9     9
98 2023-03-16 10:09:26  test_0_89   9     9
99 2023-03-16 10:09:26  test_0_99   9     9

[100 rows x 4 columns]
```

**Note:**

- As shown in the result, if the key column (“flag“) contains duplicate values, `upsert` only updates the first record with the same key. 
- In essence, the TableUpserter class encapsulates the DolphinDB server function `upsert!`, allowing a Pandas DataFrame to be passed as an argument and upserted into a DolphinDB table. For more information on the server function `upsert!`, see DolphinDB User Manual - upsert!.