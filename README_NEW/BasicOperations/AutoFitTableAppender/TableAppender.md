# TableAppender

The data types in Python and DolphinDB do not correspond one-to-one. For example, Python DataFrames only support the temporal data type datetime64\[ns]. If a DataFrame is uploaded to DolphinDB, all time values will be converted into the DolphinDB NANOTIMESTAMP type. When you append the DataFrame to a table, the data type of these time values must be manually converted to fit the schema of the target table.

Therefore, the Python API provides the TableAppender class to simplify the uploading and appending of DataFrames. With the `append` method of TableAppender, local DataFrames can be appended to DolphinDB in-memory tables or DFS tables with automatic data type conversion.

**Note:**

1. For DolphinDB Python API 1.30.19.4 and earlier versions, TableAppender only converted the data types of temporal values. Starting from 1.30.21.1, conversion of all data types are supported.
2. Since DolphinDB server 1.30.16/2.00.4, the built-in function `tableInsert` supports automatic data type conversion of temporal values. You can use this function as an alternative when inserting data. For details, see Section xxxxxxxxx.
3. For DolphinDB Python API 1.30.22.1 and later versions, the class name “tableAppender“ has been renamed to “TableAppender“. The old name can still be used.

## 1. Methods

The following script creates a TableAppender object with default parameter values:

```
TableAppender(dbPath=None, tableName=None, ddbSession=None, action="fitColumnType")
```

- **dbPath:** The address to the DFS database. For an in-memory table, leave this parameter unspecified.
- **tableName:** Name of the DFS table or in-memory table to append data to.
- **ddbSession:** *session object.* The session connecting to a DolphinDB server.
- **action:** The action to take when appending data to table. Currently, only the value “*fitColumnType*” is supported, which means to convert the data types of columns*.*

TableAppender has only one method, `append`:

```
append(table)
```

- **table:** the data to be appended to the DolphinDB table. It is usually a local pandas.DataFrame.

This method returns an integer indicating the number of records appended.

## 2. Example

In the following script, we use a TableAppender to `append` data to the shared table *t*. 

```
import pandas as pd
import dolphindb as ddb
import numpy as np
s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")


s.run("share table(1000:0, `sym`timestamp`qty, [SYMBOL, TIMESTAMP, INT]) as t")
appender = ddb.TableAppender(tableName="t", ddbSession=s)
data = pd.DataFrame({
    'sym': ['A1', 'A2', 'A3', 'A4', 'A5'], 
    'timestamp': np.array(['2012-06-13 13:30:10.008', 'NaT','2012-06-13 13:30:10.008', '2012-06-13 15:30:10.008', 'NaT'], dtype="datetime64[ms]"), 
    'qty': np.arange(1, 6).astype("int32"),
})
num = appender.append(data)
print("append rows: ", num)
t = s.run("t")
print(t)
schema = s.run("schema(t)")
print(schema["colDefs"])
```

Output:

```
append rows:  5
  sym               timestamp  qty
0  A1 2012-06-13 13:30:10.008    1
1  A2                     NaT    2
2  A3 2012-06-13 13:30:10.008    3
3  A4 2012-06-13 15:30:10.008    4
4  A5                     NaT    5
        name typeString  typeInt  extra comment
0        sym     SYMBOL       17    NaN        
1  timestamp  TIMESTAMP       12    NaN        
2        qty        INT        4    NaN   
```

The "timestamp" column of *t* is defined as TIMESTAMP type in DolphinDB, whereas the "timestamp" column of the pd.DataFrame object is of datetime64\[ns] type in Python. The result confirms that TableAppender implicitly converted the DataFrame's "timestamp" column to DolphinDB's TIMESTAMP type.

This automatic type conversion ensured the data uploaded correctly and matched the schema of table *t*.

## 3. FAQs

### 3.1 Automatic Data Type Conversion 

Starting from DolphinDB server 1.30.16/2.00.4, the built-in function `tableInsert` ensures that **inserted time values always match the schema of the target in-memory table.** Therefore, you can call `tableInsert` through the API `run` method to append an uploaded DataFrame to the target DolphinDB table, without using the TableAppender class. 

**What data types will be auto converted?**

Prior to DolphinDB Python API version 1.30.19.4, as the DolphinDB data types UUID, INT128, IPADDR, and BLOB are all represented by the Python str type, these values cannot be directly uploaded and inserted into a DolphinDB table. 

Starting from API version 1.30.19.4, TableAppender ensures that the appended data matches the schema of the target table. If the data to be appended contains the aforementioned special data types (or temporal data types), the TableAppender will convert these strings (or time values) to the appropriate data types as defined by the schema of the target table. For details on the data type conversions during data insertion, see [Data Type Casting](../../AdvancedOperations/DataTypeCasting/TypeCasting.md).

```
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd

s = ddb.Session(protocol=keys.PROTOCOL_DDB)
s.connect("192.168.1.113", 8848, "admin", "123456")

s.run("share table(1000:0, `sym`uuid`int128`ipaddr`blob, [SYMBOL, UUID, INT128, IPADDR, BLOB]) as t")
appender = ddb.TableAppender(tableName="t", ddbSession=s)
data = pd.DataFrame({
    'sym': ["A1", "A2", "A3"],
    'uuid': ["5d212a78-cc48-e3b1-4235-b4d91473ee87", "b93b8253-8d5e-c609-260a-86522b99864e", ""],
    'int128': [None, "073dc3bc505dd1643d11a4ac4271d2f2", "e60c84f21b6149959bcf0bd6b509ff6a"],
    'ipaddr': ["2c24:d056:2f77:62c0:c48d:6782:e50:6ad2", "", "192.168.1.0"],
    'blob': ["testBLOB1", "testBLOB2", "testBLOB3"],
})

appender.append(data)

t = s.run("t")
print(t)
```

**Note:** In this example, as we need to download and print BLOB data in Python, PROTOCOL_DDB is used during session construction instead of PROTOCOL_PICKLE, the default protocol for object serialization.  

Output:

```
  sym                                  uuid                            int128                                  ipaddr       blob
0  A1  5d212a78-cc48-e3b1-4235-b4d91473ee87  00000000000000000000000000000000  2c24:d056:2f77:62c0:c48d:6782:e50:6ad2  testBLOB1
1  A2  b93b8253-8d5e-c609-260a-86522b99864e  073dc3bc505dd1643d11a4ac4271d2f2                                 0.0.0.0  testBLOB2
2  A3  00000000-0000-0000-0000-000000000000  e60c84f21b6149959bcf0bd6b509ff6a                             192.168.1.0  testBLOB3
```

### 3.2 Pandas Warning

For DolphinDB Python API 1.30.19.4 and later versions, a warning may be raised when you append data using `TableAppender.append()`:

```
UserWarning: Pandas doesn't allow columns to be created via a new attribute name - see https://pandas.pydata.org/pandas-docs/stable/indexing.html#attribute-access
```

This warning does not have any impact on the execution of scripts. To ignore this warning, execute the following script:

```
import warnings
warnings.filterwarnings("ignore","Pandas doesn't allow columns to be created via a new attribute name - see https://pandas.pydata.org/pandas-docs/stable/indexing.html#attribute-access", UserWarning)
```

