# Asynchronous Task Submission

For high throughput data processing, especially frequent writes of small data volumes, you can enable the asynchronous mode to submit tasks asynchronously. Asynchronous task submission has the following characteristics:

- Once a task is submitted, the task is deemed completed by the API client as soon as the server receives it.
- The client cannot obtain the status or result of the task executed on the server.
- The total time to submit asynchronous tasks depends on the time taken for parameter serialization and network transmission.

**Note**: The asynchronous mode is unsuitable when there are dependencies between consecutive tasks. For example, if one task writes data to a DFS database and the next task analyzes the written data against historical data, the asynchronous mode would not work.

To enable asynchronous mode, specify the *enableASYNC* parameter as True when constructing a session (see [Creating a Session](../Session/Constructor.md)). By writing data in asynchronous mode, the time for the API to detect task return is saved.

```
s = ddb.Session(enableASYNC=True)
```

In this example, we append data to a partitioned DFS table in asynchronous mode:

```
import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd

s = ddb.Session(enableASYNC=True) # enable asynchronous mode
s.connect("localhost", 8848, "admin", "123456")
dbPath = "dfs://testDB"
tbName = "tb1"


script = """
    dbPath="dfs://testDB"
    tbName=`tb1
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath, VALUE, ["AAPL", "AMZN", "A"])
    testDictSchema=table(5:0, `id`ticker`price, [INT,SYMBOL,DOUBLE])
    tb1=db.createPartitionedTable(testDictSchema, tbName, `ticker)
"""
s.run(script)   #run script on server       

tb = pd.DataFrame({
    'id': np.array([1, 2, 2, 3], dtype="int32"),
    'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
    'price': [22, 3.5, 21, 26],
})

s.run(f"append!{{loadTable('{dbPath}', `{tbName})}}", tb)
```

**Note:** In asynchronous mode, only the `Session.run()` method can be used to communicate with the DolphinDB server. **There is no return value for each task in asynchronous mode.**

## Example 1

The asynchronous mode is optimal for high-throughput scenarios. In this example, we asynchronously write data to a stream table in using the Python API:

```
import dolphindb as ddb

import numpy as np
import pandas as pd
import random
import datetime

s = ddb.Session(enableASYNC=True)
s.connect("localhost", 8848, "admin", "123456")

n = 100

script = """
    share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades
"""
s.run(script) #run script on server

# create a DataFrame
time_list = [np.datetime64(datetime.date(2020, random.randint(1, 12), random.randint(1, 20))) for _ in range(n)]
sym_list = np.random.choice(['IBN', 'GTYU', 'FHU', 'DGT', 'FHU', 'YUG', 'EE', 'ZD', 'FYU'], n)
price_list = [round(np.random.uniform(1, 100), 1) for _ in range(n)]
id_list = np.random.choice([1, 2, 3, 4, 5], n)

tb = pd.DataFrame({
    'time': time_list,
    'sym': sym_list,
    'price': price_list,
    'id': id_list,
})

for _ in range(50000):
    s.run("tableInsert{trades}", tb)
```

To measure script execution time, use the `time` command on Linux and PowerShell's `Measure-Command` on Windows. 

In this example, the `time` command is used: In synchronous mode (`enableAsync=False`), the execution time of the script was 8.39 seconds; In asynchronous mode (`enableAsync=Trie`), the execution time was 4.89 seconds. 

In synchronous mode, frequent writes of small data volumes can cause significant network overhead. The asynchronous mode addresses this issue by uploading data to the server without awaiting a response, optimizing overall performance for slow networks and frequent write tasks. However, with the asynchronous approach, failed writes may go undetected as errors are not raised upon failure.

## Example 2

When appending time values requiring type conversion in asynchronous mode, it is not viable to convert them with SQL in DolphinDB after `upload()`. This is because in asynchronous mode, SQL execution may begin before all data is uploaded. To address this, we recommend defining a function view in DolphinDB beforehand. By calling the function view during the append, type conversion is performed seamlessly.

In this example, we first define a function view `appendStreamingData` in DolphinDB for inserting data to a stream table with auto type conversion:

```
login("admin","123456")
share streamTable(10000:0,`time`sym`price`id, [DATE,SYMBOL,DOUBLE,INT]) as trades
def appendStreamingData(mutable data){
    tableInsert(trades, data.replaceColumn!(`time, date(data.time)))
}
addFunctionView(appendStreamingData)
```

Then, in the Python client, append data to the DolphinDB stream table in asynchronous mode:

```
import dolphindb as ddb
import numpy as np
import pandas as pd
import random
import datetime

s = ddb.Session(enableASYNC=True)
s.connect("localhost", 8848, "admin", "123456")

n = 100

# create a DataFrame
time_list = [np.datetime64(datetime.date(2020, random.randint(1, 12), random.randint(1, 20))) for _ in range(n)]
sym_list = np.random.choice(['IBN', 'GTYU', 'FHU', 'DGT', 'FHU', 'YUG', 'EE', 'ZD', 'FYU'], n)
price_list = [round(np.random.uniform(1, 100), 1) for _ in range(n)]
id_list = np.random.choice([1, 2, 3, 4, 5], n)

tb = pd.DataFrame({
    'time': time_list,
    'sym': sym_list,
    'price': price_list,
    'id': id_list,
})

for _ in range(50000):
    s.run("appendStreamingData", tb)
```

Function views enable passing data to DolphinDB as a function argument. This facilitates data cleansing and type conversion within DolphinDB, improving overall performance when combined with asynchronous mode. 