# MultithreadedTableWriter

For frequent insertion of a single record, you can use the MultithreadedTableWriter (**recommended**) or BatchTableWriter class to append data in batches asynchronously. The client maintains a buffer queue. When the server is busy with network I/O activities, the client threads can continue writing data to the buffer queue and return immediately after writing. This allows the client to sustain high throughput even when the server is occupied.

If the *threadCount* parameter of MultithreadedTableWriter is not specified as 1, ensure that the target table supports concurrent writes.

## Internal Working of MultithreadedTableWriter

The MultithreadedTableWriter (MTW) class collects single inserted records, batches them together, and asynchronously writes the batches to the server using multiple threads. When constructing an MTW object, you must specify the number of writer threads (*threadCount*). MTW creates a total of *threadCount*+1 threads:

- *threadCount* writer threads
- 1 converter thread

This is how MTW works internally:

1. The `insert` method accepts records (Python objects) one at a time and adds them to a converter queue. It returns an ErrorCodeInfo object. MTW only checks if the incoming data has the correct column count for the target table. **The data types are not validated.** If an error (e.g., incorrect column count, writer thread has quit) occurs during insert(), *ErrorCodeInfo.hasError()* returns True.

2. During construction of the MTW object, a converter thread is spawned to convert the incoming record (Python object) into a C++ object, decreasing the performance impact of the Python Global Interpreter Lock (GIL). If an error occurs, all background threads including the converter thread will terminate, and the error will be logged to MultithreadedWriterStatus, which you can obtain via `MultithreadedTableWriter.getStatus()`. The converted C++ objects are distributed to the writer threads, and the Python objects that failed conversion can be accessed by calling `MultithreadedTableWriter.getUnwrittenData()`.

3. The *threadCount* writer threads transmit the data and associated script to the DolphinDB server according to the specified *batchSize* and *throttle* parameters. As with the previous step, if an error occurs during this process, all background threads will terminate and the error will be logged to the MultithreadedWriterStatus.

4. The `waitForThreadCompletion()` method of the MTW class will wait in a blocking manner for all writing tasks to complete, similar to calling `.join()` on the internal threads.

    **Note:** After this method has been called, the MTW object can no longer be used to insert additional data, as it signifies all writing operations have ended. A new MTW object must be created to continue data insertion.

## MultithreadedTableWriter

```
MultithreadedTableWriter(host, port, userId, password, dbPath, tableName, useSSL=False, enableHighAvailability=False, highAvailabilitySites=[], batchSize=1, throttle=1, threadCount=1, partitionCol="", compressMethods=[], mode="", modeOption=[])
```

### Connectivity Parameters 

- **host**: *str, required.* The IP address of the server to connect to.
- **port**: *int, required.* The port number of the server to connect to.
- **userName:** *str, optional.* The username for server login.
- **password**: *str, optional.* The password for server login.
- **dbPath**: *str, required*. The DFS database path. Leave it unspecified for an in-memory table.
- **tableName**: *str, required*. The in-memory or DFS table name. 

**Note:** For API 1.30.17.4 or lower versions, when writing to an in-memory table, specify the in-memory table name for *dbPath* and leave *tableName* empty.

- **useSSL**: *bool, default False.* Whether to enable SSL. Similar to the Session class, to enable SSL, the configuration parameter *enableHTTPS* must be set to true on the DolphinDB server. For information on setting *enableHTTPS*, refer to DolphinDB User Manual - Secure Communication.
- **enableHighAvailability, highAvailabilitySites**: parameters for enabling high availability. See the same parameters in [Creating a Session](../Session/Constructor.md).

### Other Parameters

- **batchSize**: *int, default 1.* The number of records the MTW object will buffer before transmitting them to the server. 1 means the MTW will send each record to the server immediately after receiving it from client. If *batchSize* is greater than 1, the MTW will buffer incoming records until it has received *batchSize* records before sending them to the server in a batch.
- **throttle**: *float, must be greater than 0.* Sets a time limit (in seconds) for how long the MTW will buffer incoming records before sending them to the server, even if the *batchSize* has not been reached.
- **threadCount**: *int, default 1.* The number of threads to create. The default value is 1, indicating single-threaded process. It must be 1 for tables that do not support concurrent writing.
- **partitionCol**: *str, default None.* Only takes effect when *threadCount* is greater than 1. For a partitioned table, it must be a partitioning column; otherwise, it must be a column name.
- **compressMethods:** *list, optional.* Compression methods to apply on a per-column basis. If left unspecified, no compression is used. Each element in the list corresponds to one column of data, in order. Valid compression types (case insensitive) are:
  - "LZ4": LZ4 algorithm
  - "DELTA": Delta-of-delta encoding
- **mode**: *str, default “Append“.* The write mode. It can be: "Upsert" (call server function `upsert!`(see DolphinDB User Manual - upsert!)) or "Append" (call server function `tableInsert`(see DolphinDB User Manual - tableInsert)).
- **modeOption**: *list of strings.* The optional parameters of `upsert!`](see DolphinDB User Manual - upsert!). *Only takes effect when mode = "upsert".* 

### Example: Constructing an MTW

```
 writer = ddb.MultithreadedTableWriter(
    "localhost", 8848, "admin", "123456", "dfs://testMTW", "pt", batchSize=2, throttle=0.01,  
    threadCount=3, partitionCol="date", compressMethods=["LZ4", "DELTA", "LZ4", "DELTA"],
    mode="Upsert", modeOption=["true", "`index"]
)
```

This example constructs a MultithreadedTableWriter (MTW) with 3 writer threads to write to a partitioned "pt" table in the DolphinDB DFS database named "dfs://testMTW".

- By setting *batchSize*=2 and *throttle*=0.01, the MTW will send all buffered records to the server either when the buffer reaches 2 records, or when 0.01 seconds has passed, whichever comes first.
- *partitionCol*="date" distributes incoming records to the 3 writer thread buffers based on the "date" column value.
- *compressMethods*=\["LZ4", "DELTA", "LZ4", "DELTA"] individually configures LZ4 and DELTA compression for each of the 4 columns in the data. 
- mode="*Upsert*" calls the DolphinDB [upsert!](https://dolphindb.com/help200/FunctionsandCommands/FunctionReferences/u/upsert!.html) function to write the records. By passing *modeOption*=\["true","index"], the *ignoreNull* parameter is set to true and *keyColNames* is set to `index for the `upsert!` call.

### **insert**

```
res:ErrorCodeInfo = writer.insert(*args)
```

**Details**

Inserts a single record.

Returns an ErrorCodeInfo object containing the error code (*errorCode*)  and the error information (*errorInfo*).

**ErrorCodeInfo** 

- *errorCode:* Empty string "" indicates no error. If an error occurred, contains the error code, e.g. "A2". 
- *errorinfo:* Provides error information and details. Returns None if no error occurred.
- The ErrorCodeInfo class also provides methods to verify if data was successfully added to the converter queue:
- `hasError()`: Returns True if an error was encountered adding data to the converter queue, False otherwise. 
- `succeed()`: Returns True if data was successfully added to the converter queue, False otherwise.
- After adding a record, it is recommended to call `hasError()` to validate the operation.

**Arguments**

- **args:** a variable-length argument indicating the record to be inserted.

**Example**

This example uses the MTW to write data to a shared DolphinDB table. After data is inserted, `hasError` is called to validate whether data has been successfully added to the converter queue.

```
import numpy as np
import pandas as pd
import dolphindb as ddb
import random

s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")

script = """
    t = table(1000:0, `date`ticker`price, [DATE,SYMBOL,LONG]);
    share t as tglobal;
"""
s.run(script)

writer = ddb.MultithreadedTableWriter(
    "localhost", 8848, "admin", "123456", dbPath="", tableName="tglobal", 
    batchSize=10, throttle=1, threadCount=5, partitionCol="date"
)

for i in range(10):
    if i == 3:
        res = writer.insert(np.datetime64(f'2022-03-2{i%6}'), random.randint(1,10000))
    else:
        res = writer.insert(np.datetime64(f'2022-03-2{i%6}'), "AAAA", random.randint(1,10000))
    if res.hasError():
        print("insert error: ", res.errorInfo)

writer.waitForThreadCompletion()
print(s.run("""select count(*) from tglobal"""))

# output
"""
insert error:  Column counts don't match 3
   count
0      9
"""
```

In this example, 10 records were inserted in total. One of the records has only 2 columns. When this record was inserted, `res.hasError()` returns True, and the error message was printed. As a result, the total count of inserted records were 9.

### **getUnwrittenData**

```
data:list = writer.getUnwrittenData()
```

**Details**

Returns a nested list of records that have not yet been written to the DolphinDB server. This includes records that failed to be sent and records yet to be sent.

**Note:** Calling `getUnwrittenData()` will remove those records from the MTW, releasing them from memory.

### **insertUnwrittenData**

```
res:ErrorCodeInfo = insertUnwrittenData(unwrittenData)
```

**Details**

Inserts multiple unwritten records into the table. Returns an ErrorCodeInfo object as with `insert()`.

The difference from `insert()` is that `insertUnwrittenData()` can add multiple records at once, whereas `insert()` only adds a single record. `insertUnwrittenData()` is typically used to re-add records that previously failed to be written, often retrieved with `getUnwrittenData()`.

**Arguments**

- **unwrittenData:** the records that have not yet been written to the DolphinDB server. Often obtained by calling `getUnwrittenData()`.

### **getStatus**



```
status:MultithreadedTableWriterStatus = writer.getStatus()
```

**Details**

Returns a `MultithreadedTableWriterStatus` object providing status details for the MTW.

**MultithreadedTableWriterStatus** 

- **isExiting:** Whether the writer thread(s) are exiting.
- **errorCode:** The error code. Empty string if no error.
- **errorInfo:** The error message. None if no error. 
- **sentRows:** The number of records successfully sent (written) to server.
- **unsentRows:** The number of records to be sent, including the records pending conversion and the converted records pending insertion into the sending queue.
- **sendFailedRows:** The number of records failed to be sent (including records in the sending queue)
- **threadStatus:** a list of status for each writer thread, including:
  - threadId: The thread ID.
  - sentRows: The number of records this thread has sent.
  - unsentRows: The number of records pending to be sent by this thread. 
  - sendFailedRows: The number of records this thread has failed to send.

The MultithreadedTableWriterStatus class provides:

- `hasError()`: Returns True if an error was encountered, False otherwise. 
- `succeed()`: Returns True if no errors were encountered, False otherwise.

For example:

```
 errorCode     : A1
 errorInfo     : Data conversion error: Cannot convert double to LONG
 isExiting     : True
 sentRows      : 2493
 unsentRows    : 0
 sendFailedRows: 7507
 threadStatus  : 
        threadId        sentRows        unsentRows      sendFailedRows
               0               0                 0                7507
        3567691520           415                 0                   0
        3489658624           831                 0                   0
        3481265920           416                 0                   0
        3472873216           416                 0                   0
        3464480512           415                 0                   0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x7f0102c76d30>
```

The output shows an error with error code "A1" occurred during the MTW write process. The error message is `Data conversion error: Cannot convert double to LONG`. 

- `isExiting=True` indicates the current thread is exiting.
- `sentRows=2493` indicates that 249,3 records have been sent to the DolphinDB server. 
- `unsentRows=0/sendFailedRows=7507` indicates that `0+7507` records are not yet sent to the server. 

In the thread status table, the number of records successfully sent by each thread (*sentRows*) are displayed in the corresponding rows. The number of unsent records (*unsentRows* and *sendFailedRows*) are only displayed in the first row of the table (`threadId=0`), indicating the total amount of data failed to be sent.

### **waitForThreadCompletion**

```
writer.waitForThreadCompletion()
```

**Details**

After the method is called, the MTW will wait until all threads complete their tasks.

## Standard Workflow

The methods of `MultithreadedTableWriter` are usually used in the following order:

```
# prepare data
prepre_Data()
# Construct an MTW object
writer = ddb.MultithreadedTableWriter(...)

try:
    for data in datas:
        # insert data
        res = writer.insert(data)
        # check if data has been added to the conversion queue
        if res.hasError():
            print(res.errorInfo)
    # wait for MTW to complete
    writer.waitForThreadCompletion()
except Exception as e:
    # Get the status of MTW and check for errors
    writeStatus = writer.getStatus()
    print(e)
    if writeStatus.hasError():
        # obtain unwritten data, fix the issues, and insert again
        print(writeStatus)
        unwrittendata = writer.getUnwrittenData()
        # fix the unwritten data with a predefined function
        unwrittendata = revise(unwrittendata)
        newwriter = ddb.MultithreadedTableWriter(...)
        newwriter.insertUnwrittenData(unwrittendata)
```

## Example

This example uses the MTW to write data to a DolphinDB DFS partitioned table.

1. **Create a DolphinDB DFS partitioned table**

```
import numpy as np
import pandas as pd
import dolphindb as ddb
import time
import random

s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")

script = """
    dbName = 'dfs://valuedb3';
    if(exists(dbName)){
        dropDatabase(dbName);
    }
    datetest=table(1000:0,`date`symbol`id,[DATE,SYMBOL,INT]);
    db = database(directory=dbName, partitionType=HASH, partitionScheme=[INT, 10]);
    pt=db.createPartitionedTable(datetest,'pdatetest','id');
"""
s.run(script)
```

**2. Create an MTW object**

```
writer = ddb.MultithreadedTableWriter(
    "localhost", 8848, "admin", "123456", dbPath="dfs://valuedb3", tableName="pdatetest", batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4","LZ4","DELTA"]
)
```

In this step, we create an MTW object with 5 threads to write data to the partitioned table “pdatetest“ under DFS database “dfs://valuedb3“. The *batchSize*, *throttle*, *partitionCol,* and the compression method for each column are specified.

**3. Write data**

```
try:
    # insert 100 records with correct schema
    for i in range(100):
        res = writer.insert(random.randint(1,10000),"AAAAAAAB", random.randint(1,10000))
        if res.hasError():
            print("MTW insert error: ", res.errorInfo)
except Exception as ex:
    # exception handling
    print("MTW exited with exception: ", ex)
# wait for completion
writer.waitForThreadCompletion()
writeStatus = writer.getStatus()
if writeStatus.succeed():
    print("Write successful!")
print("writeStatus: \n", writeStatus)
print(s.run("select count(*) from pt"))
```

Call `writer.insert()` insert data and `writer.getStatus()` to check the status of writer.

**Output**

```
Write successful!
writeStatus: 
 errorCode     : None
 errorInfo     : 
 isExiting     : True
 sentRows      : 100
 unsentRows    : 0
 sendFailedRows: 0
 threadStatus  : 
        threadId        sentRows        unsentRows      sendFailedRows
               0               0                 0                   0
        1677719296            22                 0                   0
        1669326592            20                 0                   0
        1660933888            19                 0                   0
        1652541184            14                 0                   0
        1644148480            25                 0                   0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x7fe896260d30>
   count
0    100
```

The output shows that all data has been successfully written to the DFS table. The *errorCode* is “None“.

**4. Error Handling**

The following example demonstrates how to catch exceptions with `getStatus` and `try-catch` during data writes.

**Note**

1. The DFS table already has 100 records from the previous steps.
2. As we have called `waitForCompletion` in step 3, the previous thread has exited. Now we need to construct a new MTW object to continue data insertion.

```
writer = ddb.MultithreadedTableWriter(
    "localhost", 8848, "admin", "123456", dbPath="dfs://valuedb3", tableName="pdatetest",
    batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4","LZ4","DELTA"]
)

try:
    # insert 100 records with the correct data types and column count
    for i in range(100):
        res = writer.insert(np.datetime64('2022-03-23'),"AAAAAAAB", random.randint(1,10000))
    # Insert 10 records with incorrect data types. As no type check is performed at this stage, these records will be added to the converter queue
    # The incorrect data types will be detected only when conversion starts. Then all background threads of MTW will be terminated.
    for i in range(10):
        res = writer.insert(np.datetime64('2022-03-23'),222, random.randint(1,10000))
        if res.hasError():
            # no error will be detected at this stage
            print("Insert wrong format data:\n", res)
    # Insert a record with incorrect column count. An error will be raised immediately
    # This record will not be added to converter queue
    res = writer.insert(np.datetime64('2022-03-23'),"AAAAAAAB")
    if res.hasError():
        # print error message
        print("Column counts don't match:\n", res)
    # Sleep for 1 second to wait for the conversion starts. The 10 records with incorrect data types will be detected.  
    # At this stage, all threads are terminated. MTW status is changed accordingly. 
    time.sleep(1)

    # # insert 1 record of correct type and column count. As MTW has exited, the record is not inserted.
    res = writer.insert(np.datetime64('2022-03-23'),"AAAAAAAB", random.randint(1,10000))
    print("MTW has exited")
except Exception as ex:
    # exception message
    print("MTW exited with exception %s" % ex)
# wait for completion
writer.waitForThreadCompletion()
writeStatus = writer.getStatus()
if writeStatus.hasError():
    print("Error in writing:")
print(writeStatus)
print(s.run("select count(*) from pt"))
"""
Column counts don't match:
 errorCode: A2
 errorInfo: Column counts don't match 3
<dolphindb.session.ErrorCodeInfo object at 0x7f0d52947040>
MTW exited with exception <Exception> in insert: thread is exiting.
Error in writing:
errorCode     : A1
 errorInfo     : Data conversion error: Cannot convert long to SYMBOL
 isExiting     : True
 sentRows      : 0
 unsentRows    : 4
 sendFailedRows: 106
 threadStatus  : 
        threadId        sentRows        unsentRows      sendFailedRows
               0               0                 0                 106
        511690496              0                 2                   0
        520083200              0                 1                   0
        528475904              0                 0                   0
        618751744              0                 0                   0
        536868608              0                 1                   0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x7f0d25d68910>
   count
0    100
"""
```

**5. Re-insert**

When the MTW encounters an error during data conversion or insertion, it will terminate all threads. In this situation, you can call `writer.getUnwrittenData()` to retrieve the data that was not inserted, then call `insertUnwrittenData(unwrittendata)` to re-attempt inserting the unwritten data. Note that since the original MTW has terminated its threads, a new MTW must be created before the unwritten data can be inserted again.

```
if writeStatus.hasError():
    print("Error in writing:")
    unwrittendata = writer.getUnwrittenData()
    print("Unwrittendata: %d" % len(unwrittendata))
    # construct a new MTW
    newwriter = ddb.MultithreadedTableWriter(
        "localhost", 8848, "admin", "123456", dbPath="dfs://valuedb3", tableName="pdatetest",
        batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4","LZ4","DELTA"]
    )
    try:
        # fix the issue with the unwritten data and insert again
        for row in unwrittendata:
            row[1]="aaaaa"
        res = newwriter.insertUnwrittenData(unwrittendata)
        if res.hasError():
            print("Failed to write data again: \n", res)
    except Exception as ex:
        # print error message
        print("MTW exit with exception %s" % ex)
    finally:
        # wait for newwriter to complete
        newwriter.waitForThreadCompletion()
        writeStatus = newwriter.getStatus()
        print("Write again:\n", writeStatus)
else:
    print("Write successfully:\n", writeStatus)

print(s.run("select count(*) from pt"))
"""
Error in writing:
Unwrittendata: 110
Write again:
 errorCode     : None
 errorInfo     : 
 isExiting     : True
 sentRows      : 110
 unsentRows    : 0
 sendFailedRows: 0
 threadStatus  : 
        threadId        sentRows        unsentRows      sendFailedRows
               0               0                 0                   0
        618751744             17                 0                   0
        528475904             21                 0                   0
        520083200             24                 0                   0
        511690496             25                 0                   0
        503297792             23                 0                   0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x7f0d52947040>
   count
0    210
"""
```

## Multithreading

The DolphinDB Python API ensures thread safety of Python multi-threaded call of MTW. For example:

```
import numpy as np
import pandas as pd
import dolphindb as ddb
import time
import threading
import random

s = ddb.Session()
s.connect("localhost", 8848, "admin", "123456")

script = """
    dbName = 'dfs://valuedb3';
    if(exists(dbName)){
        dropDatabase(dbName);
    }
    datetest=table(1000:0,`date`symbol`id,[DATE,SYMBOL,INT]);
    db = database(directory=dbName, partitionType=HASH, partitionScheme=[INT, 10]);
    pt=db.createPartitionedTable(datetest,'pdatetest','id');
"""
s.run(script)

writer = ddb.MultithreadedTableWriter(
    "localhost", 8848, "admin", "123456", dbPath="dfs://valuedb3", tableName="pdatetest",
    batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4","LZ4","DELTA"]
)

def insert_MTW(writer):
    try:
        # insert 100 records of correct data types and schema
        for i in range(100):
            res = writer.insert(random.randint(1,10000),"AAAAAAAB", random.randint(1,10000))
    except Exception as ex:
        # print error message
        print("MTW exit with exception %s" % ex)

# create 10 threads to insert data into MTW
threads = []
for i in range(10):
    threads.append(threading.Thread(target=insert_MTW, args=(writer,)))

for thread in threads:
    thread.start()

# execute other tasks while the threads insert data. Sleeping for 10 seconds in this example.
time.sleep(10)

# finish the task
# 1 - wait for threads to quit
for thread in threads:
    thread.join()
# 2 - wait for MTW to complete
writer.waitForThreadCompletion()
# 3 - check insertion result
writeStatus = writer.getStatus()
print("writeStatus:\n", writeStatus)
print(s.run("select count(*) from pt"))
```

Output:

```
writeStatus:
 errorCode     : None
 errorInfo     : 
 isExiting     : True
 sentRows      : 1000
 unsentRows    : 0
 sendFailedRows: 0
 threadStatus  : 
        threadId        sentRows        unsentRows      sendFailedRows
               0               0                 0                   0
        1309443840           194                 0                   0
        1301051136           188                 0                   0
        1292658432           215                 0                   0
        1284265728           208                 0                   0
        1275873024           195                 0                   0
<dolphindb.session.MultithreadedTableWriterStatus object at 0x7ff2802bf9a0>
   count
0   1000
```

This example demonstrates using the MTW to insert data with multiple Python threads. In this example, 10 Python threads were started and each thread inserts 100 records into the MTW. A total of 1000 records (10 threads x 100 records) were successfully written to the table through the MTW.