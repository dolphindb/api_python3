# Methods for Asynchronous Execution and Others

# Methods

This section describes the methods of DBConnectionPool.
With these functions, there are 3 ways to asynchronously execute scripts and you can choose which method to use based on your needs.

- Coroutine function `run`: Run tasks asynchronously with coroutines. Task IDs increase sequentially. 
- `addTask`, `isFinished`, `getData`: Submit scripts to DBConnectionPool which handles the asynchronous execution and obtains the result. Task IDs are specified by users.
- `runTaskAsync` - Create an event loop in the connection pool. Tasks are executed within the event loop using the `run` method and a concurrent.futures.Future object is returned. Task IDs increase sequentially. 

**Note:** Task IDs are generated automatically in the first and third methods, but specified by users in the second method. To prevent ID conflicts, do not use the second method and first/third methods simultaneously.

## `run`



```
run(script, *args, **kwargs)
```

- **script:** the DolphinDB script to be executed.
- ***args:** the parameters to be passed into the functions in script
- ***\*kwargs** 
  - **clearMemory:** *bool, default True.* Whether to release variables after query.
  - **pickleTableToList:** *bool, default False.* True means to download returned Table objects as list objects. False means to download returned Table objects as DataFrame objects. For details, see [PROTOCOL_DDB](../../AdvancedOperations/DataTypeCasting/PROTOCOL_DDB.md) and [PROTOCOL_PICKLE](../../AdvancedOperations/DataTypeCasting/PROTOCOL_PICKLE.md).

The `run` method in DBConnectionPool is made into a coroutine function, which passes scripts to a thread pool for execution. `run` must be called in a coroutine.

**Example 1. Executing tasks with predefined logic** 

1. Construct a DBConnectionPool with a max size of 8 connections. **Note:** Unlike a typical connection pool, when a connection of DBConnectionPool is no longer in use, it is not immediately destroyed. Instead, the connections are retained in the pool until the pool itself is destructed or explicitly closed by the method `shutDown()`.

```
import dolphindb as ddb
import time
import asyncio

pool = ddb.DBConnectionPool("localhost", 8848, 8)
```

2. Define an async function `test_run`, which sleeps 2 seconds and then returns 1+*i*.

```
async def test_run(i):
    try:
        return await pool.run(f"sleep(2000);1+{i}")
    except Exception as e:
        print(e)
```

3. Create a list of 4 tasks and create an event loop. Run the event loop until all 4 coroutines complete.

```
tasks = [
    asyncio.ensure_future(test_run(1)),
    asyncio.ensure_future(test_run(3)),
    asyncio.ensure_future(test_run(5)),
    asyncio.ensure_future(test_run(7)),
]

loop = asyncio.get_event_loop()
try:
    time_st = time.time()
    loop.run_until_complete(asyncio.wait(tasks))
    time_ed = time.time()
except Exception as e:
    print("catch e:")
    print(e)
```

4. Print the total time taken for all the tasks to complete; Loop through each of the tasks and print their result; Shut down the connection.

```
print("time: ", time_ed-time_st)

for task in tasks:
    print(task.result())

pool.shutDown()
```

Expected output:

```
time:  2.0017542839050293
2
4
6
8
```

In this example, although only 1 main thread is created, we use coroutines to execute asynchronous tasks in the DBConnectionPool, enabling concurrent execution. 

**Note:** The DolphinDB Python API uses C++ threads internally to handle each connection. If the number of submitted tasks exceeds the actual number of threads, some tasks may be delayed.

**Example 2. Executing tasks which accept user-defined scripts**

The following example defines a class that accepts user scripts as parameters.

```
import dolphindb as ddb
import time
import asyncio
import threading

# The main thread creates coroutines and schedules them to run, while offloading the actual event loop execution to another thread.

class DolphinDBHelper(object):
    pool = ddb.DBConnectionPool("localhost", 8848, 10)
    @classmethod
    async def test_run(cls,script):
        print(f"run script: [{script}]")
        return await cls.pool.run(script)

    @classmethod
    async def runTest(cls,script):
        start = time.time()
        task = loop.create_task(cls.test_run(script))
        result = await asyncio.gather(task)
        print(f"""[{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}] time: {time.time()-start} result: {result}""")
        return result

# Define a thread function to run the event loop
def start_thread_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

if __name__=="__main__":
    start = time.time()
    print("In main thread",threading.current_thread())
    loop = asyncio.get_event_loop()
    # A child thread is launched to call start_thread_loop and run the event loop forever
    t = threading.Thread(target= start_thread_loop, args=(loop,))
    t.start()
    task1 = asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(1000);1+1"),loop)
    task2 = asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(3000);1+2"),loop)
    task3 = asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(5000);1+3"),loop)
    task4 = asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(1000);1+4"),loop)

    end = time.time()
    print("main thread time: ", end - start)
```

Output:

```
In main thread <_MainThread(MainThread, started 139838803788160)>
main thread time:  0.00039839744567871094
run script: [sleep(1000);1+1]
run script: [sleep(3000);1+2]
run script: [sleep(5000);1+3]
run script: [sleep(1000);1+4]
[2023-03-14 16:46:56] time: 1.0044968128204346 result: [2]
[2023-03-14 16:46:56] time: 1.0042989253997803 result: [5]
[2023-03-14 16:46:58] time: 3.0064148902893066 result: [3]
[2023-03-14 16:47:00] time: 5.005709409713745 result: [4]
```

In this example, the main thread creates a child thread that keeps running the event loop, then schedules 4 coroutines on that event loop. Each coroutine has a sleep time of 1s, 3s, 5s and 1s, respectively. 

The output shows that the coroutines were executed concurrently:

\- `main thread time:  0.00039839744567871094` indicates very little time spent in the main thread, as the tasks were executed asynchronously in the event loop;

\- Coroutines  with sleep times of 1s (task1 and task4) finished at the same time, then the coroutine with 3s (task2) finished 2s later, and the coroutine with 5s (task3) finished 2s after that. 

## `addTask`, `isFinished`, `getData`

`addTask` submits user scripts to DBConnectionPool by with user-specified task ID. Use `isFinished` to check if a task in the connection pool has completed, and use `getData` to retrieve task result.

### addTask

```
addTask(script, taskId, *args, **kwargs)
```

- **script:** the DolphinDB script to be executed.
- **taskId:** *int.* The ID of the task
- \***args:** the parameters to be passed into the functions in the script.
- \***\*kwargs** 
  - **clearMemory:** *bool, default True.* Whether to release variables after the query.
  - **pickleTableToList:** *bool, default False.* True means to convert returned Table objects into list objects. False means to convert returned Table objects into DataFrame objects.

`addTask` submit a task to DBConnectionPool by task ID. The submitted tasks are executed using connections allocated by the connection pool.

**Example**

Add a task with the ID of 12 to the connection pool.

```
pool.addTask("sleep(1000);1+2", taskId=12)
```

### isFinished

```
isFinished(taskId)
```

- **taskID:** *int.* The ID of the task.

`isFinished` checks if a task has completed by task ID. Returns True if the task has completed, otherwise returns False.

**Example**

```
if pool.isFinished(taskId=12):
    print("task has done!")
```

### getData

```
getData(taskId)
```

- **taskID:** *int.* The ID of the task.

`getData` queries the result of a task by task ID. 

**Example**

```
res = pool.getData(taskId=12)
```

**Note:** 

If the result of a task is not retrieved by `getData`, and a new task is later submitted with the same taskID, the result of the old task will be overwritten.

### A Complete Example

In the following script, we first create a DBConnectionPool object and add a task to the connection pool with ID 12. Then continuously check if task ID 12 is finished using `isFinished`. Once the task completes, get the execution result using `getData` and print it.

```
import dolphindb as ddb
import time
pool = ddb.DBConnectionPool("localhost", 8848, 8)
taskid = 12
pool.addTask("sleep(1500);1+2", taskId=taskid)
while True:
    if pool.isFinished(taskId=taskid):
        break
    time.sleep(0.01)

res = pool.getData(taskId=taskid)
print(res)

# output:
3
```

## runTaskAsync

```
runTaskAsync(script, *args, **kwargs)
```

- **script:** the DolphinDB script to be executed.
- ***args:** the parameters to be passed into the script
- ***\*kwargs** 
  - **clearMemory:** *bool, default True.* Whether to release variables after the query.
  - **pickleTableToList:** *bool, default False.* True means to convert returned Table objects into list objects. False means to convert returned Table objects into DataFrame objects.

**Note**

- In Python API 1.30.17.4 and earlier versions, this method is known as `runTaskAsyn`.
- When this method is used to execute script asynchronously, make sure to call `pool.shutDown()` after the tasks have finished to properly destruct the connection pool.

Besides `run` and `addTask`, you can also use the `runTaskAsync` method of DBConnectionPool to execute scripts asynchronously.

You can add a task to the connection pool by calling `runTaskAsync`, which returns a `concurrent.futures.Future` object. Then get the task result by calling the object’s `result(timeout=None)` method.

The *timeout* parameter of `result` sets the maximum wait time in seconds for task completion. If a timeout value is passed, `result` will return the task result if it completes within the timeout duration. Otherwise, `result` will raise a TimeoutError. The default value of *timeout* is None, indicating no timeout.

**Example**

```
import dolphindb as ddb
import time
pool = ddb.DBConnectionPool("localhost", 8848, 10)

t1 = time.time()
task1 = pool.runTaskAsync("sleep(1000); 1+0");
task2 = pool.runTaskAsync("sleep(2000); 1+1");
task3 = pool.runTaskAsync("sleep(4000); 1+2");
task4 = pool.runTaskAsync("sleep(1000); 1+3");
t2 = time.time()
print(task1.result())
t3 = time.time()
print(task2.result())
t4 = time.time()
print(task4.result())
t5 = time.time()
print(task3.result())
t6 = time.time()

print(t2-t1)
print(t3-t1)
print(t4-t1)
print(t5-t1)
print(t6-t1)
pool.shutDown()
```

Output:

```
Task1 Result:  1
Task2 Result:  2
Task4 Result:  4
Task3 Result:  3
Add Tasks:  0.0015881061553955078
Get Task1:  1.0128183364868164
Get Task2:  2.0117716789245605
Get Task4:  2.0118134021759033
Get Task3:  4.012163162231445
```

This script creates a DBConnectionPool, then runs 4 tasks (with different time durations) asynchronously on the pool by calling `runTaskAsync`, which returns 4 `concurrent.futures.Future` objects. Then we print the result of each task in blocking order using the `result` method. Then print the time measured at various points:

- t2 - t1: Time to add all tasks to the connection pool.
- t3 - t1: Time taken to get the result of task1. As task1 costs 1 sec, the result is 1sec.
- t4 - t1 = 2 sec: Time taken to get the result of task2. As task2 costs 2 sec, the result is 2 sec.
- t5 - t1 = 2 sec: Time taken to get the result of task4. As task4 costs 1 sec, plus the time waiting for task2 to finish, the result is 2 sec.
- t6 - t1 = 4 sec: Time taken to get the result of task3. As task3 costs 4 sec, the result is 4 sec.

The overall output demonstrates that all tasks are run concurrently. 

## Other Methods

### shutDown

```
pool.shutDown()
```

Use this method to shut down a DBConnectionPool that is no longer in use, terminate the event loops, and stop all asynchronous tasks. After this method is called, the connection pool can no longer be used.

**Note:**  If you have created asynchronous tasks using `runTaskAsync`, make sure to `shutDown`  the connections when they are not in use.

### getSessionId

```
 sessionids = pool.getSessionId()
```

`getSessionId` returns the ID of all sessions in the current connection pool.