# Constructing a DBConnectionPool

As introduced previously, Sessions enable you to communicate with the DolphinDB server. However, scripts can only be executed sequentially within a single session using the `run` method. Multi-threading is not allowed within the same session. 

If you want to execute scripts concurrently, it is recommended to use the DBConnectionPool class, which enables concurrent task execution by maintaining multiple threads. 

The following script creates a DBConnectionPool object with default parameter values:

```
DBConnectionPool(host, port, threadNum=10, userid=None, password=None, 
                 loadBalance=False, highAvailability=False, compress=False,  
                 reConnect=False, python=False, protocol=PROTOCOL_DEFAULT)
```

You can obtain the ID of all the sessions created by the DBConnectionPool with its method `getSessionId()`. Connections will be released when DBConnectionPool is deconstructed.

In the following sections, we will explain the parameters in detail. 

## *host*, *port*, *threadNum*, *userid*, *password*

- **host:** *required*. The server address to connect to.
- **port:** *required.* The port number of the server.
- **threadNum:** *int, optional, default 10.* The number of connections to create.
- **userid:** *optional.* The username for server login.
- **password:** *optional.* The password for server login.

Connect DBConnectionPool to a DolphinDB server by specifying the domain/IP address and the port number. You can also log in to the server by specifying the user credentials.

**Example**

```
import dolphindb as ddb

# Create 10 connections to the local DolphinDB server
pool = ddb.DBConnectionPool("localhost", 8848)

# Create 8 connections to the local DolphinDB server and log in as the user "admin"
pool = ddb.DBConnectionPool("localhost", 8848, 8, "admin", "123456")
```

**Note**

- If the connectivity parameters are incorrect, the DBConnectionPool object will not be created and the connection will fail.

## *loadBalance*

*bool, default False.* Whether to enable load balancing when connecting to the DolphinDB server. To enable load balancing, specify this parameter as True.

**Example**

```
import dolphindb as ddb

# create DBConnectionPool with load balancing enabled
pool = ddb.DBConnectionPool("localhost", 8848, 8, loadBalance=True)
```

**Note that in load balancing mode:**

- if API high availability (HA) mode is enabled, all data nodes and compute nodes in the cluster are available for connection. In this case, *loadBalance* does not take effect.
- if API HA mode is disabled, DBConnectionPool will evenly distribute connections to all available nodes. For example, there are 3 nodes in the cluster and each has \[5, 12, 13] connections, respectively. A DBConnectionPool needs to establish 6 connections. As a result, connections for each node is increased by 2, and the final connections on each node would be \[7, 14, 15], respectively.

## *highAvailability*

*bool, default False.* Whether to enable high availability on all cluster nodes.

**Note:** In API HA mode, if load balancing is disabled, DBConnectionPool will connect to the cluster node with the minimum load. As all the connections are created at the same time, they will all be added to the same node with the lowest load at the time, causing load imbalance.

**Example**

```
import dolphindb as ddb

# create DBConnectionPool with HA mode enabled. all data/compute nodes are available for connection.
pool = ddb.DBConnectionPool("localhost", 8848, 8, "admin", "123456", highAvailability=True)
```

## *compress*

*bool, default False.* Whether to enable compressed communication on the connections.

Compressed communication can be a useful option when writing or querying large amounts of data. When *compress* is set to True, the data will be compressed before being transferred, which can reduce bandwidth usage. Note that it increases the computational complexity on the server and API client, which may impact performance.

**Note**

- The *compress* parameter is supported on DolphinDB server since version 1.30.6.
- Compress communication is supported only when *protocol* = PROTOCOL_DDB. (In API versions 1.30.19.4 and below, *compress* is always supported because these versions use PROTOCOL_DDB by default. )

**Example**

```
import dolphindb as ddb
import dolphindb.settings as keys

# when API version >= 1.30.21.1, specify PROTOCOL_DDB to enable compression 
pool = ddb.DBConnectionPool("localhost", 8848, 8, compress=True, protocol=keys.PROTOCOL_DDB)

# when API version <= 1.30.19.4, PROTOCOL_DDB is used by default, i.e., enablePickle False
pool = ddb.DBConnectionPool("localhost", 8848, 8, compress=True)
```

## *reconnect*

*bool, default False.* Whether to reconnect if the API detects a connection exception when HA mode is disabled.

When HA mode is enabled, the system automatically reconnects when connection exception is detected, and it is not required to specify *reconnect*. Otherwise, set *reconnect* to True for auto reconnection.

```
import dolphindb as ddb

# create DBConnectionPool with auto reconnection
pool = ddb.DBConnectionPool("localhost", 8848, 8, reconnect=True)
```

## *protocol*

*protocol name, default PROTOCOL_DEFAULT (equivalent to PROTOCOL_PICKLE).* The protocol for object serialization in communication between the API client and the DolphinDB server.

Currently, the protocols PROTOCOL_DDB, PROTOCOL_PICKLE, and PROTOCOL_ARROW are supported. Protocols determine the format of data returned after running DolphinDB scripts through Python API. For more information about the protocols, see [Data Type Conversion](../../AdvancedOperations/DataTypeCasting/TypeCasting.md).

```
import dolphindb.settings as keys

# use PROTOCOL_DDB
pool = ddb.DBConnectionPool("localhost", 8848, 10, protocol=keys.PROTOCOL_DDB)

# use PROTOCOL_PICKLE
pool = ddb.DBConnectionPool("localhost", 8848, 10, protocol=keys.PROTOCOL_PICKLE)

# use PROTOCOL_ARROW
pool = ddb.DBConnectionPool("localhost", 8848, 10, protocol=keys.PROTOCOL_ARROW)
```

**Note:** Starting from 1.30.21.1, the *protocol* parameter is provided for specifying the object serialization protocol. Prior to 1.30.19.4, PROTOCOL_DDB is used by default, i.e., *enablePickle* = False.

## *python*

*bool, default False.* Whether to enable the Python Parser feature when running scripts with `DBConnectionPool.run`.

**Example**

```
import dolphindb as ddb

# enable python parser 
pool = ddb.DBConnectionPool("localhost", 8848, 10, python=True)
```

**Note:** This parameter will be supported in the upcoming version 2.10 of the DolphinDB server.
