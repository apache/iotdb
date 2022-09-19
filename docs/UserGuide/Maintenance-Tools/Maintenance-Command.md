<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Maintenance Command
## FLUSH

Persist all the data points in the memory table of the storage group to the disk, and seal the data file. In cluster mode, we provide commands to persist the specified storage group cache of local node and persist the specified storage group cache of the cluster.

Note: This command does not need to be invoked manually by the client. IoTDB has WAL to ensure data security
and IoTDB will flush when appropriate.
Frequently call flush can result in small data files that degrade query performance.

```sql
IoTDB> FLUSH 
IoTDB> FLUSH ON LOCAL
IoTDB> FLUSH ON CLUSTER
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2 ON LOCAL
IoTDB> FLUSH root.sg1,root.sg2 ON CLUSTER
```

## MERGE

Execute Level Compaction and unsequence Compaction task. Currently IoTDB supports the following two types of SQL to manually trigger the compaction process of data files:

* `MERGE` Execute the level compaction first and then execute the unsequence compaction. In unsequence compaction process, this command is executed very fast by rewriting the overlapped Chunks only, while there is some redundant data on the disk eventually.
* `FULL MERGE` Execute the level compaction first and then execute the unsequence compaction. In unsequence compaction process, this command is executed slow due to it takes more time to rewrite all data in overlapped files. However, there won't be any redundant data on the disk eventually.

```sql
IoTDB> MERGE
IoTDB> FULL MERGE
```
At the same time, manually trigger the compaction process of data files are supported for local node or the entire cluster in cluster mode:
```sql
IoTDB> MERGE ON LOCAL
IoTDB> MERGE ON CLUSTER
IoTDB> FULL MERGE ON LOCAL
IoTDB> FULL MERGE ON CLUSTER
```

## CLEAR CACHE

Clear the cache of chunk, chunk metadata and timeseries metadata to release the memory footprint. In cluster mode, we provide commands to clear local node cache and clear the cluster cache.

```sql
IoTDB> CLEAR CACHE
IoTDB> CLEAR CACHE ON LOCAL
IoTDB> CLEAR CACHE ON CLUSTER
```


## SET SYSTEM TO READONLY / RUNNING

Manually set IoTDB system to running, read-only mode. In cluster mode, we provide commands to set the local node status and set the cluster status, valid for the entire cluster by default.

```sql
IoTDB> SET SYSTEM TO RUNNING
IoTDB> SET SYSTEM TO READONLY ON LOCAL
IoTDB> SET SYSTEM TO READONLY ON CLUSTER
```


## Timeout

IoTDB supports session and query level timeout.

### Session timeout

Session timeout controls when idle sessions are closed. An idle session is one that had not initiated any query or non-query operations for a period of time.

Session timeout is disabled by default and can be set using the `session_timeout_threshold` parameter in IoTDB configuration file.

### Query timeout

For queries that take too long to execute, IoTDB will forcibly interrupt the query and throw a timeout exception, as shown in the figure: 

```sql
IoTDB> select * from root;
Msg: 701 Current query is time out, please check your statement or modify timeout parameter.
```

The default timeout of a query is 60000 ms，which can be customized in the configuration file through the `query_timeout_threshold` parameter.

If you use JDBC or Session, we also support setting a timeout for a single query（Unit: ms）：

```java
((IoTDBStatement) statement).executeQuery(String sql, long timeoutInMS)
session.executeQueryStatement(String sql, long timeout)
```


> If the timeout parameter is not configured or with a negative number, the default timeout time will be used. 
> If value 0 is used, timeout function will be disabled.

### Query abort

In addition to waiting for the query to time out passively, IoTDB also supports stopping the query actively:

```sql
KILL QUERY <queryId>
```

You can abort the specified query by specifying `queryId`. If `queryId` is not specified, all executing queries will be killed.

To get the executing `queryId`，you can use the `show query processlist` command，which will show the list of all executing queries，with the following result set：

| Time | queryId | statement |
| ---- | ------- | --------- |
|      |         |           |

The maximum display length of statement is 64 characters. For statements with more than 64 characters, the intercepted part will be displayed.

## Monitoring tool for cluster Node distribution

### Show all DataNode information

Currently, IoTDB supports DataNode query using the following SQL：

```
SHOW DATANODES
```

Eg :

```sql
IoTDB> create timeseries root.sg.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.sg.d2.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.ln.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> show regions
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|  Role|
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         1|127.0.0.1|6667|Leader|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|6668|Leader|
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
Total line number = 2
It costs 0.013s

IoTDB> show datanodes
+------+-------+---------+----+-------------+---------------+
|NodeID| Status|     Host|Port|DataRegionNum|SchemaRegionNum|
+------+-------+---------+----+-------------+---------------+
|     1|Running|127.0.0.1|6667|            0|              1|
|     2|Running|127.0.0.1|6668|            0|              1|
+------+-------+---------+----+-------------+---------------+
Total line number = 2
It costs 0.007s

IoTDB> insert into root.ln.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.
IoTDB> show regions
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|  Role|
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         1|127.0.0.1|6667|Leader|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|6668|Leader|
|       2|  DataRegion|    Up|      root.ln|           1|         1|         1|127.0.0.1|6667|Leader|
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
Total line number = 3
It costs 0.008s
IoTDB> show datanodes
+------+-------+---------+----+-------------+---------------+
|NodeID| Status|     Host|Port|DataRegionNum|SchemaRegionNum|
+------+-------+---------+----+-------------+---------------+
|     1|Running|127.0.0.1|6667|            1|              1|
|     2|Running|127.0.0.1|6668|            0|              1|
+------+-------+---------+----+-------------+---------------+
Total line number = 2
It costs 0.006s
```

After a DataNode is stopped, its status will change, as shown below:

```
IoTDB> show datanodes
+------+-------+---------+----+-------------+---------------+
|NodeID| Status|     Host|Port|DataRegionNum|SchemaRegionNum|
+------+-------+---------+----+-------------+---------------+
|     3|Running|127.0.0.1|6667|            0|              0|
|     4|Unknown|127.0.0.1|6669|            0|              0|
|     5|Running|127.0.0.1|6671|            0|              0|
+------+-------+---------+----+-------------+---------------+
Total line number = 3
It costs 0.009s
```

### Show all ConfigNode information

Currently, IoTDB supports ConfigNode query using the following SQL：

```
SHOW CONFIGNODES
```

Eg :

```
IoTDB> show confignodes
+------+-------+-------+-----+--------+
|NodeID| Status|   Host| Port|    Role|
+------+-------+-------+-----+--------+
|     0|Running|0.0.0.0|22277|  Leader|
|     1|Running|0.0.0.0|22279|Follower|
|     2|Running|0.0.0.0|22281|Follower|
+------+-------+-------+-----+--------+
Total line number = 3
It costs 0.030s
```

After a ConfigNode is stopped, its status will change, as shown below:

```
IoTDB> show confignodes
+------+-------+-------+-----+--------+
|NodeID| Status|   Host| Port|    Role|
+------+-------+-------+-----+--------+
|     0|Running|0.0.0.0|22277|  Leader|
|     1|Running|0.0.0.0|22279|Follower|
|     2|Unknown|0.0.0.0|22281|Follower|
+------+-------+-------+-----+--------+
Total line number = 3
It costs 0.009s
```

### Show all Node information

Currently, iotdb supports the following SQL to view the information of all nodes :

```
SHOW CLUSTER
```

Eg：

```
IoTDB> show cluster
+------+----------+-------+---------+-----+
|NodeID|  NodeType| Status|     Host| Port|
+------+----------+-------+---------+-----+
|     0|ConfigNode|Running|  0.0.0.0|22277|
|     1|ConfigNode|Running|  0.0.0.0|22279|
|     2|ConfigNode|Running|  0.0.0.0|22281|
|     3|  DataNode|Running|127.0.0.1| 9003|
|     4|  DataNode|Running|127.0.0.1| 9005|
|     5|  DataNode|Running|127.0.0.1| 9007|
+------+----------+-------+---------+-----+
Total line number = 6
It costs 0.011s
```

After a node is stopped, its status will change, as shown below:

```
IoTDB> show cluster
+------+----------+-------+---------+-----+
|NodeID|  NodeType| Status|     Host| Port|
+------+----------+-------+---------+-----+
|     0|ConfigNode|Running|  0.0.0.0|22277|
|     1|ConfigNode|Unknown|  0.0.0.0|22279|
|     2|ConfigNode|Running|  0.0.0.0|22281|
|     3|  DataNode|Running|127.0.0.1| 9003|
|     4|  DataNode|Running|127.0.0.1| 9005|
|     5|  DataNode|Running|127.0.0.1| 9007|
+------+----------+-------+---------+-----+
Total line number = 6
It costs 0.012s
```

## Monitoring tool for cluster Region distribution

A cluster uses a Region as a unit for data replication and data management . The Region status and distribution is helpful for system operation and maintenance testing , as shown in the following scenario ：

-  Check which Datanodes are allocated to each Region in the cluster and whether the balance is correct.

Currently, IoTDB supports Region query using the following SQL：

- `SHOW REGIONS`: Show all Region
- `SHOW SCHEMA REGIONS`: Show all SchemaRegion distribution
- `SHOW DATA REGIONS`: Show all DataRegion distribution
- `SHOW (DATA|SCHEMA)? REGIONS OF STORAGE GROUP <sg1,sg2,...>`: Show region distribution of specified storage groups

First, let's take a look at the distribution of Regions under three copies:

```
IoTDB> create timeseries root.sg.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.sg.d2.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.ln.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.

+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|    Role|
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         5|127.0.0.1|6671|Follower|
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         4|127.0.0.1|6669|Follower|
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         3|127.0.0.1|6667|  Leader|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         5|127.0.0.1|6671|Follower|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         4|127.0.0.1|6669|Follower|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         3|127.0.0.1|6667|  Leader|
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
Total line number = 6
It costs 0.032s
```

Then take a look at the distribution of Regions under a single copy:


```sql
IoTDB> show regions
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|  Role|
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         5|127.0.0.1|6671|Leader|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         4|127.0.0.1|6669|Leader|
+--------+------------+------+-------------+------------+----------+----------+---------+----+------+
Total line number = 2
It costs 0.128s
```

Show the distribution information of Schema Region and Data Region:

```
IoTDB> insert into root.sg.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.
IoTDB> insert into root.ln.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.

IoTDB> show data regions
+--------+----------+------+-------------+------------+----------+----------+---------+----+--------+
|RegionId|      Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|    Role|
+--------+----------+------+-------------+------------+----------+----------+---------+----+--------+
|       2|DataRegion|    Up|      root.sg|           1|         1|         5|127.0.0.1|6671|Follower|
|       2|DataRegion|    Up|      root.sg|           1|         1|         4|127.0.0.1|6669|  Leader|
|       2|DataRegion|    Up|      root.sg|           1|         1|         3|127.0.0.1|6667|Follower|
|       3|DataRegion|    Up|      root.ln|           1|         1|         5|127.0.0.1|6671|  Leader|
|       3|DataRegion|    Up|      root.ln|           1|         1|         4|127.0.0.1|6669|Follower|
|       3|DataRegion|    Up|      root.ln|           1|         1|         3|127.0.0.1|6667|Follower|
+--------+----------+------+-------------+------------+----------+----------+---------+----+--------+
Total line number = 2
It costs 0.011s
IoTDB> show schema regions
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|    Role|
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         5|127.0.0.1|6671|Follower|
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         4|127.0.0.1|6669|  Leader|
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         3|127.0.0.1|6667|Follower|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         5|127.0.0.1|6671|Follower|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         4|127.0.0.1|6669|Follower|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         3|127.0.0.1|6667|  Leader|
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
Total line number = 2
It costs 0.012s
```

Show region distribution of specified storage groups

```
IoTDB> show regions of storage group root.sg
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|    Role|
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         5|127.0.0.1|6671|Follower|
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         4|127.0.0.1|6669|  Leader|
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         3|127.0.0.1|6667|Follower|
|       2|  DataRegion|    Up|      root.sg|           1|         1|         5|127.0.0.1|6671|Follower|
|       2|  DataRegion|    Up|      root.sg|           1|         1|         4|127.0.0.1|6669|  Leader|
|       2|  DataRegion|    Up|      root.sg|           1|         1|         3|127.0.0.1|6667|Follower|
+--------+------------+------+-------------+------------+----------+----------+---------+----+--------+
Total line number = 2
It costs 0.005s

IoTDB> create timeseries root.sgcc.wf01.d1.wt01 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> show regions of storage group root.*.wf01
+--------+------------+------+--------------+------------+----------+----------+---------+----+--------+
|RegionId|        Type|Status| storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|    Role|
+--------+------------+------+--------------+------------+----------+----------+---------+----+--------+
|       4|SchemaRegion|    Up|root.sgcc.wf01|           1|         0|         5|127.0.0.1|6671|  Leader|
|       4|SchemaRegion|    Up|root.sgcc.wf01|           1|         0|         4|127.0.0.1|6669|Follower|
|       4|SchemaRegion|    Up|root.sgcc.wf01|           1|         0|         3|127.0.0.1|6667|Follower|
+--------+------------+------+--------------+------------+----------+----------+---------+----+--------+
Total line number = 3
It costs 0.012s
```

