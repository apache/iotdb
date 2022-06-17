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

Persist all the data points in the memory table of the storage group to the disk, and seal the data file.

Note: This command does not need to be invoked manually by the client. IoTDB has WAL to ensure data security
and IoTDB will flush when appropriate.
Frequently call flush can result in small data files that degrade query performance.

```sql
IoTDB> FLUSH 
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2
```

## MERGE

Execute Level Compaction and unsequence Compaction task. Currently IoTDB supports the following two types of SQL to manually trigger the compaction process of data files:

* `MERGE` Execute the level compaction first and then execute the unsequence compaction. In unsequence compaction process, this command is executed very fast by rewriting the overlapped Chunks only, while there is some redundant data on the disk eventually.
* `FULL MERGE` Execute the level compaction first and then execute the unsequence compaction. In unsequence compaction process, this command is executed slow due to it takes more time to rewrite all data in overlapped files. However, there won't be any redundant data on the disk eventually.

```sql
IoTDB> MERGE
IoTDB> FULL MERGE
```

## CLEAR CACHE

Clear the cache of chunk, chunk metadata and timeseries metadata to release the memory footprint.

```sql
IoTDB> CLEAR CACHE
```


## SET STSTEM TO READONLY / WRITABLE

Manually set IoTDB system to read-only or writable mode.

```sql
IoTDB> SET SYSTEM TO READONLY
IoTDB> SET SYSTEM TO WRITABLE
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



## Monitoring tool for cluster Region distribution

A cluster uses a Region as a unit for data replication and data management . The Region status and distribution is helpful for system operation and maintenance testing , as shown in the following scenario ：

-  Check which Datanodes are allocated to each Region in the cluster and whether the balance is correct.

Currently, IoTDB supports Region query using the following SQL：

- `SHOW REGIONS`: Show all Region
- `SHOW SCHEMA REGIONS`: Show all SchemaRegion distribution
- `SHOW DATA REGIONS`: Show all DataRegion distribution

```sql
IoTDB> show regions
+--------+------------+------+-------------+-----+----------+----------+----+
|RegionId|        Type|Status|storage group|Slots|DataNodeId|HostAdress|Port|
+--------+------------+------+-------------+-----+----------+----------+----+
|       5|  DataRegion|    Up|      root.sg|    1|         2| 127.0.0.1|6671|
|       6|  DataRegion|    Up|      root.sg|    0|         3| 127.0.0.1|6669|
|       3|  DataRegion|    Up|      root.sg|    0|         3| 127.0.0.1|6669|
|       4|  DataRegion|    Up|      root.sg|    0|         2| 127.0.0.1|6671|
|       9|  DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|      10|  DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|       7|  DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|       8|  DataRegion|    Up|      root.sg|    0|         2| 127.0.0.1|6671|
|      11|  DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|      12|  DataRegion|    Up|      root.sg|    0|         3| 127.0.0.1|6669|
|       2|SchemaRegion|    Up|      root.sg|    1|         3| 127.0.0.1|6669|
|       0|SchemaRegion|    Up|      root.sg|    0|         2| 127.0.0.1|6671|
|       1|SchemaRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
+--------+------------+------+-------------+-----+----------+----------+----+
Total line number = 13
It costs 0.015s
IoTDB> show schema regions
+--------+------------+------+-------------+-----+----------+----------+----+
|RegionId|        Type|Status|storage group|Slots|DataNodeId|HostAdress|Port|
+--------+------------+------+-------------+-----+----------+----------+----+
|       2|SchemaRegion|    Up|      root.sg|    1|         3| 127.0.0.1|6669|
|       0|SchemaRegion|    Up|      root.sg|    0|         2| 127.0.0.1|6671|
|       1|SchemaRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
+--------+------------+------+-------------+-----+----------+----------+----+
Total line number = 3
It costs 0.008s
IoTDB> show data regions
+--------+----------+------+-------------+-----+----------+----------+----+
|RegionId|      Type|Status|storage group|Slots|DataNodeId|HostAdress|Port|
+--------+----------+------+-------------+-----+----------+----------+----+
|       5|DataRegion|    Up|      root.sg|    1|         2| 127.0.0.1|6671|
|       6|DataRegion|    Up|      root.sg|    0|         3| 127.0.0.1|6669|
|       3|DataRegion|    Up|      root.sg|    0|         3| 127.0.0.1|6669|
|       4|DataRegion|    Up|      root.sg|    0|         2| 127.0.0.1|6671|
|       9|DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|      10|DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|       7|DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|       8|DataRegion|    Up|      root.sg|    0|         2| 127.0.0.1|6671|
|      11|DataRegion|    Up|      root.sg|    0|         1| 127.0.0.1|6667|
|      12|DataRegion|    Up|      root.sg|    0|         3| 127.0.0.1|6669|
+--------+----------+------+-------------+-----+----------+----------+----+
Total line number = 10
It costs 0.038s
```

