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

Persist all the data points in the memory table of the database to the disk, and seal the data file. In cluster mode, we provide commands to persist the specified database cache of local node and persist the specified database cache of the cluster.

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


## Kill Query

IoTDB supports setting session connection timeouts and query timeouts, and also allows to stop the executing query manually.

### Session timeout

Session timeout controls when idle sessions are closed. An idle session is one that had not initiated any query or non-query operations for a period of time.

Session timeout is disabled by default and can be set using the `dn_session_timeout_threshold` parameter in IoTDB configuration file.

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

#### Kill specific query

```sql
KILL QUERY <queryId>
```

You can kill the specified query by specifying `queryId`. `queryId` is a string, so you need to put quotes around it.

To get the executing `queryId`，you can use the [show queries](#show-queries) command, which will show the list of all executing queries.

##### Example
```sql
kill query '20221205_114444_00003_5'
```

#### Kill all queries

```sql
KILL ALL QUERIES
```

Kill all queries on all DataNodes.

## SHOW QUERIES

This command is used to display all ongoing queries, here are usage scenarios：
- When you want to kill a query, you need to get the queryId of it
- Verify that a query has been killed after killing

### Grammar

```sql
SHOW QUERIES | (QUERY PROCESSLIST)
[WHERE whereCondition]
[ORDER BY sortKey {ASC | DESC}]
[LIMIT rowLimit] [OFFSET rowOffset]
```
Note：
- Compatibility with old syntax `show query processlist`
- When using WHERE clause, ensure that target columns of filter are existed in the result set
- When using ORDER BY clause, ensure that sortKeys are existed in the result set

### ResultSet
Time：Start time of query，DataType is `INT64`  
QueryId：Cluster - level unique query identifier，DataType is `TEXT`, format is `yyyyMMdd_HHmmss_index_dataNodeId`  
DataNodeId：DataNode which do execution of query，DataType is `INT32`  
ElapsedTime：Execution time of query (Imperfectly accurate)，`second` for unit，DataType is `FLOAT`  
Statement：Origin string of query，DataType is `TEXT`

```
+-----------------------------+-----------------------+----------+-----------+------------+
|                         Time|                QueryId|DataNodeId|ElapsedTime|   Statement|
+-----------------------------+-----------------------+----------+-----------+------------+
|2022-12-30T13:26:47.260+08:00|20221230_052647_00005_1|         1|      0.019|show queries|
+-----------------------------+-----------------------+----------+-----------+------------+
```
Note：
- Result set is arranged in Time ASC as default, use ORDER BY clause if you want to sort it by other keys.

### SQL Example
#### Example1：Obtain all current queries whose execution time is longer than 30 seconds

SQL string：
```sql
SHOW QUERIES WHERE ElapsedTime > 30
```

SQL result：
```
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|                         Time|                QueryId|DataNodeId|ElapsedTime|                    Statement|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:44.515+08:00|20221205_114444_00002_2|         2|     31.111|     select * from root.test1|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:45.515+08:00|20221205_114445_00003_2|         2|     30.111|     select * from root.test2|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:43.515+08:00|20221205_114443_00001_3|         3|     32.111|        select * from root.**|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
```

#### Example2：Obtain the Top5 queries in the current execution time

SQL string：
```sql
SHOW QUERIES limit 5
```

Equivalent to
```sql
SHOW QUERIES ORDER BY ElapsedTime DESC limit 5
```

SQL result：
```
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|                         Time|                QueryId|DataNodeId|ElapsedTime|                    Statement|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:44.515+08:00|20221205_114444_00003_5|         5|     31.111|     select * from root.test1|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:45.515+08:00|20221205_114445_00003_2|         2|     30.111|     select * from root.test2|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:46.515+08:00|20221205_114446_00003_3|         3|     29.111|     select * from root.test3|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:47.515+08:00|20221205_114447_00003_2|         2|     28.111|     select * from root.test4|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
|2022-12-05T11:44:48.515+08:00|20221205_114448_00003_4|         4|     27.111|     select * from root.test5|
+-----------------------------+-----------------------+----------+-----------+-----------------------------+
```

