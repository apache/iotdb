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
## Maintenance Command
### FLUSH

Persist all the data points in the memory table of the storage group to the disk, and seal the data file.

```
IoTDB> FLUSH 
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2
```

### MERGE

Merge sequence and unsequence data. Currently IoTDB supports the following two types of SQL to manually trigger the merge process of data files:

* `MERGE` Only rewrite overlapped Chunks, the merge speed is quick, while there will be redundant data on the disk eventually.
* `FULL MERGE` Rewrite all data in overlapped files, the merge speed is slow, but there will be no redundant data on the disk eventually.

```
IoTDB> MERGE
IoTDB> FULL MERGE
```

### CLEAR CACHE

Clear the cache of chunk, chunk metadata and timeseries metadata to release the memory footprint.

```
IoTDB> CLEAR CACHE
```

### SCHEMA SNAPSHOT

To speed up restarting of IoTDB, users can create snapshot of schema and avoid recovering schema from mlog file.

```
IoTDB> CREATE SNAPSHOT FOR SCHEMA
```


### Kill Query

When using IoTDB, you may encounter the following situations: you have entered a query statement, but can not get the result for a long time, as this query contains too much data or some other reasons, and have to wait until the query ends.
Since version 0.12, IoTDB has provided two solutions for queries with long execution time: query timeout and query abort.

#### Query timeout

For queries that take too long to execute, IoTDB will forcibly interrupt the query and throw a timeout exception, as shown in the figure: 

```
IoTDB> select * from root;
Msg: 701 Current query is time out, please check your statement or modify timeout parameter.
```

The default timeout of the system is 60000 ms，which can be customized in the configuration file through the `query_timeout_threshold` parameter.

If you use JDBC or Session, we also support setting a timeout for a single query（Unit: ms）：

```
E.g. ((IoTDBStatement) statement).executeQuery(String sql, long timeoutInMS)
E.g. session.executeQueryStatement(String sql, long timeout)
```

If the timeout parameter is not configured or with value 0, the default timeout time will be used.

#### Query abort

In addition to waiting for the query to time out passively, IoTDB also supports stopping the query actively:

```
KILL QUERY <queryId>
```

You can abort the specified query by specifying `queryId`. If `queryId` is not specified, all executing queries will be killed.

To get the executing `queryId`，you can use the `show query processlist` command，which will show the list of all executing queries，with the following result set：

| Time | queryId | statement |
| ---- | ------- | --------- |
|      |         |           |

The maximum display length of statement is 64 characters. For statements with more than 64 characters, the intercepted part will be displayed.