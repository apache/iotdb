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

# Kill Query

When using IoTDB, you may encounter the following situations: you have entered a query statement, but can not get the result for a long time, as this query contains too much data or some other reasons, and have to wait until the query ends.
Since version 0.12, IoTDB has provided two solutions for queries with long execution time: query timeout and query abort.

## Query timeout

For queries that take too long to execute, IoTDB will forcibly interrupt the query and throw a timeout exception, as shown in the figure: 

![image](https://user-images.githubusercontent.com/34242296/104586593-a224aa00-56a0-11eb-9c52-241dcdb68ecb.png)

The default timeout of the system is 60000 ms，which can be customized in the configuration file through the `query_time_threshold` parameter.

If you use JDBC or Session, we also support setting a timeout for a single query（Unit: ms）：

```
E.g. ((IoTDBStatement) statement).executeQuery(String sql, long timeoutInMS)
E.g. session.executeQueryStatement(String sql, long timeout)
```

If the timeout parameter is not configured or with value 0, the default timeout time will be used.

## Query abort

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