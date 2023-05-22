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
# 运维命令

## FLUSH

将指定 database 的内存缓存区 Memory Table 的数据持久化到磁盘上，并将数据文件封口。在集群模式下，我们提供了持久化本节点的指定 database 的缓存、持久化整个集群指定 database 的缓存命令。

注意：此命令客户端不需要手动调用，IoTDB 有 wal 保证数据安全，IoTDB 会选择合适的时机进行 flush。
如果频繁调用 flush 会导致数据文件很小，降低查询性能。

```sql
IoTDB> FLUSH 
IoTDB> FLUSH ON LOCAL
IoTDB> FLUSH ON CLUSTER
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2 ON LOCAL
IoTDB> FLUSH root.sg1,root.sg2 ON CLUSTER
```

## CLEAR CACHE


手动清除chunk, chunk metadata和timeseries metadata的缓存，在内存资源紧张时，可以通过此命令，释放查询时缓存所占的内存空间。在集群模式下，我们提供了清空本节点缓存、清空整个集群缓存命令。

```sql
IoTDB> CLEAR CACHE
IoTDB> CLEAR CACHE ON LOCAL
IoTDB> CLEAR CACHE ON CLUSTER
```


## SET SYSTEM TO READONLY / RUNNING

手动设置系统为正常运行、只读状态。在集群模式下，我们提供了设置本节点状态、设置整个集群状态的命令，默认对整个集群生效。

```sql
IoTDB> SET SYSTEM TO RUNNING
IoTDB> SET SYSTEM TO READONLY ON LOCAL
IoTDB> SET SYSTEM TO READONLY ON CLUSTER
```

## 终止查询

IoTDB 支持设置 Session 连接超时和查询超时时间，并支持手动终止正在执行的查询。

### Session 超时

Session 超时控制何时关闭空闲 Session。空闲 Session 指在一段时间内没有发起任何操作的 Session。

Session 超时默认未开启。可以在配置文件中通过 `dn_session_timeout_threshold` 参数进行配置。

### 查询超时

对于执行时间过长的查询，IoTDB 将强行中断该查询，并抛出超时异常，如下所示：

```sql
IoTDB> select * from root.**;
Msg: 701 Current query is time out, please check your statement or modify timeout parameter.
```

系统默认的超时时间为 60000 ms，可以在配置文件中通过 `query_timeout_threshold` 参数进行自定义配置。

如果您使用 JDBC 或 Session，还支持对单个查询设置超时时间（单位为 ms）：

```java
((IoTDBStatement) statement).executeQuery(String sql, long timeoutInMS)
session.executeQueryStatement(String sql, long timeout)
```

> 如果不配置超时时间参数或将超时时间设置为负数，将使用服务器端默认的超时时间。 
> 如果超时时间设置为0，则会禁用超时功能。

### 查询终止

除了被动地等待查询超时外，IoTDB 还支持主动地终止查询：

#### 终止指定查询

```sql
KILL QUERY <queryId>
```

通过指定 `queryId` 可以中止指定的查询，`queryId`是一个字符串，所以使用时需要添加引号。

为了获取正在执行的查询 id，用户可以使用 [show queries](#show-queries) 命令，该命令将显示所有正在执行的查询列表。

##### 示例
```sql
kill query '20221205_114444_00003_5'
```

#### 终止所有查询

```sql
KILL ALL QUERIES
```

终止所有DataNode上的所有查询。

## SHOW QUERIES

该命令用于显示所有正在执行的查询，有以下使用场景：
- 想要中止某个查询时，需要获取查询对应的queryId
- 中止某个查询后验证查询是否已被中止

### 语法

```SQL
SHOW QUERIES | (QUERY PROCESSLIST)
[WHERE whereCondition]
[ORDER BY sortKey {ASC | DESC}]
[LIMIT rowLimit] [OFFSET rowOffset]
```
注意：
- 兼容旧语法`show query processlist`
- 使用WHERE时请保证过滤的目标列是结果集中存在的列
- 使用ORDER BY时请保证sortKey是结果集中存在的列

### 结果集
Time：查询开始时间，数据类型为`INT64`  
QueryId：集群级别唯一的查询标识，数据类型为`TEXT`，格式为`yyyyMMdd_HHmmss_index_dataNodeId`
DataNodeId：执行该查询的节点，数据类型为`INT32`  
ElapsedTime：查询已执行时间（不完全精确），以`秒`为单位，数据类型为`FLOAT`  
Statement：查询的原始语句，数据类型为`TEXT` 

```
+-----------------------------+-----------------------+----------+-----------+------------+
|                         Time|                QueryId|DataNodeId|ElapsedTime|   Statement|
+-----------------------------+-----------------------+----------+-----------+------------+
|2022-12-30T13:26:47.260+08:00|20221230_052647_00005_1|         1|      0.019|show queries|
+-----------------------------+-----------------------+----------+-----------+------------+
```
注意：
- 结果集默认按照Time列升序排列，如需按其他key进行排序，请使用ORDER BY子句

### SQL示例
#### 示例1：获取当前所有执行时间大于30s的查询  

SQL 语句为：
```SQL
SHOW QUERIES WHERE ElapsedTime > 30
```

该 SQL 语句的执行结果如下：
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

#### 示例2：获取当前执行耗时Top5的查询

SQL 语句为：
```SQL
SHOW QUERIES limit 5
```

等价于
```SQL
SHOW QUERIES ORDER BY ElapsedTime DESC limit 5
```

该 SQL 语句的执行结果如下：
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