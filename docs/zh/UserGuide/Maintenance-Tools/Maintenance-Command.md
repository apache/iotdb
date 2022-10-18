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

将指定存储组的内存缓存区 Memory Table 的数据持久化到磁盘上，并将数据文件封口。在集群模式下，我们提供了持久化本节点的指定存储组的缓存、持久化整个集群指定存储组的缓存命令。

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

## MERGE

触发层级合并和乱序合并。当前 IoTDB 支持使用如下两种 SQL 手动触发数据文件的合并：

* `MERGE` 先触发层级合并，等层级合并执行完后，再触发乱序合并。在乱序合并中，仅重写重复的 Chunk，整理速度快，但是最终磁盘会存在多余数据。
* `FULL MERGE` 先触发层级合并，等层级合并执行完后，再触发乱序合并。在乱序合并中，将需要合并的顺序和乱序文件的所有数据都重新写一份，整理速度慢，最终磁盘将不存在无用的数据。

```sql
IoTDB> MERGE
IoTDB> FULL MERGE
```
同时，在集群模式中支持对本节点或整个集群手动触发数据文件的合并:
```sql
IoTDB> MERGE ON LOCAL
IoTDB> MERGE ON CLUSTER
IoTDB> FULL MERGE ON LOCAL
IoTDB> FULL MERGE ON CLUSTER
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

Session 超时默认未开启。可以在配置文件中通过 `session_timeout_threshold` 参数进行配置。

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

除了被动地等待查询超时外，IoTDB 还支持主动地终止查询，命令为：

```sql
KILL QUERY <queryId>
```

通过指定 `queryId` 可以中止指定的查询，而如果不指定 `queryId`，将中止所有正在执行的查询。

为了获取正在执行的查询 id，用户可以使用 `show query processlist` 命令，该命令将显示所有正在执行的查询列表，结果形式如下：

| Time | queryId | statement |
|------|---------|-----------|
|      |         |           |

其中 statement 最大显示长度为 64 字符。对于超过 64 字符的查询语句，将截取部分进行显示。

## 集群节点分布式监控工具

### 查看 DataNode 信息

当前 IoTDB 支持使用如下 SQL 查看 DataNode 的信息：

```
SHOW DATANODES
```

示例：

```sql
IoTDB> create timeseries root.sg.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.sg.d2.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.ln.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> show regions
+--------+------------+------+-------------+------------+----------+----------+---------+-------+------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|RpcPort|  Role|
+--------+------------+------+-------------+------------+----------+----------+---------+-------+------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         1|127.0.0.1|   6667|Leader|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|   6668|Leader|
+--------+------------+------+-------------+------------+----------+----------+---------+-------+------+
Total line number = 2
It costs 0.013s

IoTDB> show datanodes
+------+-------+---------+-------+-------------+---------------+
|NodeID| Status|     Host|RpcPort|DataRegionNum|SchemaRegionNum|
+------+-------+---------+-------+-------------+---------------+
|     1|Running|127.0.0.1|   6667|            0|              1|
|     2|Running|127.0.0.1|   6668|            0|              1|
+------+-------+---------+-------+-------------+---------------+
Total line number = 2
It costs 0.007s

IoTDB> insert into root.ln.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.
IoTDB> show regions
+--------+------------+------+-------------+------------+----------+----------+---------+-------+------+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|RpcPort|  Role|
+--------+------------+------+-------------+------------+----------+----------+---------+-------+------+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         1|127.0.0.1|   6667|Leader|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|   6668|Leader|
|       2|  DataRegion|    Up|      root.ln|           1|         1|         1|127.0.0.1|   6667|Leader|
+--------+------------+------+-------------+------------+----------+----------+---------+-------+------+
Total line number = 3
It costs 0.008s
IoTDB> show datanodes
+------+-------+---------+-------+-------------+---------------+
|NodeID| Status|     Host|RpcPort|DataRegionNum|SchemaRegionNum|
+------+-------+---------+-------+-------------+---------------+
|     1|Running|127.0.0.1|   6667|            1|              1|
|     2|Running|127.0.0.1|   6668|            0|              1|
+------+-------+---------+-------+-------------+---------------+
Total line number = 2
It costs 0.006s
```

#### DataNode 状态定义
对 DataNode 各状态定义如下：

- **Running**: DataNode 正常运行，可读可写
- **Unknown**: DataNode 未正常上报心跳，ConfigNode 将认为该 DataNode 不可读写
- **Removing**: DataNode 正在移出集群，不可读写
- **ReadOnly**: DataNode 磁盘剩余空间低于 disk_full_threshold（默认 5%），此 DataNode 将不再能写入，不再能同步数据

### 查看 ConfigNode 节点信息

当前 IoTDB 支持使用如下 SQL 查看 ConfigNode 的信息：

```
SHOW CONFIGNODES
```

示例：

```
IoTDB> show confignodes
+------+-------+-------+------------+--------+
|NodeID| Status|   Host|InternalPort|    Role|
+------+-------+-------+------------+--------+
|     0|Running|0.0.0.0|       22277|  Leader|
|     1|Running|0.0.0.0|       22279|Follower|
|     2|Running|0.0.0.0|       22281|Follower|
+------+-------+-------+------------+--------+
Total line number = 3
It costs 0.030s
```

#### ConfigNode 状态定义
对 ConfigNode 各状态定义如下：

- **Running**: ConfigNode 正常运行
- **Unknown**: ConfigNode 未正常上报心跳

### 查看全部节点信息

当前 IoTDB 支持使用如下 SQL 查看全部节点的信息：

```
SHOW CLUSTER
```

示例：

```
IoTDB> show cluster
+------+----------+-------+---------+------------+
|NodeID|  NodeType| Status|     Host|InternalPort|
+------+----------+-------+---------+------------+
|     0|ConfigNode|Running|  0.0.0.0|       22277|
|     1|ConfigNode|Running|  0.0.0.0|       22279|
|     2|ConfigNode|Running|  0.0.0.0|       22281|
|     3|  DataNode|Running|127.0.0.1|        9003|
|     4|  DataNode|Running|127.0.0.1|        9005|
|     5|  DataNode|Running|127.0.0.1|        9007|
+------+----------+-------+---------+------------+
Total line number = 6
It costs 0.011s
```

## 集群 Region 分布监控工具

集群中以 Region 作为数据复制和数据管理的单元，Region 的状态和分布对于系统运维和测试有很大帮助，如以下场景：

- 查看集群中各个 Region 被分配到了哪些 DataNode，是否均衡

当前 IoTDB 支持使用如下 SQL 查看 Region：

- `SHOW REGIONS`: 展示所有 Region 分布
- `SHOW SCHEMA REGIONS`: 展示所有 SchemaRegion 分布
- `SHOW DATA REGIONS`: 展示所有 DataRegion 分布
- `SHOW (DATA|SCHEMA)? REGIONS OF STORAGE GROUP <sg1,sg2,...>`: 展示指定存储组 <sg1,sg2,...> 对应的 Region 分布

展示所有 Region 的分布：
```
IoTDB> show regions
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|RegionId|        Type| Status|Storage Group|SeriesSlots|TimeSlots|DataNodeId|   Host|RpcPort|    Role|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|       0|  DataRegion|Running|     root.sg1|          1|        1|         1|0.0.0.0|   6667|Follower|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         2|0.0.0.0|   6668|  Leader|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         3|0.0.0.0|   6669|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         1|0.0.0.0|   6667|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         2|0.0.0.0|   6668|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         3|0.0.0.0|   6669|  Leader|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         1|0.0.0.0|   6667|  Leader|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         2|0.0.0.0|   6668|Follower|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         3|0.0.0.0|   6669|Follower|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         1|0.0.0.0|   6667|Follower|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         2|0.0.0.0|   6668|  Leader|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         3|0.0.0.0|   6669|Follower|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
Total line number = 12
It costs 0.165s
```

展示 SchemaRegion 或 DataRegion 的分布：
```
IoTDB> show data regions
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|RegionId|        Type| Status|Storage Group|SeriesSlots|TimeSlots|DataNodeId|   Host|RpcPort|    Role|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|       0|  DataRegion|Running|     root.sg1|          1|        1|         1|0.0.0.0|   6667|Follower|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         2|0.0.0.0|   6668|  Leader|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         3|0.0.0.0|   6669|Follower|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         1|0.0.0.0|   6667|  Leader|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         2|0.0.0.0|   6668|Follower|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         3|0.0.0.0|   6669|Follower|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
Total line number = 6
It costs 0.011s

IoTDB> show schema regions
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|RegionId|        Type| Status|Storage Group|SeriesSlots|TimeSlots|DataNodeId|   Host|RpcPort|    Role|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         1|0.0.0.0|   6667|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         2|0.0.0.0|   6668|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         3|0.0.0.0|   6669|  Leader|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         1|0.0.0.0|   6667|Follower|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         2|0.0.0.0|   6668|  Leader|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         3|0.0.0.0|   6669|Follower|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
Total line number = 6
It costs 0.012s
```

展示指定存储组 <sg1,sg2,...> 对应的 Region 分布：
```
IoTDB> show regions of storage group root.sg1
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|RegionId|        Type| Status|Storage Group|SeriesSlots|TimeSlots|DataNodeId|   Host|RpcPort|    Role|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|       0|  DataRegion|Running|     root.sg1|          1|        1|         1|0.0.0.0|   6667|Follower|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         2|0.0.0.0|   6668|  Leader|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         3|0.0.0.0|   6669|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         1|0.0.0.0|   6667|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         2|0.0.0.0|   6668|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         3|0.0.0.0|   6669|  Leader|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
Total line number = 6
It costs 0.007s

IoTDB> show regions of storage group root.sg1, root.sg2
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|RegionId|        Type| Status|Storage Group|SeriesSlots|TimeSlots|DataNodeId|   Host|RpcPort|    Role|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|       0|  DataRegion|Running|     root.sg1|          1|        1|         1|0.0.0.0|   6667|Follower|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         2|0.0.0.0|   6668|  Leader|
|       0|  DataRegion|Running|     root.sg1|          1|        1|         3|0.0.0.0|   6669|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         1|0.0.0.0|   6667|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         2|0.0.0.0|   6668|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         3|0.0.0.0|   6669|  Leader|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         1|0.0.0.0|   6667|  Leader|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         2|0.0.0.0|   6668|Follower|
|       2|  DataRegion|Running|     root.sg2|          1|        1|         3|0.0.0.0|   6669|Follower|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         1|0.0.0.0|   6667|Follower|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         2|0.0.0.0|   6668|  Leader|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         3|0.0.0.0|   6669|Follower|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
Total line number = 12
It costs 0.009s

IoTDB> show data regions of storage group root.sg1, root.sg2
+--------+----------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|RegionId|      Type| Status|Storage Group|SeriesSlots|TimeSlots|DataNodeId|   Host|RpcPort|    Role|
+--------+----------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|       0|DataRegion|Running|     root.sg1|          1|        1|         1|0.0.0.0|   6667|Follower|
|       0|DataRegion|Running|     root.sg1|          1|        1|         2|0.0.0.0|   6668|  Leader|
|       0|DataRegion|Running|     root.sg1|          1|        1|         3|0.0.0.0|   6669|Follower|
|       2|DataRegion|Running|     root.sg2|          1|        1|         1|0.0.0.0|   6667|  Leader|
|       2|DataRegion|Running|     root.sg2|          1|        1|         2|0.0.0.0|   6668|Follower|
|       2|DataRegion|Running|     root.sg2|          1|        1|         3|0.0.0.0|   6669|Follower|
+--------+----------+-------+-------------+-----------+---------+----------+-------+-------+--------+
Total line number = 6
It costs 0.007s

IoTDB> show schema regions of storage group root.sg1, root.sg2
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|RegionId|        Type| Status|Storage Group|SeriesSlots|TimeSlots|DataNodeId|   Host|RpcPort|    Role|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         1|0.0.0.0|   6667|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         2|0.0.0.0|   6668|Follower|
|       1|SchemaRegion|Running|     root.sg1|          1|        0|         3|0.0.0.0|   6669|  Leader|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         1|0.0.0.0|   6667|Follower|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         2|0.0.0.0|   6668|  Leader|
|       3|SchemaRegion|Running|     root.sg2|          1|        0|         3|0.0.0.0|   6669|Follower|
+--------+------------+-------+-------------+-----------+---------+----------+-------+-------+--------+
Total line number = 6
It costs 0.009s
```

### Region 状态定义
对 Region 各状态定义如下：

- **Running**: Region 正常运行，可读可写
- **Removing**: Region 所在 DataNode 正在被移出集群，不可读写
- **Unknown**: Region 所在 DataNode 未正常上报心跳，ConfigNode 认为该 Region 不可读写

## 集群槽路径监控工具

集群使用分片来管理数据和元数据，一个存储组的元数据分片定义为序列槽，而数据分片定义为<序列槽，时间分区槽>的数对。为了得到分片相关的信息，可以使用以下SQL来查询：
### 追踪数据分片的分区

追踪一个数据分片（或一个序列槽下的所有数据分片）的对应分区:
- `SHOW DATA REGIONID OF root.sg WHERE SERIESSLOTID=s0 (AND TIMESLOTID=t0)`

示例:
```
IoTDB> show data regionid of root.sg where seriesslotid=5286 and timeslotid=0
+--------+
|RegionId|
+--------+
|       1|
+--------+
Total line number = 1
It costs 0.006s

IoTDB> show data regionid of root.sg where seriesslotid=5286
+--------+
|RegionId|
+--------+
|       1|
|       2|
+--------+
Total line number = 2
It costs 0.006s
```

### 追踪元数据分片的分区
追踪一个元数据分片下的对应分区：
- `SHOW SCHEMA REGIONID OF root.sg WHERE SERIESSLOTID=s0`

示例:
```
IoTDB> show schema regionid of root.sg where seriesslotid=5286
+--------+
|RegionId|
+--------+
|       0|
+--------+
Total line number = 1
It costs 0.007s
```
### 追踪序列槽下的时间槽
展示一个存储组内，一个特定序列槽下的所有时间槽：
- `SHOW TIMESLOTID OF root.sg WHERE SERIESLOTID=s0 (AND STARTTIME=t1) (AND ENDTIME=t2)`

示例:
```
IoTDB> show timeslotid of root.sg where seriesslotid=5286
+---------+
|Timeslots|
+---------+
|        0|
|     1000|
+---------+
Total line number = 1
It costs 0.007s
```
### 追踪存储组的序列槽
展示一个存储组内，数据，元数据或是所有的序列槽：
- `SHOW (DATA|SCHEMA)? SERIESSLOTID OF root.sg`

示例:
```
IoTDB> show data seriesslotid of root.sg
+-----------+
|SeriesSlots|
+-----------+
|       5286|
+-----------+
Total line number = 1
It costs 0.007s

IoTDB> show schema seriesslotid of root.sg
+-----------+
|SeriesSlots|
+-----------+
|       5286|
+-----------+
Total line number = 1
It costs 0.006s

IoTDB> show seriesslotid of root.sg
+-----------+
|SeriesSlots|
+-----------+
|       5286|
+-----------+
Total line number = 1
It costs 0.006s
```
#### 注意:
通常情况下，一个存储组内，数据和元数据的序列槽是相同的。然而，你可以仍然使用不同的sql，以防它们在某些意外情况下并不相同。