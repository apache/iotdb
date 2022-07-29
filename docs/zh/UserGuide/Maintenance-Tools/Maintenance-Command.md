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

将指定存储组的内存缓存区 Memory Table 的数据持久化到磁盘上，并将数据文件封口。

注意：此命令客户端不需要手动调用，IoTDB 有 wal 保证数据安全，IoTDB 会选择合适的时机进行 flush。
如果频繁调用 flush 会导致数据文件很小，降低查询性能。

```sql
IoTDB> FLUSH 
IoTDB> FLUSH root.ln
IoTDB> FLUSH root.sg1,root.sg2
```

## MERGE

触发层级合并和乱序合并。当前 IoTDB 支持使用如下两种 SQL 手动触发数据文件的合并：

* `MERGE` 先触发层级合并，等层级合并执行完后，再触发乱序合并。在乱序合并中，仅重写重复的 Chunk，整理速度快，但是最终磁盘会存在多余数据。
* `FULL MERGE` 先触发层级合并，等层级合并执行完后，再触发乱序合并。在乱序合并中，将需要合并的顺序和乱序文件的所有数据都重新写一份，整理速度慢，最终磁盘将不存在无用的数据。

```sql
IoTDB> MERGE
IoTDB> FULL MERGE
```

## CLEAR CACHE


手动清除chunk, chunk metadata和timeseries metadata的缓存，在内存资源紧张时，可以通过此命令，释放查询时缓存所占的内存空间。在集群模式下，我们提供了清空单个节点缓存、清空整个集群缓存命令。

```sql
IoTDB> CLEAR CACHE
IoTDB> CLEAR CACHE ON LOCAL
IoTDB> CLEAR CACHE ON CLUSTER
```

## SET STSTEM TO READONLY / WRITABLE

手动设置系统为只读或者可写入模式。

```sql
IoTDB> SET SYSTEM TO READONLY
IoTDB> SET SYSTEM TO WRITABLE
```

## 超时

IoTDB 支持 Session 超时和查询超时。

### Session 超时

Session 超时控制何时关闭空闲 Session。空闲 Session 指在一段时间内没有发起任何操作的 Session。

Session 超时默认未开启。可以在配置文件中通过 `session_timeout_threshold` 参数进行配置。

### 查询超时

对于执行时间过长的查询，IoTDB 将强行中断该查询，并抛出超时异常，如下所示：

```sql
IoTDB> select * from root;
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

### 查询中止

除了被动地等待查询超时外，IoTDB 还支持主动地中止查询，命令为：

```sql
KILL QUERY <queryId>
```

通过指定 `queryId` 可以中止指定的查询，而如果不指定 `queryId`，将中止所有正在执行的查询。

为了获取正在执行的查询 id，用户可以使用 `show query processlist` 命令，该命令将显示所有正在执行的查询列表，结果形式如下：

| Time | queryId | statement |
| ---- | ------- | --------- |
|      |         |           |

其中 statement 最大显示长度为 64 字符。对于超过 64 字符的查询语句，将截取部分进行显示。

## 集群 Region 分布监控工具

集群中以 Region 作为数据复制和数据管理的单元，Region 的状态和分布对于系统运维和测试有很大帮助，如以下场景：

- 查看集群中各个 Region 被分配到了哪些 DataNode，是否均衡

当前 IoTDB 支持使用如下 SQL 查看 Region：

- `SHOW REGIONS`: 展示所有 Region
- `SHOW SCHEMA REGIONS`: 展示所有 SchemaRegion 分布
- `SHOW DATA REGIONS`: 展示所有 DataRegion 分布
- `SHOW (DATA|SCHEMA)? REGIONS OF STORAGE GROUP <sg1,sg2,...>`: 展示指定的存储组<sg1,sg2,...>对应的Region分布


```sql
IoTDB> create timeseries root.sg.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.sg.d2.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.ln.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> show regions
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         3|127.0.0.1|6671|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|6667|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
Total line number = 2
It costs 0.035s

IoTDB> insert into root.sg.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.
IoTDB> show regions
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         3|127.0.0.1|6671|
|       1|  DataRegion|    Up|      root.sg|           1|         1|         1|127.0.0.1|6669|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|6667|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
Total line number = 3
It costs 0.010s

IoTDB> insert into root.ln.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.
IoTDB> show data regions
+--------+----------+------+-------------+------------+----------+----------+---------+----+
|RegionId|      Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+----------+------+-------------+------------+----------+----------+---------+----+
|       1|DataRegion|    Up|      root.sg|           1|         1|         1|127.0.0.1|6669|
|       2|DataRegion|    Up|      root.ln|           1|         1|         1|127.0.0.1|6669|
+--------+----------+------+-------------+------------+----------+----------+---------+----+
Total line number = 2
It costs 0.011s
IoTDB> show schema regions
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         3|127.0.0.1|6671|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|6667|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
Total line number = 2
It costs 0.012s

IoTDB> show regions of storage group root.sg1
show regions of storage group root.sg1
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|      10|SchemaRegion|    Up|     root.sg1|           1|         0|         4|127.0.0.1|6669|
|      11|  DataRegion|    Up|     root.sg1|           1|         1|         5|127.0.0.1|6671|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
Total line number = 2
It costs 0.005s

IoTDB> show regions of storage group root.sg1, root.sg2
show regions of storage group root.sg1, root.sg2
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|      10|SchemaRegion|    Up|     root.sg1|           1|         0|         4|127.0.0.1|6669|
|      11|  DataRegion|    Up|     root.sg1|           1|         1|         5|127.0.0.1|6671|
|      12|SchemaRegion|    Up|     root.sg2|           1|         0|         4|127.0.0.1|6669|
|      13|  DataRegion|    Up|     root.sg2|           1|         1|         4|127.0.0.1|6669|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
Total line number = 4
It costs 0.005s

IoTDB> show regions of storage group root.*.sg_2
show regions of storage group root.*.sg_2
+--------+------------+------+--------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status| storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+--------------+------------+----------+----------+---------+----+
|      14|SchemaRegion|    Up|root.sg_1.sg_2|           1|         0|         3|127.0.0.1|6667|
|      15|  DataRegion|    Up|root.sg_1.sg_2|           1|         1|         5|127.0.0.1|6671|
+--------+------------+------+--------------+------------+----------+----------+---------+----+
Total line number = 2
It costs 0.004s

IoTDB> show data regions of storage group root.*.sg_2
show data regions of storage group root.*.sg_2
+--------+----------+------+--------------+------------+----------+----------+---------+----+
|RegionId|      Type|Status| storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+----------+------+--------------+------------+----------+----------+---------+----+
|      15|DataRegion|    Up|root.sg_1.sg_2|           1|         1|         5|127.0.0.1|6671|
+--------+----------+------+--------------+------------+----------+----------+---------+----+
Total line number = 1
It costs 0.004s


IoTDB> show schema regions of storage group root.*.sg_2
show schema regions of storage group root.*.sg_2
+--------+------------+------+--------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status| storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+--------------+------------+----------+----------+---------+----+
|       14|SchemaRegion|    Up|root.sg_1.sg_2|           1|         0|         5|127.0.0.1|6671|
+--------+------------+------+--------------+------------+----------+----------+---------+----+
Total line number = 1
It costs 0.102s
```
## 集群节点分布式监控工具

### 查看DataNode节点信息

当前 IoTDB 支持使用如下 SQL 查看 DataNode的信息：

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
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         1|127.0.0.1|6667|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|6668|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
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
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|RegionId|        Type|Status|storage group|Series Slots|Time Slots|DataNodeId|     Host|Port|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
|       0|SchemaRegion|    Up|      root.sg|           2|         0|         1|127.0.0.1|6667|
|       1|SchemaRegion|    Up|      root.ln|           1|         0|         2|127.0.0.1|6668|
|       2|  DataRegion|    Up|      root.ln|           1|         1|         1|127.0.0.1|6667|
+--------+------------+------+-------------+------------+----------+----------+---------+----+
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

停止一个节点之后，节点的状态会发生改变，状态显示如下：

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

### 查看ConfigNode节点信息

当前 IoTDB 支持使用如下 SQL 查看 ConfigNode的信息：

```
SHOW CONFIGNODES
```

示例：

```
IoTDB> show confignodes
+------+-------+-------+-----+
|NodeID| Status|   Host| Port|
+------+-------+-------+-----+
|     0|Running|0.0.0.0|22277|
|     1|Running|0.0.0.0|22279|
|     2|Running|0.0.0.0|22281|
+------+-------+-------+-----+
Total line number = 3
It costs 0.030s
```

停止一个节点之后，节点的状态会发生改变，状态显示如下：

```
IoTDB> show confignodes
+------+-------+-------+-----+
|NodeID| Status|   Host| Port|
+------+-------+-------+-----+
|     0|Running|0.0.0.0|22277|
|     1|Running|0.0.0.0|22279|
|     2|Unknown|0.0.0.0|22281|
+------+-------+-------+-----+
Total line number = 3
It costs 0.009s
```

### 查看全部节点信息

当前 IoTDB 支持使用如下 SQL 查看全部节点的信息：

```
SHOW CLUSTER
```

示例：

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

停止一个节点之后，节点的状态会发生改变，状态显示如下：

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

