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

# 集群运维命令

## 展示 ConfigNode 信息

当前 IoTDB 支持使用如下 SQL 展示 ConfigNode 的信息：
```
SHOW CONFIGNODES
```

示例：
```
IoTDB> show confignodes
+------+-------+---------------+------------+--------+
|NodeID| Status|InternalAddress|InternalPort|    Role|
+------+-------+---------------+------------+--------+
|     0|Running|      127.0.0.1|       22277|  Leader|
|     1|Running|      127.0.0.1|       22279|Follower|
|     2|Running|      127.0.0.1|       22281|Follower|
+------+-------+---------------+------------+--------+
Total line number = 3
It costs 0.030s
```

### ConfigNode 状态定义
对 ConfigNode 各状态定义如下：

- **Running**: ConfigNode 正常运行
- **Unknown**: ConfigNode 未正常上报心跳

## 展示 DataNode 信息

当前 IoTDB 支持使用如下 SQL 展示 DataNode 的信息：
```
SHOW DATANODES
```

示例：
```
IoTDB> create timeseries root.sg.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.sg.d2.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> create timeseries root.ln.d1.s1 with datatype=BOOLEAN,encoding=PLAIN
Msg: The statement is executed successfully.
IoTDB> show datanodes
+------+-------+----------+-------+-------------+---------------+
|NodeID| Status|RpcAddress|RpcPort|DataRegionNum|SchemaRegionNum|
+------+-------+----------+-------+-------------+---------------+
|     1|Running| 127.0.0.1|   6667|            0|              1|
|     2|Running| 127.0.0.1|   6668|            0|              1|
+------+-------+----------+-------+-------------+---------------+

Total line number = 2
It costs 0.007s

IoTDB> insert into root.ln.d1(timestamp,s1) values(1,true)
Msg: The statement is executed successfully.
IoTDB> show datanodes
+------+-------+----------+-------+-------------+---------------+
|NodeID| Status|RpcAddress|RpcPort|DataRegionNum|SchemaRegionNum|
+------+-------+----------+-------+-------------+---------------+
|     1|Running| 127.0.0.1|   6667|            1|              1|
|     2|Running| 127.0.0.1|   6668|            0|              1|
+------+-------+----------+-------+-------------+---------------+
Total line number = 2
It costs 0.006s
```

### DataNode 状态定义
DataNode 的状态机如下图所示：
<img style="width:100%; max-width:500px; max-height:500px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Cluster/DataNode-StateMachine-ZH.jpg?raw=true">

对 DataNode 各状态定义如下：

- **Running**: DataNode 正常运行，可读可写
- **Unknown**: DataNode 未正常上报心跳，ConfigNode 认为该 DataNode 不可读写
- **Removing**: DataNode 正在移出集群，不可读写
- **ReadOnly**: DataNode 磁盘剩余空间低于 disk_warning_threshold（默认 5%），DataNode 可读但不能写入，不能同步数据

## 展示全部节点信息

当前 IoTDB 支持使用如下 SQL 展示全部节点的信息：
```
SHOW CLUSTER
```

示例：
```
IoTDB> show cluster
+------+----------+-------+---------------+------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|
+------+----------+-------+---------------+------------+
|     0|ConfigNode|Running|      127.0.0.1|       22277|
|     1|ConfigNode|Running|      127.0.0.1|       22279|
|     2|ConfigNode|Running|      127.0.0.1|       22281|
|     3|  DataNode|Running|      127.0.0.1|        9003|
|     4|  DataNode|Running|      127.0.0.1|        9005|
|     5|  DataNode|Running|      127.0.0.1|        9007|
+------+----------+-------+---------------+------------+
Total line number = 6
It costs 0.011s
```

在节点被关停后，它的状态也会改变，如下所示：
```
IoTDB> show cluster
+------+----------+-------+---------------+------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|
+------+----------+-------+---------------+------------+
|     0|ConfigNode|Running|      127.0.0.1|       22277|
|     1|ConfigNode|Unknown|      127.0.0.1|       22279|
|     2|ConfigNode|Running|      127.0.0.1|       22281|
|     3|  DataNode|Running|      127.0.0.1|        9003|
|     4|  DataNode|Running|      127.0.0.1|        9005|
|     5|  DataNode|Running|      127.0.0.1|        9007|
+------+----------+-------+---------------+------------+
Total line number = 6
It costs 0.012s
```

展示全部节点的详细配置信息：
```
SHOW CLUSTER DETAILS
```

示例：
```
IoTDB> show cluster details
+------+----------+-------+---------------+------------+-------------------+----------+-------+-----------------+-------------------+-------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|DataConsensusPort|SchemaConsensusPort|MppPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-----------------+-------------------+-------+
|     0|ConfigNode|Running|      127.0.0.1|       22277|              22278|          |       |                 |                   |       |
|     1|ConfigNode|Running|      127.0.0.1|       22279|              22280|          |       |                 |                   |       |
|     2|ConfigNode|Running|      127.0.0.1|       22281|              22282|          |       |                 |                   |       |
|     3|  DataNode|Running|      127.0.0.1|        9003|                   | 127.0.0.1|   6667|            40010|              40030|   8777|
|     4|  DataNode|Running|      127.0.0.1|        9004|                   | 127.0.0.1|   6668|            40011|              50011|   8778|
|     5|  DataNode|Running|      127.0.0.1|        9005|                   | 127.0.0.1|   6669|            40012|              50012|   8779|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-----------------+-------------------+-------+
Total line number = 6
It costs 0.340s
```

## 展示 Region 信息

集群中以 SchemaRegion/DataRegion 作为元数据/数据的复制和管理单元，Region 的状态和分布对于系统运维和测试有很大帮助，如以下场景：

- 查看集群中各个 Region 被分配到了哪些 DataNode，是否均衡
- 查看集群中各个 Region 被分配了哪些分区，是否均衡
- 查看集群中各个 RegionGroup 的 leader 被分配到了哪些 DataNode，是否均衡

当前 IoTDB 支持使用如下 SQL 展示 Region 信息：

- `SHOW REGIONS`: 展示所有 Region 分布
- `SHOW SCHEMA REGIONS`: 展示所有 SchemaRegion 分布
- `SHOW DATA REGIONS`: 展示所有 DataRegion 分布
- `SHOW (DATA|SCHEMA)? REGIONS OF DATABASE <sg1,sg2,...>`: 展示指定数据库 <sg1,sg2,...> 对应的 Region 分布

展示所有 Region 的分布：
```
IoTDB> show regions
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 12
It costs 0.165s
```

展示 SchemaRegion 或 DataRegion 的分布：
```
IoTDB> show data regions
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.011s

IoTDB> show schema regions
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.012s
```

展示指定数据库 <sg1,sg2,...> 对应的 Region 分布：
```
IoTDB> show regions of database root.sg1
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+-- -----+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.007s

IoTDB> show regions of database root.sg1, root.sg2
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|  DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|  DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|  DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|  DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|  DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 12
It costs 0.009s

IoTDB> show data regions of database root.sg1, root.sg2
+--------+----------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|      Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+----------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       0|DataRegion|Running|root.sg1|          1|        1|         1| 127.0.0.1|   6667|Follower|
|       0|DataRegion|Running|root.sg1|          1|        1|         2| 127.0.0.1|   6668|  Leader|
|       0|DataRegion|Running|root.sg1|          1|        1|         3| 127.0.0.1|   6669|Follower|
|       2|DataRegion|Running|root.sg2|          1|        1|         1| 127.0.0.1|   6667|  Leader|
|       2|DataRegion|Running|root.sg2|          1|        1|         2| 127.0.0.1|   6668|Follower|
|       2|DataRegion|Running|root.sg2|          1|        1|         3| 127.0.0.1|   6669|Follower|
+--------+----------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.007s

IoTDB> show schema regions of database root.sg1, root.sg2
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|RegionId|        Type| Status|Database|SeriesSlots|TimeSlots|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
|       1|SchemaRegion|Running|root.sg1|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         2| 127.0.0.1|   6668|Follower|
|       1|SchemaRegion|Running|root.sg1|          1|        0|         3| 127.0.0.1|   6669|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         1| 127.0.0.1|   6667|Follower|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         2| 127.0.0.1|   6668|  Leader|
|       3|SchemaRegion|Running|root.sg2|          1|        0|         3| 127.0.0.1|   6669|Follower|
+--------+------------+-------+--------+-----------+---------+----------+----------+-------+--------+
Total line number = 6
It costs 0.009s
```

### Region 状态定义
Region 继承所在 DataNode 的状态，对 Region 各状态定义如下：

- **Running**: Region 所在 DataNode 正常运行，Region 可读可写
- **Unknown**: Region 所在 DataNode 未正常上报心跳，ConfigNode 认为该 Region 不可读写
- **Removing**: Region 所在 DataNode 正在被移出集群，Region 不可读写
- **ReadOnly**: Region 所在 DataNode 的磁盘剩余空间低于 disk_warning_threshold（默认 5%），Region 可读，但不能写入，不能同步数据

## 展示集群槽信息

集群使用分区来管理元数据和数据，分区定义如下：

- **元数据分区**：序列槽
- **数据分区**：<序列槽，时间分区槽>

可以使用以下 SQL 来查询分区对应信息：

### 展示数据分区所在的 DataRegion

展示一个数据分区（或一个序列槽下的所有数据分区）所在的 DataRegion:
- `SHOW DATA REGIONID OF root.sg WHERE SERIESSLOTID=s0 (AND TIMESLOTID=t0)`
  
其中，”SERIESSLOTID=s0”可以被替换为”DEVICEID=xxx.xx.xx“. 这样的话，sql会自动计算对应该设备id的序列槽。

同样的，"TIMESLOTID=t0"也可以被替换为"TIMESTAMP=t1"。这样，SQL会计算该时间戳对应的时间槽，也就是时间段包含该时间戳的时间槽。


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

### 展示元数据分区所在的 SchemaRegion

展示一个元数据分区所在的 SchemaRegion：
- `SHOW SCHEMA REGIONID OF root.sg WHERE SERIESSLOTID=s0`

同样的，”SERIESSLOTID“与”TIMESLOTID“依然是可替换的。

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

### 展示序列槽下的时间槽
展示一个序列槽下的所有时间槽：
- `SHOW TIMESLOTID OF root.sg WHERE SERIESLOTID=s0 (AND STARTTIME=t1) (AND ENDTIME=t2)`

示例:
```
IoTDB> show timeslotid of root.sg where seriesslotid=5286
+----------+
|TimeSlotId|
+----------+
|         0|
|      1000|
+----------+
Total line number = 1
It costs 0.007s
```

### 展示数据库的序列槽
展示一个数据库内，数据，元数据或是所有的序列槽：
- `SHOW (DATA|SCHEMA)? SERIESSLOTID OF root.sg`

示例:
```
IoTDB> show data seriesslotid of root.sg
+------------+
|SeriesSlotId|
+------------+
|        5286|
+------------+
Total line number = 1
It costs 0.007s

IoTDB> show schema seriesslotid of root.sg
+------------+
|SeriesSlotId|
+------------+
|        5286|
+------------+
Total line number = 1
It costs 0.006s

IoTDB> show seriesslotid of root.sg
+------------+
|SeriesSlotId|
+------------+
|        5286|
+------------+
Total line number = 1
It costs 0.006s
```

## 迁移 Region
以下 SQL 语句可以被用于手动迁移一个 region， 可用于负载均衡或其他目的。
```
MIGRATE REGION <Region-id> FROM <original-DataNodeId> TO <dest-DataNodeId>
```
示例:
```
IoTDB> SHOW REGIONS
+--------+------------+-------+-------------+------------+----------+----------+----------+-------+--------+
|RegionId|        Type| Status|     Database|SeriesSlotId|TimeSlotId|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+-------------+------------+----------+----------+----------+-------+--------+
|       0|SchemaRegion|Running|root.test.g_0|         500|         0|         3| 127.0.0.1|   6670|  Leader|
|       0|SchemaRegion|Running|root.test.g_0|         500|         0|         4| 127.0.0.1|   6681|Follower|
|       0|SchemaRegion|Running|root.test.g_0|         500|         0|         5| 127.0.0.1|   6668|Follower|
|       1|  DataRegion|Running|root.test.g_0|         183|       200|         1| 127.0.0.1|   6667|Follower|
|       1|  DataRegion|Running|root.test.g_0|         183|       200|         3| 127.0.0.1|   6670|Follower|
|       1|  DataRegion|Running|root.test.g_0|         183|       200|         7| 127.0.0.1|   6669|  Leader|
|       2|  DataRegion|Running|root.test.g_0|         181|       200|         3| 127.0.0.1|   6670|  Leader|
|       2|  DataRegion|Running|root.test.g_0|         181|       200|         4| 127.0.0.1|   6681|Follower|
|       2|  DataRegion|Running|root.test.g_0|         181|       200|         5| 127.0.0.1|   6668|Follower|
|       3|  DataRegion|Running|root.test.g_0|         180|       200|         1| 127.0.0.1|   6667|Follower|
|       3|  DataRegion|Running|root.test.g_0|         180|       200|         5| 127.0.0.1|   6668|  Leader|
|       3|  DataRegion|Running|root.test.g_0|         180|       200|         7| 127.0.0.1|   6669|Follower|
|       4|  DataRegion|Running|root.test.g_0|         179|       200|         3| 127.0.0.1|   6670|Follower|
|       4|  DataRegion|Running|root.test.g_0|         179|       200|         4| 127.0.0.1|   6681|  Leader|
|       4|  DataRegion|Running|root.test.g_0|         179|       200|         7| 127.0.0.1|   6669|Follower|
|       5|  DataRegion|Running|root.test.g_0|         179|       200|         1| 127.0.0.1|   6667|  Leader|
|       5|  DataRegion|Running|root.test.g_0|         179|       200|         4| 127.0.0.1|   6681|Follower|
|       5|  DataRegion|Running|root.test.g_0|         179|       200|         5| 127.0.0.1|   6668|Follower|
+--------+------------+-------+-------------+------------+----------+----------+----------+-------+--------+
Total line number = 18
It costs 0.161s

IoTDB> MIGRATE REGION 1 FROM 3 TO 4
Msg: The statement is executed successfully.

IoTDB> SHOW REGIONS
+--------+------------+-------+-------------+------------+----------+----------+----------+-------+--------+
|RegionId|        Type| Status|     Database|SeriesSlotId|TimeSlotId|DataNodeId|RpcAddress|RpcPort|    Role|
+--------+------------+-------+-------------+------------+----------+----------+----------+-------+--------+
|       0|SchemaRegion|Running|root.test.g_0|         500|         0|         3| 127.0.0.1|   6670|  Leader|
|       0|SchemaRegion|Running|root.test.g_0|         500|         0|         4| 127.0.0.1|   6681|Follower|
|       0|SchemaRegion|Running|root.test.g_0|         500|         0|         5| 127.0.0.1|   6668|Follower|
|       1|  DataRegion|Running|root.test.g_0|         183|       200|         1| 127.0.0.1|   6667|Follower|
|       1|  DataRegion|Running|root.test.g_0|         183|       200|         4| 127.0.0.1|   6681|Follower|
|       1|  DataRegion|Running|root.test.g_0|         183|       200|         7| 127.0.0.1|   6669|  Leader|
|       2|  DataRegion|Running|root.test.g_0|         181|       200|         3| 127.0.0.1|   6670|  Leader|
|       2|  DataRegion|Running|root.test.g_0|         181|       200|         4| 127.0.0.1|   6681|Follower|
|       2|  DataRegion|Running|root.test.g_0|         181|       200|         5| 127.0.0.1|   6668|Follower|
|       3|  DataRegion|Running|root.test.g_0|         180|       200|         1| 127.0.0.1|   6667|Follower|
|       3|  DataRegion|Running|root.test.g_0|         180|       200|         5| 127.0.0.1|   6668|  Leader|
|       3|  DataRegion|Running|root.test.g_0|         180|       200|         7| 127.0.0.1|   6669|Follower|
|       4|  DataRegion|Running|root.test.g_0|         179|       200|         3| 127.0.0.1|   6670|Follower|
|       4|  DataRegion|Running|root.test.g_0|         179|       200|         4| 127.0.0.1|   6681|  Leader|
|       4|  DataRegion|Running|root.test.g_0|         179|       200|         7| 127.0.0.1|   6669|Follower|
|       5|  DataRegion|Running|root.test.g_0|         179|       200|         1| 127.0.0.1|   6667|  Leader|
|       5|  DataRegion|Running|root.test.g_0|         179|       200|         4| 127.0.0.1|   6681|Follower|
|       5|  DataRegion|Running|root.test.g_0|         179|       200|         5| 127.0.0.1|   6668|Follower|
+--------+------------+-------+-------------+------------+----------+----------+----------+-------+--------+
Total line number = 18
It costs 0.165s
```