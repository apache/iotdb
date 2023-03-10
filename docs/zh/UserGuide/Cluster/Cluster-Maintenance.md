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

## 集群运维命令

### 展示集群配置

当前 IoTDB 支持使用如下 SQL 展示集群的关键参数：
```
SHOW VARIABLES
```

示例：
```
IoTDB> show variables
+----------------------------------+-----------------------------------------------------------------+
|                         Variables|                                                            Value|
+----------------------------------+-----------------------------------------------------------------+
|                       ClusterName|                                                   defaultCluster|
|             DataReplicationFactor|                                                                1|
|           SchemaReplicationFactor|                                                                1|
|  DataRegionConsensusProtocolClass|                      org.apache.iotdb.consensus.iot.IoTConsensus|
|SchemaRegionConsensusProtocolClass|                  org.apache.iotdb.consensus.ratis.RatisConsensus|
|  ConfigNodeConsensusProtocolClass|                  org.apache.iotdb.consensus.ratis.RatisConsensus|
|             TimePartitionInterval|                                                        604800000|
|                    DefaultTTL(ms)|                                              9223372036854775807|
|              ReadConsistencyLevel|                                                           strong|
|           SchemaRegionPerDataNode|                                                              1.0|
|            DataRegionPerProcessor|                                                              1.0|
|           LeastDataRegionGroupNum|                                                                5|
|                     SeriesSlotNum|                                                            10000|
|           SeriesSlotExecutorClass|org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor|
|         DiskSpaceWarningThreshold|                                                             0.05|
+----------------------------------+-----------------------------------------------------------------+
Total line number = 15
It costs 0.225s
```

**注意：** 必须保证该 SQL 展示的所有配置参数在同一集群各个节点完全一致

### 展示 ConfigNode 信息

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
|     0|Running|      127.0.0.1|       10710|  Leader|
|     1|Running|      127.0.0.1|       10711|Follower|
|     2|Running|      127.0.0.1|       10712|Follower|
+------+-------+---------------+------------+--------+
Total line number = 3
It costs 0.030s
```

#### ConfigNode 状态定义
对 ConfigNode 各状态定义如下：

- **Running**: ConfigNode 正常运行
- **Unknown**: ConfigNode 未正常上报心跳
  - 无法接收其它 ConfigNode 同步来的数据
  - 不会被选为集群的 ConfigNode-leader

### 展示 DataNode 信息

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

#### DataNode 状态定义
DataNode 的状态机如下图所示：
<img style="width:100%; max-width:500px; max-height:500px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Cluster/DataNode-StateMachine-ZH.jpg?raw=true">

对 DataNode 各状态定义如下：

- **Running**: DataNode 正常运行，可读可写
- **Unknown**: DataNode 未正常上报心跳，ConfigNode 认为该 DataNode 不可读写
  - 少数 Unknown DataNode 不影响集群读写
- **Removing**: DataNode 正在移出集群，不可读写
  - 少数 Removing DataNode 不影响集群读写 
- **ReadOnly**: DataNode 磁盘剩余空间低于 disk_warning_threshold（默认 5%），DataNode 可读但不能写入，不能同步数据
  - 少数 ReadOnly DataNode 不影响集群读写
  - ReadOnly DataNode 可以查询元数据和数据
  - ReadOnly DataNode 可以删除元数据和数据
  - ReadOnly DataNode 可以创建元数据，不能写入数据
  - 所有 DataNode 处于 ReadOnly 状态时，集群不能写入数据，仍可以创建 Database 和元数据

**对于一个 DataNode**，不同状态元数据查询、创建、删除的影响如下表所示：

| DataNode 状态 | 可读  | 可创建 | 可删除 |
|-------------|-----|-----|-----|
| Running     | 是   | 是   | 是   |
| Unknown     | 否   | 否   | 否   |
| Removing    | 否   | 否   | 否   |
| ReadOnly    | 是   | 是   | 是   |

**对于一个 DataNode**，不同状态数据查询、写入、删除的影响如下表所示：

| DataNode 状态 | 可读  | 可写  | 可删除 |
|-------------|-----|-----|-----|
| Running     | 是   | 是   | 是   |
| Unknown     | 否   | 否   | 否   |
| Removing    | 否   | 否   | 否   |
| ReadOnly    | 是   | 否   | 是   |

### 展示全部节点信息

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
|     0|ConfigNode|Running|      127.0.0.1|       10710|
|     1|ConfigNode|Running|      127.0.0.1|       10711|
|     2|ConfigNode|Running|      127.0.0.1|       10712|
|     3|  DataNode|Running|      127.0.0.1|       10730|
|     4|  DataNode|Running|      127.0.0.1|       10731|
|     5|  DataNode|Running|      127.0.0.1|       10732|
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
|     0|ConfigNode|Running|      127.0.0.1|       10710|
|     1|ConfigNode|Unknown|      127.0.0.1|       10711|
|     2|ConfigNode|Running|      127.0.0.1|       10712|
|     3|  DataNode|Running|      127.0.0.1|       10730|
|     4|  DataNode|Running|      127.0.0.1|       10731|
|     5|  DataNode|Running|      127.0.0.1|       10732|
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
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|NodeID|  NodeType| Status|InternalAddress|InternalPort|ConfigConsensusPort|RpcAddress|RpcPort|MppPort|SchemaConsensusPort|DataConsensusPort|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
|     0|ConfigNode|Running|      127.0.0.1|       10710|              10720|          |       |       |                   |                 |
|     1|ConfigNode|Running|      127.0.0.1|       10711|              10721|          |       |       |                   |                 |
|     2|ConfigNode|Running|      127.0.0.1|       10712|              10722|          |       |       |                   |                 |
|     3|  DataNode|Running|      127.0.0.1|       10730|                   | 127.0.0.1|   6667|  10740|              10750|            10760|
|     4|  DataNode|Running|      127.0.0.1|       10731|                   | 127.0.0.1|   6668|  10741|              10751|            10761|
|     5|  DataNode|Running|      127.0.0.1|       10732|                   | 127.0.0.1|   6669|  10742|              10752|            10762|
+------+----------+-------+---------------+------------+-------------------+----------+-------+-------+-------------------+-----------------+
Total line number = 6
It costs 0.340s
```

### 展示 Region 信息

集群中以 SchemaRegion/DataRegion 作为元数据/数据的复制和管理单元，Region 的状态和分布对于系统运维和测试有很大帮助，如以下场景：

- 查看集群中各个 Region 被分配到了哪些 DataNode，是否均衡
- 查看集群中各个 Region 被分配了哪些分区，是否均衡
- 查看集群中各个 RegionGroup 的 leader 被分配到了哪些 DataNode，是否均衡

当前 IoTDB 支持使用如下 SQL 展示 Region 信息：

- `SHOW REGIONS`: 展示所有 Region 分布
- `SHOW SCHEMA REGIONS`: 展示所有 SchemaRegion 分布
- `SHOW DATA REGIONS`: 展示所有 DataRegion 分布
- `SHOW (DATA|SCHEMA)? REGIONS OF DATABASE <sg1,sg2,...>`: 展示指定数据库 <sg1,sg2,...> 对应的 Region 分布
- `SHOW (DATA|SCHEMA)? REGIONS ON NODEID <id1,id2,...>`: 展示指定节点 <id1,id2,...> 对应的 Region 分布
- `SHOW (DATA|SCHEMA)? REGIONS (OF DATABASE <sg1,sg2,...>)? (ON NODEID <id1,id2,...>)?`: 展示指定数据库 <sg1,sg2,...> 在指定节点 <id1,id2,...> 对应的 Region 分布

展示所有 Region 的分布：
```
IoTDB> show regions
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       0|  DataRegion|Running|root.sg1|            1|          1|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:18.749|
|       0|  DataRegion|Running|root.sg1|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.013|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:18.245|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         3| 127.0.0.1|   6669|  Leader|2023-03-07T17:32:18.398|
|       2|  DataRegion|Running|root.sg2|            1|          1|         1| 127.0.0.1|   6667|  Leader|2023-03-07T17:32:19.834|
|       2|  DataRegion|Running|root.sg2|            1|          1|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:20.011|
|       2|  DataRegion|Running|root.sg2|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:20.395|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:19.232|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:19.450|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.637|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 12
It costs 0.165s
```
其中，SeriesSlotNum 指的是 region 内 seriesSlot 的个数。同样地，TimeSlotNum 也指 region 内 timeSlot 的个数。

展示 SchemaRegion 或 DataRegion 的分布：
```
IoTDB> show data regions
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       0|  DataRegion|Running|root.sg1|            1|          1|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:18.749|
|       0|  DataRegion|Running|root.sg1|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.013|
|       2|  DataRegion|Running|root.sg2|            1|          1|         1| 127.0.0.1|   6667|  Leader|2023-03-07T17:32:19.834|
|       2|  DataRegion|Running|root.sg2|            1|          1|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:20.011|
|       2|  DataRegion|Running|root.sg2|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:20.395|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 6
It costs 0.011s

IoTDB> show schema regions
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:18.245|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         3| 127.0.0.1|   6669|  Leader|2023-03-07T17:32:18.398|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:19.232|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:19.450|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.637|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 6
It costs 0.012s
```

展示指定数据库 <sg1,sg2,...> 对应的 Region 分布：
```
IoTDB> show regions of database root.sg1
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+-- -----+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       0|  DataRegion|Running|root.sg1|            1|          1|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:18.749|
|       0|  DataRegion|Running|root.sg1|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.013|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:18.245|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         3| 127.0.0.1|   6669|  Leader|2023-03-07T17:32:18.398|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 6
It costs 0.007s

IoTDB> show regions of database root.sg1, root.sg2
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       0|  DataRegion|Running|root.sg1|            1|          1|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:18.749|
|       0|  DataRegion|Running|root.sg1|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.013|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:18.245|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         3| 127.0.0.1|   6669|  Leader|2023-03-07T17:32:18.398|
|       2|  DataRegion|Running|root.sg2|            1|          1|         1| 127.0.0.1|   6667|  Leader|2023-03-07T17:32:19.834|
|       2|  DataRegion|Running|root.sg2|            1|          1|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:20.011|
|       2|  DataRegion|Running|root.sg2|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:20.395|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:19.232|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:19.450|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.637|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 12
It costs 0.009s

IoTDB> show data regions of database root.sg1, root.sg2
+--------+----------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|      Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+----------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       0|DataRegion|Running|root.sg1|            1|          1|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:18.749|
|       0|DataRegion|Running|root.sg1|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.013|
|       2|DataRegion|Running|root.sg2|            1|          1|         1| 127.0.0.1|   6667|  Leader|2023-03-07T17:32:19.834|
|       2|DataRegion|Running|root.sg2|            1|          1|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:20.011|
|       2|DataRegion|Running|root.sg2|            1|          1|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:20.395|
+--------+----------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 6
It costs 0.007s

IoTDB> show schema regions of database root.sg1, root.sg2
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:18.245|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         3| 127.0.0.1|   6669|  Leader|2023-03-07T17:32:18.398|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:19.232|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:19.450|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         3| 127.0.0.1|   6669|Follower|2023-03-07T17:32:19.637|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 6
It costs 0.009s
```

展示指定节点 <id1,id2,...> 对应的 Region 分布：
```
IoTDB> show regions on nodeid 1
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
|       2|  DataRegion|Running|root.sg2|            1|          1|         1| 127.0.0.1|   6667|  Leader|2023-03-07T17:32:19.834|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:19.232|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 4
It costs 0.165s

IoTDB> show regions on nodeid 1, 2
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       0|  DataRegion|Running|root.sg1|            1|          1|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:18.749|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:18.245|
|       2|  DataRegion|Running|root.sg2|            1|          1|         1| 127.0.0.1|   6667|  Leader|2023-03-07T17:32:19.834|
|       2|  DataRegion|Running|root.sg2|            1|          1|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:19.011|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:19.232|
|       3|SchemaRegion|Running|root.sg2|            1|          0|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:19.450|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 8
It costs 0.165s
```

展示指定数据库 <sg1,sg2,...> 在指定节点 <id1,id2,...> 对应的 Region 分布：
```
IoTDB> show regions of database root.sg1 on nodeid 1
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       1|SchemaRegion|Running|root.sg1|            1|          0|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.111|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 2
It costs 0.165s

IoTDB> show data regions of database root.sg1, root.sg2 on nodeid 1, 2 
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|RegionId|        Type| Status|Database|SeriesSlotNum|TimeSlotNum|DataNodeId|RpcAddress|RpcPort|    Role|             CreateTime|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
|       0|  DataRegion|Running|root.sg1|            1|          1|         1| 127.0.0.1|   6667|Follower|2023-03-07T17:32:18.520|
|       0|  DataRegion|Running|root.sg1|            1|          1|         2| 127.0.0.1|   6668|  Leader|2023-03-07T17:32:18.749|
|       2|  DataRegion|Running|root.sg2|            1|          1|         1| 127.0.0.1|   6667|  Leader|2023-03-07T17:32:19.834|
|       2|  DataRegion|Running|root.sg2|            1|          1|         2| 127.0.0.1|   6668|Follower|2023-03-07T17:32:19.011|
+--------+------------+-------+--------+-------------+-----------+----------+----------+-------+--------+-----------------------+
Total line number = 4
It costs 0.165s
```

#### Region 状态定义
Region 继承所在 DataNode 的状态，对 Region 各状态定义如下：

- **Running**: Region 所在 DataNode 正常运行，Region 可读可写
- **Unknown**: Region 所在 DataNode 未正常上报心跳，ConfigNode 认为该 Region 不可读写
- **Removing**: Region 所在 DataNode 正在被移出集群，Region 不可读写
- **ReadOnly**: Region 所在 DataNode 的磁盘剩余空间低于 disk_warning_threshold（默认 5%），Region 可读，但不能写入，不能同步数据

**单个 Region 的状态切换不会影响所属 RegionGroup 的运行**，
在设置多副本集群时（即元数据副本数和数据副本数大于 1），
同 RegionGroup 其它 Running 状态的 Region 能保证该 RegionGroup 的高可用性。 

**对于一个 RegionGroup：**
- 当且仅当严格多于一半的 Region 处于 Running 状态时， 该 RegionGroup 可进行数据的查询、写入和删除操作
- 如果处于 Running 状态的 Region 少于一半，该 RegionGroup 不可进行数据的数据的查询、写入和删除操作

### 展示集群槽信息

集群使用分区来管理元数据和数据，分区定义如下：

- **元数据分区**：SeriesSlot
- **数据分区**：<SeriesSlot，SeriesTimeSlot>

在文档[Cluster-Concept](./Cluster-Concept.md)中可以查看详细信息。

可以使用以下 SQL 来查询分区对应信息：

#### 展示数据分区所在的 DataRegion

展示一个数据分区（或一个序列槽下的所有数据分区）所在的 DataRegion:
- `SHOW DATA REGIONID OF root.sg WHERE SERIESSLOTID=s0 (AND TIMESLOTID=t0)`
  
有如下几点说明：

1. s0、t0均只能为数字。 
   
2. TimeSlotId 是 SeriesTimeSlotId 的简称。

2. ”SERIESSLOTID=s0”可以被替换为”DEVICEID=xxx.xx.xx“。 这样的话，sql会自动计算对应该设备id的序列槽。

3. 同样的，"TIMESLOTID=t0"也可以被替换为"TIMESTAMP=t1"。这样，SQL会计算该时间戳对应的时间槽，也就是时间段包含该时间戳的时间槽。


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

#### 展示元数据分区所在的 SchemaRegion

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

#### 展示序列槽下的时间槽
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

#### 展示数据库的序列槽
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

### 迁移 Region
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