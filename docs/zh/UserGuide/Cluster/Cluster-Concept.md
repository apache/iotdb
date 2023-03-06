<!--

```
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
```

-->

# 分布式
## 集群基本概念

Apache IoTDB 集群版包含两种角色的节点，ConfigNode 和 DataNode，分别为不同的进程，可独立部署。

集群架构示例如下图：

<img style="width:100%; max-width:500px; max-height:400px; margin-left:auto; margin-right:auto; display:block;" src="/img/UserGuide/Cluster/Architecture.png?raw=true">


ConfigNode 是集群的控制节点，管理集群的节点状态、分区信息等，集群所有 ConfigNode 组成一个高可用组，数据全量备份。

注意：ConfigNode 的副本数是集群当前加入的 ConfigNode 个数，一半以上的 ConfigNode 存活集群才能提供服务。

DataNode 是集群的数据节点，管理多个数据分片、元数据分片，数据即时间序列中的时间戳和值，元数据为时间序列的路径信息、数据类型等。

Client 只能通过 DataNode 进行数据读写。

### 名词解释

| 名词                | 类型      | 解释                                                 |
|:------------------|:--------|:---------------------------------------------------|
| ConfigNode        | 节点角色    | 配置节点，管理集群节点信息、分区信息，监控集群状态、控制负载均衡                   |
| DataNode          | 节点角色    | 数据节点，管理数据、元数据                                      |
| Database          | 元数据     | 数据库，不同数据库的数据物理隔离                                   |
| DeviceId          | 设备名     | 元数据树中从 root 到倒数第二级的全路径表示一个设备名                      |
| SeriesSlot        | 元数据分区   | 每个 Database 包含多个元数据分区，根据设备名进行分区                    |
| SchemaRegion      | 一组元数据分区 | 多个 SeriesSlot 的集合                                  |
| SchemaRegionGroup | 逻辑概念    | 包含元数据副本数个 SchemaRegion，管理相同的元数据，互为备份               |
| SeriesTimeSlot    | 数据分区    | 一个元数据分区的一段时间的数据对应一个数据分区，每个元数据分区对应多个数据分区，根据时间范围进行分区 |
| DataRegion        | 一组数据分区  | 多个 SeriesTimeSlot 的集合                              |
| DataRegionGroup   | 逻辑概念    | 包含数据副本数个 DataRegion，管理相同的数据，互为备份                   |

## 集群特点

* 原生分布式
    * IoTDB 各模块原生支持分布式。
    * Standalone 是分布式的一种特殊的部署形态。
* 扩展性
    * 支持秒级增加节点，无需进行数据迁移。
* 大规模并行处理架构 MPP
    * 采用大规模并行处理架构及火山模型进行数据处理，具有高扩展性。
* 可根据不同场景需求选择不同的共识协议
    * 数据副本组和元数据副本组，可以采用不同的共识协议。
* 可扩展分区策略
    * 集群采用分区表管理数据和元数据分区，自定义灵活的分配策略。
* 内置监控框架
    * 内置集群监控，可以监控集群节点。

## 分区策略

分区策略将数据和元数据划分到不同的 RegionGroup 中，并把 RegionGroup 的 Region 分配到不同的 DataNode。

推荐设置 1 个 database，集群会根据节点数和核数动态分配资源。

Database 包含多个 SchemaRegion 和 DataRegion，由 DataNode 管理。

* 元数据分区策略 
    * 对于一条未使用模板的时间序列的元数据，ConfigNode 会根据设备 ID （从 root 到倒数第二层节点的全路径）映射到一个序列分区，并将此序列分区分配到一组 SchemaRegion 中。

* 数据分区策略 
    * 对于一个时间序列数据点，ConfigNode 会根据设备 ID 映射到一个序列分区（纵向分区），再根据时间戳映射到一个序列时间分区（横向分区），并将此序列时间分区分配到一组 DataRegion 中。

IoTDB 使用了基于槽的分区策略，因此分区信息的大小是可控的，不会随时间序列或设备数无限增长。

Region 会分配到不同的 DataNode 上，分配 Region 时会保证不同 DataNode 的负载均衡。

## 复制策略

复制策略将数据复制多份，互为副本，多个副本可以一起提供高可用服务，容忍部分副本失效的情况。

Region 是数据复制的基本单位，一个 Region 的多个副本构成了一个高可用复制组，数据互为备份。

* 集群内的副本组
    * ConfigNodeGroup：由所有 ConfigNode 组成。
    * SchemaRegionGroup：集群有多个元数据组，每个 SchemaRegionGroup 内有多个 ID 相同的 SchemaRegion。
    * DataRegionGroup：集群有多个数据组，每个 DataRegionGroup 内有多个 ID 相同的 DataRegion。
    

完整的集群分区复制的示意图如下：

<img style="width:100%; max-width:500px; max-height:500px; margin-left:auto; margin-right:auto; display:block;" src="/img/UserGuide/Cluster/Data-Partition.png?raw=true">

图中包含 1 个 SchemaRegionGroup，元数据采用 3 副本，因此 3 个白色的 SchemaRegion-0 组成了一个副本组。

图中包含 3 个 DataRegionGroup，数据采用 3 副本，因此一共有 9 个 DataRegion。

## 共识协议（一致性协议）

每个副本组的多个副本之间，都通过一个具体的共识协议保证数据一致性，共识协议会将读写请求应用到多个副本上。

* 现有的共识协议
    * SimpleConsensus：提供强一致性，仅单副本时可用，一致性协议的极简实现，效率最高。
    * IoTConsensus：提供最终一致性，任意副本数可用，2 副本时可容忍 1 节点失效，当前仅可用于 DataRegion 的副本上，写入可以在任一副本进行，并异步复制到其他副本。
    * RatisConsensus：提供强一致性，Raft 协议的一种实现，任意副本数可用，当前可用于任意副本组上。目前DataRegion使用RatisConsensus时暂不支持多数据目录，预计会在后续版本中支持这一功能。
