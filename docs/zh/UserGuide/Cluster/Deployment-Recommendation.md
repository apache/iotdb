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

## IoTDB 部署推荐
### 背景

系统能力
- 性能需求：系统读写速度，压缩比
- 扩展性：系统能够用多节点管理数据，本质上是数据是否可分区管理
- 高可用：系统能够容忍节点失效，本质上是数据是否有副本
- 一致性：当数据有多副本时，不同副本是否一致，本质上用户是否能将数据库当做单机看待

缩写
- C：ConfigNode
- D：DataNode
- aCbD：a 个 ConfigNode 和 b 个 DataNode

### 部署模式选型

|      模式      | 性能  | 扩展性 | 高可用 | 一致性 |
|:------------:|:----|:----|:----|:----|
|    轻量单机模式    | 最高  | 无   | 无   | 高   |
|   可扩展单节点模式 （默认）  | 高   | 高   | 中   | 高   |
| 高性能分布式模式     | 高   | 高   | 高   | 中   |
|   强一致分布式模式   | 中   | 高   | 高   | 高   |


|                           配置                           | 轻量单机模式 | 可扩展单节点模式 | 高性能分布式模式 | 强一致分布式模式 |
|:------------------------------------------------------:|:-------|:---------|:---------|:---------|
|                     ConfigNode 个数                      | 1      | ≥1 （奇数）  | ≥1 （奇数）  | ≥1（奇数）   |
|                      DataNode 个数                       | 1      | ≥1       | ≥3       | ≥3       |
|            元数据副本 schema_replication_factor             | 1      | 1        | 3        | 3        |
|              数据副本 data_replication_factor              | 1      | 1        | 2        | 3        |
|   ConfigNode 协议 config_node_consensus_protocol_class   | Simple | Ratis    | Ratis    | Ratis    |
| SchemaRegion 协议 schema_region_consensus_protocol_class | Simple | Ratis    | Ratis    | Ratis    |
|   DataRegion 协议 data_region_consensus_protocol_class   | Simple | IoT      | IoT      | Ratis    |


### 部署配置推荐

#### 从 0.13 版本升级到 1.0

场景：
已在 0.13 版本存储了部分数据，希望迁移到 1.0 版本，并且与 0.13 表现保持一致。

可选方案：
1. 升级到 1C1D 单机版，ConfigNode 分配 2G 内存，DataNode 与 0.13 一致。
2. 升级到 3C3D 高性能分布式，ConfigNode 分配 2G 内存，DataNode 与 0.13 一致。

配置修改：
1.0 配置参数修改：
- 数据目录不要指向0.13原有数据目录
- region_group_extension_strategy=COSTOM
- data_region_group_per_database
    - 如果是 3C3D 高性能分布式：则改为：集群 CPU 总核数/ 数据副本数
    - 如果是 1C1D，则改为：等于 0.13 的 virtual_storage_group_num 即可 （"database"一词 与 0.13 中的 "sg" 同义）

数据迁移：
配置修改完成后，通过 load-tsfile 工具将 0.13 的 TsFile 都加载进 1.0 的 IoTDB 中，即可使用。

#### 直接使用 1.0

**推荐用户仅设置 1 个 Database**

##### 内存设置方法
###### 根据活跃序列数估计内存

集群 DataNode 总堆内内存（GB） = 活跃序列数/100000 * 数据副本数

每个 DataNode 堆内内存（GB）= 集群DataNode总堆内内存 / DataNode 个数

> 假设需要用3C3D管理100万条序列，数据采用3副本，则：
> - 集群 DataNode 总堆内内存（GB）：1,000,000 / 100,000 * 3 = 30G
> - 每台 DataNode 的堆内内存配置为：30 / 3 = 10G

###### 根据总序列数估计内存
  
集群 DataNode 总堆内内存 （B） = 20 * （180 + 2 * 序列的全路径的平均字符数）* 序列总量 * 元数据副本数

每个 DataNode 内存配置推荐：集群 DataNode 总堆内内存 / DataNode 数目

> 假设需要用3C3D管理100万条序列，元数据采用3副本，序列名形如 root.sg_1.d_10.s_100（约20字符），则：
> - 集群 DataNode 总堆内内存：20 * （180 + 2 * 20）* 1,000,000 * 3 = 13.2 GB
> - 每台 DataNode 的堆内内存配置为：13.2 GB / 3 = 4.4 GB

##### 磁盘估计

IoTDB 存储空间=数据存储空间 + 元数据存储空间 + 临时存储空间

###### 数据磁盘空间

序列数量 * 采样频率 * 每个数据点大小 * 存储时长 * 副本数 /  10 倍压缩比

|       数据类型 \ 数据点大小        | 时间戳（字节） | 值（字节） | 总共（字节） |
|:-------------------------:|:--------|:------|:-------|
|       开关量（Boolean）        | 8       | 1     | 9      |
|  整型（INT32）/单精度浮点数（FLOAT）  | 8       | 4     | 12     |
| 长整型（INT64）/双精度浮点数（DOUBLE） | 8       | 8     | 16     |
|         字符串（TEXT）         | 8       | 假设为 a | 8+a    | 


> 示例：1000设备，每个设备100 测点，共 100000 序列。整型。采样频率1Hz（每秒一次），存储1年，3副本，压缩比按 10 算，则数据存储空间占用： 
>  * 简版：1000 * 100 * 12 * 86400 * 365 * 3 / 10 = 11T 
>  * 完整版：1000设备 * 100测点 * 12字节每数据点 * 86400秒每天 * 365天每年 * 3副本 / 10压缩比 = 11T

###### 元数据磁盘空间

每条序列在磁盘日志文件中大约占用 序列字符数 + 20 字节。
若序列有tag描述信息，则仍需加上约 tag 总字符数字节的空间。

###### 临时磁盘空间

临时磁盘空间 = 写前日志 + 共识协议 + 合并临时空间

1. 写前日志

最大写前日志空间占用 = memtable 总内存占用 ÷ 最小有效信息占比
- memtable 总内存占用和 storage_query_schema_consensus_free_memory_proportion、storage_engine_memory_proportion、write_memory_proportion 三个参数有关
- 最小有效信息占比由 wal_min_effective_info_ratio 决定
  
> 示例：为 IoTDB 分配 16G 内存，配置文件如下
>  storage_query_schema_consensus_free_memory_proportion=3:3:1:1:2
>  storage_engine_memory_proportion=8:2
>  write_memory_proportion=19:1
>  wal_min_effective_info_ratio=0.1
>  最大写前日志空间占用 = 16 * (3 / 10) * (8 / 10) * (19 / 20)  ÷ 0.1 = 36.48G

2. 共识协议

Ratis共识协议
采用Ratis共识协议的情况下，需要额外磁盘空间存储Raft Log。Raft Log会在每一次状态机 Snapshot 之后删除，因此可以通过调整 trigger_snapshot_threshold 参数控制Raft Log最大空间占用。

每一个 Region Raft Log占用最大空间 = 平均请求大小 * trigger_snapshot_threshold。
集群中一个Region总的Raft Log占用空间和Raft数据副本数成正比。

> 示例：DataRegion, 平均每一次插入20k数据，data_region_trigger_snapshot_threshold = 400,000，那么Raft Log最大占用 = 20K * 400,000 = 8G。
Raft Log会从0增长到8G，接着在snapshot之后重新变成0。平均占用为4G。
当副本数为3时，集群中这个DataRegion总Raft Log最大占用 3 * 8G = 24G。
 
此外，可以通过data_region_ratis_log_max_size规定每一个DataRegion的Raft Log磁盘占用最大值，
默认为20G，能够保障运行全程中单DataRegion Raft Log总大小不超过20G。

3. 合并临时空间

 - 空间内合并
   临时文件的磁盘空间 = 源文件大小总和
   
   > 示例：10个源文件，每个文件大小为100M
   > 临时文件的磁盘空间 = 10 * 100 = 1000M


 - 跨空间合并
   跨空间合并的临时文件大小与源文件大小和顺乱序数据的重叠度有关，当乱序数据与顺序数据有相同的时间戳时，就认为有重叠。
   乱序数据的重叠度 = 重叠的乱序数据量 / 总的乱序数据量

   临时文件的磁盘空间 = 源顺序文件总大小 + 源乱序文件总大小 *（1 - 重叠度）
   > 示例：10个顺序文件，10个乱序文件，每个顺序文件100M，每个乱序文件50M，每个乱序文件里有一半的数据与顺序文件有相同的时间戳 
   > 乱序数据的重叠度 = 25M/50M * 100% = 50% 
   > 临时文件的磁盘空间 = 10 * 100 + 10 * 50 * 50% = 1250M


