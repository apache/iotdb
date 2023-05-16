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

# 参考

## 公共配置参数

IoTDB ConfigNode 和 DataNode 的公共配置参数位于 `conf` 目录下。

* `iotdb-common.properties`：IoTDB 集群的公共配置。

### 改后生效方式
不同的配置参数有不同的生效方式，分为以下三种：

+ **仅允许在第一次启动服务前修改：** 在第一次启动 ConfigNode/DataNode 后即禁止修改，修改会导致 ConfigNode/DataNode 无法启动。
+ **重启服务生效：** ConfigNode/DataNode 启动后仍可修改，但需要重启 ConfigNode/DataNode 后才生效。
+ **热加载：** 可在 ConfigNode/DataNode 运行时修改，修改后通过 Session 或 Cli 发送 ```load configuration``` 命令（SQL）至 IoTDB 使配置生效。

### 系统配置项

#### 副本配置

* config\_node\_consensus\_protocol\_class

|   名字   | config\_node\_consensus\_protocol\_class        |
|:------:|:------------------------------------------------|
|   描述   | ConfigNode 副本的共识协议，仅支持 RatisConsensus           |
|   类型   | String                                          |
|  默认值   | org.apache.iotdb.consensus.ratis.RatisConsensus |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                  |

* schema\_replication\_factor

|   名字   | schema\_replication\_factor |
|:------:|:----------------------------|
|   描述   | Database 的默认元数据副本数          |
|   类型   | int32                       |
|  默认值   | 1                           |
| 改后生效方式 | 重启服务后对**新的 Database** 生效    |


* schema\_region\_consensus\_protocol\_class

|   名字   | schema\_region\_consensus\_protocol\_class                      |
|:------:|:----------------------------------------------------------------|
|   描述   | 元数据副本的共识协议，1 副本时可以使用 SimpleConsensus 协议，多副本时只能使用 RatisConsensus |
|   类型   | String                                                          |
|  默认值   | org.apache.iotdb.consensus.ratis.RatisConsensus                 |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                                  |

* data\_replication\_factor

|   名字   | data\_replication\_factor |
|:------:|:--------------------------|
|   描述   | Database 的默认数据副本数         |
|   类型   | int32                     |
|  默认值   | 1                         |
| 改后生效方式 | 重启服务后对**新的 Database** 生效  |

* data\_region\_consensus\_protocol\_class

|   名字   | data\_region\_consensus\_protocol\_class                                      |
|:------:|:------------------------------------------------------------------------------|
|   描述   | 数据副本的共识协议，1 副本时可以使用 SimpleConsensus 协议，多副本时可以使用 IoTConsensus 或 RatisConsensus |
|   类型   | String                                                                        |
|  默认值   | org.apache.iotdb.consensus.iot.IoTConsensus                                   |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                                                |

#### 负载均衡配置

* series\_partition\_slot\_num

|   名字   | series\_slot\_num |
|:------:|:------------------|
|   描述   | 序列分区槽数            |
|   类型   | int32             |
|  默认值   | 10000             |
| 改后生效方式 | 仅允许在第一次启动服务前修改    |

* series\_partition\_executor\_class

|   名字   | series\_partition\_executor\_class                                |
|:------:|:------------------------------------------------------------------|
|   描述   | 序列分区哈希函数                                                          |
|   类型   | String                                                            |
|  默认值   | org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                                    |

* schema\_region\_group\_extension\_policy

|   名字   | schema\_region\_group\_extension\_policy |
|:------:|:-----------------------------------------|
|   描述   | SchemaRegionGroup 的扩容策略                  |
|   类型   | string                                   |
|  默认值   | AUTO                                     |
| 改后生效方式 | 重启服务生效                                   |

* default\_schema\_region\_group\_num\_per\_database

|   名字   | default\_schema\_region\_group\_num\_per\_database                                                                                                      |
|:------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------|
|   描述   | 当选用 CUSTOM-SchemaRegionGroup 扩容策略时，此参数为每个 Database 拥有的 SchemaRegionGroup 数量；当选用 AUTO-SchemaRegionGroup 扩容策略时，此参数为每个 Database 最少拥有的 SchemaRegionGroup 数量 |
|   类型   | int                                                                                                                                                     |
|  默认值   | 1                                                                                                                                                       |
| 改后生效方式 | 重启服务生效                                                                                                                                                  |

* schema\_region\_per\_data\_node

|   名字   | schema\_region\_per\_data\_node       |
|:------:|:--------------------------------------|
|   描述   | 期望每个 DataNode 可管理的 SchemaRegion 的最大数量 |
|   类型   | double                                |
|  默认值   | 与 schema_replication_factor 相同        |
| 改后生效方式 | 重启服务生效                                |

* data\_region\_group\_extension\_policy

|   名字   | data\_region\_group\_extension\_policy |
|:------:|:---------------------------------------|
|   描述   | DataRegionGroup 的扩容策略                  |
|   类型   | string                                 |
|  默认值   | AUTO                                   |
| 改后生效方式 | 重启服务生效                                 |

* default\_data\_region\_group\_num\_per\_database

|   名字   | data\_region\_group\_per\_database                                                                                                              |
|:------:|:------------------------------------------------------------------------------------------------------------------------------------------------|
|   描述   | 当选用 CUSTOM-DataRegionGroup 扩容策略时，此参数为每个 Database 拥有的 DataRegionGroup 数量；当选用 AUTO-DataRegionGroup 扩容策略时，此参数为每个 Database 最少拥有的 DataRegionGroup 数量 |
|   类型   | int                                                                                                                                             |
|  默认值   | 2                                                                                                                                               |
| 改后生效方式 | 重启服务生效                                                                                                                                          |

* data\_region\_per\_processor

|   名字   | data\_region\_per\_processor |
|:------:|:-----------------------------|
|   描述   | 期望每个处理器可管理的 DataRegion 的最大数量 |
|   类型   | double                       |
|  默认值   | 1.0                          |
| 改后生效方式 | 重启服务生效                       |

* enable\_data\_partition\_inherit\_policy

|   名字   | enable\_data\_partition\_inherit\_policy                        |
|:------:|:----------------------------------------------------------------|
|   描述   | 开启 DataPartition 继承策略后，同一个序列分区槽内的 DataPartition 会继承之前时间分区槽的分配结果 |
|   类型   | Boolean                                                         |
|  默认值   | false                                                           |
| 改后生效方式 | 重启服务生效                                                          |

* leader\_distribution\_policy

|   名字   | leader\_distribution\_policy |
|:------:|:-----------------------------|
|   描述   | 集群 RegionGroup 的 leader 分配策略 |
|   类型   | String                       |
|  默认值   | MIN_COST_FLOW                |
| 改后生效方式 | 重启服务生效                       |

* enable\_auto\_leader\_balance\_for\_ratis\_consensus

|   名字   | enable\_auto\_leader\_balance\_for\_ratis\_consensus |
|:------:|:-----------------------------------------------------|
|   描述   | 是否为 Ratis 共识协议开启自动均衡 leader 策略                       |
|   类型   | Boolean                                              |
|  默认值   | false                                                |
| 改后生效方式 | 重启服务生效                                               |

* enable\_auto\_leader\_balance\_for\_iot\_consensus

|   名字   | enable\_auto\_leader\_balance\_for\_iot\_consensus |
|:------:|:---------------------------------------------------|
|   描述   | 是否为 IoT 共识协议开启自动均衡 leader 策略                       |
|   类型   | Boolean                                            |
|  默认值   | true                                               |
| 改后生效方式 | 重启服务生效                                             |

#### 集群管理

* time\_partition\_interval

|   名字   | time\_partition\_interval |
|:------:|:--------------------------|
|   描述   | Database 默认的数据时间分区间隔      |
|   类型   | Long                      |
|   单位   | 毫秒                        |
|  默认值   | 604800000                 |
| 改后生效方式 | 仅允许在第一次启动服务前修改            |

* heartbeat\_interval\_in\_ms

|   名字   | heartbeat\_interval\_in\_ms |
|:------:|:----------------------------|
|   描述   | 集群节点间的心跳间隔                  |
|   类型   | Long                        |
|   单位   | ms                          |
|  默认值   | 1000                        |
| 改后生效方式 | 重启服务生效                      |

* disk\_space\_warning\_threshold

|   名字   | disk\_space\_warning\_threshold |
|:------:|:--------------------------------|
|   描述   | DataNode 磁盘剩余阈值                 |
|   类型   | double(percentage)              |
|  默认值   | 0.05                            |
| 改后生效方式 | 重启服务生效                          |

#### 内存控制配置

* enable\_mem\_control

|     名字     | enable\_mem\_control     |
| :----------: | :----------------------- |
|     描述     | 开启内存控制，避免爆内存 |
|     类型     | Boolean                  |
|    默认值    | true                     |
| 改后生效方式 | 重启服务生效             |

* storage\_query_schema_consensus_free_memory_proportion

|名字| storage\_query\_schema\_consensus\_free\_memory\_proportion           |
|:---:|:----------------------------------------------------------------------|
|描述| 存储，查询，元数据，共识层，空闲内存比例                                                  |
|类型| Ratio                                                                 |
|默认值| 3:3:1:1:2                                                             |
|改后生效方式| 重启服务生效                                                                |

* schema\_memory\_allocate\_proportion

|名字| schema\_memory\_allocate\_proportion                        |
|:---:|:------------------------------------------------------------|
|描述| SchemaRegion， SchemaCache，PartitionCache，LastCache 占元数据内存比例 |
|类型| Ratio                                                       |
|默认值| 5:3:1:1                                                     |
|改后生效方式| 重启服务生效                                                      |

* storage\_engine\_memory\_proportion

|名字| storage\_engine\_memory\_proportion |
|:---:|:------------------------------------|
|描述| 写入和合并占存储内存比例                        |
|类型| Ratio                               |
|默认值| 8:2                                 |
|改后生效方式| 重启服务生效                              |

* write\_memory\_proportion

|名字| write\_memory\_proportion            |
|:---:|:-------------------------------------|
|描述| Memtable 和 TimePartitionInfo 占写入内存比例 |
|类型| Ratio                                |
|默认值| 19:1                                 |
|改后生效方式| 重启服务生效                               |

* concurrent\_writing\_time\_partition

|名字| concurrent\_writing\_time\_partition                  |
|:---:|:------------------------------------------------------|
|描述| 最大可同时写入的时间分区个数，默认1个分区, enable\_mem\_control=false 时有效 |
|类型| Int64                                                 |
|默认值| 1                                                     |
|改后生效方式| 重启服务生效                                                |

* primitive\_array\_size

|   名字   | primitive\_array\_size |
|:------:|:-----------------------|
|   描述   | 数组池中的原始数组大小（每个数组的长度）   |
|   类型   | int32                  |
|  默认值   | 64                     |
| 改后生效方式 | 重启服务生效                 |

* flush\_proportion

|     名字     | flush\_proportion                                                                                           |
| :----------: | :---------------------------------------------------------------------------------------------------------- |
|     描述     | 调用flush disk的写入内存比例，默认0.4,若有极高的写入负载力（比如batch=1000），可以设置为低于默认值，比如0.2 |
|     类型     | Double                                                                                                      |
|    默认值    | 0.4                                                                                                         |
| 改后生效方式 | 重启服务生效                                                                                                |

* buffered\_arrays\_memory\_proportion

|     名字     | buffered\_arrays\_memory\_proportion    |
| :----------: | :-------------------------------------- |
|     描述     | 为缓冲数组分配的写入内存比例，默认为0.6 |
|     类型     | Double                                  |
|    默认值    | 0.6                                     |
| 改后生效方式 | 重启服务生效                            |

* reject\_proportion

|     名字     | reject\_proportion                                                                                                       |
| :----------: | :----------------------------------------------------------------------------------------------------------------------- |
|     描述     | 拒绝插入的写入内存比例，默认0.8，若有极高的写入负载力（比如batch=1000）并且物理内存足够大，它可以设置为高于默认值，如0.9 |
|     类型     | Double                                                                                                                   |
|    默认值    | 0.8                                                                                                                      |
| 改后生效方式 | 重启服务生效                                                                                                             |

* write\_memory\_variation\_report\_proportion

|     名字     | write\_memory\_variation\_report\_proportion                                      |
| :----------: | :-------------------------------------------------------------------------------- |
|     描述     | 如果 DataRegion 的内存增加超过写入可用内存的一定比例，则向系统报告。默认值为0.001 |
|     类型     | Double                                                                            |
|    默认值    | 0.001                                                                             |
| 改后生效方式 | 重启服务生效                                                                      |

* check\_period\_when\_insert\_blocked

|名字| check\_period\_when\_insert\_blocked |
|:---:|:---|
|描述| 当插入被拒绝时，等待时间（以毫秒为单位）去再次检查系统，默认为50。若插入被拒绝，读取负载低，可以设置大一些。 |
|类型| int32 |
|默认值| 50 |
|改后生效方式|重启服务生效|

* io\_task\_queue\_size\_for\_flushing

|名字| io\_task\_queue\_size\_for\_flushing |
|:---:|:---|
|描述| ioTaskQueue 的大小。默认值为10。|
|类型| int32 |
|默认值| 10 |
|改后生效方式|重启服务生效|

* enable\_query\_memory\_estimation

|名字| enable\_query\_memory\_estimation |
|:---:|:----------------------------------|
|描述| 开启后会预估每次查询的内存使用量，如果超过可用内存，会拒绝本次查询 |
|类型| bool                              |
|默认值| true                              |
|改后生效方式| 热加载                              |

* partition\_cache\_size

|名字| partition\_cache\_size |
|:---:|:---|
|描述| 分区信息缓存的最大缓存条目数。|
|类型| Int32 |
|默认值| 1000 |
|改后生效方式|重启服务生效|

#### 元数据引擎配置

* mlog\_buffer\_size

|名字| mlog\_buffer\_size |
|:---:|:---|
|描述| mlog 的 buffer 大小 |
|类型| int32 |
|默认值| 1048576 |
|改后生效方式|热加载|

* sync\_mlog\_period\_in\_ms

|     名字     | sync\_mlog\_period\_in\_ms                                                                            |
| :----------: | :---------------------------------------------------------------------------------------------------- |
|     描述     | mlog定期刷新到磁盘的周期，单位毫秒。如果该参数为0，则表示每次对元数据的更新操作都会被立即写到磁盘上。 |
|     类型     | Int64                                                                                                 |
|    默认值    | 100                                                                                                   |
| 改后生效方式 | 重启服务生效                                                                                          |

* tag\_attribute\_total\_size

|名字| tag\_attribute\_total\_size |
|:---:|:---|
|描述| 每个时间序列标签和属性的最大持久化字节数 |
|类型| int32 |
|默认值| 700 |
|改后生效方式|仅允许在第一次启动服务前修改|

* tag\_attribute\_flush\_interval

|名字| tag\_attribute\_flush\_interval |
|:---:|:--------------------------------|
|描述| 标签和属性记录的间隔数，达到此记录数量时将强制刷盘       |
|类型| int32                           |
|默认值| 1000                            |
|改后生效方式| 仅允许在第一次启动服务前修改                  |

* schema\_region\_device\_node\_cache\_size

|名字| schema\_region\_device\_node\_cache\_size |
|:---:|:--------------------------------|
|描述| schemaRegion中用于加速device节点访问所设置的device节点缓存的大小       |
|类型| Int32                           |
|默认值| 10000                          |
|改后生效方式| 重启服务生效         |

* max\_measurement\_num\_of\_internal\_request

|名字| max\_measurement\_num\_of\_internal\_request |
|:---:|:--------------------------------|
|描述| 一次注册序列请求中若物理量过多，在系统内部执行时将被拆分为若干个轻量级的子请求，每个子请求中的物理量数目不超过此参数设置的最大值。    |
|类型| Int32                           |
|默认值| 10000                          |
|改后生效方式| 重启服务生效         |

#### 数据类型自动推断

* enable\_auto\_create\_schema

|     名字     | enable\_auto\_create\_schema           |
| :----------: | :------------------------------------- |
|     描述     | 当写入的序列不存在时，是否自动创建序列 |
|     取值     | true or false                          |
|    默认值    | true                                   |
| 改后生效方式 | 重启服务生效                           |

* default\_storage\_group\_level

|名字| default\_storage\_group\_level |
|:---:|:---|
|描述| 当写入的数据不存在且自动创建序列时，若需要创建相应的 database，将序列路径的哪一层当做 database。例如，如果我们接到一个新序列 root.sg0.d1.s2, 并且 level=1， 那么 root.sg0 被视为database（因为 root 是 level 0 层）|
|取值| int32 |
|默认值| 1 |
|改后生效方式|重启服务生效|

* boolean\_string\_infer\_type

|     名字     | boolean\_string\_infer\_type               |
| :----------: | :----------------------------------------- |
|     描述     | "true" 或者 "false" 字符串被推断的数据类型 |
|     取值     | BOOLEAN 或者 TEXT                          |
|    默认值    | BOOLEAN                                    |
| 改后生效方式 | 重启服务生效                               |

* integer\_string\_infer\_type

|     名字     | integer\_string\_infer\_type      |
| :----------: | :-------------------------------- |
|     描述     | 整型字符串推断的数据类型          |
|     取值     | INT32, INT64, FLOAT, DOUBLE, TEXT |
|    默认值    | FLOAT                             |
| 改后生效方式 | 重启服务生效                      |

* long\_string\_infer\_type

|     名字     | long\_string\_infer\_type                |
| :----------: | :--------------------------------------- |
|     描述     | 大于 2 ^ 24 的整形字符串被推断的数据类型 |
|     取值     | DOUBLE, FLOAT or TEXT                    |
|    默认值    | DOUBLE                                   |
| 改后生效方式 | 重启服务生效                             |

* floating\_string\_infer\_type

|     名字     | floating\_string\_infer\_type |
| :----------: | :---------------------------- |
|     描述     | "6.7"等字符串被推断的数据类型 |
|     取值     | DOUBLE, FLOAT or TEXT         |
|    默认值    | FLOAT                         |
| 改后生效方式 | 重启服务生效                  |

* nan\_string\_infer\_type

|     名字     | nan\_string\_infer\_type     |
| :----------: | :--------------------------- |
|     描述     | "NaN" 字符串被推断的数据类型 |
|     取值     | DOUBLE, FLOAT or TEXT        |
|    默认值    | DOUBLE                       |
| 改后生效方式 | 重启服务生效                 |

* default\_boolean\_encoding

|     名字     | default\_boolean\_encoding |
| :----------: | :------------------------- |
|     描述     | BOOLEAN 类型编码格式       |
|     取值     | PLAIN, RLE                 |
|    默认值    | RLE                        |
| 改后生效方式 | 重启服务生效               |

* default\_int32\_encoding

|     名字     | default\_int32\_encoding                |
| :----------: |:----------------------------------------|
|     描述     | int32 类型编码格式                            |
|     取值     | PLAIN, RLE, TS\_2DIFF, REGULAR, GORILLA |
|    默认值    | RLE                                     |
| 改后生效方式 | 重启服务生效                                  |

* default\_int64\_encoding

|     名字     | default\_int64\_encoding                |
| :----------: |:----------------------------------------|
|     描述     | int64 类型编码格式                            |
|     取值     | PLAIN, RLE, TS\_2DIFF, REGULAR, GORILLA |
|    默认值    | RLE                                     |
| 改后生效方式 | 重启服务生效                                  |

* default\_float\_encoding

|     名字     | default\_float\_encoding       |
| :----------: |:-------------------------------|
|     描述     | float 类型编码格式                   |
|     取值     | PLAIN, RLE, TS\_2DIFF, GORILLA |
|    默认值    | GORILLA                        |
| 改后生效方式 | 重启服务生效                         |

* default\_double\_encoding

|     名字     | default\_double\_encoding      |
| :----------: |:-------------------------------|
|     描述     | double 类型编码格式                  |
|     取值     | PLAIN, RLE, TS\_2DIFF, GORILLA |
|    默认值    | GORILLA                        |
| 改后生效方式 | 重启服务生效                         |

* default\_text\_encoding

|     名字     | default\_text\_encoding |
| :----------: | :---------------------- |
|     描述     | text 类型编码格式       |
|     取值     | PLAIN                   |
|    默认值    | PLAIN                   |
| 改后生效方式 | 重启服务生效            |

#### 查询配置

* read\_consistency\_level

|名字| read\_consistency\_level |
|:---:|:---|
|描述| 查询一致性等级，取值 “strong” 时从 Leader 副本查询，取值 “weak” 时随机查询一个副本。|
|类型| String |
|默认值| strong |
|改后生效方式| 重启服务生效 |

* meta\_data\_cache\_enable

|名字| meta\_data\_cache\_enable |
|:---:|:---|
|描述| 是否缓存元数据（包括 BloomFilter、Chunk Metadata 和 TimeSeries Metadata。）|
|类型|Boolean|
|默认值| true |
|改后生效方式| 重启服务生效|

* chunk\_timeseriesmeta\_free\_memory\_proportion

|     名字     | chunk\_timeseriesmeta\_free\_memory\_proportion                                                                                                                                                                            |
| :----------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|     描述     | 读取内存分配比例，BloomFilterCache、ChunkCache、TimeseriesMetadataCache、数据集查询的内存和可用内存的查询。参数形式为a : b : c : d : e，其中a、b、c、d、e为整数。 例如“1 : 1 : 1 : 1 : 1” ，“1 : 100 : 200 : 300 : 400” 。 |
|     类型     | String                                                                                                                                                                                                                     |
|    默认值    | 1 : 100 : 200 : 300 : 400                                                                                                                                                                                                  |
| 改后生效方式 | 重启服务生效                                                                                                                                                                                                               |

* enable\_last\_cache

|     名字     | enable\_last\_cache |
| :----------: | :------------------ |
|     描述     | 是否开启最新点缓存  |
|     类型     | Boolean             |
|    默认值    | true                |
| 改后生效方式 | 重启服务生效        |

* mpp\_data\_exchange\_core\_pool\_size

|     名字     | mpp\_data\_exchange\_core\_pool\_size |
| :----------: | :------------------------------------ |
|     描述     | MPP 数据交换线程池核心线程数          |
|     类型     | int32                                   |
|    默认值    | 10                                    |
| 改后生效方式 | 重启服务生效                          |

* mpp\_data\_exchange\_max\_pool\_size

|     名字     | mpp\_data\_exchange\_max\_pool\_size |
| :----------: | :----------------------------------- |
|     描述     | MPP 数据交换线程池最大线程数         |
|     类型     | int32                                  |
|    默认值    | 10                                   |
| 改后生效方式 | 重启服务生效                         |

* mpp\_data\_exchange\_keep\_alive\_time\_in\_ms

|     名字     | mpp\_data\_exchange\_keep\_alive\_time\_in\_ms |
| :----------: | :--------------------------------------------- |
|     描述     | MPP 数据交换最大等待时间                       |
|     类型     | int32                                            |
|    默认值    | 1000                                           |
| 改后生效方式 | 重启服务生效                                   |

* driver\_task\_execution\_time\_slice\_in\_ms

|     名字     | driver\_task\_execution\_time\_slice\_in\_ms |
| :----------: | :------------------------------------------- |
|     描述     | 单个 DriverTask 最长执行时间                 |
|     类型     | int32                                          |
|    默认值    | 100                                          |
| 改后生效方式 | 重启服务生效                                 |

* max\_tsblock\_size\_in\_bytes

|     名字     | max\_tsblock\_size\_in\_bytes |
| :----------: | :---------------------------- |
|     描述     | 单个 TsBlock 的最大容量       |
|     类型     | int32                           |
|    默认值    | 1024 * 1024 (1 MB)            |
| 改后生效方式 | 重启服务生效                  |

* max\_tsblock\_line\_numbers

|     名字     | max\_tsblock\_line\_numbers |
| :----------: | :-------------------------- |
|     描述     | 单个 TsBlock 的最大行数     |
|     类型     | int32                         |
|    默认值    | 1000                        |
| 改后生效方式 | 重启服务生效                |

* slow\_query\_threshold

|名字| slow\_query\_threshold |
|:---:|:---|
|描述| 慢查询的时间阈值。单位：毫秒。|
|类型| Int32 |
|默认值| 5000 |
|改后生效方式|热加载|

* query\_timeout\_threshold

|名字| query\_timeout\_threshold |
|:---:|:---|
|描述| 查询的最大执行时间。单位：毫秒。|
|类型| Int32 |
|默认值| 60000 |
|改后生效方式| 重启服务生效|

* max\_allowed\_concurrent\_queries

|名字| max\_allowed\_concurrent\_queries |
|:---:|:---|
|描述| 允许的最大并发查询数量。 |
|类型| Int32 |
|默认值| 1000 |
|改后生效方式|重启服务生效|

* query\_thread\_count

|名字| query\_thread\_count                                                   |
|:---:|:----------------------------------------------------------------------------|
|描述| 当 IoTDB 对内存中的数据进行查询时，最多启动多少个线程来执行该操作。如果该值小于等于 0，那么采用机器所安装的 CPU 核的数量。 |
|类型| Int32                                                                       |
|默认值| CPU 核数                                                                          |
|改后生效方式| 重启服务生效 |

* batch\_size

|名字| batch\_size |
|:---:|:---|
|描述| 服务器中每次迭代的数据量（数据条目，即不同时间戳的数量。） |
|类型| Int32 |
|默认值| 100000 |
|改后生效方式|重启服务生效|

#### 存储引擎配置

* timestamp\_precision

|     名字     | timestamp\_precision        |
| :----------: | :-------------------------- |
|     描述     | 时间戳精度，支持 ms、us、ns |
|     类型     | String                      |
|    默认值    | ms                          |
| 改后生效方式 | 热加载                    |

* default\_ttl\_in\_ms

|     名字     | default\_ttl\_in\_ms                                        |
| :----------: | :---------------------------------------------------------- |
|     描述     | 数据保留时间，会丢弃 now()-default\_ttl 之前的数据，单位 ms |
|     类型     | long                                                        |
|    默认值    | 36000000                                                    |
| 改后生效方式 | 重启服务生效                                                |

* max\_waiting\_time\_when\_insert\_blocked

|     名字     | max\_waiting\_time\_when\_insert\_blocked |
| :----------: |:------------------------------------------|
|     描述     | 当插入请求等待超过这个时间，则抛出异常，单位 ms                 |
|     类型     | Int32                                     |
|    默认值    | 10000                                     |
| 改后生效方式 | 重启服务生效                                 |   

* enable\_discard\_out\_of\_order\_data

|     名字     | enable\_discard\_out\_of\_order\_data |
| :----------: |:--------------------------------------|
|     描述     | 是否支持写入乱序数据                            |
|     类型     | Boolean                               |
|    默认值    | false                                 |
| 改后生效方式 | 重启服务生效                                |

* handle\_system\_error

|     名字     | handle\_system\_error  |
| :----------: |:-----------------------|
|     描述     | 当系统遇到不可恢复的错误时的处理方法     |
|     类型     | String                 |
|    默认值    | CHANGE\_TO\_READ\_ONLY |
| 改后生效方式 | 重启服务生效                 |

* memtable\_size\_threshold

|     名字     | memtable\_size\_threshold                          |
| :----------: | :------------------------------------------------- |
|     描述     | 内存缓冲区 memtable 阈值                           |
|     类型     | Long                                               |
|    默认值    | 1073741824                                         |
| 改后生效方式 | enable\_mem\_control 为 false 时生效、重启服务生效 |

* enable\_timed\_flush\_seq\_memtable

|   名字   | enable\_timed\_flush\_seq\_memtable |
|:------:|:------------------------------------|
|   描述   | 是否开启定时刷盘顺序 memtable                 |
|   类型   | Boolean                             |
|  默认值   | true                                |
| 改后生效方式 | 热加载                                 |

* seq\_memtable\_flush\_interval\_in\_ms

|   名字   | seq\_memtable\_flush\_interval\_in\_ms       |
|:------:|:---------------------------------------------|
|   描述   | 当 memTable 的创建时间小于当前时间减去该值时，该 memtable 需要被刷盘 |
|   类型   | int32                                        |
|  默认值   | 10800000                                     |
| 改后生效方式 | 热加载                                          |

* seq\_memtable\_flush\_check\_interval\_in\_ms

|名字| seq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|描述| 检查顺序 memtable 是否需要刷盘的时间间隔 |
|类型| int32 |
|默认值| 600000 |
|改后生效方式| 热加载 |

* enable\_timed\_flush\_unseq\_memtable

|     名字     | enable\_timed\_flush\_unseq\_memtable |
| :----------: | :------------------------------------ |
|     描述     | 是否开启定时刷新乱序 memtable         |
|     类型     | Boolean                               |
|    默认值    | true                                  |
| 改后生效方式 | 热加载                              |

* unseq\_memtable\_flush\_interval\_in\_ms

|   名字   | unseq\_memtable\_flush\_interval\_in\_ms     |
|:------:|:---------------------------------------------|
|   描述   | 当 memTable 的创建时间小于当前时间减去该值时，该 memtable 需要被刷盘 |
|   类型   | int32                                        |
|  默认值   | 10800000                                     |
| 改后生效方式 | 热加载                                          |

* unseq\_memtable\_flush\_check\_interval\_in\_ms

|名字| unseq\_memtable\_flush\_check\_interval\_in\_ms |
|:---:|:---|
|描述| 检查乱序 memtable 是否需要刷盘的时间间隔 |
|类型| int32 |
|默认值| 600000 |
|改后生效方式| 热加载 |

* tvlist\_sort\_algorithm

|名字| tvlist\_sort\_algorithm |
|:---:|:------------------------|
|描述| memtable中数据的排序方法        |
|类型| String                  |
|默认值| TIM                     |
|改后生效方式| 重启服务生效                  |

* avg\_series\_point\_number\_threshold

|名字| avg\_series\_point\_number\_threshold |
|:---:|:--------------------------------------|
|描述| 内存中平均每个时间序列点数最大值，达到触发 flush           |
|类型| int32                                 |
|默认值| 100000                                |
|改后生效方式| 重启服务生效                                |

* flush\_thread\_count

|名字| flush\_thread\_count |
|:---:|:---|
|描述| 当 IoTDB 将内存中的数据写入磁盘时，最多启动多少个线程来执行该操作。如果该值小于等于 0，那么采用机器所安装的 CPU 核的数量。默认值为 0。|
|类型| int32 |
|默认值| 0 |
|改后生效方式|重启服务生效|

* enable\_partial\_insert

|     名字     | enable\_partial\_insert                                            |
| :----------: | :----------------------------------------------------------------- |
|     描述     | 在一次 insert 请求中，如果部分测点写入失败，是否继续写入其他测点。 |
|     类型     | Boolean                                                            |
|    默认值    | true                                                               |
| 改后生效方式 | 重启服务生效                                                       |

* recovery\_log\_interval\_in\_ms

|     名字     | recovery\_log\_interval\_in\_ms |
| :----------: |:--------------------------------|
|     描述     | data region的恢复过程中打印日志信息的间隔      |
|     类型     | Int32                           |
|    默认值    | 5000                            |
| 改后生效方式 | 重启服务生效                          |

* 0.13\_data\_insert\_adapt

|     名字     | 0.13\_data\_insert\_adapt         |
| :----------: |:----------------------------------|
|     描述     | 如果 0.13 版本客户端进行写入，需要将此配置项设置为 true |
|     类型     | Boolean                           |
|    默认值    | false                             |
| 改后生效方式 | 重启服务生效                            |

* upgrade\_thread\_count

|     名字     | upgrade\_thread\_count          |
| :----------: |:--------------------------------|
|     描述     | 当存在老版本TsFile(v2)，执行文件升级任务使用的线程数 |
|     类型     | Int32                           |
|    默认值    | 1                               |
| 改后生效方式 | 重启服务生效                          |

* device\_path\_cache\_size

|     名字     | device\_path\_cache\_size                             |
| :----------: |:------------------------------------------------------|
|     描述     | Device Path 缓存的最大数量，这个缓存可以避免写入过程中重复的 Device Path 对象创建 |
|     类型     | Int32                                                 |
|    默认值    | 500000                                                |
| 改后生效方式 | 重启服务生效                                                |

* insert\_multi\_tablet\_enable\_multithreading\_column\_threshold

|     名字     | insert\_multi\_tablet\_enable\_multithreading\_column\_threshold |
| :----------: |:-----------------------------------------------------------------|
|     描述     | 插入时启用多线程插入列数的阈值                                                  |
|     类型     | int32                                                            |
|    默认值    | 10                                                               |
| 改后生效方式 | 重启服务生效                                                     |

#### 合并配置

* enable\_seq\_space\_compaction

|     名字     | enable\_seq\_space\_compaction         |
| :----------: | :------------------------------------- |
|     描述     | 顺序空间内合并，开启顺序文件之间的合并 |
|     类型     | Boolean                                |
|    默认值    | true                                   |
| 改后生效方式 | 重启服务生效                           |

* enable\_unseq\_space\_compaction

|     名字     | enable\_unseq\_space\_compaction       |
| :----------: | :------------------------------------- |
|     描述     | 乱序空间内合并，开启乱序文件之间的合并 |
|     类型     | Boolean                                |
|    默认值    | false                                  |
| 改后生效方式 | 重启服务生效                           |

* enable\_cross\_space\_compaction

|     名字     | enable\_cross\_space\_compaction           |
| :----------: | :----------------------------------------- |
|     描述     | 跨空间合并，开启将乱序文件合并到顺序文件中 |
|     类型     | Boolean                                    |
|    默认值    | true                                       |
| 改后生效方式 | 重启服务生效                               |

* cross\_selector

|名字| cross\_selector |
|:---:|:----------------|
|描述| 跨空间合并任务选择器的类型   |
|类型| String          |
|默认值| rewrite         |
|改后生效方式| 重启服务生效          |

* cross\_performer

|名字| cross\_performer |
|:---:|:-----------------|
|描述| 跨空间合并任务执行器的类型，可选项是read_point和fast，默认是read_point，fast还在测试中   |
|类型| String           |
|默认值| read\_point      |
|改后生效方式| 重启服务生效           |

* inner\_seq\_selector

|名字| inner\_seq\_selector |
|:---:|:---------------------|
|描述| 顺序空间内合并任务选择器的类型      |
|类型| String               |
|默认值| size\_tiered         |
|改后生效方式| 重启服务生效               |

* inner\_seq\_performer

|名字| inner\_seq\_performer                                       |
|:---:|:------------------------------------------------------------|
|描述| 顺序空间内合并任务执行器的类型，可选项是read_chunk和fast，默认是read_chunk，fast还在测试中 |
|类型| String                                                      |
|默认值| read\_chunk                                                 |
|改后生效方式| 重启服务生效                                                      |

* inner\_unseq\_selector

|名字| inner\_unseq\_selector |
|:---:|:-----------------------|
|描述| 乱序空间内合并任务选择器的类型        |
|类型| String                 |
|默认值| size\_tiered           |
|改后生效方式| 重启服务生效                 |

* inner\_unseq\_performer

|名字| inner\_unseq\_performer                                     |
|:---:|:------------------------------------------------------------|
|描述| 乱序空间内合并任务执行器的类型，可选项是read_point和fast，默认是read_point，fast还在测试中 |
|类型| String                                                      |
|默认值| read\_point                                                 |
|改后生效方式| 重启服务生效                                                      |

* compaction\_priority

|     名字     | compaction\_priority                                                                                                                               |
| :----------: | :------------------------------------------------------------------------------------------------------------------------------------------------- |
|     描述     | 合并时的优先级，BALANCE 各种合并平等，INNER_CROSS 优先进行顺序文件和顺序文件或乱序文件和乱序文件的合并，CROSS_INNER 优先将乱序文件合并到顺序文件中 |
|     类型     | String                                                                                                                                             |
|    默认值    | BALANCE                                                                                                                                            |
| 改后生效方式 | 重启服务生效                                                                                                                                       |

* target\_compaction\_file\_size

|     名字     | target\_compaction\_file\_size |
| :----------: |:-------------------------------|
|     描述     | 合并后的目标文件大小                     |
|     类型     | Int64                          |
|    默认值    | 2147483648                     |
| 改后生效方式 | 重启服务生效                         |

* target\_chunk\_size

|     名字     | target\_chunk\_size     |
| :----------: | :---------------------- |
|     描述     | 合并时 Chunk 的目标大小 |
|     类型     | Int64                   |
|    默认值    | 1048576                 |
| 改后生效方式 | 重启服务生效            |

* target\_chunk\_point\_num

|名字| target\_chunk\_point\_num |
|:---:|:---|
|描述| 合并时 Chunk 的目标点数 |
|类型| int32 |
|默认值| 100000 |
|改后生效方式|重启服务生效|

* chunk\_size\_lower\_bound\_in\_compaction

|     名字     | chunk\_size\_lower\_bound\_in\_compaction |
| :----------: |:------------------------------------------|
|     描述     | 合并时源 Chunk 的大小小于这个值，将被解开成点进行合并            |
|     类型     | Int64                                     |
|    默认值    | 10240                                     |
| 改后生效方式 | 重启服务生效                                    |

* chunk\_point\_num\_lower\_bound\_in\_compaction

|名字| chunk\_point\_num\_lower\_bound\_in\_compaction |
|:---:|:------------------------------------------------|
|描述| 合并时源 Chunk 的点数小于这个值，将被解开成点进行合并                  |
|类型| int32                                           |
|默认值| 1000                                            |
|改后生效方式| 重启服务生效                                          |

* max\_inner\_compaction\_candidate\_file\_num

|名字| max\_inner\_compaction\_candidate\_file\_num |
|:---:|:---|
|描述| 空间内合并中一次合并最多参与的文件数 |
|类型| int32 |
|默认值| 30|
|改后生效方式|重启服务生效|

* max\_cross\_compaction\_candidate\_file\_num

|名字| max\_cross\_compaction\_candidate\_file\_num |
|:---:|:---|
|描述| 跨空间合并中一次合并最多参与的文件数 |
|类型| int32 |
|默认值| 1000|
|改后生效方式|重启服务生效|

* max\_cross\_compaction\_candidate\_file\_size

|名字| max\_cross\_compaction\_candidate\_file\_size |
|:---:|:----------------------------------------------|
|描述| 跨空间合并中一次合并最多参与的文件总大小                          |
|类型| Int64                                         |
|默认值| 5368709120                                          |
|改后生效方式| 重启服务生效                                        |

* cross\_compaction\_file\_selection\_time\_budget

|名字| cross\_compaction\_file\_selection\_time\_budget |
|:---:|:---|
|描述| 若一个合并文件选择运行的时间超过这个时间，它将结束，并且当前的文件合并选择将用作为最终选择。当时间小于0 时，则表示时间是无边界的。单位：ms。|
|类型| int32 |
|默认值| 30000 |
|改后生效方式| 重启服务生效|

* compaction\_thread\_count

|名字| compaction\_thread\_count |
|:---:|:---|
|描述| 执行合并任务的线程数目 |
|类型| int32 |
|默认值| 10 |
|改后生效方式|重启服务生效|

* compaction\_schedule\_interval\_in\_ms

|     名字     | compaction\_schedule\_interval\_in\_ms |
| :----------: | :------------------------------------- |
|     描述     | 合并调度的时间间隔                     |
|     类型     | Int64                                  |
|    默认值    | 60000                                  |
| 改后生效方式 | 重启服务生效                           |

* compaction\_submission\_interval\_in\_ms

|     名字     | compaction\_submission\_interval\_in\_ms |
| :----------: | :--------------------------------------- |
|     描述     | 合并任务提交的间隔                       |
|     类型     | Int64                                    |
|    默认值    | 60000                                    |
| 改后生效方式 | 重启服务生效                             |

* compaction\_write\_throughput\_mb\_per\_sec

|名字| compaction\_write\_throughput\_mb\_per\_sec |
|:---:|:---|
|描述| 每秒可达到的写入吞吐量合并限制。|
|类型| int32 |
|默认值| 16 |
|改后生效方式| 重启服务生效|

* sub\_compaction\_thread\_count

|名字| sub\_compaction\_thread\_count  |
|:---:|:--------------------------------|
|描述| 每个合并任务的子任务线程数，只对跨空间合并和乱序空间内合并生效 |
|类型| int32                           |
|默认值| 4                               |
|改后生效方式| 重启服务生效                          |

* enable\_compaction\_validation

|名字| enable\_compaction\_validation |
|:---:|:-------------------------------|
|描述| 开启合并结束后对顺序文件时间范围的检查            |
|类型| Boolean                        |
|默认值| true                           |
|改后生效方式| 重启服务生效                         |

* candidate\_compaction\_task\_queue\_size

|名字| candidate\_compaction\_task\_queue\_size |
|:---:|:-----------------------------------------|
|描述| 合并任务优先级队列的大小                             |
|类型| int32                                    |
|默认值| 50                                       |
|改后生效方式| 重启服务生效                                   |

#### 写前日志配置

* wal\_mode

|   名字   | wal\_mode                                                                           |
|:------:|:------------------------------------------------------------------------------------|
|   描述   | 写前日志的写入模式. DISABLE 模式下会关闭写前日志；SYNC 模式下写入请求会在成功写入磁盘后返回； ASYNC 模式下写入请求返回时可能尚未成功写入磁盘后。 |
|   类型   | String                                                                              |
|  默认值   | ASYNC                                                                               |
| 改后生效方式 | 重启服务生效                                                                              |

* max\_wal\_nodes\_num

|   名字   | max\_wal\_nodes\_num         |
|:------:|:-----------------------------|
|   描述   | 写前日志节点的最大数量，默认值 0 表示数量由系统控制。 |
|   类型   | int32                        |
|  默认值   | 0                            |
| 改后生效方式 | 重启服务生效                       |

* wal\_async\_mode\_fsync\_delay\_in\_ms

|   名字   | wal\_async\_mode\_fsync\_delay\_in\_ms |
|:------:|:---------------------------------------|
|   描述   | async 模式下写前日志调用 fsync 前的等待时间           |
|   类型   | int32                                  |
|  默认值   | 1000                                   |
| 改后生效方式 | 热加载                                    |

* wal\_sync\_mode\_fsync\_delay\_in\_ms

|   名字   | wal\_sync\_mode\_fsync\_delay\_in\_ms |
|:------:|:--------------------------------------|
|   描述   | sync 模式下写前日志调用 fsync 前的等待时间           |
|   类型   | int32                                 |
|  默认值   | 3                                     |
| 改后生效方式 | 热加载                                   |

* wal\_buffer\_size\_in\_byte

|   名字   | wal\_buffer\_size\_in\_byte |
|:------:|:----------------------------|
|   描述   | 写前日志的 buffer 大小             |
|   类型   | int32                       |
|  默认值   | 33554432                    |
| 改后生效方式 | 重启服务生效                      |

* wal\_buffer\_queue\_capacity

|   名字   | wal\_buffer\_queue\_capacity |
|:------:|:-----------------------------|
|   描述   | 写前日志阻塞队列大小上限                 |
|   类型   | int32                        |
|  默认值   | 500                          |
| 改后生效方式 | 重启服务生效                       |

* wal\_file\_size\_threshold\_in\_byte

|   名字   | wal\_file\_size\_threshold\_in\_byte |
|:------:|:-------------------------------------|
|   描述   | 写前日志文件封口阈值                           |
|   类型   | int32                                |
|  默认值   | 31457280                             |
| 改后生效方式 | 热加载                                  |

* wal\_min\_effective\_info\_ratio

|   名字   | wal\_min\_effective\_info\_ratio |
|:------:|:---------------------------------|
|   描述   | 写前日志最小有效信息比                      |
|   类型   | double                           |
|  默认值   | 0.1                              |
| 改后生效方式 | 热加载                              |

* wal\_memtable\_snapshot\_threshold\_in\_byte

|   名字   | wal\_memtable\_snapshot\_threshold\_in\_byte |
|:------:|:---------------------------------------------|
|   描述   | 触发写前日志中内存表快照的内存表大小阈值                         |
|   类型   | int64                                        |
|  默认值   | 8388608                                      |
| 改后生效方式 | 热加载                                          |

* max\_wal\_memtable\_snapshot\_num

|   名字   | max\_wal\_memtable\_snapshot\_num |
|:------:|:----------------------------------|
|   描述   | 写前日志中内存表的最大数量上限                   |
|   类型   | int32                             |
|  默认值   | 1                                 |
| 改后生效方式 | 热加载                               |

* delete\_wal\_files\_period\_in\_ms

|   名字   | delete\_wal\_files\_period\_in\_ms |
|:------:|:-----------------------------------|
|   描述   | 删除写前日志的检查间隔                        |
|   类型   | int64                              |
|  默认值   | 20000                              |
| 改后生效方式 | 热加载                                |

#### TsFile 配置

* group\_size\_in\_byte

|名字| group\_size\_in\_byte |
|:---:|:---|
|描述| 每次将内存中的数据写入到磁盘时的最大写入字节数 |
|类型| int32 |
|默认值| 134217728 |
|改后生效方式|热加载|

* page\_size\_in\_byte

|名字| page\_size\_in\_byte |
|:---:|:---|
|描述| 内存中每个列写出时，写成的单页最大的大小，单位为字节 |
|类型| int32 |
|默认值| 65536 |
|改后生效方式|热加载|

* max\_number\_of\_points\_in\_page

|名字| max\_number\_of\_points\_in\_page |
|:---:|:----------------------------------|
|描述| 一个页中最多包含的数据点（时间戳-值的二元组）数量         |
|类型| int32                             |
|默认值| 10000                             |
|改后生效方式| 热加载                              |

* pattern\_matching\_threshold

|名字| pattern\_matching\_threshold |
|:---:|:-----------------------------|
|描述| 正则表达式匹配时最大的匹配次数              |
|类型| int32                        |
|默认值| 1000000                        |
|改后生效方式| 热加载                          |

* max\_string\_length

|名字| max\_string\_length |
|:---:|:---|
|描述| 针对字符串类型的数据，单个字符串最大长度，单位为字符|
|类型| int32 |
|默认值| 128 |
|改后生效方式|热加载|

* float\_precision

|名字| float\_precision |
|:---:|:---|
|描述| 浮点数精度，为小数点后数字的位数 |
|类型| int32 |
|默认值| 默认为 2 位。注意：32 位浮点数的十进制精度为 7 位，64 位浮点数的十进制精度为 15 位。如果设置超过机器精度将没有实际意义。 |
|改后生效方式|热加载|

* time\_encoder

|     名字     | time\_encoder                         |
| :----------: | :------------------------------------ |
|     描述     | 时间列编码方式                        |
|     类型     | 枚举 String: “TS_2DIFF”,“PLAIN”,“RLE” |
|    默认值    | TS_2DIFF                              |
| 改后生效方式 | 热加载                              |

* value\_encoder

|     名字     | value\_encoder                        |
| :----------: | :------------------------------------ |
|     描述     | value 列编码方式                      |
|     类型     | 枚举 String: “TS_2DIFF”,“PLAIN”,“RLE” |
|    默认值    | PLAIN                                 |
| 改后生效方式 | 热加载                              |

* compressor

|   名字   | compressor                                          |
|:------:|:----------------------------------------------------|
|   描述   | 数据压缩方法                                              |
|   类型   | 枚举 String : “UNCOMPRESSED”, “SNAPPY”, “LZ4”, “ZSTD” |
|  默认值   | SNAPPY                                              |
| 改后生效方式 | 热加载                                                 |

* max\_degree\_of\_index\_node

|名字| max\_degree\_of\_index\_node |
|:---:|:---|
|描述| 元数据索引树的最大度（即每个节点的最大子节点个数）。 |
|类型| int32 |
|默认值| 256 |
|改后生效方式|仅允许在第一次启动服务前修改|

* frequency\_interval\_in\_minute

|名字| frequency\_interval\_in\_minute |
|:---:|:---|
|描述| 计算查询频率的时间间隔（以分钟为单位）。 |
|类型| int32 |
|默认值| 1 |
|改后生效方式|热加载|

* freq\_snr

|     名字     | freq\_snr               |
| :----------: | :--------------------- |
|     描述     | 有损的FREQ编码的信噪比 |
|     类型     | Double                 |
|    默认值    | 40.0                   |
| 改后生效方式 | 热加载               |

* freq\_block\_size

|名字| freq\_block\_size |
|:---:|:---|
|描述| FREQ编码的块大小，即一次时频域变换的数据点个数。为了加快编码速度，建议将其设置为2的幂次。 |
|类型|int32|
|默认值| 1024 |
|改后生效方式|热加载|

#### 授权配置

* authorizer\_provider\_class

|     名字     | authorizer\_provider\_class                             |
| :----------: | :------------------------------------------------------ |
|     描述     | 权限服务的类名                                          |
|     类型     | String                                                  |
|    默认值    | org.apache.iotdb.commons.auth.authorizer.LocalFileAuthorizer |
| 改后生效方式 | 重启服务生效                                            |
|  其他可选值  | org.apache.iotdb.commons.auth.authorizer.OpenIdAuthorizer    |

* openID\_url

|     名字     | openID\_url                                                |
| :----------: | :--------------------------------------------------------- |
|     描述     | openID 服务器地址 （当 OpenIdAuthorizer 被启用时必须设定） |
|     类型     | String（一个 http 地址）                                   |
|    默认值    | 无                                                         |
| 改后生效方式 | 重启服务生效                                               |

* admin\_name

|     名字     | admin\_name                  |
| :----------: | :--------------------------- |
|     描述     | 管理员用户名，默认为root     |
|     类型     | String                       |
|    默认值    | root                         |
| 改后生效方式 | 仅允许在第一次启动服务前修改 |

* admin\_password

|     名字     | admin\_password              |
| :----------: | :--------------------------- |
|     描述     | 管理员密码，默认为root       |
|     类型     | String                       |
|    默认值    | root                         |
| 改后生效方式 | 仅允许在第一次启动服务前修改 |

* iotdb\_server\_encrypt\_decrypt\_provider

|     名字     | iotdb\_server\_encrypt\_decrypt\_provider                      |
| :----------: | :------------------------------------------------------------- |
|     描述     | 用于用户密码加密的类                                             |
|     类型     | String                                                         |
|    默认值    | org.apache.iotdb.commons.security.encrypt.MessageDigestEncrypt |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                   |

* iotdb\_server\_encrypt\_decrypt\_provider\_parameter

|     名字     | iotdb\_server\_encrypt\_decrypt\_provider\_parameter |
| :----------: | :--------------------------------------------------- |
|     描述     | 用于初始化用户密码加密类的参数                       |
|     类型     | String                                               |
|    默认值    | 空                                                   |
| 改后生效方式 | 仅允许在第一次启动服务前修改                         |

* author\_cache\_size

|     名字     | author\_cache\_size      |
| :----------: | :----------------------- |
|     描述     | 用户缓存与角色缓存的大小 |
|     类型     | int32                    |
|    默认值    | 1000                     |
| 改后生效方式 | 重启服务生效             |

* author\_cache\_expire\_time

|     名字     | author\_cache\_expire\_time            |
| :----------: | :------------------------------------- |
|     描述     | 用户缓存与角色缓存的有效期，单位为分钟 |
|     类型     | int32                                  |
|    默认值    | 30                                     |
| 改后生效方式 | 重启服务生效                           |

#### UDF查询配置

* udf\_initial\_byte\_array\_length\_for\_memory\_control

|名字| udf\_initial\_byte\_array\_length\_for\_memory\_control |
|:---:|:---|
|描述| 用于评估UDF查询中文本字段的内存使用情况。建议将此值设置为略大于所有文本的平均长度记录。 |
|类型| int32 |
|默认值| 48 |
|改后生效方式|重启服务生效|

* udf\_memory\_budget\_in\_mb

|     名字     | udf\_memory\_budget\_in\_mb                                                    |
| :----------: | :----------------------------------------------------------------------------- |
|     描述     | 在一个UDF查询中使用多少内存（以 MB 为单位）。上限为已分配内存的 20% 用于读取。 |
|     类型     | Float                                                                          |
|    默认值    | 30.0                                                                           |
| 改后生效方式 | 重启服务生效                                                                   |

* udf\_reader\_transformer\_collector\_memory\_proportion

|     名字     | udf\_reader\_transformer\_collector\_memory\_proportion   |
| :----------: | :-------------------------------------------------------- |
|     描述     | UDF内存分配比例。参数形式为a : b : c，其中a、b、c为整数。 |
|     类型     | String                                                    |
|    默认值    | 1:1:1                                                     |
| 改后生效方式 | 重启服务生效                                              |

* udf\_lib\_dir

|     名字     | udf\_lib\_dir                |
| :----------: | :--------------------------- |
|     描述     | UDF 日志及jar文件存储路径    |
|     类型     | String                       |
|    默认值    | ext/udf（Windows：ext\\udf） |
| 改后生效方式 | 重启服务生效                 |

#### 触发器配置

* trigger\_lib\_dir

|     名字     | trigger\_lib\_dir |
| :----------: |:------------------|
|     描述     | 触发器 JAR 包存放的目录    |
|     类型     | String            |
|    默认值    | ext/trigger                  |
| 改后生效方式 | 重启服务生效            |

* stateful\_trigger\_retry\_num\_when\_not\_found

|     名字     | stateful\_trigger\_retry\_num\_when\_not\_found |
| :----------: |:------------------------------------------------|
|     描述     | 有状态触发器触发无法找到触发器实例时的重试次数                         |
|     类型     | Int32                                           |
|    默认值    | 3                                               |
| 改后生效方式 | 重启服务生效                                          |


#### SELECT-INTO配置

* into\_operation\_buffer\_size\_in\_byte

|     名字     | into\_operation\_buffer\_size\_in\_byte                              |
| :----------: | :-------------------------------------------------------------------- |
|     描述     | 执行 select-into 语句时，待写入数据占用的最大内存（单位：Byte） |
|     类型     | int64                                                        |
|    默认值    | 100MB                                                        |
| 改后生效方式 | 热加载                                                     |

* select\_into\_insert\_tablet\_plan\_row\_limit

|     名字     | select\_into\_insert\_tablet\_plan\_row\_limit                              |
| :----------: | :-------------------------------------------------------------------- |
|     描述     | 执行 select-into 语句时，一个 insert-tablet-plan 中可以处理的最大行数 |
|     类型     | int32                                                        |
|    默认值    | 10000                                                        |
| 改后生效方式 | 热加载                                                     |

* into\_operation\_execution\_thread\_count

|     名字     | into\_operation\_execution\_thread\_count |
| :---------: | :---------------------------------------- |
|     描述     | SELECT INTO 中执行写入任务的线程池的线程数      |
|     类型     | int32                                     |
|    默认值    | 2                                         |
| 改后生效方式  | 重启服务生效                                 |

#### 连续查询配置

* continuous\_query\_submit\_thread\_count

|     名字     | continuous\_query\_execution\_thread |
| :----------: |:----------------------------------|
|     描述     | 执行连续查询任务的线程池的线程数                  |
|     类型     | int32                             |
|    默认值    | 2                                 |
| 改后生效方式 | 重启服务生效                      |

* continuous\_query\_min\_every\_interval\_in\_ms

|     名字     | continuous\_query\_min\_every\_interval\_in\_ms |
| :----------: |:------------------------------------------------|
|     描述     | 连续查询执行时间间隔的最小值                                  |
|     类型     | long (duration)                                 |
|    默认值    | 1000                                            |
| 改后生效方式 | 重启服务生效                                          |

#### PIPE 配置

* ip\_white\_list

|     名字     | ip\_white\_list                                                                                                    |
| :----------: | :----------------------------------------------------------------------------------------------------------------- |
|     描述     | 设置同步功能发送端 IP 地址的白名单，以网段的形式表示，多个网段之间用逗号分隔。发送端向接收端同步数据时，只有当该发送端 IP 地址处于该白名单设置的网段范围内，接收端才允许同步操作。如果白名单为空，则接收端不允许任何发送端同步数据。默认接收端拒绝除了本地以外的全部 IP 的同步请求。 对该参数进行配置时，需要保证发送端所有 DataNode 地址均被覆盖。 |
|     类型     | String                                                                                                             |
|    默认值    | 127.0.0.1/32                                                                                                          |
| 改后生效方式 | 热加载                                                                                                      |

* max\_number\_of\_sync\_file\_retry

|     名字     | max\_number\_of\_sync\_file\_retry |
| :----------: | :---------------------------- |
|     描述     | 同步文件最大重试次数          |
|     类型     | int32                           |
|    默认值    | 5                             |
| 改后生效方式 | 热加载                  |

#### IoT 共识协议配置

当Region配置了IoTConsensus共识协议之后，下述的配置项才会生效

* data_region_iot_max_log_entries_num_per_batch

|     名字     | data_region_iot_max_log_entries_num_per_batch     |
| :----------: | :-------------------------------- |
|     描述     | IoTConsensus batch 的最大日志条数 |
|     类型     | int32                             |
|    默认值    | 1024                              |
| 改后生效方式 | 重启生效                          |

* data_region_iot_max_size_per_batch

|     名字     | data_region_iot_max_size_per_batch            |
| :----------: | :---------------------------- |
|     描述     | IoTConsensus batch 的最大大小 |
|     类型     | int32                         |
|    默认值    | 16MB                          |
| 改后生效方式 | 重启生效                      |

* data_region_iot_max_pending_batches_num

|     名字     | data_region_iot_max_pending_batches_num             |
| :----------: | :---------------------------------- |
|     描述     | IoTConsensus batch 的流水线并发阈值 |
|     类型     | int32                               |
|    默认值    | 12                                  |
| 改后生效方式 | 重启生效                            |

* data_region_iot_max_memory_ratio_for_queue

|     名字     | data_region_iot_max_memory_ratio_for_queue    |
| :----------: | :---------------------------- |
|     描述     | IoTConsensus 队列内存分配比例 |
|     类型     | double                        |
|    默认值    | 0.6                           |
| 改后生效方式 | 重启生效                      |

#### Ratis 共识协议配置
当Region配置了RatisConsensus共识协议之后，下述的配置项才会生效

* config\_node\_ratis\_log\_appender\_buffer\_size\_max

|   名字   | config\_node\_ratis\_log\_appender\_buffer\_size\_max |
|:------:|:-----------------------------------------------|
|   描述   | confignode 一次同步日志RPC最大的传输字节限制                  |
|   类型   | int32                                          |
|  默认值   | 4MB                                            |
| 改后生效方式 | 重启生效                                           |


* schema\_region\_ratis\_log\_appender\_buffer\_size\_max

|   名字   | schema\_region\_ratis\_log\_appender\_buffer\_size\_max |
|:------:|:-------------------------------------------------|
|   描述   | schema region 一次同步日志RPC最大的传输字节限制                 |
|   类型   | int32                                            |
|  默认值   | 4MB                                              |
| 改后生效方式 | 重启生效                                             |

* data\_region\_ratis\_log\_appender\_buffer\_size\_max

|   名字   | data\_region\_ratis\_log\_appender\_buffer\_size\_max |
|:------:|:-----------------------------------------------|
|   描述   | data region 一次同步日志RPC最大的传输字节限制                 |
|   类型   | int32                                          |
|  默认值   | 4MB                                            |
| 改后生效方式 | 重启生效                                           |

* config\_node\_ratis\_snapshot\_trigger\_threshold

|   名字   | config\_node\_ratis\_snapshot\_trigger\_threshold |
|:------:|:---------------------------------------------|
|   描述   | confignode 触发snapshot需要的日志条数                 |
|   类型   | int32                                        |
|  默认值   | 400,000                                      |
| 改后生效方式 | 重启生效                                         |

* schema\_region\_ratis\_snapshot\_trigger\_threshold

|   名字   | schema\_region\_ratis\_snapshot\_trigger\_threshold |
|:------:|:-----------------------------------------------|
|   描述   | schema region 触发snapshot需要的日志条数                |
|   类型   | int32                                          |
|  默认值   | 400,000                                        |
| 改后生效方式 | 重启生效                                           |

* data\_region\_ratis\_snapshot\_trigger\_threshold

|   名字   | data\_region\_ratis\_snapshot\_trigger\_threshold |
|:------:|:---------------------------------------------|
|   描述   | data region 触发snapshot需要的日志条数                |
|   类型   | int32                                        |
|  默认值   | 400,000                                      |
| 改后生效方式 | 重启生效                                         |

* config\_node\_ratis\_log\_unsafe\_flush\_enable

|   名字   | config\_node\_ratis\_log\_unsafe\_flush\_enable |
|:------:|:------------------------------------------|
|   描述   | confignode 是否允许Raft日志异步刷盘                 |
|   类型   | boolean                                   |
|  默认值   | false                                     |
| 改后生效方式 | 重启生效                                      |

* schema\_region\_ratis\_log\_unsafe\_flush\_enable

|   名字   | schema\_region\_ratis\_log\_unsafe\_flush\_enable |
|:------:|:--------------------------------------------|
|   描述   | schema region 是否允许Raft日志异步刷盘                |
|   类型   | boolean                                     |
|  默认值   | false                                       |
| 改后生效方式 | 重启生效                                        |

* data\_region\_ratis\_log\_unsafe\_flush\_enable

|   名字   | data\_region\_ratis\_log\_unsafe\_flush\_enable |
|:------:|:------------------------------------------|
|   描述   | data region 是否允许Raft日志异步刷盘                |
|   类型   | boolean                                   |
|  默认值   | false                                     |
| 改后生效方式 | 重启生效                                      |

* config\_node\_ratis\_log\_segment\_size\_max\_in\_byte

|   名字   | config\_node\_ratis\_log\_segment\_size\_max\_in\_byte |
|:------:|:-----------------------------------------------|
|   描述   | confignode 一个RaftLog日志段文件的大小                   |
|   类型   | int32                                          |
|  默认值   | 24MB                                           |
| 改后生效方式 | 重启生效                                           |

* schema\_region\_ratis\_log\_segment\_size\_max\_in\_byte

|   名字   | schema\_region\_ratis\_log\_segment\_size\_max\_in\_byte |
|:------:|:-------------------------------------------------|
|   描述   | schema region 一个RaftLog日志段文件的大小                  |
|   类型   | int32                                            |
|  默认值   | 24MB                                             |
| 改后生效方式 | 重启生效                                             |

* data\_region\_ratis\_log\_segment\_size\_max\_in\_byte

|   名字   | data\_region\_ratis\_log\_segment\_size\_max\_in\_byte |
|:------:|:-----------------------------------------------|
|   描述   | data region 一个RaftLog日志段文件的大小                  |
|   类型   | int32                                          |
|  默认值   | 24MB                                           |
| 改后生效方式 | 重启生效                                           |

* config\_node\_ratis\_grpc\_flow\_control\_window

|   名字   | config\_node\_ratis\_grpc\_flow\_control\_window |
|:------:|:-------------------------------------------|
|   描述   | confignode grpc 流式拥塞窗口大小                   |
|   类型   | int32                                      |
|  默认值   | 4MB                                        |
| 改后生效方式 | 重启生效                                       |

* schema\_region\_ratis\_grpc\_flow\_control\_window

|   名字   | schema\_region\_ratis\_grpc\_flow\_control\_window |
|:------:|:---------------------------------------------|
|   描述   | schema region grpc 流式拥塞窗口大小                  |
|   类型   | int32                                        |
|  默认值   | 4MB                                          |
| 改后生效方式 | 重启生效                                         |

* data\_region\_ratis\_grpc\_flow\_control\_window

|   名字   | data\_region\_ratis\_grpc\_flow\_control\_window |
|:------:|:-------------------------------------------|
|   描述   | data region grpc 流式拥塞窗口大小                  |
|   类型   | int32                                      |
|  默认值   | 4MB                                        |
| 改后生效方式 | 重启生效                                       |

* data_region_ratis_grpc_leader_outstanding_appends_max

|     名字     | data_region_ratis_grpc_leader_outstanding_appends_max |
| :----------: | :---------------------------------------------------- |
|     描述     | data region grpc 流水线并发阈值                       |
|     类型     | int32                                                 |
|    默认值    | 128                                                   |
| 改后生效方式 | 重启生效                                              |

* data_region_ratis_log_force_sync_num

|     名字     | data_region_ratis_log_force_sync_num |
| :----------: | :----------------------------------- |
|     描述     | data region fsync 阈值               |
|     类型     | int32                                |
|    默认值    | 128                                  |
| 改后生效方式 | 重启生效                             |

* config\_node\_ratis\_rpc\_leader\_election\_timeout\_min\_ms

|   名字   | config\_node\_ratis\_rpc\_leader\_election\_timeout\_min\_ms |
|:------:|:-----------------------------------------------------|
|   描述   | confignode leader 选举超时最小值                            |
|   类型   | int32                                                |
|  默认值   | 2000ms                                               |
| 改后生效方式 | 重启生效                                                 |

* schema\_region\_ratis\_rpc\_leader\_election\_timeout\_min\_ms

|   名字   | schema\_region\_ratis\_rpc\_leader\_election\_timeout\_min\_ms |
|:------:|:-------------------------------------------------------|
|   描述   | schema region leader 选举超时最小值                           |
|   类型   | int32                                                  |
|  默认值   | 2000ms                                                 |
| 改后生效方式 | 重启生效                                                   |

* data\_region\_ratis\_rpc\_leader\_election\_timeout\_min\_ms

|   名字   | data\_region\_ratis\_rpc\_leader\_election\_timeout\_min\_ms |
|:------:|:-----------------------------------------------------|
|   描述   | data region leader 选举超时最小值                           |
|   类型   | int32                                                |
|  默认值   | 2000ms                                               |
| 改后生效方式 | 重启生效                                                 |

* config\_node\_ratis\_rpc\_leader\_election\_timeout\_max\_ms

|   名字   | config\_node\_ratis\_rpc\_leader\_election\_timeout\_max\_ms |
|:------:|:-----------------------------------------------------|
|   描述   | confignode leader 选举超时最大值                            |
|   类型   | int32                                                |
|  默认值   | 2000ms                                               |
| 改后生效方式 | 重启生效                                                 |

* schema\_region\_ratis\_rpc\_leader\_election\_timeout\_max\_ms

|   名字   | schema\_region\_ratis\_rpc\_leader\_election\_timeout\_max\_ms |
|:------:|:-------------------------------------------------------|
|   描述   | schema region leader 选举超时最大值                           |
|   类型   | int32                                                  |
|  默认值   | 2000ms                                                 |
| 改后生效方式 | 重启生效                                                   |

* data\_region\_ratis\_rpc\_leader\_election\_timeout\_max\_ms

|   名字   | data\_region\_ratis\_rpc\_leader\_election\_timeout\_max\_ms |
|:------:|:-----------------------------------------------------|
|   描述   | data region leader 选举超时最大值                           |
|   类型   | int32                                                |
|  默认值   | 2000ms                                               |
| 改后生效方式 | 重启生效                                                 |

* config\_node\_ratis\_request\_timeout\_ms

|   名字   | config\_node\_ratis\_request\_timeout\_ms |
|:------:|:-------------------------------------|
|   描述   | confignode Raft 客户端重试超时              |
|   类型   | int32                                |
|  默认值   | 10s                                  |
| 改后生效方式 | 重启生效                                 |

* schema\_region\_ratis\_request\_timeout\_ms

|   名字   | schema\_region\_ratis\_request\_timeout\_ms |
|:------:|:---------------------------------------|
|   描述   | schema region Raft 客户端重试超时             |
|   类型   | int32                                  |
|  默认值   | 10s                                    |
| 改后生效方式 | 重启生效                                   |

* data\_region\_ratis\_request\_timeout\_ms

|   名字   | data\_region\_ratis\_request\_timeout\_ms |
|:------:|:-------------------------------------|
|   描述   | data region Raft 客户端重试超时             |
|   类型   | int32                                |
|  默认值   | 10s                                  |
| 改后生效方式 | 重启生效                                 |

* config\_node\_ratis\_max\_retry\_attempts

|   名字   | config\_node\_ratis\_max\_retry\_attempts |
|:------:|:-------------------------------------|
|   描述   | confignode Raft客户端最大重试次数             |
|   类型   | int32                                |
|  默认值   | 10                                   |
| 改后生效方式 | 重启生效                                 |

* config\_node\_ratis\_initial\_sleep\_time\_ms

|   名字   | config\_node\_ratis\_initial\_sleep\_time\_ms |
|:------:|:----------------------------------------|
|   描述   | confignode Raft客户端初始重试睡眠时长              |
|   类型   | int32                                   |
|  默认值   | 100ms                                   |
| 改后生效方式 | 重启生效                                    |

* config\_node\_ratis\_max\_sleep\_time\_ms

|   名字   | config\_node\_ratis\_max\_sleep\_time\_ms |
|:------:|:------------------------------------|
|   描述   | confignode Raft客户端最大重试睡眠时长          |
|   类型   | int32                               |
|  默认值   | 10s                                 |
| 改后生效方式 | 重启生效                                |

* schema\_region\_ratis\_max\_retry\_attempts

|   名字   | schema\_region\_ratis\_max\_retry\_attempts |
|:------:|:---------------------------------------|
|   描述   | schema region Raft客户端最大重试次数            |
|   类型   | int32                                  |
|  默认值   | 10                                     |
| 改后生效方式 | 重启生效                                   |

* schema\_region\_ratis\_initial\_sleep\_time\_ms

|   名字   | schema\_region\_ratis\_initial\_sleep\_time\_ms |
|:------:|:------------------------------------------|
|   描述   | schema region Raft客户端初始重试睡眠时长             |
|   类型   | int32                                     |
|  默认值   | 100ms                                     |
| 改后生效方式 | 重启生效                                      |

* schema\_region\_ratis\_max\_sleep\_time\_ms

|   名字   | schema\_region\_ratis\_max\_sleep\_time\_ms |
|:------:|:--------------------------------------|
|   描述   | schema region Raft客户端最大重试睡眠时长         |
|   类型   | int32                                 |
|  默认值   | 10s                                   |
| 改后生效方式 | 重启生效                                  |

* data\_region\_ratis\_max\_retry\_attempts

|   名字   | data\_region\_ratis\_max\_retry\_attempts |
|:------:|:-------------------------------------|
|   描述   | data region Raft客户端最大重试次数            |
|   类型   | int32                                |
|  默认值   | 10                                   |
| 改后生效方式 | 重启生效                                 |

* data\_region\_ratis\_initial\_sleep\_time\_ms

|   名字   | data\_region\_ratis\_initial\_sleep\_time\_ms |
|:------:|:----------------------------------------|
|   描述   | data region Raft客户端初始重试睡眠时长             |
|   类型   | int32                                   |
|  默认值   | 100ms                                   |
| 改后生效方式 | 重启生效                                    |

* data\_region\_ratis\_max\_sleep\_time\_ms

|   名字   | data\_region\_ratis\_max\_sleep\_time\_ms |
|:------:|:------------------------------------|
|   描述   | data region Raft客户端最大重试睡眠时长         |
|   类型   | int32                               |
|  默认值   | 10s                                 |
| 改后生效方式 | 重启生效                                |

* config\_node\_ratis\_preserve\_logs\_num\_when\_purge

|   名字   | config\_node\_ratis\_preserve\_logs\_num\_when\_purge |
|:------:|:-----------------------------------------------|
|   描述   | confignode snapshot后保持一定数量日志不删除                |
|   类型   | int32                                          |
|  默认值   | 1000                                           |
| 改后生效方式 | 重启生效                                           |

* schema\_region\_ratis\_preserve\_logs\_num\_when\_purge

|   名字   | schema\_region\_ratis\_preserve\_logs\_num\_when\_purge |
|:------:|:-------------------------------------------------|
|   描述   | schema region snapshot后保持一定数量日志不删除               |
|   类型   | int32                                            |
|  默认值   | 1000                                             |
| 改后生效方式 | 重启生效                                             |

* data\_region\_ratis\_preserve\_logs\_num\_when\_purge

|   名字   | data\_region\_ratis\_preserve\_logs\_num\_when\_purge |
|:------:|:-----------------------------------------------|
|   描述   | data region snapshot后保持一定数量日志不删除               |
|   类型   | int32                                          |
|  默认值   | 1000                                           |
| 改后生效方式 | 重启生效                                           |

#### Procedure 配置

* procedure\_core\_worker\_thread\_count

|     名字     | procedure\_core\_worker\_thread\_count |
| :----------: | :--------------------------------- |
|     描述     | 工作线程数量                       |
|     类型     | int32                                |
|    默认值    | 4                                  |
| 改后生效方式 | 重启服务生效                       |

* procedure\_completed\_clean\_interval

|     名字     | procedure\_completed\_clean\_interval |
| :----------: | :--------------------------------- |
|     描述     | 清理已完成的 procedure 时间间隔    |
|     类型     | int32                                |
|    默认值    | 30(s)                              |
| 改后生效方式 | 重启服务生效                       |

* procedure\_completed\_evict\_ttl

|     名字     | procedure\_completed\_evict\_ttl     |
| :----------: | :-------------------------------- |
|     描述     | 已完成的 procedure 的数据保留时间 |
|     类型     | int32                               |
|    默认值    | 800(s)                            |
| 改后生效方式 | 重启服务生效                      |

#### MQTT代理配置

* enable\_mqtt\_service

|     名字     | enable\_mqtt\_service。 |
| :----------: | :---------------------- |
|     描述     | 是否开启MQTT服务        |
|     类型     | Boolean                 |
|    默认值    | false                   |
| 改后生效方式 | 热加载                |

* mqtt\_host

|     名字     | mqtt\_host           |
| :----------: | :------------------- |
|     描述     | MQTT服务绑定的host。 |
|     类型     | String               |
|    默认值    | 0.0.0.0              |
| 改后生效方式 | 热加载             |

* mqtt\_port

|名字| mqtt\_port |
|:---:|:---|
|描述| MQTT服务绑定的port。 |
|类型| int32 |
|默认值| 1883 |
|改后生效方式|热加载|

* mqtt\_handler\_pool\_size

|名字| mqtt\_handler\_pool\_size |
|:---:|:---|
|描述| 用于处理MQTT消息的处理程序池大小。 |
|类型| int32 |
|默认值| 1 |
|改后生效方式|热加载|

* mqtt\_payload\_formatter

|     名字     | mqtt\_payload\_formatter     |
| :----------: | :--------------------------- |
|     描述     | MQTT消息有效负载格式化程序。 |
|     类型     | String                       |
|    默认值    | json                         |
| 改后生效方式 | 热加载                     |

* mqtt\_max\_message\_size

|名字| mqtt\_max\_message\_size |
|:---:|:---|
|描述| MQTT消息的最大长度（以字节为单位）。 |
|类型| int32 |
|默认值| 1048576 |
|改后生效方式|热加载|

#### REST 服务配置

* enable\_rest\_service

|名字| enable\_rest\_service |
|:---:|:--------------------|
|描述| 是否开启Rest服务。         |
|类型| Boolean             |
|默认值| false               |
|改后生效方式| 重启生效                |

* rest\_service\_port

|名字| rest\_service\_port |
|:---:|:------------------|
|描述| Rest服务监听端口号       |
|类型| int32             |
|默认值| 18080             |
|改后生效方式| 重启生效              |

* enable\_swagger

|名字| enable\_swagger         |
|:---:|:-----------------------|
|描述| 是否启用swagger来展示rest接口信息 |
|类型| Boolean                |
|默认值| false                  |
|改后生效方式| 重启生效                   |

* rest\_query\_default\_row\_size\_limit

|名字| rest\_query\_default\_row\_size\_limit |
|:---:|:----------------------------------|
|描述| 一次查询能返回的结果集最大行数                   |
|类型| int32                             |
|默认值| 10000                             |
|改后生效方式| 重启生效                              |

* cache\_expire

|名字| cache\_expire  |
|:---:|:--------------|
|描述| 缓存客户登录信息的过期时间 |
|类型| int32         |
|默认值| 28800         |
|改后生效方式| 重启生效          |

* cache\_max\_num

|名字| cache\_max\_num |
|:---:|:--------------|
|描述| 缓存中存储的最大用户数量  |
|类型| int32         |
|默认值| 100           |
|改后生效方式| 重启生效          |

* cache\_init\_num

|名字| cache\_init\_num |
|:---:|:---------------|
|描述| 缓存初始容量         |
|类型| int32          |
|默认值| 10             |
|改后生效方式| 重启生效           |

* enable\_https

|名字| cache\_init\_num           |
|:---:|:-------------------------|
|描述| REST Service 是否开启 SSL 配置 |
|类型| Boolean                  |
|默认值| false                    |
|改后生效方式| 重启生效                     |

* key\_store\_path

|名字| key\_store\_path |
|:---:|:---------------|
|描述| keyStore 所在路径（非必填）  |
|类型| String         |
|默认值| ""          |
|改后生效方式| 重启生效           |

* key\_store\_pwd

|名字| key\_store\_pwd |
|:---:|:---------------|
|描述| keyStore 密码（非必填） |
|类型| String         |
|默认值| ""          |
|改后生效方式| 重启生效           |

* trust\_store\_path

|名字| trust\_store\_path |
|:---:|:---------------|
|描述| keyStore 密码（非必填） |
|类型| String         |
|默认值| ""          |
|改后生效方式| 重启生效           |

* trust\_store\_pwd

|名字| trust\_store\_pwd |
|:---:|:---------------|
|描述| trustStore 密码（非必填） |
|类型| String         |
|默认值| ""          |
|改后生效方式| 重启生效           |

* idle\_timeout

|名字| idle\_timeout  |
|:---:|:--------------|
|描述| SSL 超时时间，单位为秒 |
|类型| int32         |
|默认值| 5000          |
|改后生效方式| 重启生效          |

#### InfluxDB 协议适配器配置

* enable\_influxdb\_rpc\_service

|     名字     | enable\_influxdb\_rpc\_service  |
| :----------: | :--------------------------- |
|     描述     | 是否开启InfluxDB RPC service |
|     类型     | Boolean                      |
|    默认值    | true                         |
| 改后生效方式 | 重启服务生效                 |

* influxdb\_rpc\_port

|     名字     | influxdb\_rpc\_port            |
| :----------: | :--------------------------- |
|     描述     | influxdb rpc service占用端口 |
|     类型     | int32                          |
|    默认值    | 8086                         |
| 改后生效方式 | 重启服务生效                 |