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

# 通用配置参数

IoTDB ConfigNode 和 DataNode 的通用配置参数位于 `conf` 目录下。

* `iotdb-common.properties`：IoTDB 的通用配置文件。

## 系统配置项

### 副本配置

* config\_node\_consensus\_protocol\_class

|     名字     | config\_node\_consensus\_protocol\_class         |
| :----------: | :----------------------------------------------- |
|     描述     | ConfigNode 副本的共识协议，仅支持 RatisConsensus |
|     类型     | String                                           |
|    默认值    | org.apache.iotdb.consensus.ratis.RatisConsensus  |
| 改后生效方式 | 仅允许在第一次启动服务前修改                     |

* schema\_replication\_factor

|     名字     | schema\_replication\_factor |
| :----------: | :-------------------------- |
|     描述     | Database 的默认元数据副本数 |
|     类型     | Int                         |
|    默认值    | 1                           |
| 改后生效方式 | 重启服务生效                |

* schema\_region\_consensus\_protocol\_class

|     名字     | schema\_region\_consensus\_protocol\_class                                                   |
| :----------: | :------------------------------------------------------------------------------------------- |
|     描述     | 元数据副本的共识协议，1 副本时可以使用 SimpleConsensus 协议，多副本时只能使用 RatisConsensus |
|     类型     | String                                                                                       |
|    默认值    | org.apache.iotdb.consensus.simple.SimpleConsensus                                            |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                                                 |

* data\_replication\_factor

|     名字     | data\_replication\_factor |
| :----------: | :------------------------ |
|     描述     | Database 的默认数据副本数 |
|     类型     | Int                       |
|    默认值    | 1                         |
| 改后生效方式 | 重启服务生效              |

* data\_region\_consensus\_protocol\_class

|     名字     | data\_region\_consensus\_protocol\_class                                                                           |
| :----------: | :----------------------------------------------------------------------------------------------------------------- |
|     描述     | 数据副本的共识协议，1 副本时可以使用 SimpleConsensus 协议，多副本时可以使用 MultiLeaderConsensus 或 RatisConsensus |
|     类型     | String                                                                                                             |
|    默认值    | org.apache.iotdb.consensus.simple.SimpleConsensus                                                                  |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                                                                       |

### 分区（负载均衡）配置

* series\_partition\_slot\_num

|     名字     | series\_partition\_slot\_num |
| :----------: | :--------------------------- |
|     描述     | 序列分区槽数                 |
|     类型     | Int                          |
|    默认值    | 10000                        |
| 改后生效方式 | 仅允许在第一次启动服务前修改 |

* series\_partition\_executor\_class

|     名字     | series\_partition\_executor\_class                                |
| :----------: | :---------------------------------------------------------------- |
|     描述     | 序列分区槽数                                                      |
|     类型     | String                                                            |
|    默认值    | org.apache.iotdb.commons.partition.executor.hash.BKDRHashExecutor |
| 改后生效方式 | 仅允许在第一次启动服务前修改                                      |

* region\_allocate\_strategy

|     名字     | region\_allocate\_strategy                                                             |
| :----------: | :------------------------------------------------------------------------------------- |
|     描述     | 元数据和数据的节点分配策略，COPY_SET适用于大集群；当数据节点数量较少时，GREEDY表现更佳 |
|     类型     | String                                                                                 |
|    默认值    | GREEDY                                                                                 |
| 改后生效方式 | 重启服务生效                                                                           |


### 集群管理

* time\_partition\_interval

|     名字     | time\_partition\_interval       |
| :----------: | :------------------------------ |
|     描述     | Database 默认的数据时间分区间隔 |
|     类型     | Long                            |
|     单位     | 毫秒                            |
|    默认值    | 604800000                       |
| 改后生效方式 | 仅允许在第一次启动服务前修改    |

* heartbeat\_interval\_in\_ms

|     名字     | heartbeat\_interval\_in\_ms |
| :----------: | :-------------------------- |
|     描述     | 集群节点间的心跳间隔        |
|     类型     | Long                        |
|     单位     | ms                          |
|    默认值    | 1000                        |
| 改后生效方式 | 重启服务生效                |

* disk\_space\_warning\_threshold

|     名字     | disk\_space\_warning\_threshold |
| :----------: | :------------------------------ |
|     描述     | DataNode 磁盘剩余阈值           |
|     类型     | double(percentage)              |
|    默认值    | 0.05                            |
| 改后生效方式 | 重启服务生效                    |

### 内存控制配置

* enable\_mem\_control

|     名字     | enable\_mem\_control     |
| :----------: | :----------------------- |
|     描述     | 开启内存控制，避免爆内存 |
|     类型     | Boolean                  |
|    默认值    | true                     |
| 改后生效方式 | 重启服务生效             |

* concurrent\_writing\_time\_partition

|     名字     | concurrent\_writing\_time\_partition        |
| :----------: | :------------------------------------------ |
|     描述     | 最大可同时写入的时间分区个数，默认500个分区 |
|     类型     | Int64                                       |
|    默认值    | 500                                         |
| 改后生效方式 | 重启服务生效                                |

* primary\_array\_size

|     名字     | primary\_array\_size                     |
| :----------: | :--------------------------------------- |
|     描述     | 数组池中的原始数组大小（每个数组的长度） |
|     类型     | Int32                                    |
|    默认值    | 32                                       |
| 改后生效方式 | 重启服务生效                             |

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

|     名字     | check\_period\_when\_insert\_blocked                                                                         |
| :----------: | :----------------------------------------------------------------------------------------------------------- |
|     描述     | 当插入被拒绝时，等待时间（以毫秒为单位）去再次检查系统，默认为50。若插入被拒绝，读取负载低，可以设置大一些。 |
|     类型     | Int32                                                                                                        |
|    默认值    | 50                                                                                                           |
| 改后生效方式 | 重启服务生效                                                                                                 |

* io\_task\_queue\_size\_for\_flushing

|     名字     | io\_task\_queue\_size\_for\_flushing |
| :----------: | :----------------------------------- |
|     描述     | ioTaskQueue 的大小。默认值为10。     |
|     类型     | Int32                                |
|    默认值    | 10                                   |
| 改后生效方式 | 重启服务生效                         |

* partition\_cache\_size

|名字| partition\_cache\_size |
|:---:|:---|
|描述| 分区信息缓存的最大缓存条目数。|
|类型| Int32 |
|默认值| 1000 |
|改后生效方式|重启服务生效|

### 元数据引擎配置

* mlog\_buffer\_size

|     名字     | mlog\_buffer\_size  |
| :----------: | :------------------ |
|     描述     | mlog 的 buffer 大小 |
|     类型     | Int32               |
|    默认值    | 1048576             |
| 改后生效方式 | 重启服务生效            |

* sync\_mlog\_period\_in\_ms

|     名字     | sync\_mlog\_period\_in\_ms                                                                            |
| :----------: | :---------------------------------------------------------------------------------------------------- |
|     描述     | mlog定期刷新到磁盘的周期，单位毫秒。如果该参数为0，则表示每次对元数据的更新操作都会被立即写到磁盘上。 |
|     类型     | Int64                                                                                                 |
|    默认值    | 100                                                                                                   |
| 改后生效方式 | 重启服务生效                                                                                          |

* tag\_attribute\_total\_size

|     名字     | tag\_attribute\_total\_size              |
| :----------: | :--------------------------------------- |
|     描述     | 每个时间序列标签和属性的最大持久化字节数 |
|     类型     | Int32                                    |
|    默认值    | 700                                      |
| 改后生效方式 | 仅允许在第一次启动服务前修改             |

* tag\_attribute\_flush\_interval

|     名字     | tag\_attribute\_flush\_interval                    |
| :----------: | :------------------------------------------------- |
|     描述     | 标签和属性记录的间隔数，达到此记录数量时将强制刷盘 |
|     类型     | Int32                                              |
|    默认值    | 1000                                               |
| 改后生效方式 | 仅允许在第一次启动服务前修改                       |

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

### 数据类型自动推断

* enable\_auto\_create\_schema

|     名字     | enable\_auto\_create\_schema           |
| :----------: | :------------------------------------- |
|     描述     | 当写入的序列不存在时，是否自动创建序列 |
|     取值     | true or false                          |
|    默认值    | true                                   |
| 改后生效方式 | 重启服务生效                           |

* default\_storage\_group\_level

|     名字     | default\_storage\_group\_level                                                                                                                                                                                      |
| :----------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
|     描述     | 当写入的数据不存在且自动创建序列时，若需要创建相应的 database，将序列路径的哪一层当做 database。例如，如果我们接到一个新序列 root.sg0.d1.s2, 并且 level=1， 那么 root.sg0 被视为database（因为 root 是 level 0 层） |
|     取值     | Int32                                                                                                                                                                                                               |
|    默认值    | 1                                                                                                                                                                                                                   |
| 改后生效方式 | 重启服务生效                                                                                                                                                                                                        |

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

|     名字     | default\_int32\_encoding               |
| :----------: | :------------------------------------- |
|     描述     | int32 类型编码格式                     |
|     取值     | PLAIN, RLE, TS_2DIFF, REGULAR, GORILLA |
|    默认值    | RLE                                    |
| 改后生效方式 | 重启服务生效                           |

* default\_int64\_encoding

|     名字     | default\_int64\_encoding               |
| :----------: | :------------------------------------- |
|     描述     | int64 类型编码格式                     |
|     取值     | PLAIN, RLE, TS_2DIFF, REGULAR, GORILLA |
|    默认值    | RLE                                    |
| 改后生效方式 | 重启服务生效                           |

* default\_float\_encoding

|     名字     | default\_float\_encoding      |
| :----------: | :---------------------------- |
|     描述     | float 类型编码格式            |
|     取值     | PLAIN, RLE, TS_2DIFF, GORILLA |
|    默认值    | GORILLA                       |
| 改后生效方式 | 重启服务生效                  |

* default\_double\_encoding

|     名字     | default\_double\_encoding     |
| :----------: | :---------------------------- |
|     描述     | double 类型编码格式           |
|     取值     | PLAIN, RLE, TS_2DIFF, GORILLA |
|    默认值    | GORILLA                       |
| 改后生效方式 | 重启服务生效                  |

* default\_text\_encoding

|     名字     | default\_text\_encoding |
| :----------: | :---------------------- |
|     描述     | text 类型编码格式       |
|     取值     | PLAIN                   |
|    默认值    | PLAIN                   |
| 改后生效方式 | 重启服务生效            |

### 查询配置

* meta\_data\_cache\_enable

|     名字     | meta\_data\_cache\_enable                               |
| :----------: | :------------------------------------------------------ |
|     描述     | 是否缓存元数据Chunk Metadata 和 TimeSeries Metadata）。 |
|     类型     | Boolean                                                 |
|    默认值    | true                                                    |
| 改后生效方式 | 重启服务生效                                            |

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

* metadata\_node\_cache\_size

|     名字     | metadata\_node\_cache\_size                                                                                      |
| :----------: | :--------------------------------------------------------------------------------------------------------------- |
|     描述     | SchemaRegion的缓存大小。所有路径检查和将具有相应路径的SchemaRegion中的TSDataType的缓存，都将被用作提高写入速度。 |
|     类型     | Int32                                                                                                            |
|    默认值    | 300000                                                                                                           |
| 改后生效方式 | 重启服务生效                                                                                                     |

* mpp\_data\_exchange\_core\_pool\_size

|     名字     | mpp\_data\_exchange\_core\_pool\_size |
| :----------: | :------------------------------------ |
|     描述     | MPP 数据交换线程池核心线程数          |
|     类型     | int                                   |
|    默认值    | 10                                    |
| 改后生效方式 | 重启服务生效                          |

* mpp\_data\_exchange\_max\_pool\_size

|     名字     | mpp\_data\_exchange\_max\_pool\_size |
| :----------: | :----------------------------------- |
|     描述     | MPP 数据交换线程池最大线程数         |
|     类型     | int                                  |
|    默认值    | 10                                   |
| 改后生效方式 | 重启服务生效                         |

* mpp\_data\_exchange\_keep\_alive\_time\_in\_ms

|     名字     | mpp\_data\_exchange\_keep\_alive\_time\_in\_ms |
| :----------: | :--------------------------------------------- |
|     描述     | MPP 数据交换最大等待时间                       |
|     类型     | int                                            |
|    默认值    | 1000                                           |
| 改后生效方式 | 重启服务生效                                   |

* default\_fill\_interval

|     名字     | default\_fill\_interval                                            |
| :----------: | :----------------------------------------------------------------- |
|     描述     | 填充查询中使用的默认时间段，默认-1表示无限过去时间，以毫秒ms为单位 |
|     类型     | Int32                                                              |
|    默认值    | -1                                                                 |
| 改后生效方式 | 重启服务生效                                                       |

* group_by_fill_cache_size_in_mb

|     名字     | group_by_fill_cache_size_in_mb     |
| :----------: | :--------------------------------- |
|     描述     | 填充查询中使用的缓存大小，单位是MB |
|     类型     | Float                              |
|    默认值    | 1.0                                |
| 改后生效方式 | 重启服务生效                       |

* driver\_task\_execution\_time\_slice\_in\_ms

|     名字     | driver\_task\_execution\_time\_slice\_in\_ms |
| :----------: | :------------------------------------------- |
|     描述     | 单个 DriverTask 最长执行时间                 |
|     类型     | int                                          |
|    默认值    | 100                                          |
| 改后生效方式 | 重启服务生效                                 |

* max\_tsblock\_size\_in\_bytes

|     名字     | max\_tsblock\_size\_in\_bytes |
| :----------: | :---------------------------- |
|     描述     | 单个 TsBlock 的最大容量       |
|     类型     | int                           |
|    默认值    | 1024 * 1024 (1 MB)            |
| 改后生效方式 | 重启服务生效                  |

* max\_tsblock\_line\_numbers

|     名字     | max\_tsblock\_line\_numbers |
| :----------: | :-------------------------- |
|     描述     | 单个 TsBlock 的最大行数     |
|     类型     | int                         |
|    默认值    | 1000                        |
| 改后生效方式 | 重启服务生效                |

* slow\_query\_threshold

|     名字     | slow\_query\_threshold           |
| :----------: | :------------------------------- |
|     描述     | 慢查询的时间成本（毫秒ms）阈值。 |
|     类型     | Int32                            |
|    默认值    | 5000                             |
| 改后生效方式 | 触发生效                         |

* enable\_external\_sort

|     名字     | enable\_external\_sort |
| :----------: | :--------------------- |
|     描述     | 是否开启外部排序功能   |
|     类型     | Boolean                |
|    默认值    | true                   |
| 改后生效方式 | 重启服务生效           |

* external\_sort\_threshold

|     名字     | external\_sort\_threshold                                                                                                                                                                                                                            |
| :----------: | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|     描述     | 单个时间序列的最大同时块读取数。若同时chunk读取的数量大于external_sort_threshold，则使用外部排序。当external_sort_threshold增加时，内存中同时排序的chunk数量可能会增加，这会占用更多的内存；external_sort_threshold 减小时，触发外部排序会增加耗时。 |
|     类型     | Int32                                                                                                                                                                                                                                                |
|    默认值    | 1000                                                                                                                                                                                                                                                 |
| 改后生效方式 | 重启服务生效                                                                                                                                                                                                                                         |

* coordinator\_read\_executor\_size

|名字| coordinator\_read\_executor\_size |
|:---:|:---|
|描述| coordinator中用于执行查询操作的线程数 |
|类型| Int32 |
|默认值| 50 |
|改后生效方式|重启服务生效|

* coordinator\_write\_executor\_size

|名字| coordinator\_write\_executor\_size |
|:---:|:---|
|描述| coordinator中用于执行写入操作的线程数 |
|类型| Int32 |
|默认值| 50 |
|改后生效方式|重启服务生效|

### 存储引擎配置

* timestamp\_precision

|     名字     | timestamp\_precision        |
| :----------: | :-------------------------- |
|     描述     | 时间戳精度，支持 ms、us、ns |
|     类型     | String                      |
|    默认值    | ms                          |
| 改后生效方式 | 触发生效                    |

* default\_ttl\_in\_ms

|     名字     | default\_ttl\_in\_ms                                        |
| :----------: | :---------------------------------------------------------- |
|     描述     | 数据保留时间，会丢弃 now()-default\_ttl 之前的数据，单位 ms |
|     类型     | long                                                        |
|    默认值    | 36000000                                                    |
| 改后生效方式 | 重启服务生效                                                |

* memtable\_size\_threshold

|     名字     | memtable\_size\_threshold                          |
| :----------: | :------------------------------------------------- |
|     描述     | 内存缓冲区 memtable 阈值                           |
|     类型     | Long                                               |
|    默认值    | 1073741824                                         |
| 改后生效方式 | enable\_mem\_control 为 false 时生效、重启服务生效 |

* enable\_timed\_flush\_seq\_memtable

|     名字     | enable\_timed\_flush\_seq\_memtable |
| :----------: | :---------------------------------- |
|     描述     | 是否开启定时刷盘顺序 memtable       |
|     类型     | Boolean                             |
|    默认值    | false                               |
| 改后生效方式 | 触发生效                            |

* seq\_memtable\_flush\_interval\_in\_ms

|     名字     | seq\_memtable\_flush\_interval\_in\_ms                               |
| :----------: | :------------------------------------------------------------------- |
|     描述     | 当 memTable 的创建时间小于当前时间减去该值时，该 memtable 需要被刷盘 |
|     类型     | Int32                                                                |
|    默认值    | 3600000                                                              |
| 改后生效方式 | 触发生效                                                             |

* seq\_memtable\_flush\_check\_interval\_in\_ms

|     名字     | seq\_memtable\_flush\_check\_interval\_in\_ms |
| :----------: | :-------------------------------------------- |
|     描述     | 检查顺序 memtable 是否需要刷盘的时间间隔      |
|     类型     | Int32                                         |
|    默认值    | 600000                                        |
| 改后生效方式 | 触发生效                                      |

* enable\_timed\_flush\_unseq\_memtable

|     名字     | enable\_timed\_flush\_unseq\_memtable |
| :----------: | :------------------------------------ |
|     描述     | 是否开启定时刷新乱序 memtable         |
|     类型     | Boolean                               |
|    默认值    | true                                  |
| 改后生效方式 | 触发生效                              |

* unseq\_memtable\_flush\_interval\_in\_ms

|     名字     | unseq\_memtable\_flush\_interval\_in\_ms                             |
| :----------: | :------------------------------------------------------------------- |
|     描述     | 当 memTable 的创建时间小于当前时间减去该值时，该 memtable 需要被刷盘 |
|     类型     | Int32                                                                |
|    默认值    | 3600000                                                              |
| 改后生效方式 | 触发生效                                                             |

* unseq\_memtable\_flush\_check\_interval\_in\_ms

|     名字     | unseq\_memtable\_flush\_check\_interval\_in\_ms |
| :----------: | :---------------------------------------------- |
|     描述     | 检查乱序 memtable 是否需要刷盘的时间间隔        |
|     类型     | Int32                                           |
|    默认值    | 600000                                          |
| 改后生效方式 | 触发生效                                        |

* avg\_series\_point\_number\_threshold

|     名字     | avg\_series\_point\_number\_threshold            |
| :----------: | :----------------------------------------------- |
|     描述     | 内存中平均每个时间序列点数最大值，达到触发 flush |
|     类型     | Int32                                            |
|    默认值    | 100000                                           |
| 改后生效方式 | 重启服务生效                                     |

* flush\_thread\_count

|     名字     | flush\_thread\_count                                                                                                                   |
| :----------: | :------------------------------------------------------------------------------------------------------------------------------------- |
|     描述     | 当 IoTDB 将内存中的数据写入磁盘时，最多启动多少个线程来执行该操作。如果该值小于等于 0，那么采用机器所安装的 CPU 核的数量。默认值为 0。 |
|     类型     | Int32                                                                                                                                  |
|    默认值    | 0                                                                                                                                      |
| 改后生效方式 | 重启服务生效                                                                                                                           |

* query\_thread\_count

|     名字     | query\_thread\_count                                                                                                                    |
| :----------: | :-------------------------------------------------------------------------------------------------------------------------------------- |
|     描述     | 当 IoTDB 对内存中的数据进行查询时，最多启动多少个线程来执行该操作。如果该值小于等于 0，那么采用机器所安装的 CPU 核的数量。默认值为 16。 |
|     类型     | Int32                                                                                                                                   |
|    默认值    | 16                                                                                                                                      |
| 改后生效方式 | 重启服务生效                                                                                                                            |

* sub\_rawQuery\_thread\_count

|     名字     | sub\_rawQuery\_thread\_count                                                            |
| :----------: | :-------------------------------------------------------------------------------------- |
|     描述     | 原始数据查询时，最多启动多少个线程来执行该操作。如果设置小于等于 0，会采用机器 CPU 核数 |
|     类型     | Int32                                                                                   |
|    默认值    | 8                                                                                       |
| 改后生效方式 | 重启服务生效                                                                            |

* raw\_query\_blocking\_queue\_capacity

|     名字     | raw\_query\_blocking\_queue\_capacity            |
| :----------: | :----------------------------------------------- |
|     描述     | 原始数据查询中，读任务的阻塞队列长度。默认值为 5 |
|     类型     | Int32                                            |
|    默认值    | 5                                                |
| 改后生效方式 | 重启服务生效                                     |

* chunk\_buffer\_pool\_enable

|     名字     | chunk\_buffer\_pool\_enable                                                                |
| :----------: | :----------------------------------------------------------------------------------------- |
|     描述     | 在将 memtable 序列化为内存中的字节时，是否开启由 IoTDB 而不是 JVM 接管内存管理，默认关闭。 |
|     类型     | Boolean                                                                                    |
|    默认值    | false                                                                                      |
| 改后生效方式 | 重启服务生效                                                                               |

* batch\_size

|     名字     | batch\_size                                                |
| :----------: | :--------------------------------------------------------- |
|     描述     | 服务器中每次迭代的数据量（数据条目，即不同时间戳的数量。） |
|     类型     | Int32                                                      |
|    默认值    | 100000                                                     |
| 改后生效方式 | 重启服务生效                                               |

* enable\_partial\_insert

|     名字     | enable\_partial\_insert                                            |
| :----------: | :----------------------------------------------------------------- |
|     描述     | 在一次 insert 请求中，如果部分测点写入失败，是否继续写入其他测点。 |
|     类型     | Boolean                                                            |
|    默认值    | true                                                               |
| 改后生效方式 | 重启服务生效                                                       |

* insert\_multi\_tablet\_enable\_multithreading\_column\_threshold

|     名字     | insert\_multi\_tablet\_enable\_multithreading\_column\_threshold |
| :----------: | :--------------------------------------------------------------- |
|     描述     | 插入时启用多线程插入列数的阈值                                   |
|     类型     | Int32                                                            |
|    默认值    | 10                                                               |
| 改后生效方式 | 重启服务生效                                                     |

### 合并配置

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

* compaction\_priority

|     名字     | compaction\_priority                                                                                                                               |
| :----------: | :------------------------------------------------------------------------------------------------------------------------------------------------- |
|     描述     | 合并时的优先级，BALANCE 各种合并平等，INNER_CROSS 优先进行顺序文件和顺序文件或乱序文件和乱序文件的合并，CROSS_INNER 优先将乱序文件合并到顺序文件中 |
|     类型     | String                                                                                                                                             |
|    默认值    | BALANCE                                                                                                                                            |
| 改后生效方式 | 重启服务生效                                                                                                                                       |

* target\_compaction\_file\_size

|     名字     | target\_compaction\_file\_size |
| :----------: | :----------------------------- |
|     描述     | 空间内合并的目标文件大小       |
|     类型     | Int64                          |
|    默认值    | 1073741824                     |
| 改后生效方式 | 重启服务生效                   |

* target\_chunk\_size

|     名字     | target\_chunk\_size     |
| :----------: | :---------------------- |
|     描述     | 合并时 Chunk 的目标大小 |
|     类型     | Int64                   |
|    默认值    | 1048576                 |
| 改后生效方式 | 重启服务生效            |

* target\_chunk\_point\_num

|     名字     | target\_chunk\_point\_num |
| :----------: | :------------------------ |
|     描述     | 合并时 Chunk 的目标点数   |
|     类型     | Int32                     |
|    默认值    | 100000                    |
| 改后生效方式 | 重启服务生效              |

* chunk\_size\_lower\_bound\_in\_compaction

|     名字     | chunk\_size\_lower\_bound\_in\_compaction             |
| :----------: | :---------------------------------------------------- |
|     描述     | 合并时源 Chunk 的大小小于这个值，将被解开成点进行合并 |
|     类型     | Int64                                                 |
|    默认值    | 128                                                   |
| 改后生效方式 | 重启服务生效                                          |

* chunk\_point\_num\_lower\_bound\_in\_compaction

|     名字     | chunk\_point\_num\_lower\_bound\_in\_compaction       |
| :----------: | :---------------------------------------------------- |
|     描述     | 合并时源 Chunk 的点数小于这个值，将被解开成点进行合并 |
|     类型     | Int32                                                 |
|    默认值    | 100                                                   |
| 改后生效方式 | 重启服务生效                                          |

* max\_inner\_compaction\_candidate\_file\_num

|     名字     | max\_inner\_compaction\_candidate\_file\_num |
| :----------: | :------------------------------------------- |
|     描述     | 空间内合并中一次合并最多参与的文件数         |
|     类型     | Int32                                        |
|    默认值    | 30                                           |
| 改后生效方式 | 重启服务生效                                 |

* max\_cross\_compaction\_candidate\_file\_num

|     名字     | max\_cross\_compaction\_candidate\_file\_num |
| :----------: | :------------------------------------------- |
|     描述     | 跨空间合并中一次合并最多参与的文件数         |
|     类型     | Int32                                        |
|    默认值    | 1000                                         |
| 改后生效方式 | 重启服务生效                                 |

* cross\_compaction\_file\_selection\_time\_budget

|     名字     | cross\_compaction\_file\_selection\_time\_budget                                                                                             |
| :----------: | :------------------------------------------------------------------------------------------------------------------------------------------- |
|     描述     | 若一个合并文件选择运行的时间超过这个时间，它将结束，并且当前的文件合并选择将用作为最终选择。当时间小于0 时，则表示时间是无边界的。单位：ms。 |
|     类型     | Int32                                                                                                                                        |
|    默认值    | 30000                                                                                                                                        |
| 改后生效方式 | 重启服务生效                                                                                                                                 |

* cross\_compaction\_memory\_budget

|     名字     | cross\_compaction\_memory\_budget                                                                                                                                                                                               |
| :----------: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
|     描述     | 一个合并任务可以使用多少内存（以字节为单位），默认为最大JVM内存的10%。这只是一个粗略的估计，从一个比较小的值开始，避免OOM。每个新的合并线程可能会占用这样的内存，所以merge_thread_num * merge_memory_budget是合并的预估总内存。 |
|     类型     | Int32                                                                                                                                                                                                                           |
|    默认值    | 2147483648                                                                                                                                                                                                                      |
| 改后生效方式 | 重启服务生效                                                                                                                                                                                                                    |

* compaction\_thread\_count

|     名字     | compaction\_thread\_count |
| :----------: | :------------------------ |
|     描述     | 执行合并任务的线程数目    |
|     类型     | Int32                     |
|    默认值    | 10                        |
| 改后生效方式 | 重启服务生效              |

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

|     名字     | compaction\_write\_throughput\_mb\_per\_sec |
| :----------: | :------------------------------------------ |
|     描述     | 每秒可达到的写入吞吐量合并限制。            |
|     类型     | Int32                                       |
|    默认值    | 16                                          |
| 改后生效方式 | 重启服务生效                                |

* query\_timeout\_threshold

|     名字     | query\_timeout\_threshold        |
| :----------: | :------------------------------- |
|     描述     | 查询的最大执行时间。单位：毫秒。 |
|     类型     | Int32                            |
|    默认值    | 60000                            |
| 改后生效方式 | 重启服务生效                     |

### 写前日志配置

* wal\_buffer\_size

|     名字     | wal\_buffer\_size      |
| :----------: | :--------------------- |
|     描述     | 写前日志的 buffer 大小 |
|     类型     | Int32                  |
|    默认值    | 16777216               |
| 改后生效方式 | 触发生效               |

### TsFile 配置

* group\_size\_in\_byte

|     名字     | group\_size\_in\_byte                          |
| :----------: | :--------------------------------------------- |
|     描述     | 每次将内存中的数据写入到磁盘时的最大写入字节数 |
|     类型     | Int32                                          |
|    默认值    | 134217728                                      |
| 改后生效方式 | 触发生效                                       |

* page\_size\_in\_byte

|     名字     | page\_size\_in\_byte                                 |
| :----------: | :--------------------------------------------------- |
|     描述     | 内存中每个列写出时，写成的单页最大的大小，单位为字节 |
|     类型     | Int32                                                |
|    默认值    | 65536                                                |
| 改后生效方式 | 触发生效                                             |

* max\_number\_of\_points\_in\_page

|     名字     | max\_number\_of\_points\_in\_page                 |
| :----------: | :------------------------------------------------ |
|     描述     | 一个页中最多包含的数据点（时间戳-值的二元组）数量 |
|     类型     | Int32                                             |
|    默认值    | 1048576                                           |
| 改后生效方式 | 触发生效                                          |

* max\_string\_length

|     名字     | max\_string\_length                                  |
| :----------: | :--------------------------------------------------- |
|     描述     | 针对字符串类型的数据，单个字符串最大长度，单位为字符 |
|     类型     | Int32                                                |
|    默认值    | 128                                                  |
| 改后生效方式 | 触发生效                                             |

* float\_precision

|     名字     | float\_precision                                                                                                         |
| :----------: | :----------------------------------------------------------------------------------------------------------------------- |
|     描述     | 浮点数精度，为小数点后数字的位数                                                                                         |
|     类型     | Int32                                                                                                                    |
|    默认值    | 默认为 2 位。注意：32 位浮点数的十进制精度为 7 位，64 位浮点数的十进制精度为 15 位。如果设置超过机器精度将没有实际意义。 |
| 改后生效方式 | 触发生效                                                                                                                 |

* time\_encoder

|     名字     | time\_encoder                         |
| :----------: | :------------------------------------ |
|     描述     | 时间列编码方式                        |
|     类型     | 枚举 String: “TS_2DIFF”,“PLAIN”,“RLE” |
|    默认值    | TS_2DIFF                              |
| 改后生效方式 | 触发生效                              |

* value\_encoder

|     名字     | value\_encoder                        |
| :----------: | :------------------------------------ |
|     描述     | value 列编码方式                      |
|     类型     | 枚举 String: “TS_2DIFF”,“PLAIN”,“RLE” |
|    默认值    | PLAIN                                 |
| 改后生效方式 | 触发生效                              |

* compressor

|     名字     | compressor                                    |
| :----------: | :-------------------------------------------- |
|     描述     | 数据压缩方法                                  |
|     类型     | 枚举 String : “UNCOMPRESSED”, “SNAPPY”, “LZ4” |
|    默认值    | SNAPPY                                        |
| 改后生效方式 | 触发生效                                      |

* max\_degree\_of\_index\_node

|     名字     | max\_degree\_of\_index\_node                         |
| :----------: | :--------------------------------------------------- |
|     描述     | 元数据索引树的最大度（即每个节点的最大子节点个数）。 |
|     类型     | Int32                                                |
|    默认值    | 256                                                  |
| 改后生效方式 | 仅允许在第一次启动服务前修改                         |

* frequency\_interval\_in\_minute

|     名字     | frequency\_interval\_in\_minute          |
| :----------: | :--------------------------------------- |
|     描述     | 计算查询频率的时间间隔（以分钟为单位）。 |
|     类型     | Int32                                    |
|    默认值    | 1                                        |
| 改后生效方式 | 触发生效                                 |

* freq_snr

|     名字     | freq_snr               |
| :----------: | :--------------------- |
|     描述     | 有损的FREQ编码的信噪比 |
|     类型     | Double                 |
|    默认值    | 40.0                   |
| 改后生效方式 | 触发生效               |

* freq_block_size

|     名字     | freq_block_size                                                                           |
| :----------: | :---------------------------------------------------------------------------------------- |
|     描述     | FREQ编码的块大小，即一次时频域变换的数据点个数。为了加快编码速度，建议将其设置为2的幂次。 |
|     类型     | Int32                                                                                     |
|    默认值    | 1024                                                                                      |
| 改后生效方式 | 触发生效                                                                                  |

### 水印模块配置

* watermark\_module\_opened

|     名字     | watermark\_module\_opened |
| :----------: | :------------------------ |
|     描述     | 是否开启水印水印嵌入功能  |
|     取值     | true or false             |
|    默认值    | false                     |
| 改后生效方式 | 重启服务生效              |

* watermark\_secret\_key

|     名字     | watermark\_secret\_key |
| :----------: | :--------------------- |
|     描述     | 水印嵌入功能秘钥       |
|     类型     | String                 |
|    默认值    | IoTDB * 2019@Beijing   |
| 改后生效方式 | 重启服务生效           |

* watermark\_bit\_string

|     名字     | watermark\_bit\_string |
| :----------: | :--------------------- |
|     描述     | 水印比特字符串         |
|     类型     | Int32                  |
|    默认值    | 100101110100           |
| 改后生效方式 | 重启服务生效           |

* watermark\_method

|     名字     | watermark\_method                                      |
| :----------: | :----------------------------------------------------- |
|     描述     | 水印嵌入方法                                           |
|     类型     | String                                                 |
|    默认值    | GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5) |
| 改后生效方式 | 重启服务生效                                           |

### 授权配置

* authorizer\_provider\_class

|     名字     | authorizer\_provider\_class                             |
| :----------: | :------------------------------------------------------ |
|     描述     | 权限服务的类名                                          |
|     类型     | String                                                  |
|    默认值    | org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer |
| 改后生效方式 | 重启服务生效                                            |
|  其他可选值  | org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer    |

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


### UDF查询配置

* udf\_initial\_byte\_array\_length\_for\_memory\_control

|     名字     | udf\_initial\_byte\_array\_length\_for\_memory\_control                                 |
| :----------: | :-------------------------------------------------------------------------------------- |
|     描述     | 用于评估UDF查询中文本字段的内存使用情况。建议将此值设置为略大于所有文本的平均长度记录。 |
|     类型     | Int32                                                                                   |
|    默认值    | 48                                                                                      |
| 改后生效方式 | 重启服务生效                                                                            |

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

### 触发器配置

* concurrent_window_evaluation_thread

|     名字     | concurrent_window_evaluation_thread |
| :----------: | :---------------------------------- |
|     描述     | 窗口计算线程池的默认线程数          |
|     类型     | Int32                               |
|    默认值    | CPU核数                             |
| 改后生效方式 | 重启服务生效                        |

* max_pending_window_evaluation_tasks

|     名字     | max_pending_window_evaluation_tasks |
| :----------: | :---------------------------------- |
|     描述     | 最多允许堆积的窗口计算任务          |
|     类型     | Int32                               |
|    默认值    | 64                                  |
| 改后生效方式 | 重启服务生效                        |

### SELECT-INTO配置

* select_into_insert_tablet_plan_row_limit

|     名字     | select_into_insert_tablet_plan_row_limit                              |
| :----------: | :-------------------------------------------------------------------- |
|     描述     | 执行 select-into 语句时，一个 insert-tablet-plan 中可以处理的最大行数 |
|     类型     | Int32                                                                 |
|    默认值    | 10000                                                                 |
| 改后生效方式 | 触发生效                                                              |

### 连续查询配置

* max_pending_continuous_query_tasks

|     名字     | max_pending_continuous_query_tasks |
| :----------: | :--------------------------------- |
|     描述     | 队列中连续查询最大任务堆积数       |
|     类型     | Int32                              |
|    默认值    | 64                                 |
| 改后生效方式 | 重启服务生效                       |

* continuous_query_submit_thread_count

|     名字     | continuous_query_execution_thread |
| :----------: | :-------------------------------- |
|     描述     | 执行连续查询任务的线程池的线程数  |
|     类型     | Int32                             |
|    默认值    | 2                                 |
| 改后生效方式 | 重启服务生效                      |

* continuous\_query\_min\_every\_interval\_in\_ms

|     名字     | continuous_query_min_every_interval\_in\_ms |
| :----------: | :------------------------------------------ |
|     描述     | 连续查询执行时间间隔的最小值                |
|     类型     | long (duration)                             |
|    默认值    | 1000                                        |
| 改后生效方式 | 重启服务生效                                |

### PIPE 配置

* ip\_white\_list

|     名字     | ip\_white\_list                                                                                                    |
| :----------: | :----------------------------------------------------------------------------------------------------------------- |
|     描述     | 同步客户端的白名单。请用网段形式表示IP范围，例如：192.168.0.0/16，若有多个IP段，请用逗号隔开，默认是允许所有IP同步 |
|     类型     | String                                                                                                             |
|    默认值    | 0.0.0.0/0                                                                                                          |
| 改后生效方式 | 重启服务生效                                                                                                       |

* max_number_of_sync_file_retry

|     名字     | max_number_of_sync_file_retry |
| :----------: | :---------------------------- |
|     描述     | 同步文件最大重试次数          |
|     类型     | int                           |
|    默认值    | 5                             |
| 改后生效方式 | 重启服务生效                  |

### Ratis 共识协议配置

### Procedure 配置

* procedure_core_worker_thread_count

|     名字     | procedure_core_worker_thread_count |
| :----------: | :--------------------------------- |
|     描述     | 工作线程数量                       |
|     类型     | int                                |
|    默认值    | 4                                  |
| 改后生效方式 | 重启服务生效                       |

* procedure_completed_clean_interval

|     名字     | procedure_completed_clean_interval |
| :----------: | :--------------------------------- |
|     描述     | 清理已完成的 procedure 时间间隔    |
|     类型     | int                                |
|    默认值    | 30(s)                              |
| 改后生效方式 | 重启服务生效                       |

* procedure_completed_evict_ttl

|     名字     | procedure_completed_evict_ttl     |
| :----------: | :-------------------------------- |
|     描述     | 已完成的 procedure 的数据保留时间 |
|     类型     | int                               |
|    默认值    | 800(s)                            |
| 改后生效方式 | 重启服务生效                      |

### MQTT代理配置

* enable\_mqtt\_service

|     名字     | enable\_mqtt\_service。 |
| :----------: | :---------------------- |
|     描述     | 是否开启MQTT服务        |
|     类型     | Boolean                 |
|    默认值    | false                   |
| 改后生效方式 | 触发生效                |

* mqtt\_host

|     名字     | mqtt\_host           |
| :----------: | :------------------- |
|     描述     | MQTT服务绑定的host。 |
|     类型     | String               |
|    默认值    | 0.0.0.0              |
| 改后生效方式 | 触发生效             |

* mqtt\_port

|     名字     | mqtt\_port           |
| :----------: | :------------------- |
|     描述     | MQTT服务绑定的port。 |
|     类型     | Int32                |
|    默认值    | 1883                 |
| 改后生效方式 | 触发生效             |

* mqtt\_handler\_pool\_size

|     名字     | mqtt\_handler\_pool\_size          |
| :----------: | :--------------------------------- |
|     描述     | 用于处理MQTT消息的处理程序池大小。 |
|     类型     | Int32                              |
|    默认值    | 1                                  |
| 改后生效方式 | 触发生效                           |

* mqtt\_payload\_formatter

|     名字     | mqtt\_payload\_formatter     |
| :----------: | :--------------------------- |
|     描述     | MQTT消息有效负载格式化程序。 |
|     类型     | String                       |
|    默认值    | json                         |
| 改后生效方式 | 触发生效                     |

* mqtt\_max\_message\_size

|     名字     | mqtt\_max\_message\_size             |
| :----------: | :----------------------------------- |
|     描述     | MQTT消息的最大长度（以字节为单位）。 |
|     类型     | Int32                                |
|    默认值    | 1048576                              |
| 改后生效方式 | 触发生效                             |

### REST 服务配置

### InfluxDB 协议适配器配置

* enable_influxdb_rpc_service

|     名字     | enable_influxdb_rpc_service  |
| :----------: | :--------------------------- |
|     描述     | 是否开启InfluxDB RPC service |
|     类型     | Boolean                      |
|    默认值    | true                         |
| 改后生效方式 | 重启服务生效                 |

* influxdb_rpc_port

|     名字     | influxdb_rpc_port            |
| :----------: | :--------------------------- |
|     描述     | influxdb rpc service占用端口 |
|     类型     | int                          |
|    默认值    | 8086                         |
| 改后生效方式 | 重启服务生效                 |