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

## 时间序列数据库比较

### Overview

![TSDB Comparison](https://alioss.timecho.com/docs/img/github/119833923-182ffc00-bf32-11eb-8b3f-9f95d3729ad2.png)

**表格外观启发自 [Andriy Zabavskyy: How to Select Time Series DB](https://towardsdatascience.com/how-to-select-time-series-db-123b0eb4ab82)*

### 1. 已知的时间序列数据库

随着时间序列数据变得越来越重要，一些开源的时间序列数据库（Time Series Databases，or TSDB）诞生了。

但是，它们中很少有专门为物联网（IoT）或者工业物联网（Industrial IoT，缩写 IIoT）场景开发的。

本文把 IoTDB 和下述三种类型的时间序列数据库进行了比较：

-   InfluxDB - 原生时间序列数据库

    InfluxDB 是最流行的时间序列数据库之一。

    接口：InfluxQL and HTTP API

-   OpenTSDB 和 KairosDB - 基于 NoSQL 的时间序列数据库

    这两种数据库是相似的，但是 OpenTSDB 基于 HBase 而 KairosDB 基于 Cassandra。

    它们两个都提供 RESTful 风格的 API。

    接口：Restful API

-   TimescaleDB - 基于关系型数据库的时间序列数据库

    接口：SQL

Prometheus 和 Druid 也因为时间序列数据管理而闻名，但是 Prometheus 聚焦在数据采集、可视化和报警，Druid 聚焦在 OLAP 负载的数据分析，因此本文省略了 Prometheus 和 Druid。

### 2. 比较

本文将从以下两个角度比较时间序列数据库：功能比较、性能比较。

#### 2.1 功能比较

以下两节分别是时间序列数据库的基础功能比较（2.1.1）和高级功能比较（2.1.2）。

表格中符号的含义：

-   `++`：强大支持
-   `+`：支持
-   `+-`：支持但欠佳
-   `-`：不支持
-   `?`：未知

##### 2.1.1 基础功能

| TSDB                          |          IoTDB          |  InfluxDB  |  OpenTSDB  |  KairosDB  | TimescaleDB |
| ----------------------------- | :---------------------: | :--------: | :--------: | :--------: | :---------: |
| *OpenSource*                  |          **+**          |     +      |     +      |   **+**    |      +      |
| *SQL\-like*                   |            +            |     +      |     -      |     -      |   **++**    |
| *Schema*                      | Tree\-based, tag\-based | tag\-based | tag\-based | tag\-based | Relational  |
| *Writing out\-of\-order data* |            +            |     +      |     +      |     +      |      +      |
| *Schema\-less*                |            +            |     +      |     +      |     +      |      +      |
| *Batch insertion*             |            +            |     +      |     +      |     +      |      +      |
| *Time range filter*           |            +            |     +      |     +      |     +      |      +      |
| *Order by time*               |         **++**          |     +      |     -      |     -      |      +      |
| *Value filter*                |            +            |     +      |     -      |     -      |      +      |
| *Downsampling*                |         **++**          |     +      |     +      |     +      |      +      |
| *Fill*                        |         **++**          |     +      |     +      |     -      |      +      |
| *LIMIT*                       |            +            |     +      |     +      |     +      |      +      |
| *SLIMIT*                      |            +            |     +      |     -      |     -      |      ?      |
| *Latest value*                |           ++            |     +      |     +      |     -      |      +      |

具体地：

-   *OpenSource*：

    -   IoTDB 使用 Apache License 2.0。
    -   InfluxDB 使用 MIT license。但是，**它的集群版本没有开源**。
    -   OpenTSDB 使用 LGPL2.1，**和 Apache License 不兼容**。
    -   KairosDB 使用 Apache License 2.0。
    -   TimescaleDB 使用 Timescale License，对企业来说不是免费的。

-   *SQL-like*：

    -   IoTDB 和 InfluxDB 支持 SQL-like 语言。
    -   OpenTSDB 和 KairosDB 只支持 Rest API。IoTDB 也支持 Rest API。
    -   TimescaleDB 使用的是和 PostgreSQL 一样的 SQL。

-   *Schema*：

    -   IoTDB：IoTDB 提出了一种 [基于树的 schema](http://iotdb.apache.org/zh/UserGuide/Master/Data-Concept/Data-Model-and-Terminology.html)。这和其它时间序列数据库很不一样。这种 schema 有以下优点：
        -   在许多工业场景里，设备管理是有层次的，而不是扁平的。因此我们认为基于树的 schema 比基于 tag-value 的 schema 更好。
        -   在许多现实应用中，tag 的名字是不变的。例如：风力发电机制造商总是用风机所在的国家、所属的风场以及在风场中的 ID 来标识一个风机，因此，一个 4 层高的树（“root.the-country-name.the-farm-name.the-id”）来表示就足矣。你不需要重复告诉 IoTDB”树的第二层是国家名”、“树的第三层是风场名“等等这种信息。
        -   这样的基于路径的时间序列 ID 定义还能够支持灵活的查询，例如：”root.\*.a.b.\*“，其中、*是一个通配符。
    -   InfluxDB, KairosDB, OpenTSDB：使用基于 tag-value 的 schema。现在比较流行这种 schema。
    -   TimescaleDB 使用关系表。

-   *Order by time*：

    对于时间序列数据库来说，Order by time 好像是一个琐碎的功能。但是当我们考虑另一个叫做”align by time“的功能时，事情就变得有趣起来。这就是为什么我们把 OpenTSDB 和 KairosDB 标记为”不支持“。事实上，所有时间序列数据库都支持单条时间序列的按时间戳排序。但是，OpenTSDB 和 KairosDB 不支持多条时间序列的按时间戳排序。

    下面考虑一个新的例子：这里有两条时间序列，一条是风场 1 中的风速，一条是风场 1 中的风机 1 产生的电能。如果我们想要研究风速和产生电能之间的关系，我们首先需要知道二者在相同时间戳下的值。也就是说，我们需要按照时间戳对齐这两条时间序列。因此，结果应该是：

    | 时间戳 | 风场 1 中的风速 | 风场 1 中的风机 1 产生的电能 |
    | ------ | ------------- | ------------------------ |
    | 1      | 5.0           | 13.1                     |
    | 2      | 6.0           | 13.3                     |
    | 3      | null          | 13.1                     |

    或者：

    | 时间戳 | 时间序列名               | 值   |
    | ------ | ------------------------ | ---- |
    | 1      | 风场 1 中的风速            | 5.0  |
    | 1      | 风场 1 中的风机 1 产生的电能 | 13.1 |
    | 2      | 风场 1 中的风速            | 6.0  |
    | 2      | 风场 1 中的风机 1 产生的电能 | 13.3 |
    | 3      | 风场 1 中的风机 1 产生的电能 | 13.1 |

    虽然第二个表格没有按照时间戳对齐两条时间序列，但是只需要逐行扫描数据就可以很容易地在客户端实现这个功能。

    IoTDB 支持第一种表格格式（叫做 align by time），InfluxDB 支持第二种表格格式。

-   *Downsampling*：

    Downsampling（降采样）用于改变时间序列的粒度，例如：从 10Hz 到 1Hz，或者每天 1 个点。

    和其他数据库不同的是，IoTDB 能够实时降采样数据，而其它时间序列数据库在磁盘上序列化降采样数据。

    也就是说：

    -   IoTDB 支持在任意时间对数据进行即席（ad-hoc）降采样。例如：一条 SQL 返回从 2020-04-27 08:00:00 开始的每 5 分钟采样 1 个点的降采样数据，另一条 SQL 返回从 2020-04-27 08:00:01 开始的每 5 分 10 秒采样 1 个点的降采样数据。

        （InfluxDB 也支持即席降采样，但是性能似乎并不好。）

    -   IoTDB 的降采样不占用磁盘。

-   *Fill*：

    有时候我们认为数据是按照某种固定的频率采集的，比如 1Hz（即每秒 1 个点）。但是通常我们会丢失一些数据点，可能由于网络不稳定、机器繁忙、机器宕机等等。在这些场景下，填充这些数据空洞是重要的。数据科学家可以因此避免很多所谓的”dirty work“比如数据清洗。

    InfluxDB 和 OpenTSDB 只支持在 group by 语句里使用 fill，而 IoTDB 能支持给定一个特定的时间戳的 fill。此外，IoTDB 还支持多种填充策略。

-   *Slimit*：

    Slimit 是指返回指定数量的 measurements（或者，InfluxDB 中的 fields）。

    例如：一个风机有 1000 个测点（风速、电压等等），使用 slimit 和 soffset 可以只返回其中的一部分测点。

-   *Latest value*：

    最基础的时间序列应用之一是监视最新数据。因此，返回一条时间序列的最新点是非常重要的查询功能。

    IoTDB 和 OpenTSDB 使用一个特殊的 SQL 或 API 来支持这个功能，而 InfluxDB 使用聚合函数来支持。

    IoTDB 提供一个特殊的 SQL 的原因是 IoTDB 专门优化了查询。

**结论：**

通过对基础功能的比较，我们可以发现：

-   OpenTSDB 和 KairosDB 缺少一些重要的查询功能。
-   TimescaleDB 不能被企业免费使用。
-   IoTDB 和 InfluxDB 可以满足时间序列数据管理的大部分需求，同时它俩之间有一些不同之处。

##### 2.1.2 高级功能

| TSDB                         | IoTDB  | InfluxDB | OpenTSDB | KairosDB | TimescaleDB |
| ---------------------------- | :----: | :------: | :------: | :------: |:-----------:|
| *Align by time*              | **++** |    +     |    -     |    -     |      +      |
| *Compression*                | **++** |    +-    |    +-    |    +-    |     +-      |
| *MQTT support*               | **++** |    +     |    -     |    -     |     +-      |
| *Run on Edge-side Device*    | **++** |    +     |    -     |    +-    |      +      |
| *Multi\-instance Sync*       | **++** |    -     |    -     |    -     |      -      |
| *JDBC Driver*                | **+**  |    -     |    -     |    -     |     ++      |
| *Standard SQL*               |   +    |    -     |    -     |    -     |   **++**    |
| *Spark integration*          | **++** |    -     |    -     |    -     |      -      |
| *Hive integration*           | **++** |    -     |    -     |    -     |      -      |
| *Writing data to NFS (HDFS)* | **++** |    -     |    +     |    -     |      -      |
| *Flink integration*          | **++** |    -     |    -     |    -     |      -      |

具体地：

-   *Align by time*：上文已经介绍过，这里不再赘述。

-   *Compression*：

    -   IoTDB 支持许多时间序列编码和压缩方法，比如 RLE, 2DIFF, Gorilla 等等，以及 Snappy 压缩。在 IoTDB 里，你可以根据数据分布选择你想要的编码方法。更多信息参考 [这里](http://iotdb.apache.org/UserGuide/Master/Data-Concept/Encoding.html)。
    -   InfluxDB 也支持编码和压缩，但是你不能定义你想要的编码方法，编码只取决于数据类型。更多信息参考 [这里](https://docs.influxdata.com/influxdb/v1.7/concepts/storage_engine/)。
    -   OpenTSDB 和 KairosDB 在后端使用 HBase 和 Cassandra，并且没有针对时间序列的特殊编码。

-   *MQTT protocol support*：

    MQTT protocol 是一个被工业用户广泛知晓的国际标准。只有 IoTDB 和 InfluxDB 支持用户使用 MQTT 客户端来写数据。

-   *Running on Edge-side Device*：

    现在，边缘计算变得越来越重要，边缘设备有越来越强大的计算资源。

    在边缘侧部署时间序列数据库，对于管理边缘侧数据、服务于边缘计算来说，是有用的。

    由于 OpenTSDB 和 KairosDB 依赖另外的数据库，它们的体系结构是臃肿的。特别是很难在边缘侧运行 Hadoop。

-   *Multi-instance Sync*：

    现在假设我们在边缘侧有许多时间序列数据库实例，考虑如何把它们的数据上传到数据中心去形成一个数据湖。

    一个解决方法是从这些实例读取数据，然后逐点写入到数据中心。

    IoTDB 提供了另一个选项：把数据文件增量上传到数据中心，然后数据中心可以支持在数据上的服务。

-   *JDBC driver*：

    现在只有 IoTDB 支持了 JDBC driver（虽然不是所有接口都实现），这使得 IoTDB 可以整合许多其它的基于 JDBC driver 的软件。


-   *Spark and Hive integration*：

    让大数据分析软件访问数据库中的数据来完成复杂数据分析是非常重要的。

    IoTDB 支持 Hive-connector 和 Spark-connector 来完成更好的整合。

-   *Writing data to NFS (HDFS)*：

    Sharing nothing 的体系结构是好的，但是有时候你不得不增加新的服务器，即便你的 CPU 和内存都是空闲的而磁盘已经满了。

    此外，如果我们能直接把数据文件存储到 HDFS 中，用 Spark 和其它软件来分析数据将会更加简单，不需要 ETL。

    -   IoTDB 支持往本地或者 HDFS 写数据。IoTDB 还允许用户扩展实现在其它 NFS 上存储数据。
    -   InfluxDB 和 KairosDB 只能往本地写数据。
    -   OpenTSDB 只能往 HDFS 写数据。

**结论：**

IoTDB 拥有许多其它时间序列数据库不支持的强大功能。

#### 2.2 性能比较

如果你觉得：”如果我只需要基础功能的话，IoTDB 好像和其它的时间序列数据库没有什么不同。“

这好像是有道理的。但是如果考虑性能的话，你也许会改变你的想法。

##### 2.2.1 快速浏览

| TSDB                 | IoTDB | InfluxDB | KairosDB | TimescaleDB |
| -------------------- | :---: | :------: | :------: | :---------: |
| *Scalable Writes*    |  ++   |    +     |    +     |      +      |
| *Raw Data Query*     |  ++   |    +     |    +     |      +      |
| *Aggregation Query*  |  ++   |    +     |    +     |      +      |
| *Downsampling Query* |  ++   |    +     |    +-    |     +-      |
| *Latest Query*       |  ++   |    +     |    +-    |      +      |

###### 写入性能

我们从两个方面来测试写性能：batch size 和 client num。存储组的数量是 10。有 1000 个设备，每个设备有 100 个传感器，也就是说一共有 100K 条时间序列。

测试使用的 IoTDB 版本是`v0.11.1`。

* 改变 batch size

10 个客户端并发地写数据。IoTDB 使用 batch insertion API，batch size 从 1ms 到 1min 变化（每次调用 write API 写 N 个数据点）。

写入吞吐率（points/second）如下图所示：

<img src="https://alioss.timecho.com/docs/img/github/106254214-6cacbe80-6253-11eb-8532-d6a1829f8f66.png" alt="Batch Size with Write Throughput (points/second)"  />

<center>Figure 1. Batch Size with Write throughput (points/second) IoTDB v0.11.1</center>

* 改变 client num

client num 从 1 到 50 变化。IoTDB 使用 batch insertion API，batch size 是 100（每次调用 write API 写 100 个数据点）。

写入吞吐率（points/second）如下图所示：

![Client Num with Write Throughput (points/second) (ms)](https://alioss.timecho.com/docs/img/github/106251411-e5aa1700-624f-11eb-8ca8-00c0627b1e96.png)

<center>Figure 3. Client Num with Write Throughput (points/second) IoTDB v0.11.1</center>

###### 查询性能

10 个客户端并发地读数据。存储组的数量是 10。有 10 个设备，每个设备有 10 个传感器，也就是说一共有 100 条时间序列。

数据类型是* double*，编码类型是* GORILLA*。

测试使用的 IoTDB 版本是`v0.11.1`。

测试结果如下图所示：

![Raw data query 1 col](https://alioss.timecho.com/docs/img/github/106251377-daef8200-624f-11eb-9678-b1d5440be2de.png)

<center>Figure 4. Raw data query 1 col time cost(ms) IoTDB v0.11.1</center>

![Aggregation query](https://alioss.timecho.com/docs/img/github/106251336-cf03c000-624f-11eb-8395-de5e349f47b5.png)

<center>Figure 5. Aggregation query time cost(ms) IoTDB v0.11.1</center>

![Downsampling query](https://alioss.timecho.com/docs/img/github/106251353-d32fdd80-624f-11eb-80c1-fdb4197939fe.png)

<center>Figure 6. Downsampling query time cost(ms) IoTDB v0.11.1</center>

![Latest query](https://alioss.timecho.com/docs/img/github/106251369-d7f49180-624f-11eb-9d19-fc7341582b90.png)

<center>Figure 7. Latest query time cost(ms) IoTDB v0.11.1</center>

可以看到，IoTDB 的 raw data query、aggregation query、downsampling query、latest query 查询性能表现都超越了其它数据库。

##### 2.2.2 更多细节

我们提供了一个 benchmark 工具，叫做 [IoTDB-benchamrk](https://github.com/thulab/iotdb-benchmark)（你可以用 dev branch 来编译它）。它支持 IoTDB, InfluxDB, KairosDB, TimescaleDB, OpenTSDB。

我们有一篇文章关于用这个 benchmark 工具比较这些时间序列数据库：[Benchmarking Time Series Databases with IoTDB-Benchmark for IoT Scenarios](https://arxiv.org/abs/1901.08304)。我们发表这个文章的时候，IoTDB 才刚刚加入 Apache incubator，所以我们在那篇文章里删去了 IoTDB 的性能测试。但是在比较之后，一些结果展示在这里：

-   对于 InfluxDB，我们把 cache-max-memory-size 和 max-series-perbase 设置成 unlimited（否则它很快就会超时）。
-   对于 KairosDB，我们把 Cassandra 的 read_repair_chance 设置为 0.1（但是这没有什么影响，因为我们只有一个结点）。
-   对于 TimescaleDB，我们用 PGTune 工具来优化 PostgreSQL。

所有的时间序列数据库运行的机器配置是：Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz, (8 cores 16 threads), 32GB memory, 256G SSD and 10T HDD, OS: Ubuntu 16.04.7 LTS, 64bits.

所有的客户端运行的机器配置是：Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz，(6 cores 12 threads), 16GB memory, 256G SSD, OS: Ubuntu 16.04.7 LTS, 64bits.

### 3. 结论

从以上所有实验中，我们可以看到 IoTDB 的性能大大优于其他数据库。

IoTDB 具有最小的写入延迟。批处理大小越大，IoTDB 的写入吞吐量就越高。这表明 IoTDB 最适合批处理数据写入方案。

在高并发方案中，IoTDB 也可以保持吞吐量的稳定增长。 （每秒 1200 万个点可能已达到千兆网卡的限制）

在原始数据查询中，随着查询范围的扩大，IoTDB 的优势开始显现。因为数据块的粒度更大，列式存储的优势体现出来，所以基于列的压缩和列迭代器都将加速查询。

在聚合查询中，我们使用文件层的统计信息并缓存统计信息。因此，多个查询仅需要执行内存计算（不需要遍历原始数据点，也不需要访问磁盘），因此聚合性能优势显而易见。

降采样查询场景更加有趣，因为时间分区越来越大，IoTDB 的查询性能逐渐提高。它可能上升了两倍，这对应于 2 个粒度（3 小时和 4.5 天）的预先计算的信息。因此，分别加快了 1 天和 1 周范围内的查询。其他数据库仅上升一次，表明它们只有一个粒度统计。

如果您正在为您的 IIoT 应用程序考虑使用 TSDB，那么新的时间序列数据库 Apache IoTDB 是您的最佳选择。

发布新版本并完成实验后，我们将更新此页面。

我们也欢迎更多的贡献者更正本文，为 IoTDB 做出贡献或复现实验。
