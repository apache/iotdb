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

# TsFile 文件格式


## 1. TsFile 设计

  本章是关于 TsFile 的设计细节。

### 1.1 变量的存储

- **大端存储**
  - 比如： `int` `0x8` 将会被存储为 `00 00 00 08`, 而不是 `08 00 00 00`
- **可变长的字符串类型**
  - 存储的方式是以一个 `int` 类型的 `Size` + 字符串组成。`Size` 的值可以为 0。
  - `Size` 指的是字符串所占的字节数，它并不一定等于字符串的长度。 
  - 举例来说，"sensor_1" 这个字符串将被存储为 `00 00 00 08` + "sensor_1" (ASCII编码)。
  - 另外需要注意的一点是文件签名 "TsFile000001" (`Magic String` + `Version`), 因为他的 `Size(12)` 和 ASCII 编码值是固定的，所以没有必要在这个字符串前的写入 `Size` 值。
- **数据类型**
  - 0: BOOLEAN
  - 1: INT32 (`int`)
  - 2: INT64 (`long`)
  - 3: FLOAT
  - 4: DOUBLE
  - 5: TEXT (`String`)
- **编码类型**
  - 0: PLAIN
  - 1: DICTIONARY
  - 2: RLE
  - 3: DIFF
  - 4: TS_2DIFF
  - 5: BITMAP
  - 6: GORILLA_V1
  - 7: REGULAR 
  - 8: GORILLA 
- **压缩类型**
  - 0: UNCOMPRESSED
  - 1: SNAPPY
  - 7: LZ4
- **预聚合信息**
  - 0: min_value
  - 1: max_value
  - 2: first_value
  - 3: last_value
  - 4: sum_value

### 1.2 TsFile 概述

<!-- TODO

下图是关于TsFile的结构图。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/123052025-f47aab80-d434-11eb-94c2-9b75429e5c54.png">

此文件包括两个设备 d1、d2，每个设备包含两个测点 s1、s2，共 4 个时间序列。每个时间序列包含两个 Chunk。

-->

TsFile 整体分为两大部分：**数据区**和**索引区**。

**数据区**所包含的概念由小到大有如下三个：

* **Page数据页**：一段时间序列，是数据块被反序列化的最小单元；

* **Chunk数据块**：包含一条时间序列的多个 Page ，是数据块被IO读取的最小单元；

* **ChunkGroup数据块组**：包含一个实体的多个 Chunk。

**索引区**分为三部分：

* 按时间序列组织的 **TimeseriesIndex**，包含1个头信息和数据块索引（ChunkIndex）列表。头信息记录文件内某条时间序列的数据类型、统计信息（最大最小时间戳等）；数据块索引列表记录该序列各Chunk在文件中的 offset，并记录相关统计信息（最大最小时间戳等）；

* **IndexOfTimeseriesIndex**，用于索引各TimeseriesIndex在文件中的 offset；

* **BloomFilter**，针对实体（Entity）的布隆过滤器。

> 注：ChunkIndex 旧称 ChunkMetadata；TimeseriesIndex 旧称 TimeseriesMetadata；IndexOfTimeseriesIndex 旧称 TsFileMetadata。v0.13版本起，根据其索引区的特性和实际所索引的内容重新命名。

下图是关于 TsFile 的结构图。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/123542462-6710c180-d77c-11eb-9afb-a1b495c82ea9.png">

此文件包括两个实体 d1、d2，每个实体分别包含三个物理量 s1、s2、s3，共 6 个时间序列。每个时间序列包含两个 Chunk。

TsFile 的查询流程，以查 d1.s1 为例：

* 反序列化 IndexOfTimeseriesIndex，得到 d1.s1 的 TimeseriesIndex 的位置
* 反序列化得到 d1.s1 的 TimeseriesIndex
* 根据 d1.s1 的 TimeseriesIndex，反序列化其所有 ChunkIndex
* 根据 d1.s1 的每一个 ChunkIndex，读取其 Chunk 数据

#### 1.2.1 文件签名和版本号

TsFile文件头由 6 个字节的 "Magic String" (`TsFile`) 和 6 个字节的版本号 (`000002`)组成。

#### 1.2.2 数据区

##### ChunkGroup 数据块组

`ChunkGroup` 存储了一个实体(Entity) 一段时间的数据。由若干个 `Chunk`, 一个字节的分隔符 `0x00` 和 一个`ChunkFooter`组成。

##### Chunk 数据块

一个 `Chunk` 存储了一个物理量(Measurement) 一段时间的数据，Chunk 内数据是按时间递增序存储的。`Chunk` 是由一个字节的分隔符 `0x01`, 一个 `ChunkHeader` 和若干个 `Page` 构成。

##### ChunkHeader 数据块头

|             成员             |  类型  | 解释 |
| :--------------------------: | :----: | :----: |
|  measurementID   | String | 传感器名称 |
|     dataSize      |  int   | chunk 大小 |
|  dataType   | TSDataType  | chunk的数据类型 |
|  compressionType   | CompressionType  | 压缩类型 |
|    encodingType    | TSEncoding  | 编码类型 |
|  numOfPages  |  int   | 包含的page数量 |

##### Page 数据页

一个 `Page` 页存储了一段时间序列，是数据块被反序列化的最小单元。 它包含一个 `PageHeader` 和实际的数据(time-value 编码的键值对)。

PageHeader 结构：

|                 成员                 |       类型       | 解释 |
| :----------------------------------: | :--------------: | :----: |
|   uncompressedSize   |       int        | 压缩前数据大小 |
| compressedSize |       int        | SNAPPY压缩后数据大小 |
|   statistics    |       Statistics        | 统计量 |

这里是`statistics`的详细信息：

|             成员               | 描述 | DoubleStatistics | FloatStatistics | IntegerStatistics | LongStatistics | BinaryStatistics | BooleanStatistics |
| :----------------------------------: | :--------------: | :----: | :----: | :----: | :----: | :----: | :----: |
| count  | 数据点个数 | long | long | long | long | long | long |
| startTime | 开始时间 | long | long | long | long | long | long |
| endTime | 结束时间 | long | long | long | long | long | long |
| minValue | 最小值 | double | float | int | long | - | - |
| maxValue | 最大值 | double | float | int | long | - | - |
| firstValue | 第一个值 | double | float | int | long | Binary | boolean|
| lastValue | 最后一个值 | double | float | int | long | Binary | boolean|
| sumValue | 和 | double | double | double | double | - | - |
| extreme | 极值 | double | float | int | long | - | - |

##### ChunkGroupFooter 数据块组结尾

|                成员                |  类型  | 解释 |
| :--------------------------------: | :----: | :----: |
| entityID  | String | 实体名称 |
|      dataSize      |  long  | ChunkGroup 大小 |
| numberOfChunks |  int   | 包含的 chunks 的数量 |

#### 1.2.3  索引区

##### 1.2.3.1 ChunkIndex 数据块索引

第一部分的索引是 `ChunkIndex` ：

|                        成员                        |   类型   | 解释 |
| :------------------------------------------------: | :------: | :----: |
|             measurementUid             |  String  | 传感器名称 |
| offsetOfChunkHeader |   long   | 文件中 ChunkHeader 开始的偏移量 |
|                tsDataType                |  TSDataType   | 数据类型 |
|   statistics    |       Statistics        | 统计量 |

##### 1.2.3.2 TimeseriesIndex 时间序列索引

第二部分的索引是 `TimeseriesIndex`：

|                        成员                        |   类型   | 解释 |
| :------------------------------------------------: | :------: | :------: |
|             measurementUid            |  String  | 物理量名称 |
|               tsDataType                |  TSDataType   |  数据类型 |
| startOffsetOfChunkIndexList |  long  | 文件中 ChunkIndex 列表开始的偏移量 |
|  ChunkIndexListDataSize  |  int  | ChunkIndex 列表的大小 |
|   statistics    |       Statistics        | 统计量 |

##### 1.2.3.3 IndexOfTimeseriesIndex 时间序列索引的索引（二级索引）

第三部分的索引是 `IndexOfTimeseriesIndex`：

|                        成员                        |   类型   | 解释 |
| :-------------------------------------------------: | :---------------------: | :---:|
| IndexTree     |   IndexNode      |索引节点 |
| offsetOfIndexArea   |                long                 | 索引区的偏移量 |
|                bloomFilter                 |                BloomFilter      | 布隆过滤器 |

索引节点 (IndexNode) 的成员和类型具体如下：

|                  成员                  |  类型  | 解释 |
| :------------------------------------: | :----: | :---: |
|      children    | List<IndexEntry> | 节点索引项列表 |
|       endOffset      | long |    此索引节点的结束偏移量 |
|   nodeType    | IndexNodeType | 节点类型 |

索引项 (MetadataIndexEntry) 的成员和类型具体如下：

|                  成员                  |  类型  | 解释 |
| :------------------------------------: | :----: | :---: |
|  name    | String | 对应实体或物理量的名字 |
|     offset     | long   | 偏移量 |

所有的索引节点构成一棵类B+树结构的**索引树（二级索引）**，这棵树由两部分组成：实体索引部分和物理量索引部分。索引节点类型有四种，分别是`INTERNAL_ENTITY`、`LEAF_ENTITY`、`INTERNAL_MEASUREMENT`、`LEAF_MEASUREMENT`，分别对应实体索引部分的中间节点和叶子节点，和物理量索引部分的中间节点和叶子节点。 只有物理量索引部分的叶子节点(`LEAF_MEASUREMENT`) 指向 `TimeseriesIndex`。

下面，我们使用四个例子来加以详细说明。

索引树节点的度（即每个节点的最大子节点个数）可以由用户进行配置，配置项为`max_degree_of_index_node`，其默认值为256。在以下例子中，我们假定 `max_degree_of_index_node = 10`。

* 例1：5个实体，每个实体有5个物理量

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/122677230-134e2780-d214-11eb-9603-ac7b95bc0668.png">

在5个实体，每个实体有5个物理量的情况下，由于实体数和物理量数均不超过 `max_degree_of_index_node`，因此索引树只有默认的物理量部分。在这部分中，每个 IndexNode 最多由10个 IndexEntry 组成。根节点的 IndexNode 是 `INTERNAL_MEASUREMENT` 类型，其中的5个 IndexEntry 指向对应的实体的 IndexNode，这些节点直接指向 `TimeseriesIndex`，是 `LEAF_MEASUREMENT`。

* 例2：1个实体，150个物理量

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/122677233-15b08180-d214-11eb-8d09-c741cca59262.png">

在1个实体，实体中有150个物理量的情况下，物理量个数超过了 `max_degree_of_index_node`，索引树有默认的物理量层级。在这个层级里，每个 IndexNode 最多由10个 IndexEntry 组成。直接指向 `TimeseriesIndex`的节点类型均为 `LEAF_MEASUREMENT`；而后续产生的中间节点和根节点不是物理量索引层级的叶子节点，这些节点是 `INTERNAL_MEASUREMENT`。

* 例3：150个实体，每个实体有1个物理量

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/122771008-9a64d380-d2d8-11eb-9044-5ac794dd38f7.png">

在150个实体，每个实体中有1个物理量的情况下，实体个数超过了 `max_degree_of_index_node`，形成索引树的物理量层级和实体索引层级。在这两个层级里，每个 IndexNode 最多由10个 IndexEntry 组成。直接指向 `TimeseriesIndex` 的节点类型为 `LEAF_MEASUREMENT`，物理量索引层级的根节点同时作为实体索引层级的叶子节点，其节点类型为 `LEAF_ENTITY`；而后续产生的中间节点和根节点不是实体索引层级的叶子节点，因此节点类型为 `INTERNAL_ENTITY`。

* 例4：150个实体，每个实体有150个物理量

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/122677241-1a753580-d214-11eb-817f-17bcf797251f.png">

在150个实体，每个实体中有150个物理量的情况下，物理量和实体个数均超过了 `max_degree_of_index_node`，形成索引树的物理量层级和实体索引层级。在这两个层级里，每个 IndexNode 均最多由10个 IndexEntry 组成。如前所述，从根节点到实体索引层级的叶子节点，类型分别为`INTERNAL_ENTITY` 和 `LEAF_ENTITY`，而每个实体索引层级的叶子节点都是物理量索引层级的根节点，从这里到物理量索引层级的叶子节点，类型分别为`INTERNAL_MEASUREMENT` 和 `LEAF_MEASUREMENT`。

索引采用树形结构进行设计的目的是在实体数或者物理量数量过大时，可以不用一次读取所有的 `TimeseriesIndex`，只需要根据所读取的物理量定位对应的节点，从而减少 I/O，加快查询速度。有关 TsFile 的读流程将在本章最后一节加以详细说明。


#### 1.2.4 Magic String

TsFile 是以6个字节的magic string (`TsFile`) 作为结束。


恭喜您, 至此您已经完成了 TsFile 的探秘之旅，祝您玩儿的开心!

### 1.3 TsFile工具集

#### 1.3.1 IoTDB Data Directory 快速概览工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.13.0-SNAPSHOT\tools\tsfileToolSet` 目录中。

使用方式:

For Windows:

```
.\print-iotdb-data-dir.bat <IoTDB数据文件夹路径，如果是多个文件夹用逗号分隔> (<输出结果的存储路径>) 
```

For Linux or MacOs:

```
./print-iotdb-data-dir.sh <IoTDB数据文件夹路径，如果是多个文件夹用逗号分隔> (<输出结果的存储路径>) 
```

在Windows系统中的示例:

```
D:\iotdb\server\target\iotdb-server-0.13.0-SNAPSHOT\tools\tsfileToolSet>.\print-iotdb-data-dir.bat D:\\data\data
｜````````````````````````
Starting Printing the IoTDB Data Directory Overview
​｜````````````````````````
output save path:IoTDB_data_dir_overview.txt
TsFile data dir num:1
21:17:38.841 [main] WARN org.apache.iotdb.tsfile.common.conf.TSFileDescriptor - Failed to find config file iotdb-engine.properties at classpath, use default configuration
|==============================================================
|D:\\data\data
|--sequence
|  |--root.ln.wf01.wt01
|  |  |--1575813520203-101-0.tsfile
|  |  |--1575813520203-101-0.tsfile.resource
|  |  |  |--device root.ln.wf01.wt01, start time 1 (1970-01-01T08:00:00.001+08:00[GMT+08:00]), end time 5 (1970-01-01T08:00:00.005+08:00[GMT+08:00])
|  |  |--1575813520669-103-0.tsfile
|  |  |--1575813520669-103-0.tsfile.resource
|  |  |  |--device root.ln.wf01.wt01, start time 100 (1970-01-01T08:00:00.100+08:00[GMT+08:00]), end time 300 (1970-01-01T08:00:00.300+08:00[GMT+08:00])
|  |  |--1575813521372-107-0.tsfile
|  |  |--1575813521372-107-0.tsfile.resource
|  |  |  |--device root.ln.wf01.wt01, start time 500 (1970-01-01T08:00:00.500+08:00[GMT+08:00]), end time 540 (1970-01-01T08:00:00.540+08:00[GMT+08:00])
|--unsequence
|  |--root.ln.wf01.wt01
|  |  |--1575813521063-105-0.tsfile
|  |  |--1575813521063-105-0.tsfile.resource
|  |  |  |--device root.ln.wf01.wt01, start time 10 (1970-01-01T08:00:00.010+08:00[GMT+08:00]), end time 50 (1970-01-01T08:00:00.050+08:00[GMT+08:00])
|==============================================================
```

#### 1.3.2 TsFileResource 打印工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.13.0-SNAPSHOT\tools\tsfileToolSet` 目录中。

使用方式:

Windows:

```
.\print-tsfile-resource-files.bat <TsFileResource文件夹路径>
```

Linux or MacOs:

```
./print-tsfile-resource-files.sh <TsFileResource文件夹路径>
```

在Windows系统中的示例:

```
D:\iotdb\server\target\iotdb-server-0.13.0-SNAPSHOT\tools\tsfileToolSet>.\print-tsfile-resource-files.bat D:\data\data\sequence\root.vehicle
｜````````````````````````
Starting Printing the TsFileResources
​｜````````````````````````
12:31:59.861 [main] WARN org.apache.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-engine.properties, use default configuration
analyzing D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile ...
device root.vehicle.d0, start time 3000 (1970-01-01T08:00:03+08:00[GMT+08:00]), end time 100999 (1970-01-01T08:01:40.999+08:00[GMT+08:00])
analyzing the resource file finished.
```

#### 1.3.3 TsFile 描述工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.13.0-SNAPSHOT\tools\tsfileToolSet` 目录中。

使用方式:

Windows:

```
.\print-tsfile-sketch.bat <TsFile文件路径> (<输出结果的存储路径>) 
```

- 注意: 如果没有设置输出文件的存储路径, 将使用 "TsFile_sketch_view.txt" 做为默认值。

Linux or MacOs:

```
./print-tsfile-sketch.sh <TsFile文件路径> (<输出结果的存储路径>) 
```

- 注意: 如果没有设置输出文件的存储路径, 将使用 "TsFile_sketch_view.txt" 做为默认值。 

在mac系统中的示例:

```
/iotdb/server/target/iotdb-server-0.13.0-SNAPSHOT/tools/tsfileToolSet$ ./print-tsfile-sketch.sh test.tsfile
｜````````````````````````
Starting Printing the TsFile Sketch
｜​````````````````````````
TsFile path:test.tsfile
Sketch save path:TsFile_sketch_view.txt
-------------------------------- TsFile Sketch --------------------------------
file path: test.tsfile
file length: 33436

            POSITION| CONTENT
            --------  -------
                   0| [magic head] TsFile
                   6| [version number] 000002
||||||||||||||||||||| [Chunk Group] of root.group_12.d2, num of Chunks:3
                  12| [Chunk] of s_INT64e_RLE, numOfPoints:10000, time range:[1,10000], tsDataType:INT64, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   2 pages
                 677| [Chunk] of s_INT64e_TS_2DIFF, numOfPoints:10000, time range:[1,10000], tsDataType:INT64, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
                1349| [Chunk] of s_INT64e_PLAIN, numOfPoints:10000, time range:[1,10000], tsDataType:INT64, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   2 pages
                5766| [Chunk Group Footer]
                    |   [marker] 0
                    |   [deviceID] root.group_12.d2
                    |   [dataSize] 5754
                    |   [num of chunks] 3
||||||||||||||||||||| [Chunk Group] of root.group_12.d2 ends
                5799| [Version Info]
                    |   [marker] 3
                    |   [version] 102
||||||||||||||||||||| [Chunk Group] of root.group_12.d1, num of Chunks:3
                5808| [Chunk] of s_INT32e_PLAIN, numOfPoints:10000, time range:[1,10000], tsDataType:INT32, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
                8231| [Chunk] of s_INT32e_TS_2DIFF, numOfPoints:10000, time range:[1,10000], tsDataType:INT32, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
                8852| [Chunk] of s_INT32e_RLE, numOfPoints:10000, time range:[1,10000], tsDataType:INT32, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
                9399| [Chunk Group Footer]
                    |   [marker] 0
                    |   [deviceID] root.group_12.d1
                    |   [dataSize] 3591
                    |   [num of chunks] 3
||||||||||||||||||||| [Chunk Group] of root.group_12.d1 ends
                9432| [Version Info]
                    |   [marker] 3
                    |   [version] 102
||||||||||||||||||||| [Chunk Group] of root.group_12.d0, num of Chunks:2
                9441| [Chunk] of s_BOOLEANe_RLE, numOfPoints:10000, time range:[1,10000], tsDataType:BOOLEAN, 
                      startTime: 1 endTime: 10000 count: 10000 [firstValue:true,lastValue:true]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
                9968| [Chunk] of s_BOOLEANe_PLAIN, numOfPoints:10000, time range:[1,10000], tsDataType:BOOLEAN, 
                      startTime: 1 endTime: 10000 count: 10000 [firstValue:true,lastValue:true]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
               10961| [Chunk Group Footer]
                    |   [marker] 0
                    |   [deviceID] root.group_12.d0
                    |   [dataSize] 1520
                    |   [num of chunks] 2
||||||||||||||||||||| [Chunk Group] of root.group_12.d0 ends
               10994| [Version Info]
                    |   [marker] 3
                    |   [version] 102
||||||||||||||||||||| [Chunk Group] of root.group_12.d5, num of Chunks:1
               11003| [Chunk] of s_TEXTe_PLAIN, numOfPoints:10000, time range:[1,10000], tsDataType:TEXT, 
                      startTime: 1 endTime: 10000 count: 10000 [firstValue:version_test,lastValue:version_test]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   3 pages
               19278| [Chunk Group Footer]
                    |   [marker] 0
                    |   [deviceID] root.group_12.d5
                    |   [dataSize] 8275
                    |   [num of chunks] 1
||||||||||||||||||||| [Chunk Group] of root.group_12.d5 ends
               19311| [Version Info]
                    |   [marker] 3
                    |   [version] 102
||||||||||||||||||||| [Chunk Group] of root.group_12.d4, num of Chunks:4
               19320| [Chunk] of s_DOUBLEe_PLAIN, numOfPoints:10000, time range:[1,10000], tsDataType:DOUBLE, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00000000123]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   2 pages
               23740| [Chunk] of s_DOUBLEe_TS_2DIFF, numOfPoints:10000, time range:[1,10000], tsDataType:DOUBLE, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.000000002045]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
               24414| [Chunk] of s_DOUBLEe_GORILLA, numOfPoints:10000, time range:[1,10000], tsDataType:DOUBLE, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.000000002045]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
               25054| [Chunk] of s_DOUBLEe_RLE, numOfPoints:10000, time range:[1,10000], tsDataType:DOUBLE, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.000000001224]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   2 pages
               25717| [Chunk Group Footer]
                    |   [marker] 0
                    |   [deviceID] root.group_12.d4
                    |   [dataSize] 6397
                    |   [num of chunks] 4
||||||||||||||||||||| [Chunk Group] of root.group_12.d4 ends
               25750| [Version Info]
                    |   [marker] 3
                    |   [version] 102
||||||||||||||||||||| [Chunk Group] of root.group_12.d3, num of Chunks:4
               25759| [Chunk] of s_FLOATe_GORILLA, numOfPoints:10000, time range:[1,10000], tsDataType:FLOAT, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
               26375| [Chunk] of s_FLOATe_PLAIN, numOfPoints:10000, time range:[1,10000], tsDataType:FLOAT, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
               28796| [Chunk] of s_FLOATe_RLE, numOfPoints:10000, time range:[1,10000], tsDataType:FLOAT, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
               29343| [Chunk] of s_FLOATe_TS_2DIFF, numOfPoints:10000, time range:[1,10000], tsDataType:FLOAT, 
                      startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]
                    |   [marker] 1
                    |   [ChunkHeader]
                    |   1 pages
               29967| [Chunk Group Footer]
                    |   [marker] 0
                    |   [deviceID] root.group_12.d3
                    |   [dataSize] 4208
                    |   [num of chunks] 4
||||||||||||||||||||| [Chunk Group] of root.group_12.d3 ends
               30000| [Version Info]
                    |   [marker] 3
                    |   [version] 102
               30009| [marker] 2
               30010| [ChunkMetadataList] of root.group_12.d0.s_BOOLEANe_PLAIN, tsDataType:BOOLEAN
                    | [startTime: 1 endTime: 10000 count: 10000 [firstValue:true,lastValue:true]] 
               30066| [ChunkMetadataList] of root.group_12.d0.s_BOOLEANe_RLE, tsDataType:BOOLEAN
                    | [startTime: 1 endTime: 10000 count: 10000 [firstValue:true,lastValue:true]] 
               30120| [ChunkMetadataList] of root.group_12.d1.s_INT32e_PLAIN, tsDataType:INT32
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]] 
               30196| [ChunkMetadataList] of root.group_12.d1.s_INT32e_RLE, tsDataType:INT32
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]] 
               30270| [ChunkMetadataList] of root.group_12.d1.s_INT32e_TS_2DIFF, tsDataType:INT32
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]] 
               30349| [ChunkMetadataList] of root.group_12.d2.s_INT64e_PLAIN, tsDataType:INT64
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]] 
               30441| [ChunkMetadataList] of root.group_12.d2.s_INT64e_RLE, tsDataType:INT64
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]] 
               30531| [ChunkMetadataList] of root.group_12.d2.s_INT64e_TS_2DIFF, tsDataType:INT64
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1,maxValue:1,firstValue:1,lastValue:1,sumValue:10000.0]] 
               30626| [ChunkMetadataList] of root.group_12.d3.s_FLOATe_GORILLA, tsDataType:FLOAT
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]] 
               30704| [ChunkMetadataList] of root.group_12.d3.s_FLOATe_PLAIN, tsDataType:FLOAT
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]] 
               30780| [ChunkMetadataList] of root.group_12.d3.s_FLOATe_RLE, tsDataType:FLOAT
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]] 
               30854| [ChunkMetadataList] of root.group_12.d3.s_FLOATe_TS_2DIFF, tsDataType:FLOAT
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00023841858]] 
               30933| [ChunkMetadataList] of root.group_12.d4.s_DOUBLEe_GORILLA, tsDataType:DOUBLE
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.000000002045]] 
               31028| [ChunkMetadataList] of root.group_12.d4.s_DOUBLEe_PLAIN, tsDataType:DOUBLE
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.00000000123]] 
               31121| [ChunkMetadataList] of root.group_12.d4.s_DOUBLEe_RLE, tsDataType:DOUBLE
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.000000001224]] 
               31212| [ChunkMetadataList] of root.group_12.d4.s_DOUBLEe_TS_2DIFF, tsDataType:DOUBLE
                    | [startTime: 1 endTime: 10000 count: 10000 [minValue:1.1,maxValue:1.1,firstValue:1.1,lastValue:1.1,sumValue:11000.000000002045]] 
               31308| [ChunkMetadataList] of root.group_12.d5.s_TEXTe_PLAIN, tsDataType:TEXT
                    | [startTime: 1 endTime: 10000 count: 10000 [firstValue:version_test,lastValue:version_test]] 
               32840| [MetadataIndex] of root.group_12.d0
               32881| [MetadataIndex] of root.group_12.d1
               32920| [MetadataIndex] of root.group_12.d2
               32959| [MetadataIndex] of root.group_12.d3
               33000| [MetadataIndex] of root.group_12.d4
               33042| [MetadataIndex] of root.group_12.d5
               33080| [TsFileMetadata]
                    |   [num of devices] 6
                    |   6 key&TsMetadataIndex
                    |   [totalChunkNum] 17
                    |   [invalidChunkNum] 0
                    |   [bloom filter bit vector byte array length] 32
                    |   [bloom filter bit vector byte array] 
                    |   [bloom filter number of bits] 256
                    |   [bloom filter number of hash functions] 5
               33426| [TsFileMetadataSize] 346
               33430| [magic tail] TsFile
               33436| END of TsFile

---------------------------------- TsFile Sketch End ----------------------------------
```

#### 1.3.4 TsFileSequenceRead

您可以使用示例中的类 `example/tsfile/org/apache/iotdb/tsfile/TsFileSequenceRead` 顺序打印 TsFile 中的内容.

### 1.4 TsFile 的总览图

#### v0.8

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png">

#### v0.9 / 000001

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/69341240-26012300-0ca4-11ea-91a1-d516810cad44.png">

#### v0.10 / 000002

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/95296983-492cc500-08ac-11eb-9f66-c9c78401c61d.png">
