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

概览：
1. TsFile Design
2. TsFile Visualization Examples
3. TsFile Tool Set
    - IoTDB Data Directory Overview Tool
    - TsFileResource Print Tool
    - TsFile Sketch Tool
    - TsFileSequenceRead
    - Vis Tool

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
  - 1: PLAIN_DICTIONARY
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

下图是关于TsFile的结构图。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/95296983-492cc500-08ac-11eb-9f66-c9c78401c61d.png">

此文件包括两个设备 d1、d2，每个设备包含两个测点 s1、s2，共 4 个时间序列。每个时间序列包含两个 Chunk。

元数据分为三部分

* 按时间序列组织的 ChunkMetadata 列表
* 按时间序列组织的 TimeseriesMetadata
* TsFileMetadata

查询流程：以查 d1.s1 为例

* 反序列化 TsFileMetadata，得到 d1.s1 的 TimeseriesMetadata 的位置
* 反序列化得到 d1.s1 的 TimeseriesMetadata
* 根据 d1.s1 的 TimeseriesMetadata，反序列化其所有 ChunkMetadata
* 根据 d1.s1 的每一个 ChunkMetadata，读取其 Chunk 数据

#### 1.2.1 文件签名和版本号

TsFile文件头由 6 个字节的 "Magic String" (`TsFile`) 和 6 个字节的版本号 (`000002`)组成。

#### 1.2.2 数据文件

TsFile文件的内容可以划分为两个部分: 数据（Chunk）和元数据（XXMetadata）。数据和元数据之间是由一个字节的 `0x02` 做为分隔符。

`ChunkGroup` 存储了一个 *设备(device)* 一段时间的数据。

##### ChunkGroup

`ChunkGroup` 由若干个 `Chunk`, 一个字节的分隔符 `0x00` 和 一个`ChunkFooter`组成。

##### Chunk

一个 `Chunk` 存储了一个 *测点(measurement)* 一段时间的数据，Chunk 内数据是按时间递增序存储的。`Chunk` 是由一个字节的分隔符 `0x01`, 一个 `ChunkHeader` 和若干个 `Page` 构成。

##### ChunkHeader

|             成员             |  类型  | 解释 |
| :--------------------------: | :----: | :----: |
|  measurementID   | String | 传感器名称 |
|     dataSize      |  int   | chunk 大小 |
|  dataType   | TSDataType  | chunk的数据类型 |
|  compressionType   | CompressionType  | 压缩类型 |
|    encodingType    | TSEncoding  | 编码类型 |
|  numOfPages  |  int   | 包含的page数量 |

##### Page

一个 `Page` 页存储了 `Chunk` 的一些数据。 它包含一个 `PageHeader` 和实际的数据(time-value 编码的键值对)。

PageHeader 结构

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
 
##### ChunkGroupFooter

|                成员                |  类型  | 解释 |
| :--------------------------------: | :----: | :----: |
|         deviceID          | String | 设备名称 |
|      dataSize      |  long  | ChunkGroup 大小 |
| numberOfChunks |  int   | 包含的 chunks 的数量 |

#### 1.2.3  元数据

##### 1.2.3.1 ChunkMetadata

第一部分的元数据是 `ChunkMetadata` 

|                        成员                        |   类型   | 解释 |
| :------------------------------------------------: | :------: | :----: |
|             measurementUid             |  String  | 传感器名称 |
| offsetOfChunkHeader |   long   | 文件中 ChunkHeader 开始的偏移量 |
|                tsDataType                |  TSDataType   | 数据类型 |
|   statistics    |       Statistics        | 统计量 |

##### 1.2.3.2 TimeseriesMetadata

第二部分的元数据是 `TimeseriesMetadata`。

|                        成员                        |   类型   | 解释 |
| :------------------------------------------------: | :------: | :------: |
|             measurementUid            |  String  | 传感器名称 |
|               tsDataType                |  TSDataType   |  数据类型 |
| startOffsetOfChunkMetadataList |  long  | 文件中 ChunkMetadata 列表开始的偏移量 |
|  chunkMetaDataListDataSize  |  int  | ChunkMetadata 列表的大小 |
|   statistics    |       Statistics        | 统计量 |

##### 1.2.3.3 TsFileMetaData

第三部分的元数据是 `TsFileMetaData`。

|                        成员                        |   类型   | 解释 |
| :-------------------------------------------------: | :---------------------: | :---:|
|       MetadataIndex              |   MetadataIndexNode      |元数据索引节点 |
|           totalChunkNum            |                int                 | 包含的 Chunk 总数 |
|          invalidChunkNum           |                int                 | 失效的 Chunk 总数 |
|                versionInfo         |             List<Pair<Long, Long>>       | 版本信息映射 |
|        metaOffset   |                long                 | MetaMarker.SEPARATOR偏移量 |
|                bloomFilter                 |                BloomFilter      | 布隆过滤器 |

元数据索引节点 (MetadataIndexNode) 的成员和类型具体如下：

|                  成员                  |  类型  | 解释 |
| :------------------------------------: | :----: | :---: |
|      children    | List<MetadataIndexEntry> | 节点元数据索引项列表 |
|       endOffset      | long |    此元数据索引节点的结束偏移量 |
|   nodeType    | MetadataIndexNodeType | 节点类型 |

元数据索引项 (MetadataIndexEntry) 的成员和类型具体如下：

|                  成员                  |  类型  | 解释 |
| :------------------------------------: | :----: | :---: |
|  name    | String | 对应设备或传感器的名字 |
|     offset     | long   | 偏移量 |

所有的元数据索引节点构成一棵**元数据索引树**，这棵树最多由两个层级组成：设备索引层级和传感器索引层级，在不同的情况下会有不同的组成方式。元数据索引节点类型有四种，分别是`INTERNAL_DEVICE`、`LEAF_DEVICE`、`INTERNAL_MEASUREMENT`、`LEAF_MEASUREMENT`，分别对应设备索引层级的中间节点和叶子节点，和传感器索引层级的中间节点和叶子节点。
只有传感器索引层级的叶子节点(`LEAF_MEASUREMENT`) 指向 `TimeseriesMetadata`。

为了更清楚的说明元数据索引树的结构，这里我们使用四个例子来加以详细说明。

元数据索引树的最大度（即每个节点的最大子节点个数）是可以由用户进行配置的，配置项为`max_degree_of_index_node`，其默认值为1024。在以下例子中，为了简化，我们假定 `max_degree_of_index_node = 10`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81935219-de3fd080-9622-11ea-9aa1-a59bef1c0001.png">

在5个设备，每个设备有5个传感器的情况下，由于设备数和传感器树均不超过 `max_degree_of_index_node`，因此元数据索引树只有默认的传感器层级。在这个层级里，每个 MetadataIndexNode 最多由10个 MetadataIndexEntry 组成。根节点的 MetadataIndexNode 是 `INTERNAL_MEASUREMENT` 类型，其中的5个 MetadataIndexEntry 指向对应的设备的 MetadataIndexNode，这些节点直接指向 `TimeseriesMetadata`，是 `LEAF_MEASUREMENT`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81935210-d97b1c80-9622-11ea-8a69-2c2c5f05a876.png">

在1个设备，设备中有150个传感器的情况下，传感器个数超过了 `max_degree_of_index_node`，元数据索引树有默认的传感器层级。在这个层级里，每个 MetadataIndexNode 最多由10个 MetadataIndexEntry 组成。直接指向 `TimeseriesMetadata`的节点类型均为 `LEAF_MEASUREMENT`；而后续产生的中间节点和根节点不是传感器索引层级的叶子节点，这些节点是 `INTERNAL_MEASUREMENT`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/95592841-c0fd1a00-0a7b-11eb-9b46-dfe8b2f73bfb.png">

在150个设备，每个设备中有1个传感器的情况下，设备个数超过了 `max_degree_of_index_node`，形成元数据索引树的传感器层级和设备索引层级。在这两个层级里，每个 MetadataIndexNode 最多由10个 MetadataIndexEntry 组成。直接指向 `TimeseriesMetadata` 的节点类型为 `LEAF_MEASUREMENT`，传感器索引层级的根节点同时作为设备索引层级的叶子节点，其节点类型为 `LEAF_DEVICE`；而后续产生的中间节点和根节点不是设备索引层级的叶子节点，因此节点类型为 `INTERNAL_DEVICE`。

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81935138-b6e90380-9622-11ea-94f9-c97bd2b5d050.png">

在150个设备，每个设备中有150个传感器的情况下，传感器和设备个数均超过了 `max_degree_of_index_node`，形成元数据索引树的传感器层级和设备索引层级。在这两个层级里，每个 MetadataIndexNode 均最多由10个 MetadataIndexEntry 组成。如前所述，从根节点到设备索引层级的叶子节点，类型分别为`INTERNAL_DEVICE` 和 `LEAF_DEVICE`，而每个设备索引层级的叶子节点都是传感器索引层级的根节点，从这里到传感器索引层级的叶子节点，类型分别为`INTERNAL_MEASUREMENT` 和 `LEAF_MEASUREMENT`。

元数据索引采用树形结构进行设计的目的是在设备数或者传感器数量过大时，可以不用一次读取所有的 `TimeseriesMetadata`，只需要根据所读取的传感器定位对应的节点，从而减少 I/O，加快查询速度。有关 TsFile 的读流程将在本章最后一节加以详细说明。

##### 1.2.3.4 TsFileMetadataSize

在TsFileMetaData之后，有一个int值用来表示TsFileMetaData的大小。


#### 1.2.4 Magic String

TsFile 是以6个字节的magic string (`TsFile`) 作为结束.


恭喜您, 至此您已经完成了 TsFile 的探秘之旅，祝您玩儿的开心!

## 2. TsFile 的总览图

### v0.8

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png">

### v0.9 / 000001

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/69341240-26012300-0ca4-11ea-91a1-d516810cad44.png">

### v0.10 / 000002

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/95296983-492cc500-08ac-11eb-9f66-c9c78401c61d.png">


## 3. TsFile工具集

### 3.1 IoTDB Data Directory 快速概览工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.11.1\tools\tsfileToolSet` 目录中。

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
D:\iotdb\server\target\iotdb-server-0.11.1-SNAPSHOT\tools\tsfileToolSet>.\print-iotdb-data-dir.bat D:\\data\data
```

Starting Printing the IoTDB Data Directory Overview
```
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

### 3.2 TsFileResource 打印工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.11.1\tools\tsfileToolSet` 目录中。

使用方式:

Windows:

```
.\print-tsfile-sketch.bat <TsFileResource文件夹路径>
```

Linux or MacOs:

```
./print-tsfile-sketch.sh <TsFileResource文件夹路径>
```

在Windows系统中的示例:

```
D:\iotdb\server\target\iotdb-server-0.11.1\tools\tsfileToolSet>.\print-tsfile-resource-files.bat D:\data\data\sequence\root.vehicle
```

Starting Printing the TsFileResources
```
12:31:59.861 [main] WARN org.apache.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-engine.properties, use default configuration
analyzing D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile ...
device root.vehicle.d0, start time 3000 (1970-01-01T08:00:03+08:00[GMT+08:00]), end time 100999 (1970-01-01T08:01:40.999+08:00[GMT+08:00])
analyzing the resource file finished.
```

### 3.3 TsFile 描述工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.11.1\tools\tsfileToolSet` 目录中。

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
/iotdb/server/target/iotdb-server-0.11.1/tools/tsfileToolSet$ ./print-tsfile-sketch.sh test.tsfile
```

Starting Printing the TsFile Sketch
```
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

### 3.4 TsFileSequenceRead

您可以使用示例中的类 `example/tsfile/org/apache/iotdb/tsfile/TsFileSequenceRead` 顺序打印 TsFile 中的内容.

### 3.5 Vis Tool

Vis是一个把TsFiles中的chunk数据的时间分布以及点数可视化的工具。你可以使用这个工具来帮助你debug，还可以用来观察数据分布等等。 
欢迎使用这个工具，在社区里交流你的想法。

![image](https://user-images.githubusercontent.com/33376433/120703097-74bd8900-c4e7-11eb-8068-ff71c775e8a0.png)

- 图中的一个窄长矩形代表的是一个TsFile里的一个chunk，其可视化信息为\[tsName, fileName, versionNum, startTime, endTime, countNum\]。
- 矩形在x轴上的位置是由该chunk的startTime和endTime决定的。
- 矩形在y轴上的位置是由以下三项共同决定的：
    - (a)`showSpecific`: 用户指定要显示的特定时间序列集合。
    - (b) seqKey/unseqKey显示规则: 从满足特定时间序列集合的keys提取seqKey或unseqKey时采取不同的显示规则：
        - b-1) unseqKey识别tsName和fileName，因此有相同tsName和fileName但是不同versionNums的chunk数据将绘制在同一行；
        - b-2) seqKey识别tsName，因此有相同tsName但是不同fileNames和versionNums的chunk数据将绘制在同一行，
    - (c)`isFileOrder`：根据`isFileOrder`对seqKey&unseqKey进行排序，true则以fileName优先的顺序排序， 
      false则以tsName优先的顺序排序。当在一张图上同时显示多条时间序列时，该参数将给用户提供这两种可选的观察视角。

#### 3.5.1 如何运行Vis

源数据包含两个文件：`TsFileExtractVisdata.java`和`vis.m`。
`TsFileExtractVisdata.java`从输入的tsfile文件中提取必要的可视化信息，`vis.m`用这些信息来完成作图。

简单说就是：先运行`TsFileExtractVisdata.java`然后再运行`vis.m`。

##### 第一步：运行TsFileExtractVisdata.java

`TsFileExtractVisdata.java`对输入的tsfiles的每一个chunk提取可视化信息[tsName, fileName, versionNum, startTime, endTime, countNum]，
并把这些信息保存到指定的输出文件里。

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.11.1\tools\tsfileToolSet` 目录中。

使用方式:

Windows:

```
.\print-tsfile-visdata.bat path1 seqIndicator1 path2 seqIndicator2 ... pathN seqIndicatorN outputPath
```

Linux or MacOs:

```
./print-tsfile-visdata.sh path1 seqIndicator1 path2 seqIndicator2 ... pathN seqIndicatorN outputPath
```

参数: [`path1` `seqIndicator1` `path2` `seqIndicator2` ... `pathN` `seqIndicatorN` `outputPath`]

细节:

-   一共2N+1个参数。
-   `seqIndicator`：'true'或者'false' （大小写不敏感）. 'true'表示是顺序文件, 'false'表示是乱序文件。
-   `Path`：可以是一个tsfile的全路径，也可以是一个文件夹路径。如果是文件夹路径，你需要确保这个文件夹下的所有tsfile文件的`seqIndicator`都是一样的。

##### 第二步：运行vis.m

`vis.m`把`TsFileExtractVisdata`生成的visdata加载进来，然后基于visdata以及两个用户绘图参数`showSpecific`和`isFileOrder`来完成作图。

```matlab
function [timeMap,countMap] = loadVisData(filePath,timestampUnit)
% Load visdata generated by TsFileExtractVisdata.
% 
% filePath: the path of visdata.
% The format is [tsName,fileName,versionNum,startTime,endTime,countNum].
% `tsName` and `fileName` are string, the others are long value.
% If the tsfile is unsequence file, `fileName` will contain "unseq" as an
% indicator, which is guaranteed by TsFileExtractVisdata.
% 
% timestampUnit(not case sensitive):
%   'us' if the timestamp is microsecond, e.g., 1621993620816000
%   'ms' if it is millisecond, e.g., 1621993620816
%   's' if it is second, e.g., 1621993620
%
% timeMap: record the time range of every chunk. 
% Key [tsName][fileName][version] identifies the only chunk. Value is
% [startTime,endTime] of the chunk.
% 
% countMap: record the point count number of every chunk. Key is the same
% as that of timeMap. Value is countNum.
```

```matlab
function draw(timeMap,countMap,showSpecific,isFileOrder)
% Plot figures given the loaded data and two plot parameters:
% `showSpecific` and `isFileOrder`.
% 
% process: 1) traverse `keys(timeMap)` to get the position arrangements on 
%          the y axis dynamically, which is defined simultaneously by 
%           (a)`showSpecific`: traverse `keys(timeMap)`, filter out keys 
%          that don't statisfy `showSpecific`.
%           (b) seqKey/unseqKey display policies: extract seqKey or unseqKey
%          from statisfied keys. Sequence and unsequence data take different
%          display policies: 
%               b-1) unseqKey identifies tsName and fileName, so data with the
%               same fileName and tsName but different versionNums are
%               plotted in the same line.
%               b-2) seqKey identifies tsName, so data with the same tsName but
%               different fileNames and versionNums are ploteed in the same
%               line.
%           (c)`isFileOrder`: sort seqKey&unseqKey according to `isFileOrder`,
%          finally get the position arrangements on the y axis. 
%          2) traverse `keys(timeMap)` again, get startTime&endTime from
%          `treeMap` as positions on the x axis, combined with the
%          positions on the y axis from the last step, finish plot.
% 
% timeMap,countMap: generated by loadVisData function.
% 
% showSpecific: the specific set of time series to be plotted.
%               If showSpecific is empty{}, then all loaded time series 
%               will be plotted.
%               Note: Wildcard matching is not supported now. In other
%               words, showSpecific only support full time series path
%               names.
% 
% isFileOrder: true to sort seqKeys&unseqKeys by fileName priority, false
%              to sort seqKeys&unseqKeys by tsName priority.
```



#### 3.5.2 例子

##### 例1

使用由`IoTDBLargeDataIT.insertData`写出的tsfiles。
小修改：`IoTDBLargeDataIT.insertData`最后添加一条`statement.execute("flush");`指令。

第一步：运行`TsFileExtractVisdata.java`

```
.\print-tsfile-visdata.bat D:\rel0.11\iotdb\server\target\data\sequence true D:\rel0.11\iotdb\server\target\data\unsequence false D:\visdata1.csv
```
或者等价地：
```
.\print-tsfile-visdata.bat D:\rel0.11\iotdb\server\target\data\sequence\root.vehicle\0\1622743492580-1-0.tsfile true D:\rel0.11\iotdb\server\target\data\sequence\root.vehicle\0\1622743505092-2-0.tsfile true D:\rel0.11\iotdb\server\target\data\sequence\root.vehicle\0\1622743505573-3-0.tsfile true D:\rel0.11\iotdb\server\target\data\unsequence\root.vehicle\0\1622743505901-4-0.tsfile false D:\visdata1.csv
```

第二步：运行`vis.m`

```matlab
clear all;close all;

% 1. load visdata generated by TsFileExtractDataToVisTool
filePath = 'D:\visdata1.csv';
[timeMap,countMap] = loadVisData(filePath,'ms'); % mind the timestamp unit

% 2. plot figures given the loaded data and two plot parameters:
% `showSpecific` and `isFileOrder`
draw(timeMap,countMap,{},false)
title("draw(timeMap,countMap,\{\},false)")

draw(timeMap,countMap,{},true)
title("draw(timeMap,countMap,\{\},true)")

draw(timeMap,countMap,{'root.vehicle.d0.s0'},false)
title("draw(timeMap,countMap,{'root.vehicle.d0.s0'},false)")

draw(timeMap,countMap,{'root.vehicle.d0.s0','root.vehicle.d0.s1'},false)
title("draw(timeMap,countMap,{'root.vehicle.d0.s0','root.vehicle.d0.s1'},false)")

draw(timeMap,countMap,{'root.vehicle.d0.s0','root.vehicle.d0.s1'},true)
title("draw(timeMap,countMap,{'root.vehicle.d0.s0','root.vehicle.d0.s1'},true)")
```

绘图结果：

![image](https://user-images.githubusercontent.com/33376433/120710311-9707d480-c4f0-11eb-80c1-d535d38921a8.png)

![image](https://user-images.githubusercontent.com/33376433/120710344-9f600f80-c4f0-11eb-9399-74638e5a3e56.png)

![image](https://user-images.githubusercontent.com/33376433/120710370-a6871d80-c4f0-11eb-9706-6219c5aee6ca.png)

![image](https://user-images.githubusercontent.com/33376433/120710394-ad159500-c4f0-11eb-9882-a0e126714233.png)

![image](https://user-images.githubusercontent.com/33376433/120710429-b4d53980-c4f0-11eb-91a3-465cb69fb9f5.png)



##### 例2

作为另一个例子，这是来自真实世界的数据：

![image](https://user-images.githubusercontent.com/33376433/120809484-afbdcc00-c57c-11eb-8f3b-a2a40462dbfc.png)

![image](https://user-images.githubusercontent.com/33376433/120809494-b2b8bc80-c57c-11eb-84e4-198e84830af8.png)

![image](https://user-images.githubusercontent.com/33376433/120809499-b4828000-c57c-11eb-920d-41d99e3747fb.png)

![image](https://user-images.githubusercontent.com/33376433/120809507-b77d7080-c57c-11eb-8f3b-6643ab446dd9.png)
