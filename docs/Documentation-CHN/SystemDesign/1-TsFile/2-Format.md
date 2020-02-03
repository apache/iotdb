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
  - 1: PLAIN_DICTIONARY
  - 2: RLE
  - 3: DIFF
  - 4: TS_2DIFF
  - 5: BITMAP
  - 6: GORILLA
  - 7: REGULAR 
- **压缩类型**
  - 0: UNCOMPRESSED
  - 1: SNAPPY
- **预聚合信息**
  - 0: min_value
  - 1: max_value
  - 2: first_value
  - 3: last_value
  - 4: sum_value

### 1.2 TsFile 概述

下图是关于TsFile的结构图。

![TsFile Breakdown](https://user-images.githubusercontent.com/40447846/61616997-6fad1300-ac9c-11e9-9c17-46785ebfbc88.png)

#### 1.2.1 文件签名和版本号

TsFile 是由 6 个字节的 "Magic String" (`TsFile`) 和 6 个字节的版本号 (`000001`)组成。


#### 1.2.2 数据文件

TsFile文件的内容可以划分为两个部分: 数据和元数据。数据和元数据之间是由一个字节的 `0x02` 做为分隔符。

`ChunkGroup` 存储了一个 *设备(device)* 一段时间的数据。

##### ChunkGroup

`ChunkGroup` 由若干个 `Chunk`, 一个字节的分隔符 `0x00` 和 一个`ChunkFooter`组成。

##### Chunk

一个 `Chunk` 存储了一个 *传感器(sensor)* 的数据。`Chunk` 是由一个字节的分隔符 `0x01`, 一个 `ChunkHeader` 和若干个 `Page` 构成。

##### ChunkHeader

|             成员             |  类型  |
| :--------------------------: | :----: |
|  传感器名称(measurementID)   | String |
|     chunk大小(dataSize)      |  int   |
|  chunk的数据类型(dataType)   | short  |
|  包含的page数量(numOfPages)  |  int   |
|  压缩类型(compressionType)   | short  |
|    编码类型(encodingType)    | short  |
| Max Tombstone Time(暂时没用) |  long  |

##### Page

一个 `Page` 页存储了 `Chunk` 的一些数据。 它包含一个 `PageHeader` 和实际的数据(time-value 编码的键值对)。

PageHeader 结构

|                 成员                 |       类型       |
| :----------------------------------: | :--------------: |
|   压缩前数据大小(uncompressedSize)   |       int        |
| SNAPPY压缩后数据大小(compressedSize) |       int        |
|   包含的values的数量(numOfValues)    |       int        |
|       最大时间戳(maxTimestamp)       |       long       |
|       最小时间戳(minTimestamp)       |       long       |
|           该页最大值(max)            | Type of the page |
|           该页最小值(min)            | Type of the page |
|         该页第一个值(first)          | Type of the page |
|           该页值的和(sum)            |      double      |
|         该页最后一个值(last)         | Type of the page |

##### ChunkGroupFooter

|                成员                |  类型  |
| :--------------------------------: | :----: |
|          设备Id(deviceID)          | String |
|      ChunkGroup大小(dataSize)      |  long  |
| 包含的chunks的数量(numberOfChunks) |  int   |

#### 1.2.3  元数据

##### 1.2.3.1 TsDeviceMetaData

第一部分的元数据是 `TsDeviceMetaData` 

|                       成员                       | 类型 |
| :----------------------------------------------: | :--: |
|               开始时间(startTime)                | long |
|                结束时间(endTime)                 | long |
|              包含的ChunkGroup的数量              | int  |
| 所有的ChunkGroupMetaData(chunkGroupMetadataList) | list |

###### ChunkGroupMetaData

|                          成员                           |  类型  |
| :-----------------------------------------------------: | :----: |
|                    设备Id(deviceID)                     | String |
| 在文件中ChunkGroup开始的偏移量(startOffsetOfChunkGroup) |  long  |
|  在文件中ChunkGroup结束的偏移量(endOffsetOfChunkGroup)  |  long  |
|                      版本(version)                      |  long  |
|                包含的ChunkMetaData的数量                |  int   |
|         所有的ChunkMetaData(chunkMetaDataList)          |  list  |

###### ChunkMetaData

|                        成员                        |   类型   |
| :------------------------------------------------: | :------: |
|             传感器名称(measurementUid)             |  String  |
| 文件中ChunkHeader开始的偏移量(offsetOfChunkHeader) |   long   |
|              数据的总数(numOfPoints)               |   long   |
|                开始时间(startTime)                 |   long   |
|                 结束时间(endTime)                  |   long   |
|                数据类型(tsDataType)                |  short   |
|                  chunk的统计信息                   | TsDigest |

###### TsDigest

目前有五项统计数据: `min_value, max_value, first_value, last_value, sum_value`。

在 v0.8.0 版本中, 统计数据使用 name-value 编码的键值对。 也就是 `Map<String, ByteBuffer> statistics`。 name使用的一个字符串类型(需要注意的是字符串前有个长度标识)。 对于值来讲，它有可能是很多种类型，所以需要用 integer 类型用来描述值的长度。 比如, 如果 `min_value` 是一个 integer 类型的 0, 那么在 TsFile 中将被存储为 [9 "min_value" 4 0]。

下面是一个调用 `TsDigest.deserializeFrom(buffer)` 方法后的数据示例。在 v0.8.0 版本中, 我们会得到 

```
Map<String, ByteBuffer> statistics = {
    "min_value" -> ByteBuffer of int value 0, 
    "last" -> ByteBuffer of int value 19,
    "sum" -> ByteBuffer of double value 1093347116,
    "first" -> ByteBuffer of int value 0,
    "max_value" -> ByteBuffer of int value 99
}
```

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/63765352-664a4280-c8fb-11e9-869e-859edf6d00bb.png">

在 v0.9.0 版本中, 为了提高空间和时间的效率，存储的结构被修改为数组的形式。也就是 `ByteBuffer[] statistics`。用固定的位置代表某一个具体的统计信息, 在 StatisticType 中定义的顺序如下:

```
enum StatisticType {
    min_value, max_value, first_value, last_value, sum_value
}
```

修改存储形式后,在上面的示例中,我们将得到

```
ByteBuffer[] statistics = [
    ByteBuffer of int value 0, // associated with "min_value"
    ByteBuffer of int value 99, // associated with "max_value"
    ByteBuffer of int value 0, // associated with "first_value"
    ByteBuffer of int value 19, // associated with "last_value"
    ByteBuffer of double value 1093347116 // associated with "sum_value"
]
```

另一个关于 v0.9.0 的示例数据, 当我们从 buffer [3, 0,4,0, 1,4,99, 3,4,19] 反序列化为 TsDigest 结构时, 我们将得到 

```
//这里可能会有些难理解，读取顺序为：1.读取一个int类型的数据总数(3) 2.读取short类型的位于数组中的位置(0) 3.读取int类型的数据长度(4) 4.根据第3步的长度读取数据(0)
//因为示例数据中，索引值只出现了(0,1,3),所以 first_value sum_value 的值为null

ByteBuffer[] statistics = [
    ByteBuffer of int value 0, // associated with "min_value"
    ByteBuffer of int value 99, // associated with "max_value"
    null, // associated with "first_value"
    ByteBuffer of int value 19, // associated with "last_value"
    null // associated with "sum_value"
]
```

##### 1.2.3.2 TsFileMetaData

上节讲到的是 `TsDeviceMetadatas` 紧跟其后的数据是 `TsFileMetaData`。

|                        成员                         |                类型                |
| :-------------------------------------------------: | :--------------------------------: |
|                   包含的设备个数                    |                int                 |
|  设备名称和设备元数据索引的键值对(deviceIndexMap)   | String, TsDeviceMetadataIndex pair |
|                  包含的传感器个数                   |                int                 |
| 传感器名称和传感器元数据的键值对(measurementSchema) |   String, MeasurementSchema pair   |
|                      水印标识                       |                byte                |
|         当标识为0x01时的水印信息(createdBy)         |               String               |
|           包含的Chunk总数(totalChunkNum)            |                int                 |
|          失效的Chunk总数(invalidChunkNum)           |                int                 |
|                布隆过滤器序列化大小                 |                int                 |
|                 布隆过滤器所有数据                  |      byte[Bloom filter size]       |
|                   布隆过滤器容量                    |                int                 |
|        布隆过滤器容量包含的HashFunction数量         |                int                 |

###### TsDeviceMetadataIndex

|                  成员                  |  类型  |
| :------------------------------------: | :----: |
|                 设备名                 | String |
| 文件中TsDeviceMetaData的偏移量(offset) |  long  |
|         序列化后数据大小(len)          |  int   |
|     存储的设备最小时间(startTime)      |  long  |
|      存储的设备最大时间(endTime)       |  long  |

###### MeasurementSchema

|           成员            |        类型         |
| :-----------------------: | :-----------------: |
| 传感器名称(measurementId) |       String        |
|      数据类型(type)       |        short        |
|    编码方式(encoding)     |        short        |
|   压缩方式(compressor)    |        short        |
|      附带参数的数量       |         int         |
|   所有附带的参数(props)   | String, String pair |

如果附带的参数数量大于 0, 传感器的附带参数会以一个数组形式的 <String, String> 键值对存储。

比如说: "max_point_number""2".

##### 1.2.3.3 TsFileMetadataSize

在TsFileMetaData之后，有一个int值用来表示TsFileMetaData的大小。


#### 1.2.4 Magic String

TsFile 是以6个字节的magic string (`TsFile`) 作为结束.


恭喜您, 至此您已经完成了 TsFile 的探秘之旅，祝您玩儿的开心!

### 1.3 TsFile工具集

#### 1.3.1 IoTDB Data Directory 快速概览工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.10.0\tools\tsfileToolSet` 目录中。

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
D:\incubator-iotdb\server\target\iotdb-server-0.10.0-SNAPSHOT\tools\tsfileToolSet>.\print-iotdb-data-dir.bat D:\\data\data
​````````````````````````
Starting Printing the IoTDB Data Directory Overview
​````````````````````````
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
````````````````````````

#### 1.3.2 TsFileResource 打印工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.10.0\tools\tsfileToolSet` 目录中。

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
D:\incubator-iotdb\server\target\iotdb-server-0.10.0\tools\tsfileToolSet>.\print-tsfile-resource-files.bat D:\data\data\sequence\root.vehicle
​````````````````````````
Starting Printing the TsFileResources
​````````````````````````
12:31:59.861 [main] WARN org.apache.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-engine.properties, use default configuration
analyzing D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile ...
device root.vehicle.d0, start time 3000 (1970-01-01T08:00:03+08:00[GMT+08:00]), end time 100999 (1970-01-01T08:01:40.999+08:00[GMT+08:00])
analyzing the resource file finished.
````````````````````````

#### 1.3.3 TsFile 描述工具

该工具的启动脚本会在编译 server 之后生成至 `server\target\iotdb-server-0.10.0\tools\tsfileToolSet` 目录中。

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

在Windows系统中的示例:

```$xslt
D:\incubator-iotdb\server\target\iotdb-server-0.10.0\tools\tsfileToolSet>.\print-tsfile-sketch.bat D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile
​````````````````````````
Starting Printing the TsFile Sketch
​````````````````````````
TsFile path:D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile
Sketch save path:TsFile_sketch_view.txt
-------------------------------- TsFile Sketch --------------------------------
file path: D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile
file length: 187382

            POSITION|   CONTENT
            --------    -------
                   0|   [magic head] TsFile
                   6|   [version number] 000001
|||||||||||||||||||||   [Chunk Group] of root.vehicle.d0 begins at pos 12, ends at pos 186469, version:102, num of Chunks:6
                  12|   [Chunk] of s3, numOfPoints:10600, time range:[3000,13599], tsDataType:TEXT,
                        TsDigest:[min_value:A,max_value:E,first_value:A,last_value:E,sum_value:0.0]
                    |           [marker] 1
                    |           [ChunkHeader]
                    |           11 pages
               55718|   [Chunk] of s4, numOfPoints:10600, time range:[3000,13599], tsDataType:BOOLEAN,
                        TsDigest:[min_value:false,max_value:true,first_value:true,last_value:false,sum_value:0.0]
                    |           [marker] 1
                    |           [ChunkHeader]
                    |           11 pages
               68848|   [Chunk] of s5, numOfPoints:10600, time range:[3000,13599], tsDataType:DOUBLE,
                        TsDigest:[min_value:3000.0,max_value:13599.0,first_value:3000.0,last_value:13599.0,sum_value:8.79747E7]
                    |           [marker] 1
                    |           [ChunkHeader]
                    |           11 pages
               98474|   [Chunk] of s0, numOfPoints:21900, time range:[3000,100999], tsDataType:INT32,
                        TsDigest:[min_value:0,max_value:99,first_value:0,last_value:19,sum_value:889750.0]
                    |           [marker] 1
                    |           [ChunkHeader]
                    |           22 pages
              123369|   [Chunk] of s1, numOfPoints:21900, time range:[3000,100999], tsDataType:INT64,
                        TsDigest:[min_value:0,max_value:39,first_value:8,last_value:19,sum_value:300386.0]
                    |           [marker] 1
                    |           [ChunkHeader]
                    |           22 pages
              144741|   [Chunk] of s2, numOfPoints:21900, time range:[3000,100999], tsDataType:FLOAT,
                        TsDigest:[min_value:0.0,max_value:122.0,first_value:8.0,last_value:52.0,sum_value:778581.0]
                    |           [marker] 1
                    |           [ChunkHeader]
                    |           22 pages
              186437|   [Chunk Group Footer]
                    |           [marker] 0
                    |           [deviceID] root.vehicle.d0
                    |           [dataSize] 186425
                    |           [num of chunks] 6
|||||||||||||||||||||   [Chunk Group] of root.vehicle.d0 ends
              186469|   [marker] 2
              186470|   [TsDeviceMetadata] of root.vehicle.d0, startTime:3000, endTime:100999
                    |           [startTime] 3000tfi
                    |           [endTime] 100999
                    |           [num of ChunkGroupMetaData] 1
                    |           1 ChunkGroupMetaData
              187133|   [TsFileMetaData]
                    |           [num of devices] 1
                    |           1 key&TsDeviceMetadataIndex
                    |           [num of measurements] 6
                    |           6 key&measurementSchema
                    |           [createBy isNotNull] false
                    |           [totalChunkNum] 6
                    |           [invalidChunkNum] 0
                    |           [bloom filter bit vector byte array length] 31
                    |           [bloom filter bit vector byte array]
                    |           [bloom filter number of bits] 256
                    |           [bloom filter number of hash functions] 5
              187372|   [TsFileMetaDataSize] 239
              187376|   [magic tail] TsFile
              187382|   END of TsFile

---------------------------------- TsFile Sketch End ----------------------------------
````````````````````````

#### 1.3.4 TsFileSequenceRead

您可以使用示例中的类 `example/tsfile/org/apache/iotdb/tsfile/TsFileSequenceRead` 顺序打印 TsFile 中的内容.

### 1.4 TsFile 的总览图

#### v0.8.0

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png">

#### v0.9.0

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/69341240-26012300-0ca4-11ea-91a1-d516810cad44.png">