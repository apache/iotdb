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

# 层次结构

## 1. TsFile设计

  这是对TsFile设计细节的介绍。

### 1.1 可变存储

- **大端**
  ​     
  - 例如, `int` `0x8`将被存储为`00 00 00 08`，而不是`08 00 00 00`。
- **可变长度的字符串**
  - 格式为`int size`加`Sring literal`。 大小可以为零。
  - 大小等于此字符串将占用的字节数，并且可能不等于该字符串的长度。
  - 例如，“ sensor_1”将被存储为`00 00 00 08`加上“ sensor_1”的编码（ASCII）。
  - 请注意，对于“魔术字符串”（文件签名）“ TsFilev0.8.0”，大小（12）和编码（ASCII）是固定的，因此无需在该字符串文字前放置大小。
- **数据类型硬编码**
  - 0: BOOLEAN
  - 1: INT32 (`int`)
  - 2: INT64 (`long`)
  - 3: FLOAT
  - 4: DOUBLE
  - 5: TEXT (`String`)
- **编码类型硬编码**
  - 0: PLAIN
  - 1: PLAIN_DICTIONARY
  - 2: RLE
  - 3: DIFF
  - 4: TS_2DIFF
  - 5: BITMAP
  - 6: GORILLA
  - 7: REGULAR 
- **压缩类型硬编码**
  - 0: UNCOMPRESSED
  - 1: SNAPPY
  - 2: GZIP
  - 3: LZO
  - 4: SDT
  - 5: PAA
  - 6: PLA
- **TsDigest统计类型硬编码**
  - 0: min_value
  - 1: max_value
  - 2: first_value
  - 3: last_value
  - 4: sum_value

### 1.2 TsFile概述

这是有关TsFile结构的图。

![TsFile Breakdown](https://user-images.githubusercontent.com/40447846/61616997-6fad1300-ac9c-11e9-9c17-46785ebfbc88.png)

#### 1.2.1 魔术字符串和版本号

TsFile以6字节的魔术字符串（`TsFile`）和6字节的版本号（`000001`）开头。


#### 1.2.2 数据

TsFile文件的内容可以分为两部分：数据和元数据。 数据和元数据之间有一个字节`0x02`作为标记。

数据部分是一个`ChunkGroup`数组，每个ChunkGroup代表一个* device *。

##### 块组

`ChunkGroup`具有一个`Chunk`数组，一个后续字节`0x00`作为标记以及一个`ChunkFooter`。

##### 块

`块`代表*传感器*。 在`ChunkHeader`和`Page`数组之后，有一个字节`0x01`作为标记。

##### 块头

|Member Description|Member Type|
|:---:|:---:|
|该传感器的名称（measurementID）|String|
|这个块的大小|int|
|该卡盘的数据类型|short|
|页数|int|
|压缩类型|short|
|编码方式|short|
|最大墓碑时间|long|

##### 页

页面代表块中的一些数据。 它包含一个PageHeader和实际数据（编码的时间值对）。

PageHeader结构

|Member Description|Member Type|
|:---:|:---:|
|压缩前的数据大小|int|
|压缩后的数据大小（如果使用SNAPPY）|int|
|值数|int|
|最大时间戳|long|
|最小时间戳|long|
|页面最大值|Type of the page|
|页面最小值|Type of the page|
|页面的第一个值|Type of the page|
|页面总和|double|
|页面的最后一个值|Type of the page|

##### ChunkGroupFooter

|Member Description|Member Type|
|:---:|:---:|
|设备id|String|
|ChunkGroup的数据大小|long|
|块数|int|

#### 1.2.3  元数据

##### 1.2.3.1 TsDeviceMetaData

元数据的第一部分是`TsDeviceMetaData`

|Member Description|Member Type|
|:---:|:---:|
|开始时间|long|
|时间结束|long|
|组块数|int|
|ChunkGroup元数据列表|list|

###### ChunkGroupMetaData

|Member Description|Member Type|
|:---:|:---:|
|设备编号|String|
|ChunkGroup的起始偏移量|long|
|块组的结束偏移|long|
|版|long|
|ChunkMetaData的数量|int|
|ChunkMetaData列表|list|

###### ChunkMetaData

|Member Description|Member Type|
|:---:|:---:|
|MeasurementId|String|
|ChunkHeader的起始偏移量|long|
|数据点数|long|
|开始时间|long|
|时间结束|long|
|数据类型|short|
|该块的统计信息|TsDigest|

###### TsDigest

现在有五个统计信息： `min_value, max_value, first_value, last_value, sum_value`.

在v0.8.x中，统计信息的存储格式为`名称/值`对。 也就是说，`Map <String，ByteBuffer>statistics`。 名称是一个字符串（记住长度在文字之前）。 但是对于该值，还有一个整数byteLength用作后续值的自我描述长度，因为该值可能是各种类型。 例如，如果`min_value`是整数0，则它将作为[9“ min_value” 4 0]存储在TsFile中。

下图显示了`TsDigest.deserializeFrom（buffer）`的示例。 在v0.8.0中，我们将获得

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

在v0.9.x中，为了节省空间和时间，将存储格式更改为数组。 即，ByteBuffer []统计信息。 遵循`StatisticType`中定义的顺序，数组的每个位置与特定类型的统计信息都有固定的关联：

```
enum StatisticType {
    min_value, max_value, first_value, last_value, sum_value
}
```

因此，在上面的示例中，我们将获得

```
ByteBuffer[] statistics = [
    ByteBuffer of int value 0, // associated with "min_value"
    ByteBuffer of int value 99, // associated with "max_value"
    ByteBuffer of int value 0, // associated with "first_value"
    ByteBuffer of int value 19, // associated with "last_value"
    ByteBuffer of double value 1093347116 // associated with "sum_value"
]
```

作为v0.9.x中的另一个示例，当从缓冲区[3，0,4,0，1,4,99，3,4,19]反序列化TsDigest时，我们得到

```
ByteBuffer[] statistics = [
    ByteBuffer of int value 0, // associated with "min_value"
    ByteBuffer of int value 99, // associated with "max_value"
    null, // associated with "first_value"
    ByteBuffer of int value 19, // associated with "last_value"
    null // associated with "sum_value"
]
```

##### 1.2.3.2 TsFileMetaData

在`TsDeviceMetadatas`之后是`TsFileMetaData`。

|Member Description|Member Type|
|:---:|:---:|
|Number of devices|int|
|Pairs of device name and deviceMetadataIndex|String, TsDeviceMetadataIndex pair|
|Number of measurements|int|
|Pairs of measurement name and schema|String, MeasurementSchema pair|
|Author byte|byte|
|Author(if author byte is 0x01)|String|
|totalChunkNum|int|
|invalidChunkNum|int|
|Bloom filter size|int|
|Bloom filter bit vector|byte[Bloom filter size]|
|Bloom filter capacity|int|
|Bloom filter hash functions size|int|

###### TsDeviceMetadataIndex

|Member Description|Member Type|
|:---:|:---:|
|DeviceId|String|
|Start offset of TsDeviceMetaData|long|
|length|int|
|Start time|long|
|End time|long|

###### MeasurementSchema
|Member Description|Member Type|
|:---:|:---:|
|MeasurementId|String|
|Data type|short|
|Encoding|short|
|Compressor|short|
|Size of props|int|

如果道具的大小大于0，则存在一个<String，String>对数组，作为此度量的属性。
​    例如“ max_point_number”“ 2”。

##### 1.2.3.3 TsFileMetadataSize

在`TsFileMetaData`之后，有一个整数，指示`TsFileMetaData`的大小。


#### 1.2.4 魔术字符串

Ts文件以6字节的魔术字符串结尾（至文件）。 恭喜你！ 您已经完成了发现TsFile的旅程。

### 1.3 Tool Set

#### 1.3.1 TsFileResource打印工具

构建服务器后，此工具的启动脚本将出现在`server \ target \ iotdb-server-0.9.1 \ tools`目录下。

命令:

对于Windows：

```
.\print-tsfile-sketch.bat <path of your TsFileResource Directory>
```

对于Linux或Mac OS：

```
./print-tsfile-sketch.sh <path of your TsFileResource Directory>
```

Windows上的示例：

```
D:\incubator-iotdb\server\target\iotdb-server-0.9.1\tools>.\print-tsfile-resource-files.bat D:\data\data\sequence\root.vehicle
​````````````````````````
Starting Printing the TsFileResources
​````````````````````````
12:31:59.861 [main] WARN org.apache.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-engine.properties, use default configuration
analyzing D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile ...
device root.vehicle.d0, start time 3000 (1970-01-01T08:00:03+08:00[GMT+08:00]), end time 100999 (1970-01-01T08:01:40.999+08:00[GMT+08:00])
analyzing the resource file finished.
```

#### 1.3.2 TsFile素描工具

构建服务器后，此工具的启动脚本将出现在` server \ target \ iotdb-server-0.9.1 \ tools`目录下。

命令：

对于Windows：

```
.\print-tsfile-sketch.bat <path of your TsFile> (<path of the file for saving the output result>) 
```

- 请注意，如果未设置 `<path of the file for saving the output result>`，则将使用默认路径"TsFile_sketch_view.txt"。

对于Linux或Mac OS：

```
./print-tsfile-sketch.sh <path of your TsFile> (<path of the file for saving the output result>) 
```

- 请注意，如果未设置 `<path of the file for saving the output result>` , 则将使用默认路径"TsFile_sketch_view.txt"。

Windows上的示例：

```$xslt
D:\incubator-iotdb\server\target\iotdb-server-0.9.1\tools>.\print-tsfile-sketch.bat D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile
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
```

#### 1.3.3 TsFileSequenceRead
您也可以使用`example / tsfile / org / apache / iotdb / tsfile / TsFileSequenceRead`顺序打印TsFile的内容。

### 1.4 TsFile可视化示例

#### v0.8.x
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png">

#### v0.9.x
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/68128717-35b60300-ff53-11e9-919e-48d80536df88.png">
