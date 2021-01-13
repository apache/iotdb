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

# TsFile Format

## 1. TsFile Design

  This is an introduction to the design details of TsFile.

### 1.1 Variable Storage

- **Big Endian**
       
  - For Example, the `int` `0x8` will be stored as `00 00 00 08`, not `08 00 00 00`
- **String with Variable Length**
  - The format is `int size` plus `String literal`. Size can be zero.
  - Size equals the number of bytes this string will take, and it may not equal to the length of the string. 
  - For example "sensor_1" will be stored as `00 00 00 08` plus the encoding(ASCII) of "sensor_1".
  - Note that for the file signature "TsFile000001" (`MAGIC STRING` + `Version Number`), the size(12) and encoding(ASCII)
    is fixed so there is no need to put the size before this string literal.
- **Data Type Hardcode**
  - 0: BOOLEAN
  - 1: INT32 (`int`)
  - 2: INT64 (`long`)
  - 3: FLOAT
  - 4: DOUBLE
  - 5: TEXT (`String`)
- **Encoding Type Hardcode**
  - 0: PLAIN
  - 1: PLAIN_DICTIONARY
  - 2: RLE
  - 3: DIFF
  - 4: TS_2DIFF
  - 5: BITMAP
  - 6: GORILLA_V1
  - 7: REGULAR 
  - 8: GORILLA
- **Compressing Type Hardcode**
  - 0: UNCOMPRESSED
  - 1: SNAPPY
  - 7: LZ4
- **TsDigest Statistics Type Hardcode**
  - 0: min_value
  - 1: max_value
  - 2: first_value
  - 3: last_value
  - 4: sum_value

### 1.2 TsFile Overview

Here is a graph about the TsFile structure.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/95296983-492cc500-08ac-11eb-9f66-c9c78401c61d.png">

This TsFile contains two devices: d1, d2. Each device contains two measurements: s1, s2. 4 timeseries in total. Each timeseries contains 2 Chunks.

Here is another graph:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/98808354-ed2f0080-2456-11eb-8e7f-b11a4759d560.png">

This TsFile contains two devices: d1, d2. Each device contains three measurements: s1, s2, s3. 6 timeseries in total. Each timeseries contains 2 Chunks.

There are three parts of metadata

* ChunkMetadata list that grouped by timeseries
* TimeseriesMetadata that ordered by timeseries
* TsFileMetadata

Query Process：e.g., read d1.s1

* deserialize TsFileMetadata，get the position of TimeseriesMetadata of d1.s1
* deserialize and get the TimeseriesMetadata of d1.s1
* according to TimeseriesMetadata of d1.s1，deserialize all ChunkMetadata of d1.s1 
* according to each ChunkMetadata of d1.s1，read its Chunk

#### 1.2.1 Magic String and Version Number

A TsFile begins with a 6-byte magic string (`TsFile`) and a 6-byte version number (`000002`).

#### 1.2.2 Data

The content of a TsFile file can be divided as two parts: data (Chunk) and metadata (XXMetadata). There is a byte `0x02` as the marker between
data and metadata.

The data section is an array of `ChunkGroup`, each ChunkGroup represents a *device*.

##### ChunkGroup

The `ChunkGroup` has an array of `Chunk`, a following byte `0x00` as the marker, and a `ChunkFooter`.

##### Chunk

A `Chunk` represents the data of a *measurement* in a time range, data points in Chunks are in time ascending order. There is a byte `0x01` as the marker, following a `ChunkHeader` and an array of `Page`.

##### ChunkHeader
|             Member             |  Type  | Description |
| :--------------------------: | :----: | :----: |
|  measurementID   | String | Name of measurement |
|     dataSize      |  int   | Size of this chunk |
|  dataType   | TSDataType  | Data type of this chuck |
|  compressionType   | CompressionType  | Compression Type |
|    encodingType    | TSEncoding  | Encoding Type |
|  numOfPages  |  int   |  Number of pages |

##### Page

A `Page` represents some data in a `Chunk`. It contains a `PageHeader` and the actual data (The encoded time-value pair).

PageHeader Structure

|             Member             |  Type  | Description |
| :----------------------------------: | :--------------: | :----: |
|   uncompressedSize   |       int        | Data size before compressing |
| compressedSize |       int        | Data size after compressing(if use SNAPPY) |
|   statistics    |       Statistics        | Statistics values |

Here is the detailed information for `statistics`:

 |             Member               | Description | DoubleStatistics | FloatStatistics | IntegerStatistics | LongStatistics | BinaryStatistics | BooleanStatistics |
 | :----------------------------------: | :--------------: | :----: | :----: | :----: | :----: | :----: | :----: |
 | count  | number of time-value points | long | long | long | long | long | long | 
 | startTime | start time | long | long | long | long | long | long | 
 | endTime | end time | long | long | long | long | long | long | 
 | minValue | min value | double | float | int | long | - | - |
 | maxValue | max value | double | float | int | long | - | - |
 | firstValue | first value | double | float | int | long | Binary | boolean|
 | lastValue | last value | double | float | int | long | Binary | boolean|
 | sumValue | sum value | double | double | double | double | - | - |
 
##### ChunkGroupFooter

|             Member             |  Type  | Description |
| :--------------------------------: | :----: | :----: |
|         deviceID          | String | Name of device |
|      dataSize      |  long  | Data size of the ChunkGroup |
| numberOfChunks |  int   | Number of chunks |

#### 1.2.3  Metadata

##### 1.2.3.1 ChunkMetadata

The first part of metadata is `ChunkMetadata` 

|             Member             |  Type  | Description |
| :------------------------------------------------: | :------: | :----: |
|             measurementUid             |  String  | Name of measurement |
| offsetOfChunkHeader |   long   | Start offset of ChunkHeader  |
|                tsDataType                |  TSDataType   | Data type |
|   statistics    |       Statistics        | Statistic values |

##### 1.2.3.2 TimeseriesMetadata

The second part of metadata is `TimeseriesMetadata`.

|             Member             |  Type  | Description |
| :------------------------------------------------: | :------: | :------: |
|             measurementUid            |  String  | Name of measurement |
|               tsDataType                |  short   |  Data type |
| startOffsetOfChunkMetadataList |  long  | Start offset of ChunkMetadata list |
|  chunkMetaDataListDataSize  |  int  | ChunkMetadata list size |
|   statistics    |       Statistics        | Statistic values |

##### 1.2.3.3 TsFileMetaData

The third part of metadata is `TsFileMetaData`.

|             Member             |  Type  | Description |
| :-------------------------------------------------: | :---------------------: | :---: |
|       MetadataIndex              |   MetadataIndexNode      | MetadataIndex node |
|           totalChunkNum            |                int                 | total chunk num |
|          invalidChunkNum           |                int                 | invalid chunk num |
|                versionInfo         |             List<Pair<Long, Long>>       | version information |
|        metaOffset   |                long                 | offset of MetaMarker.SEPARATOR |
|                bloomFilter                 |                BloomFilter      | bloom filter |

MetadataIndexNode has members as below:

|             Member             |  Type  | Description |
| :------------------------------------: | :----: | :---: |
|      children    | List<MetadataIndexEntry> | MetadataIndexEntry list |
|       endOffset      | long |    EndOffset of this MetadataIndexNode |
|   nodeType    | MetadataIndexNodeType | MetadataIndexNode type |

MetadataIndexEntry has members as below:

|             Member             |  Type  | Description |
| :------------------------------------: | :----: | :---: |
|  name    | String | Name of related device or measurement |
|     offset     | long   | offset |

All MetadataIndexNode forms a **metadata index tree**, which consists of no more than two levels: device index level and measurement index level. In different situation, the tree could have different components. The MetadataIndexNodeType has four enums: `INTERNAL_DEVICE`, `LEAF_DEVICE`, `INTERNAL_MEASUREMENT`, `LEAF_MEASUREMENT`, which indicates the internal or leaf node of device index level and measurement index level respectively. Only the `LEAF_MEASUREMENT` nodes point to `TimeseriesMetadata`.

To describe the structure of metadata index tree more clearly, we will give four examples here in details.

The max degree of the metadata index tree (that is, the max number of each node's children) could be configured by users, and is 1024 by default. In the examples below, we assume `max_degree_of_index_node = 10` in the following examples.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81935219-de3fd080-9622-11ea-9aa1-a59bef1c0001.png">

5 devices with 5 measurements each: Since the numbers of devices and measurements are both no more than `max_degree_of_index_node`, the tree has only measurement index level by default. In this level, each MetadataIndexNode is composed of no more than 10 MetadataIndex entries. The root nonde is `INTERNAL_MEASUREMENT` type, and the 5 MetadataIndex entries point to MetadataIndex nodes of related devices. These nodes point to  `TimeseriesMetadata` directly, as they are `LEAF_MEASUREMENT` type.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81935210-d97b1c80-9622-11ea-8a69-2c2c5f05a876.png">

1 device with 150 measurements: The number of measurements exceeds `max_degree_of_index_node`, so the tree has only measurement index level by default. In this level, each MetadataIndexNode is composed of no more than 10 MetadataIndex entries. The nodes that point to `TimeseriesMetadata` directly are `LEAF_MEASUREMENT` type. Other nodes and root node of index tree are not leaf nodes of measurement index level, so they are `INTERNAL_MEASUREMENT` type.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/95592841-c0fd1a00-0a7b-11eb-9b46-dfe8b2f73bfb.png">

150 device with 1 measurement each: The number of devices exceeds `max_degree_of_index_node`, so the device index level and measurement index level of the tree are both formed. In these two levels, each MetadataIndexNode is composed of no more than 10 MetadataIndex entries. The nodes that point to `TimeseriesMetadata` directly are `LEAF_MEASUREMENT` type. The root nodes of measurement index level are also the leaf nodes of device index level, which are `LEAF_DEVICE` type. Other nodes and root node of index tree are not leaf nodes of device index level, so they are `INTERNAL_DEVICE` type.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/81935138-b6e90380-9622-11ea-94f9-c97bd2b5d050.png">

150 device with 150 measurements each: The numbers of devices and measurements both exceed `max_degree_of_index_node`, so the device index level and measurement index level are both formed. In these two levels, each MetadataIndexNode is composed of no more than 10 MetadataIndex entries. As is described before, from the root node to the leaf nodes of device index level, their types are `INTERNAL_DEVICE` and `LEAF_DEVICE`; each leaf node of device index level can be seen as the root node of measurement index level, and from here to the leaf nodes of measurement index level, their types are `INTERNAL_MEASUREMENT` and `LEAF_MEASUREMENT`.

The MetadataIndex is designed as tree structure so that not all the `TimeseriesMetadata` need to be read when the number of devices or measurements is too large. Only reading specific MetadataIndex nodes according to requirement and reducing I/O could speed up the query. More reading process of TsFile in details will be described in the last section of this chapter.

##### 1.2.3.4 TsFileMetadataSize

After the TsFileMetaData, there is an int indicating the size of the TsFileMetaData.


#### 1.2.4 Magic String

A TsFile ends with a 6-byte magic string (`TsFile`).


Congratulations! You have finished the journey of discovering TsFile.

### 1.3 TsFile Tool Set

#### 1.3.1 IoTDB Data Directory Overview Tool

After building the server, the startup script of this tool will appear under the `server\target\iotdb-server-0.10.0\tools\tsfileToolSet` directory.

Command:

For Windows:

```
.\print-iotdb-data-dir.bat <path of your IoTDB data directory or directories separated by comma> (<path of the file for saving the output result>) 
```

For Linux or MacOs:

```
./print-iotdb-data-dir.sh <path of your IoTDB data directory or directories separated by comma> (<path of the file for saving the output result>) 
```

An example on Windows:

```
D:\iotdb\server\target\iotdb-server-0.12.0-SNAPSHOT\tools\tsfileToolSet>.\print-iotdb-data-dir.bat D:\\data\data
|````````````````````````
Starting Printing the IoTDB Data Directory Overview
​|````````````````````````
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



#### 1.3.2 TsFileResource Print Tool

After building the server, the startup script of this tool will appear under the `server\target\iotdb-server-0.10.0\tools\tsfileToolSet` directory.

Command:

For Windows:

```
.\print-tsfile-resource-files.bat <path of your TsFileResource directory>
```

For Linux or MacOs:

```
./print-tsfile-resource-files.sh <path of your TsFileResource directory>
```

An example on Windows:

```shell
D:\iotdb\server\target\iotdb-server-0.10.0\tools\tsfileToolSet>.\print-tsfile-resource-files.bat D:\data\data\sequence\root.vehicle
|````````````````````````
Starting Printing the TsFileResources
|​````````````````````````
12:31:59.861 [main] WARN org.apache.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-engine.properties, use default configuration
analyzing D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile ...
device root.vehicle.d0, start time 3000 (1970-01-01T08:00:03+08:00[GMT+08:00]), end time 100999 (1970-01-01T08:01:40.999+08:00[GMT+08:00])
analyzing the resource file finished.
```

#### 1.3.3 TsFile Sketch Tool

After building the server, the startup script of this tool will appear under the `server\target\iotdb-server-0.10.0\tools\tsfileToolSet` directory.

Command:

For Windows:

```
.\print-tsfile-sketch.bat <path of your TsFile> (<path of the file for saving the output result>) 
```

- Note that if `<path of the file for saving the output result>` is not set, the default path "TsFile_sketch_view.txt" will be used. 

For Linux or MacOs:

```
./print-tsfile-sketch.sh <path of your TsFile> (<path of the file for saving the output result>) 
```

- Note that if `<path of the file for saving the output result>` is not set, the default path "TsFile_sketch_view.txt" will be used. 

An example on macOS:

```shell
/iotdb/server/target/iotdb-server-0.10.0/tools/tsfileToolSet$ ./print-tsfile-sketch.sh test.tsfile
|````````````````````````
Starting Printing the TsFile Sketch
|````````````````````````
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

You can also use `example/tsfile/org/apache/iotdb/tsfile/TsFileSequenceRead` to sequentially print a TsFile's content.

### 1.4 A TsFile Visualization Example

#### v0.8

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png">

#### v0.9 / 000001

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/69341240-26012300-0ca4-11ea-91a1-d516810cad44.png">

#### v0.10 / 000002

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/95296983-492cc500-08ac-11eb-9f66-c9c78401c61d.png">
