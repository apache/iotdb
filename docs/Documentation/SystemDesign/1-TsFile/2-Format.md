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
  - 6: GORILLA
  - 7: REGULAR 
- **Compressing Type Hardcode**
  - 0: UNCOMPRESSED
  - 1: SNAPPY
- **TsDigest Statistics Type Hardcode**
  - 0: min_value
  - 1: max_value
  - 2: first_value
  - 3: last_value
  - 4: sum_value

### 1.2 TsFile Overview

Here is a graph about the TsFile structure.

![TsFile Breakdown](https://user-images.githubusercontent.com/40447846/61616997-6fad1300-ac9c-11e9-9c17-46785ebfbc88.png)

#### 1.2.1 Magic String and Version Number

A TsFile begins with a 6-byte magic string (`TsFile`) and a 6-byte version number (`000001`).


#### 1.2.2 Data

The content of a TsFile file can be divided as two parts: data and metadata. There is a byte `0x02` as the marker between
data and metadata.

The data section is an array of `ChunkGroup`, each ChunkGroup represents a *device*.

##### ChunkGroup

The `ChunkGroup` has an array of `Chunk`, a following byte `0x00` as the marker, and a `ChunkFooter`.

##### Chunk

A `Chunk` represents a *sensor*. There is a byte `0x01` as the marker, following a `ChunkHeader` and an array of `Page`.

##### ChunkHeader

|           Member Description           | Member Type |
| :------------------------------------: | :---------: |
| The name of this sensor(measurementID) |   String    |
|           Size of this chunk           |     int     |
|        Data type of this chuck         |    short    |
|            Number of pages             |     int     |
|            Compression Type            |    short    |
|             Encoding Type              |    short    |
|       Max Tombstone Time(unused)       |    long     |

##### Page

A `Page` represents some data in a `Chunk`. It contains a `PageHeader` and the actual data (The encoded time-value pair).

PageHeader Structure

|             Member Description             |   Member Type    |
| :----------------------------------------: | :--------------: |
|        Data size before compressing        |       int        |
| Data size after compressing(if use SNAPPY) |       int        |
|              Number of values              |       int        |
|             Maximum time stamp             |       long       |
|             Minimum time stamp             |       long       |
|         Maximum value of the page          | Type of the page |
|         Minimum value of the page          | Type of the page |
|          First value of the page           | Type of the page |
|              Sum of the Page               |      double      |
|           Last value of the page           | Type of the page |

##### ChunkGroupFooter

|     Member Description      | Member Type |
| :-------------------------: | :---------: |
|          DeviceId           |   String    |
| Data size of the ChunkGroup |    long     |
|      Number of chunks       |     int     |

#### 1.2.3  Metadata

##### 1.2.3.1 TsDeviceMetaData

The first part of metadata is `TsDeviceMetaData` 

|     Member Description     | Member Type |
| :------------------------: | :---------: |
|         Start time         |    long     |
|          End time          |    long     |
|   Number of chunk groups   |     int     |
| List of ChunkGroupMetaData |    list     |

###### ChunkGroupMetaData

|       Member Description       | Member Type |
| :----------------------------: | :---------: |
|            DeviceId            |   String    |
| Start offset of the ChunkGroup |    long     |
|  End offset of the ChunkGroup  |    long     |
|            Version             |    long     |
|    Number of ChunkMetaData     |     int     |
|     List of ChunkMetaData      |    list     |

###### ChunkMetaData

|      Member Description      | Member Type |
| :--------------------------: | :---------: |
|        MeasurementId         |   String    |
| Start offset of ChunkHeader  |    long     |
|    Number of data points     |    long     |
|          Start time          |    long     |
|           End time           |    long     |
|          Data type           |    short    |
| The statistics of this chunk |  TsDigest   |

###### TsDigest

Right now there are five statistics: `min_value, max_value, first_value, last_value, sum_value`.

In v0.8.0, the storage format of statistics is a name-value pair. That is, `Map<String, ByteBuffer> statistics`. The name is a string (remember the length is before the literal). But for the value, there is also an integer byteLength acting as the self description length of the following value because the value may be of various type. For example, if the `min_value` is an integer 0, then it will be stored as [9 "min_value" 4 0] in the TsFile.

The figure below shows an example of `TsDigest.deserializeFrom(buffer)`. In v0.8.0, we will get 

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

In v0.9.x, the storage format is changed to an array for space and time efficiency. That is, `ByteBuffer[] statistics`. Each position of the array has a fixed association with a specific type of statistic, following the order defined in StatisticType:

```
enum StatisticType {
    min_value, max_value, first_value, last_value, sum_value
}
```

Therefore, in the above example, we will get 

```
ByteBuffer[] statistics = [
    ByteBuffer of int value 0, // associated with "min_value"
    ByteBuffer of int value 99, // associated with "max_value"
    ByteBuffer of int value 0, // associated with "first_value"
    ByteBuffer of int value 19, // associated with "last_value"
    ByteBuffer of double value 1093347116 // associated with "sum_value"
]
```

As another example in v0.9.x, when deserializing a TsDigest from buffer [3, 0,4,0, 1,4,99, 3,4,19], we get 

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

`TsFileMetaData` follows after `TsDeviceMetadatas`.

|              Member Description              |            Member Type             |
| :------------------------------------------: | :--------------------------------: |
|              Number of devices               |                int                 |
| Pairs of device name and deviceMetadataIndex | String, TsDeviceMetadataIndex pair |
|            Number of measurements            |                int                 |
|     Pairs of measurement name and schema     |   String, MeasurementSchema pair   |
|                 Author byte                  |                byte                |
|        Author(if author byte is 0x01)        |               String               |
|                totalChunkNum                 |                int                 |
|               invalidChunkNum                |                int                 |
|              Bloom filter size               |                int                 |
|           Bloom filter bit vector            |      byte[Bloom filter size]       |
|            Bloom filter capacity             |                int                 |
|       Bloom filter hash functions size       |                int                 |

###### TsDeviceMetadataIndex

|        Member Description        | Member Type |
| :------------------------------: | :---------: |
|             DeviceId             |   String    |
| Start offset of TsDeviceMetaData |    long     |
|              length              |     int     |
|            Start time            |    long     |
|             End time             |    long     |

###### MeasurementSchema

| Member Description | Member Type |
| :----------------: | :---------: |
|   MeasurementId    |   String    |
|     Data type      |    short    |
|      Encoding      |    short    |
|     Compressor     |    short    |
|   Size of props    |     int     |

If size of props is greater than 0, there is an array of <String, String> pair as properties of this measurement.

Such as "max_point_number""2".

##### 1.2.3.3 TsFileMetadataSize

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
```



#### 1.3.2 TsFileResource Print Tool

After building the server, the startup script of this tool will appear under the `server\target\iotdb-server-0.10.0\tools\tsfileToolSet` directory.

Command:

For Windows:

```
.\print-tsfile-sketch.bat <path of your TsFileResource directory>
```

For Linux or MacOs:

```
./print-tsfile-sketch.sh <path of your TsFileResource directory>
```

An example on Windows:

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

An example on Windows:

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

You can also use `example/tsfile/org/apache/iotdb/tsfile/TsFileSequenceRead` to sequentially print a TsFile's content.

### 1.4 A TsFile Visualization Example

#### v0.8.0

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png">

#### v0.9.x

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/33376433/69341240-26012300-0ca4-11ea-91a1-d516810cad44.png">