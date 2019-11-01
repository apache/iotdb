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

# Chapter 8: System Design (Developer)

## 1. TsFile Design

  This is an introduction to the design details of TsFile.

### 1.1 Variable Storage

- **Big Endian**
       
  - For Example, the `int` `0x8` will be stored as `00 00 00 08`, not `08 00 00 00`
- **String with Variable Length**
  - The format is `int size` plus `String literal`. Size can be zero.
  - Size equals the number of bytes this string will take, and it may not equal to the length of the string. 
  - For example "sensor_1" will be stored as `00 00 00 08` plus the encoding(ASCII) of "sensor_1".
  - Note that for the "Magic String"(file signature) "TsFilev0.8.0", the size(12) and encoding(ASCII)
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
  - 2: GZIP
  - 3: LZO
  - 4: SDT
  - 5: PAA
  - 6: PLA
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

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>The name of this sensor(measurementID)</td><td>String</td>
        	<tr><td>Size of this chunk</td><td>int</td>
        	<tr><td>Data type of this chuck</td><td>short</td>
        	<tr><td>Number of pages</td><td>int</td>
        	<tr><td>Compression Type</td><td>short</td>
        	<tr><td>Encoding Type</td><td>short</td>
        	<tr><td>Max Tombstone Time</td><td>long</td>
        </table>
</center>

##### Page

A `Page` represents some data in a `Chunk`. It contains a `PageHeader` and the actual data (The encoded time-value pair).

PageHeader Structure

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Data size before compressing</td><td>int</td>
        	<tr><td>Data size after compressing(if use SNAPPY)</td><td>int</td>
        	<tr><td>Number of values</td><td>int</td>
        	<tr><td>Maximum time stamp</td><td>long</td>
        	<tr><td>Minimum time stamp</td><td>long</td>
        	<tr><td>Maximum value of the page</td><td>Type of the page</td>
          	<tr><td>Minimum value of the page</td><td>Type of the page</td>
        	<tr><td>First value of the page</td><td>Type of the page</td>
        	<tr><td>Sum of the Page</td><td>double</td>
        	<tr><td>Last value of the page</td><td>Type of the page</td>
        </table>
</center>

##### ChunkGroupFooter

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>DeviceId</td><td>String</td>
        	<tr><td>Data size of the ChunkGroup</td><td>long</td>
        	<tr><td>Number of chunks</td><td>int</td>
        </table>
</center>

#### 1.2.3  Metadata

##### 1.2.3.1 TsDeviceMetaData

The first part of metadata is `TsDeviceMetaData` 

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Start time</td><td>long</td>
        	<tr><td>End time</td><td>long</td>
        	<tr><td>Number of chunk groups</td><td>int</td>
        	<tr><td>List of ChunkGroupMetaData</td><td>list</td>
        </table>
</center>

###### ChunkGroupMetaData

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>DeviceId</td><td>String</td>
        	<tr><td>Start offset of the ChunkGroup</td><td>long</td>
        	<tr><td>End offset of the ChunkGroup</td><td>long</td>
        	<tr><td>Version</td><td>long</td>
        	<tr><td>Number of ChunkMetaData</td><td>int</td>
        	<tr><td>List of ChunkMetaData</td><td>list</td>
        </table>
</center>

###### ChunkMetaData

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>MeasurementId</td><td>String</td>
        	<tr><td>Start offset of ChunkHeader</td><td>long</td>
        	<tr><td>Number of data points</td><td>long</td>
        	<tr><td>Start time</td><td>long</td>
        	<tr><td>End time</td><td>long</td>
        	<tr><td>Data type</td><td>short</td>
        	<tr><td>The statistics of this chunk</td><td>TsDigest</td>
        </table>
</center>

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

![TsDigest ByteBuffer Breakdown comparison](https://user-images.githubusercontent.com/33376433/63765352-664a4280-c8fb-11e9-869e-859edf6d00bb.png)

In v0.9.0, the storage format is changed to an array for space and time efficiency. That is, `ByteBuffer[] statistics`. Each position of the array has a fixed association with a specific type of statistic, following the order defined in StatisticType:

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

As another example in v0.9.0, when deserializing a TsDigest from buffer [3, 0,4,0, 1,4,99, 3,4,19], we get 

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

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Number of devices</td><td>int</td>
        	<tr><td>Pairs of device name and deviceMetadataIndex</td><td>String, TsDeviceMetadataIndex pair</td>
        	<tr><td>Number of measurements</td><td>int</td>
        	<tr><td>Pairs of measurement name and schema</td><td>String, MeasurementSchema pair</td>
        	<tr><td>Author byte</td><td>byte</td>
          	<tr><td>Author(if author byte is 0x01)</td><td>String</td>
          	<tr><td>totalChunkNum</td><td>int</td>
          	<tr><td>invalidChunkNum</td><td>int</td>
        	<tr><td>Bloom filter size</td><td>int</td>
        	<tr><td>Bloom filter bit vector</td><td>byte[Bloom filter size]</td>
        	<tr><td>Bloom filter capacity</td><td>int</td>
        	<tr><td>Bloom filter hash functions size</td><td>int</td>
        </table>
</center>

###### TsDeviceMetadataIndex

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>DeviceId</td><td>String</td>
        	<tr><td>Start offset of TsDeviceMetaData</td><td>long</td>
        	<tr><td>length</td><td>int</td>
        	<tr><td>Start time</td><td>long</td>
        	<tr><td>End time</td><td>long</td>
        </table>
</center>

###### MeasurementSchema

<center>
        <table style="text-align:center">
            <tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>MeasurementId</td><td>String</td></tr>
        	<tr><td>Data type</td><td>short</td>
        	<tr><td>Encoding</td><td>short</td>
        	<tr><td>Compressor</td><td>short</td>
        	<tr><td>Size of props</td><td>int</td>
        </table>
</center>

If size of props is greater than 0, there is an array of <String, String> pair as properties of this measurement.

Such as "max_point_number""2".

##### 1.2.3.3 TsFileMetadataSize

After the TsFileMetaData, there is an int indicating the size of the TsFileMetaData.


#### 1.2.4 Magic String
A TsFile ends with a 6-byte magic string (`TsFile`).


Congratulations! You have finished the journey of discovering TsFile.

### 1.3 Tool Set

#### 1.3.1 TsFileResource Print Tool

After building the server, the startup script of this tool will appear under the `server\target\iotdb-server-0.9.0-SNAPSHOT\tools` directory.

Command:

For Windows:

```
.\print-tsfile-sketch.bat <path of your TsFileResource Directory>
```

For Linux or MacOs:

```
./print-tsfile-sketch.sh <path of your TsFileResource Directory>
```

An example on Windows:

```
D:\incubator-iotdb\server\target\iotdb-server-0.9.0-SNAPSHOT\tools>.\print-tsfile-resource-files.bat D:\data\data\sequence\root.vehicle
​````````````````````````
Starting Printing the TsFileResources
​````````````````````````
12:31:59.861 [main] WARN org.apache.iotdb.db.conf.IoTDBDescriptor - Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file iotdb-engine.properties, use default configuration
analyzing D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile ...
device root.vehicle.d0, start time 3000 (1970-01-01T08:00:03+08:00[GMT+08:00]), end time 100999 (1970-01-01T08:01:40.999+08:00[GMT+08:00])
analyzing the resource file finished.
```

#### 1.3.2 TsFile Sketch Tool

After building the server, the startup script of this tool will appear under the `server\target\iotdb-server-0.9.0-SNAPSHOT\tools` directory.

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
D:\incubator-iotdb\server\target\iotdb-server-0.9.0-SNAPSHOT\tools>.\print-tsfile-sketch.bat D:\data\data\sequence\root.vehicle\1572496142067-101-0.tsfile
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
>>>>>>>>>>>>>>>>>>>>>   [Chunk Group] of root.vehicle.d0 begins at pos 12, ends at pos 186469, version:102, num of Chunks:6
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
<<<<<<<<<<<<<<<<<<<<<   [Chunk Group] of root.vehicle.d0 ends
              186469|   [marker] 2
              186470|   [TsDeviceMetadata] of root.vehicle.d0, startTime:3000, endTime:100999
                    |           [startTime] 3000
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
You can also use `example/tsfile/org/apache/iotdb/tsfile/TsFileSequenceRead` to sequentially print a TsFile's content.

### 1.4 A TsFile Visualization Example (v0.8.0)

![A TsFile Visualization Example v0.8.0](https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png)