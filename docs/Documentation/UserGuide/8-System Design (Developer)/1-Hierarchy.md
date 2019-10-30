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

# Chapter 7: System Design (Developer)

## TsFile Hierarchy

  Here is a brief introduction of the structure of a TsFile file.

## Variable Storage

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

## TsFile Overview

Here is a graph about the TsFile structure.

![TsFile Breakdown](https://user-images.githubusercontent.com/40447846/61616997-6fad1300-ac9c-11e9-9c17-46785ebfbc88.png)

## Magic String

There is a 12 bytes magic string:

`TsFilev0.8.0`

It is in both the beginning and end of a TsFile file as signature.

## Data

The content of a TsFile file can be divided as two parts: data and metadata. There is a byte `0x02` as the marker between
data and metadata.

The data section is an array of `ChunkGroup`, each ChuckGroup represents a *device*.

#### ChuckGroup

The `ChunkGroup` has an array of `Chunk`, a following byte `0x00` as the marker, and a `ChunkFooter`.

##### Chunk

A `Chunk` represents a *sensor*. There is a byte `0x01` as the marker, following a `ChunkHeader` and an array of `Page`.

###### ChunkHeader

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

###### Page

A `Page` represents some data in a `Chunk`. It contains a `PageHeader` and the actual data (The encoded time-value pair).

PageHeader Structure

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Data size before compressing</td><td>int</td>
        	<tr><td>Data size after compressing(if use SNAPPY)</td><td>int</td>
        	<tr><td>Number of values</td><td>int</td>
        	<tr><td>Minimum time stamp</td><td>long</td>
        	<tr><td>Maximum time stamp</td><td>long</td>
        	<tr><td>Minimum value of the page</td><td>Type of the page</td>
        	<tr><td>Maximum value of the page</td><td>Type of the page</td>
        	<tr><td>First value of the page</td><td>Type of the page</td>
        	<tr><td>Last value of the page</td><td>Type of the page</td>
        	<tr><td>Sum of the Page</td><td>double</td>
        </table>
</center>

###### ChunkGroupFooter

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Deviceid</td><td>String</td>
        	<tr><td>Data size of the ChunkGroup</td><td>long</td>
        	<tr><td>Number of chunks</td><td>int</td>
        </table>
</center>

## Metadata

### TsDeviceMetaData

The first part of metadata is `TsDeviceMetaData` 

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Start time</td><td>long</td>
        	<tr><td>End time</td><td>long</td>
        	<tr><td>Number of chunk groups</td><td>int</td>
        </table>
</center>

Then there is an array of `ChunkGroupMetaData` after `TsDeviceMetaData`

### ChunkGroupMetaData

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Deviceid</td><td>String</td>
        	<tr><td>Start offset of the ChunkGroup</td><td>long</td>
        	<tr><td>End offset of the ChunkGroup</td><td>long</td>
        	<tr><td>Version</td><td>long</td>
        	<tr><td>Number of ChunkMetaData</td><td>int</td>
        </table>
</center>

Then there is an array of `ChunkMetadata` for each `ChunkGroupMetadata`

##### ChunkMetaData

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Measurementid</td><td>String</td>
        	<tr><td>Start offset of ChunkHeader</td><td>long</td>
        	<tr><td>Number of data points</td><td>long</td>
        	<tr><td>Start time</td><td>long</td>
        	<tr><td>End time</td><td>long</td>
        	<tr><td>Data type</td><td>short</td>
        	<tr><td>Number of statistics</td><td>int</td>
        	<tr><td>The statistics of this chunk</td><td>TsDigest</td>
        </table>
</center>

###### TsDigest (updated on 2019/8/27)

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

#### File Metadata

After the array of `ChunkGroupMetadata`, here is the last part of the metadata.

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Number of Devices</td><td>int</td>
        	<tr><td>Array of DeviceIndexMetadata</td><td>DeviceIndexMetadata</td>
        	<tr><td>Number of Measurements</td><td>int</td>
        	<tr><td>Array of Measurement name and schema</td><td>String, MeasurementSchema pair</td>
        	<tr><td>Current Version(3 for now)</td><td>int</td>
        	<tr><td>Author byte</td><td>byte</td>
        	<tr><td>Author(if author byte is 0x01)</td><td>String</td>
        	<tr><td>File Metadata size(not including itself)</td><td>int</td>
        </table>
</center>

##### DeviceIndexMetadata

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Deviceid</td><td>String</td>
        	<tr><td>Start offset of ChunkGroupMetaData(Or TsDeviceMetaData if it's the first one)</td><td>long</td>
        	<tr><td>length</td><td>int</td>
        	<tr><td>Start time</td><td>long</td>
        	<tr><td>End time</td><td>long</td>
        </table>
</center>

##### MeasurementSchema

<center>
        <table style="text-align:center">
            <tr><th>Member Description</th><th>Member Type</td></tr>
        	<tr><td>Measurementid</td><td>String</td></tr>
        	<tr><td>Data type</td><td>short</td>
        	<tr><td>Encoding</td><td>short</td>
        	<tr><td>Compressor</td><td>short</td>
        	<tr><td>Size of props</td><td>int</td>
        </table>
</center>

If size of props is greater than 0, there is an array of <String, String> pair as properties of this measurement.

Such as "max_point_number""2".

## Done

After the `FileMetaData`, there will be another Magic String and you have finished the journey of discovering TsFile!

You can also use /tsfile/example/TsFileSequenceRead to read and validate a TsFile.

## TsFile Sketch Tool
`org.apache.iotdb.tsfile.TsFileSketchTool` under the example/tsfile module is a tool to help you dive into the physical storage layout of a specific TsFile.  

Command:   
java -cp `<path of tsfile-example-0.9.0-SNAPSHOT.jar>` 
`<path of tsfile-0.9.0-SNAPSHOT-jar-with-dependencies.jar>` 
`org.apache.iotdb.tsfile.TsFileSketchTool`
`<path of your TsFile>` (`<path of the file for saving the output result>`) 

Note: 
- if `<path of the file for saving the output result>` is not set, 
the default path "TsFile_sketch_view.txt" will be used. 

Command example:
```
java -cp tsfile-example-0.9.0-SNAPSHOT.jar;D:\incubator-iotdb\tsfile\target\tsfile-0.9.0-SNAPSHOT-jar-with-dependencies.jar org.apache.iotdb.tsfile.TsFileSketchTool D:\1567769299010-101.tsfile
```

An example output result:
```$xslt
-------------------------------- TsFile Sketch --------------------------------
file path: D:\1568815495311-101.tsfile
file length: 1959

            POSITION|	CONTENT
            -------- 	-------
                   0|	[magic head] TsFilev0.8.0
>>>>>>>>>>>>>>>>>>>>>	[Chunk Group] of root.vehicle.d1 begins at pos 12, ends at pos 164, version:0, num of Chunks:1
                  12|	[Chunk] of s0, numOfPoints:2, time range:[1,1000], tsDataType:INT32, 
                     	TsDigest:[min_value:888,max_value:999,first_value:999,last_value:888,sum_value:1887.0]
                    |		[marker] 1
                    |		[ChunkHeader]
                    |		num of pages: 1
                    |	[marker] 0
                    |	[Chunk Group Footer]
<<<<<<<<<<<<<<<<<<<<<	[Chunk Group] of root.vehicle.d1 ends
>>>>>>>>>>>>>>>>>>>>>	[Chunk Group] of root.vehicle.d0 begins at pos 164, ends at pos 989, version:0, num of Chunks:5
                 164|	[Chunk] of s3, numOfPoints:6, time range:[60,946684800000], tsDataType:TEXT, 
                     	TsDigest:[min_value:aaaaa,max_value:good,first_value:aaaaa,last_value:good,sum_value:0.0]
                    |		[marker] 1
                    |		[ChunkHeader]
                    |		num of pages: 1
                 366|	[Chunk] of s4, numOfPoints:1, time range:[100,100], tsDataType:BOOLEAN, 
                     	TsDigest:[min_value:true,max_value:true,first_value:true,last_value:true,sum_value:0.0]
                    |		[marker] 1
                    |		[ChunkHeader]
                    |		num of pages: 1
                 461|	[Chunk] of s0, numOfPoints:11, time range:[1,1000], tsDataType:INT32, 
                     	TsDigest:[min_value:80,max_value:22222,first_value:101,last_value:22222,sum_value:42988.0]
                    |		[marker] 1
                    |		[ChunkHeader]
                    |		num of pages: 1
                 614|	[Chunk] of s1, numOfPoints:11, time range:[1,946684800000], tsDataType:INT64, 
                     	TsDigest:[min_value:100,max_value:55555,first_value:1101,last_value:100,sum_value:147922.0]
                    |		[marker] 1
                    |		[ChunkHeader]
                    |		num of pages: 1
                 822|	[Chunk] of s2, numOfPoints:6, time range:[2,1000], tsDataType:FLOAT, 
                     	TsDigest:[min_value:2.22,max_value:1000.11,first_value:2.22,last_value:1000.11,sum_value:1031.2099850177765]
                    |		[marker] 1
                    |		[ChunkHeader]
                    |		num of pages: 1
                    |	[marker] 0
                    |	[Chunk Group Footer]
<<<<<<<<<<<<<<<<<<<<<	[Chunk Group] of root.vehicle.d0 ends
                 989|	[marker] 2
                 990|	[TsDeviceMetadata] of root.vehicle.d0, startTime:1, endTime:946684800000
                    |		num of ChunkGroupMetaData: 1
                1553|	[TsDeviceMetadata] of root.vehicle.d1, startTime:1, endTime:1000
                    |		num of ChunkGroupMetaData: 1
                1718|	[TsFileMetaData]
                    |		num of TsDeviceMetadataIndex: 2
                    |		num of measurementSchema: 5
                1943|	[TsFileMetaDataSize] 225
                1947|	[magic tail] TsFilev0.8.0
                1959|	END of TsFile

---------------------------------- TsFile Sketch End ----------------------------------
```

## A TsFile Visualization Example (v0.8.0)
![A TsFile Visualization Example v0.8.0](https://user-images.githubusercontent.com/33376433/65209576-2bd36000-dacb-11e9-9e43-49e0dd01274e.png)

