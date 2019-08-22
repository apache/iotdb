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
# Chapter 8: TsFile
## TsFile Hierarchy
  Here is a brief introduction of the structure of a TsFile file.
  
## Variable Storage
 * **Big Endian**
        
     * For Example, the `int` `0x8` will be stored as `00 00 00 08`, not `08 00 00 00`
  
 * **String with Variable Length**
 
    * The format is `int size` plus `String literal`. Size can be zero.
    
    * Size equals the number of bytes this string will take, and it may not equal to the length of the string. 
    
    * For example "sensor_1" will be stored as `00 00 00 08` plus the encoding(ASCII) of "sensor_1".
    
    * Note that for the "Magic String"(file signature) "TsFilev0.8.0", the size(12) and encoding(ASCII)
    is fixed so there is no need to put the size before this string literal.
  
 * **Data Type Hardcode**
    * 0: BOOLEAN
    * 1: INT32 (`int`)
    * 2: INT64 (`long`)
    * 3: FLOAT
    * 4: DOUBLE
    * 5: TEXT (`String`)
 * **Encoding Type Hardcode**
    * 0: PLAIN
    * 1: PLAIN_DICTIONARY
    * 2: RLE
    * 3: DIFF
    * 4: TS_2DIFF
    * 5: BITMAP
    * 6: GORILLA
    * 7: REGULAR 
 * **Compressing Type Hardcode**
    * 0: UNCOMPRESSED
    * 1: SNAPPY
    
    
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

###### TsDigest

There are five statistics: `min, last, sum, first, max`

The storage format is a name-value pair. The name is a string (remember the length is before the literal).

But for the value, there is also a size integer before the data even if it is not string. For example, if the `min` is 3, then it will be
stored as 3 "min" 4 3 in the TsFile.

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