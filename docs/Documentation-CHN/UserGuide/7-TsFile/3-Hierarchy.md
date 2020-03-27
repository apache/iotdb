<!--

```
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
```

-->

# TsFile层次结构

这是TsFile文件结构的简要介绍。

# 可变存储
 * **Big Endian**
     ​     
     * 例如，`int` `0x8`将被存储为`00 00 00 08`，而不是`08 00 00 00`。

 * **可变长度的字符串**
    * 格式为 `int size` 加`String literal`. 大小可以为零。

    * 大小等于此字符串将占用的字节数，并且可能不等于该字符串的长度。

    * 例如，“ sensor_1”将被存储为`00 00 00 08`加上“ sensor_1”的编码（ASCII）。

    * 请注意，对于“魔术字符串”（文件签名）“ TsFilev0.8.0”，大小（12）和编码（ASCII）是固定的，因此无需在该字符串文字前放置大小。

 * **数据类型硬编码**
    * 0: BOOLEAN
    * 1: INT32 (`int`)
    * 2: INT64 (`long`)
    * 3: FLOAT
    * 4: DOUBLE
    * 5: TEXT (`String`)
 * **编码类型硬编码**
    * 0: PLAIN
    * 1: PLAIN_DICTIONARY
    * 2: RLE
    * 3: DIFF
    * 4: TS_2DIFF
    * 5: BITMAP
    * 6: GORILLA
    * 7: REGULAR 
 * **压缩类型硬编码**
    * 0: UNCOMPRESSED
    * 1: SNAPPY

# TsFile概述
这是有关TsFile结构的图。

![TsFile Breakdown](https://user-images.githubusercontent.com/40447846/61616997-6fad1300-ac9c-11e9-9c17-46785ebfbc88.png)

# 魔术字符串
有一个12个字节的魔术字符串：

`TsFilev0.8.0`

它在TsFile文件的开头和结尾都作为签名。

# 数据

TsFile文件的内容可以分为两部分：数据和元数据。 数据和元数据之间有一个字节“ 0x02”作为标记。

数据部分是`ChunkGroup`的数组，每个ChuckGroup代表一个* device *。

### ChuckGroup

`ChunkGroup`具有一个`Chunk`数组，一个后继字节`0x00`作为标记以及一个`ChunkFooter`。

#### Chunk

`Chunk`代表*传感器*。 在`ChunkHeader`和`Page`数组之后，有一个字节`0x01`作为标记。

##### ChunkHeader
<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>The name of this sensor(measurementID)</td><td>String</td></tr>
        	<tr><td>Size of this chunk</td><td>int</td></tr>
        	<tr><td>Data type of this chuck</td><td>short</td></tr>
        	<tr><td>Number of pages</td><td>int</td></tr>
        	<tr><td>Compression Type</td><td>short</td></tr>
        	<tr><td>Encoding Type</td><td>short</td></tr>
        	<tr><td>Max Tombstone Time</td><td>long</td></tr>
        </table>
</center>


##### Page

`Page`代表`Chunk`中的一些数据。 它包含一个`PageHeader`和实际数据（编码的时间值对）。

PageHeader结构

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Data size before compressing</td><td>int</td></tr>
        	<tr><td>Data size after compressing(if use SNAPPY)</td><td>int</td></tr>
        	<tr><td>Number of values</td><td>int</td></tr>
        	<tr><td>Minimum time stamp</td><td>long</td></tr>
        	<tr><td>Maximum time stamp</td><td>long</td></tr>
        	<tr><td>Minimum value of the page</td><td>Type of the page</td></tr>
        	<tr><td>Maximum value of the page</td><td>Type of the page</td></tr>
        	<tr><td>First value of the page</td><td>Type of the page</td></tr>
        	<tr><td>Last value of the page</td><td>Type of the page</td></tr>
        	<tr><td>Sum of the Page</td><td>double</td></tr>
        </table>
</center>


##### ChunkGroupFooter

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Deviceid</td><td>String</td></tr>
        	<tr><td>Data size of the ChunkGroup</td><td>long</td></tr>
        	<tr><td>Number of chunks</td><td>int</td></tr>
        </table>
</center>


# 元数据

## TsDeviceMetaData
元数据的第一部分是`TsDeviceMetaData`

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Start time</td><td>long</td></tr>
        	<tr><td>End time</td><td>long</td></tr>
        	<tr><td>Number of chunk groups</td><td>int</td></tr>
        </table>
</center>


然后在`TsDeviceMetaData`之后有一个`ChunkGroupMetaData`数组。
## ChunkGroupMetaData

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Deviceid</td><td>String</td></tr>
        	<tr><td>Start offset of the ChunkGroup</td><td>long</td></tr>
        	<tr><td>End offset of the ChunkGroup</td><td>long</td></tr>
        	<tr><td>Version</td><td>long</td></tr>
        	<tr><td>Number of ChunkMetaData</td><td>int</td></tr>
        </table>
</center>


然后，每个`ChunkGroupMetadata`都有一个`ChunkMetadata`数组。

#### ChunkMetaData

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Measurementid</td><td>String</td></tr>
        	<tr><td>Start offset of ChunkHeader</td><td>long</td></tr>
        	<tr><td>Number of data points</td><td>long</td></tr>
        	<tr><td>Start time</td><td>long</td></tr>
        	<tr><td>End time</td><td>long</td></tr>
        	<tr><td>Data type</td><td>short</td></tr>
        	<tr><td>Number of statistics</td><td>int</td></tr>
        	<tr><td>The statistics of this chunk</td><td>TsDigest</td></tr>
        </table>
</center>


##### TsDigest

有五个统计信息： `min, last, sum, first, max`

存储格式是名称/值对。 名称是一个字符串（记住长度在文字之前）。

但是对于该值，即使不是字符串，在数据前也有一个大小整数。 例如，如果`min`为3，则它将被存储为3“ min” 4 3在TsFile中。

### File Metadata

在`ChunkGroupMetadata`数组之后，这是元数据的最后一部分。

<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Number of Devices</td><td>int</td></tr>
        	<tr><td>Array of DeviceIndexMetadata</td><td>DeviceIndexMetadata</td></tr>
        	<tr><td>Number of Measurements</td><td>int</td></tr>
        	<tr><td>Array of Measurement name and schema</td><td>String, MeasurementSchema pair</td></tr>
        	<tr><td>Current Version(3 for now)</td><td>int</td></tr>
        	<tr><td>Author byte</td><td>byte</td></tr>
        	<tr><td>Author(if author byte is 0x01)</td><td>String</td></tr>
        	<tr><td>File Metadata size(not including itself)</td><td>int</td></tr>
        </table>
</center>


#### DeviceIndexMetadata
<center>
        <table style="text-align:center">
        	<tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Deviceid</td><td>String</td></tr>
        	<tr><td>Start offset of ChunkGroupMetaData(Or TsDeviceMetaData if it's the first one)</td><td>long</td></tr>
        	<tr><td>length</td><td>int</td></tr>
        	<tr><td>Start time</td><td>long</td></tr>
        	<tr><td>End time</td><td>long</td></tr>
        </table>
</center>


#### MeasurementSchema
<center>
        <table style="text-align:center">
            <tr><th>Member Description</th><th>Member Type</th></tr>
        	<tr><td>Measurementid</td><td>String</td></tr></tr>
        	<tr><td>Data type</td><td>short</td></tr>
        	<tr><td>Encoding</td><td>short</td></tr>
        	<tr><td>Compressor</td><td>short</td></tr>
        	<tr><td>Size of props</td><td>int</td></tr>
        </table>
</center>


如果道具的大小大于0，则存在一个<String，String>对数组，作为此度量的属性。
​    例如“ max_point_number”“ 2”。

# 完成

在`FileMetaData`之后，将有另一个魔术字符串，您已经完成了发现TsFile的旅程！

您也可以使用/ tsfile / example / TsFileSequenceRead来读取和验证TsFile。