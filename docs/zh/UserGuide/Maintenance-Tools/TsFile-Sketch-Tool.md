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

## TsFile概览工具

TsFile概览工具用于以概要模式打印出一个TsFile的内容，工具位置为 tools/tsfile/print-tsfile。

### 用法

-   Windows:

```bash
.\print-tsfile-sketch.bat <TsFile文件路径> (<输出结果的存储路径>) 
```

-   Linux or MacOs:

```shell
./print-tsfile-sketch.sh <TsFile文件路径> (<输出结果的存储路径>) 
```

注意：如果没有设置输出结果的存储路径, 将使用相对路径"TsFile_sketch_view.txt"作为默认值。

### 示例

以Windows系统为例：

`````````````````````````bash
.\print-tsfile.bat D:\github\master\1669359533965-1-0-0.tsfile D:\github\master\sketch.txt
````````````````````````
Starting Printing the TsFile Sketch
````````````````````````
TsFile path:D:\github\master\1669359533965-1-0-0.tsfile
Sketch save path:D:\github\master\sketch.txt
148  [main] WARN  o.a.i.t.c.conf.TSFileDescriptor - not found iotdb-common.properties, use the default configs.
-------------------------------- TsFile Sketch --------------------------------
file path: D:\github\master\1669359533965-1-0-0.tsfile
file length: 2974

            POSITION|   CONTENT
            --------    -------
                   0|   [magic head] TsFile
                   6|   [version number] 3
||||||||||||||||||||| [Chunk Group] of root.sg1.d1, num of Chunks:3
                   7|   [Chunk Group Header]
                    |           [marker] 0
                    |           [deviceID] root.sg1.d1
                  20|   [Chunk] of root.sg1.d1.s1, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9032452783138882770,maxValue:9117677033041335123,firstValue:7068645577795875906,lastValue:-5833792328174747265,sumValue:5.795959009889246E19]
                    |           [chunk header] marker=5, measurementID=s1, dataSize=864, dataType=INT64, compressionType=SNAPPY, encodingType=RLE
                    |           [page]  UncompressedSize:862, CompressedSize:860
                 893|   [Chunk] of root.sg1.d1.s2, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-8806861312244965718,maxValue:9192550740609853234,firstValue:1150295375739457693,lastValue:-2839553973758938646,sumValue:8.2822564314572677E18]
                    |           [chunk header] marker=5, measurementID=s2, dataSize=864, dataType=INT64, compressionType=SNAPPY, encodingType=RLE
                    |           [page]  UncompressedSize:862, CompressedSize:860
                1766|   [Chunk] of root.sg1.d1.s3, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9076669333460323191,maxValue:9175278522960949594,firstValue:2537897870994797700,lastValue:7194625271253769397,sumValue:-2.126008424849926E19]
                    |           [chunk header] marker=5, measurementID=s3, dataSize=864, dataType=INT64, compressionType=SNAPPY, encodingType=RLE
                    |           [page]  UncompressedSize:862, CompressedSize:860
||||||||||||||||||||| [Chunk Group] of root.sg1.d1 ends
                2656|   [marker] 2
                2657|   [TimeseriesIndex] of root.sg1.d1.s1, tsDataType:INT64, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9032452783138882770,maxValue:9117677033041335123,firstValue:7068645577795875906,lastValue:-5833792328174747265,sumValue:5.795959009889246E19]
                    |           [ChunkIndex] offset=20
                2728|   [TimeseriesIndex] of root.sg1.d1.s2, tsDataType:INT64, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-8806861312244965718,maxValue:9192550740609853234,firstValue:1150295375739457693,lastValue:-2839553973758938646,sumValue:8.2822564314572677E18]
                    |           [ChunkIndex] offset=893
                2799|   [TimeseriesIndex] of root.sg1.d1.s3, tsDataType:INT64, startTime: 1669359533948 endTime: 1669359534047 count: 100 [minValue:-9076669333460323191,maxValue:9175278522960949594,firstValue:2537897870994797700,lastValue:7194625271253769397,sumValue:-2.126008424849926E19]
                    |           [ChunkIndex] offset=1766
                2870|   [IndexOfTimerseriesIndex Node] type=LEAF_MEASUREMENT
                    |           <s1, 2657>
                    |           <endOffset, 2870>
||||||||||||||||||||| [TsFileMetadata] begins
                2891|   [IndexOfTimerseriesIndex Node] type=LEAF_DEVICE
                    |           <root.sg1.d1, 2870>
                    |           <endOffset, 2891>
                    |   [meta offset] 2656
                    |   [bloom filter] bit vector byte array length=31, filterSize=256, hashFunctionSize=5
||||||||||||||||||||| [TsFileMetadata] ends
                2964|   [TsFileMetadataSize] 73
                2968|   [magic tail] TsFile
                2974|   END of TsFile
---------------------------- IndexOfTimerseriesIndex Tree -----------------------------
        [MetadataIndex:LEAF_DEVICE]
        └──────[root.sg1.d1,2870]
                        [MetadataIndex:LEAF_MEASUREMENT]
                        └──────[s1,2657]
---------------------------------- TsFile Sketch End ----------------------------------
`````````````````````````

解释：

-   以"|"为分隔，左边是在TsFile文件中的实际位置，右边是梗概内容。
-   "|||||||||||||||||||||"是为增强可读性而添加的导引信息，不是TsFile中实际存储的数据。
-   最后打印的"IndexOfTimerseriesIndex Tree"是对TsFile文件末尾的元数据索引树的重新整理打印，便于直观理解，不是TsFile中存储的实际数据。