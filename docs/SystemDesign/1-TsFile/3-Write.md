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

# TsFile Write Process

- org.apache.iotdb.tsfile.write.*

The writing process of TsFile is shown in the following figure:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625238-efba2980-467e-11ea-927e-a7021f8153af.png">

Among them, each device corresponds to a ChunkGroupWriter, and each sensor corresponds to a ChunkWriter.

File writing is mainly divided into three operations, marked with 1, 2, 3 on the figure

- 1、Write memory swap area
- 2、Persistent ChunkGroup
- 3、Close file

## 1、Write memory buffer

TsFile file layer has two write interfaces

- TsFileWriter.write(TSRecord record)

Write a device with a timestamp and multiple measurement points.

- TsFileWriter.write(Tablet tablet)

 Write multiple timestamps and multiple measurement points on one device.

When the write interface is called, the data of this device will be delivered to the corresponding ChunkGroupWriter, and each measurement point will be delivered to the corresponding ChunkWriter for writing.  ChunkWriter completes coding and packaging (generating a page).

## 2、Persistent ChunkGroup

- TsFileWriter.flushAllChunkGroups()

When the data in the memory reaches a certain threshold, the persistence operation is triggered.  Each persistence will persist all the data of all devices in the current memory to the TsFile file of the disk.  Each device corresponds to a ChunkGroup and each measurement point corresponds to a Chunk.

After the persistence is complete, the corresponding metadata information is cached in memory for querying and generating the metadata at the end of the file.

## 3、File Close

- TsFileWriter.close()

Based on the metadata cached in memory, TsFileMetadata is generated and appended to the end of the file, and the file is finally closed.