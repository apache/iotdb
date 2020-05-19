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

Based on the metadata cached in memory, TsFileMetadata is generated and appended to the end of the file (`TsFileWriter.flushMetadataIndex()`), and the file is finally closed.

One of the most important steps in constructing TsFileMetadata is to construct MetadataIndex tree. As we have mentioned before, the MetadataIndex is designed as tree structure so that not all the `TimeseriesMetadata` need to be read when the number of devices or measurements is too large. Only reading specific MetadataIndex nodes according to requirement and reducing I/O could speed up the query. The whole process of constructing MetadataIndex tree is as below:

* org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor

### MetadataIndexConstructor.constructMetadataIndex()

The input params of this method:
* Map\<String, List\<TimeseriesMetadata\>\> deviceTimeseriesMetadataMap, which indicates the map from device to its `TimeseriesMetadata`
* TsFileOutput out

The whole method contains three parts:

1. In measurement index level, each device and its TimeseriesMetadata in `deviceTimeseriesMetadataMap` is converted into `deviceMetadataIndexMap`. Specificly, for each device:
  * Initialize a `queue` for MetadataIndex nodes in this device
  * Initialize a leaf node of measurement index level, which is `LEAF_MEASUREMENT` type
  * For each TimeseriesMetadata：
    * Serialize
    * Add an entry into `currentIndexNode` every `MAX_DEGREE_OF_INDEX_NODE` entries
    * After storing `MAX_DEGREE_OF_INDEX_NODE` entries, add `currentIndexNode` into `queue`, and point `currentIndexNode` to a new MetadataIndexNode
  * Generate upper-level nodes of measurement index level according to the leaf nodes in `queue`, until the final root node (this method will be described later), and put the "device-root node" map into `deviceMetadataIndexMap`

2. Then judge whether the number of devices exceed `MAX_DEGREE_OF_INDEX_NODE`. If not, the root node of MetadataIndex tree could be generated and return
  * Initialize the root node of MetadataIndex tree, which is `INTERNAL_MEASUREMENT` type
  * For each entry in `deviceMetadataIndexMap`:
    * Serialize
    * Convert it into an entry and add the entry into `metadataIndexNode`
  * Set the `endOffset` of root node and return it

3. If the number of devices exceed `MAX_DEGREE_OF_INDEX_NODE`, the device index level of MetadataIndex tree is generated
  * Initialize a `queue` for MetadataIndex nodes in device index level
  * Initialize a leaf node of device index level, which is `LEAF_DEVICE` type
  * For each entry in `deviceMetadataIndexMap`:
    * Serialize
    * Convert it into an entry and add the entry into `metadataIndexNode`
    * After storing `MAX_DEGREE_OF_INDEX_NODE` entries, add `currentIndexNode` into `queue`, and point `currentIndexNode` to a new MetadataIndexNode
  * Generate upper-level nodes of device index level according to the leaf nodes in `queue`, until the final root node (this method will be described later)
  * Set the `endOffset` of root node and return it

### MetadataIndexConstructor.generateRootNode

The input params of this method:
* Queue\<MetadataIndexNode\> metadataIndexNodeQueue
* TsFileOutput out
* MetadataIndexNodeType type, which indicates the internal nodes type of generated tree. There are two types: when the method is called by measurement index level, it is INTERNAL_MEASUREMENT; when the method is called by device index level, it is INTERNAL_DEVICE 

The method needs to generate a tree structure of nodes in metadataIndexNodeQueue, and return the root node:
1. New `currentIndexNode` in specific `type`
2. When there are more than one nodes in the queue, loop handling the queue. For each node in the queue:
  * Serialize
  * Convert it into an entry and add the entry into `currentIndexNode`
  * After storing `MAX_DEGREE_OF_INDEX_NODE` entries, add `currentIndexNode` into `queue`, and point `currentIndexNode` to a new MetadataIndexNode
3. Return the root node in the queue when the queue has only one node

### MetadataIndexConstructor.addCurrentIndexNodeToQueue

The input params of this method:
* MetadataIndexNode currentIndexNode
* Queue\<MetadataIndexNode\> metadataIndexNodeQueue
* TsFileOutput out

This method set the endOffset of current MetadataIndexNode, and put it into queue.