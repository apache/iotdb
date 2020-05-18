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

One of the most important steps in constructing TsFileMetadata is to construct MetadataIndex tree. As we have mentioned before, the MetadataIndex is designed as tree structure so that not all the `TimeseriesMetadata` need to be read when the number of devices or measurements is too large. Only reading specific MetadataIndex nodes according to requirement and reducing I/O could speed up the query. The whole process of constructing MetadataIndex tree is as below:

### org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor

* MetadataIndexConstructor.constructMetadataIndex()

The input params of this method:
* Map\<String, List\<TimeseriesMetadata\>\> deviceTimeseriesMetadataMap, which indicates the map from device to its `TimeseriesMetadata`
* TsFileOutput out

The whole method contains three parts:

1. In measurement index level, each device and its TimeseriesMetadata in `deviceTimeseriesMetadataMap` is converted into `deviceMetadataIndexMap`

```
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      // if TimeseriesMetadata list of the device is empty, continue directly
      if (entry.getValue().isEmpty()) {
        continue;
      }
      Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
      TimeseriesMetadata timeseriesMetadata;
      // when initializing MetadataIndexNode of one device, new a node of LEAF_MEASUREMENT type
      MetadataIndexNode currentIndexNode = new MetadataIndexNode(
          MetadataIndexNodeType.LEAF_MEASUREMENT);
      for (int i = 0; i < entry.getValue().size(); i++) {
        timeseriesMetadata = entry.getValue().get(i);
        // when constructing from leaf node, every "degree number of nodes" are related to an entry
        if (i % MAX_DEGREE_OF_INDEX_NODE == 0) {
          // once current MetadataIndexNode is full, add it into queue, which is used to generate later tree structure
          if (currentIndexNode.isFull()) {
            addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
            currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
          }
          // new an entry of TimeseriesMetadata, and add it in the current MetadataIndexNode
          currentIndexNode.addEntry(new MetadataIndexEntry(timeseriesMetadata.getMeasurementId(),
              out.getPosition()));
        }
        // serialize TimeseriesMetadata
        timeseriesMetadata.serializeTo(out.wrapAsStream());
      }
      // add current MetadataIndexNode, which contains some remaining entries in this device, into queue
      addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
      // generate root node of measurement index level according to queue (this method will be described later), and put the "device-root node" map into deviceMetadataIndexMap
      deviceMetadataIndexMap.put(entry.getKey(), generateRootNode(measurementMetadataIndexQueue,
          out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
    }
```

2. Then judge whether the number of devices exceed `MAX_DEGREE_OF_INDEX_NODE`. If not, the root node of MetadataIndex tree could be generated and return
```
    if (deviceMetadataIndexMap.size() <= MAX_DEGREE_OF_INDEX_NODE) {
      // new a MetadataIndexNode. Since it cannnot be the leaf node of measurement index level, it is INTERNAL_MEASUREMENT type
      MetadataIndexNode metadataIndexNode = new MetadataIndexNode(
          MetadataIndexNodeType.INTERNAL_MEASUREMENT);
      for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
        // new an entry of device metadata, and add it in the current MetadataIndexNode
        metadataIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
        // serialize device metadata
        entry.getValue().serializeTo(out.wrapAsStream());
      }
      // set the endOffset of current MetadataIndexNode and return it
      metadataIndexNode.setEndOffset(out.getPosition());
      return metadataIndexNode;
    }
```

3. If the number of devices exceed `MAX_DEGREE_OF_INDEX_NODE`, the device index level of MetadataIndex tree is generated

```
    for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
      // when constructing from internal node, each node is related to an entry. Once current MetadataIndexNode is full, add it into queue, which is used to generate later tree structure
      if (currentIndexNode.isFull()) {
        addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadaIndexQueue, out);
        // new MetadataIndexNode in LEAF_DEVICE type
        currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
      }
      // new an entry of device metadata, and add it in the current MetadataIndexNode
      currentIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
      // serialize device metadata
      entry.getValue().serializeTo(out.wrapAsStream());
    }
    // add current MetadataIndexNode, which contains some remaining entries, into queue
    addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadaIndexQueue, out);
    // generate root node of device index level according to queue (this method will be described later)
    deviceMetadataIndexNode = generateRootNode(deviceMetadaIndexQueue,
        out, MetadataIndexNodeType.INTERNAL_DEVICE);
    // set the endOffset of current MetadataIndexNode and return it
    deviceMetadataIndexNode.setEndOffset(out.getPosition());
    return deviceMetadataIndexNode;
```

* MetadataIndexConstructor.generateRootNode

The input params of this method:
* Queue\<MetadataIndexNode\> metadataIndexNodeQueue
* TsFileOutput out
* MetadataIndexNodeType type, which indicates the internal nodes type of generated tree. There are two types: when the method is called by measurement index level, it is INTERNAL_MEASUREMENT; when the method is called by device index level, it is INTERNAL_DEVICE 

The method needs to generate a tree structure of nodes in metadataIndexNodeQueue, and return the root node:

```
    int queueSize = metadataIndexNodeQueue.size();
    MetadataIndexNode metadataIndexNode;
    // new a currentIndexNode in specific type
    MetadataIndexNode currentIndexNode = new MetadataIndexNode(type);
    // loop handle the queue until which has only one root node
    while (queueSize != 1) {
      for (int i = 0; i < queueSize; i++) {
        metadataIndexNode = metadataIndexNodeQueue.poll();
        // when constructing from internal node, each node is related to an entry. Once current MetadataIndexNode is full, add it into queue
        if (currentIndexNode.isFull()) {
          addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
          currentIndexNode = new MetadataIndexNode(type);
        }
        // new an entry of the MetadataIndexNode, and add it in the current MetadataIndexNode
        currentIndexNode.addEntry(new MetadataIndexEntry(metadataIndexNode.peek().getName(),
            out.getPosition()));
        // serialize MetadataIndexNode
        metadataIndexNode.serializeTo(out.wrapAsStream());
      }
      // dd current MetadataIndexNode, which contains some remaining entries, into queue
      addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
      currentIndexNode = new MetadataIndexNode(type);
      // count current queue size
      queueSize = metadataIndexNodeQueue.size();
    }
    // return the root node, which is remaining in the queue
    return metadataIndexNodeQueue.poll();
```

* MetadataIndexConstructor.addCurrentIndexNodeToQueue

The input params of this method:
* MetadataIndexNode currentIndexNode
* Queue\<MetadataIndexNode\> metadataIndexNodeQueue
* TsFileOutput out

This method set the endOffset of current MetadataIndexNode, and put it into queue:

```
    currentIndexNode.setEndOffset(out.getPosition());
    metadataIndexNodeQueue.add(currentIndexNode);
```
