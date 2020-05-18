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

# TsFile 写流程

* org.apache.iotdb.tsfile.write.*

TsFile 的写入流程如下图所示：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625238-efba2980-467e-11ea-927e-a7021f8153af.png">

其中，每个设备对应一个 ChunkGroupWriter，每个传感器对应一个 ChunkWriter。

文件的写入主要分为三种操作，在图上用 1、2、3 标注

* 1、写内存换冲区
* 2、持久化 ChunkGroup
* 3、关闭文件

## 1、写内存缓冲区

TsFile 文件层的写入接口有两种

* TsFileWriter.write(TSRecord record)

 写入一个设备一个时间戳多个测点。

* TsFileWriter.write(Tablet tablet)

 写入一个设备多个时间戳多个测点。

当调用 write 接口时，这个设备的数据会交给对应的 ChunkGroupWriter，其中的每个测点会交给对应的 ChunkWriter 进行写入。ChunkWriter 完成编码和打包（生成 Page）。


## 2、持久化 ChunkGroup

* TsFileWriter.flushAllChunkGroups()

当内存中的数据达到一定阈值，会触发持久化操作。每次持久化会把当前内存中所有设备的数据全部持久化到磁盘的 TsFile 文件中。每个设备对应一个 ChunkGroup，每个测点对应一个 Chunk。

持久化完成后会在内存中缓存对应的元数据信息，以供查询和生成文件尾部 metadata。

## 3、关闭文件

* TsFileWriter.close()

根据内存中缓存的元数据，生成 TsFileMetadata 追加到文件尾部(`TsFileWriter.flushMetadataIndex()`)，最后关闭文件。

生成 TsFileMetadata 的过程中比较重要的一步是建立元数据索引 (MetadataIndex) 树。正如我们提到过的，元数据索引采用树形结构进行设计的目的是在设备数或者传感器数量过大时，可以不用一次读取所有的 `TimeseriesMetadata`，只需要根据所读取的传感器定位对应的节点，从而减少 I/O，加快查询速度。以下是建立元数据索引树的详细算法和过程：

### org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor

* MetadataIndexConstructor.constructMetadataIndex()

方法的输入包括：
* Map\<String, List\<TimeseriesMetadata\>\> deviceTimeseriesMetadataMap，表示设备到其 TimeseriesMetadata 列表的映射
* TsFileOutput out，是包装好的 TsFile output

整个方法分成三大部分：
1. 在传感器索引层级，把 `deviceTimeseriesMetadataMap` 中的每一个设备及其 `TimeseriesMetadata` 列表，转化为 MetadataIndexNode 并放进 `deviceMetadataIndexMap` 中

```
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      // 如果设备对应的 TimeseriesMetadata 列表为空，则直接跳过
      if (entry.getValue().isEmpty()) {
        continue;
      }
      Queue<MetadataIndexNode> measurementMetadataIndexQueue = new ArrayDeque<>();
      TimeseriesMetadata timeseriesMetadata;
      // 对于每一个设备的 MetadataIndexNode 初始化时，新建一个节点，类型为 LEAF_MEASUREMENT
      MetadataIndexNode currentIndexNode = new MetadataIndexNode(
          MetadataIndexNodeType.LEAF_MEASUREMENT);
      for (int i = 0; i < entry.getValue().size(); i++) {
        timeseriesMetadata = entry.getValue().get(i);
        // 当使用 TimeseriesMetadata 建立叶子节点的时候，每 MAX_DEGREE_OF_INDEX_NODE 个节点对应一个索引项 entry
        if (i % MAX_DEGREE_OF_INDEX_NODE == 0) {
          // 一旦当前节点满了，就将其加入节点队列，此队列用于后面不断形成树的层级结构
          if (currentIndexNode.isFull()) {
            addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
            currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
          }
          // 将 TimeseriesMetadata 对应成一个索引项放入当前索引节点中
          currentIndexNode.addEntry(new MetadataIndexEntry(timeseriesMetadata.getMeasurementId(),
              out.getPosition()));
        }
        // 将 TimeseriesMetadata 序列化，一个 TimeseriesMetadata 的处理结束
        timeseriesMetadata.serializeTo(out.wrapAsStream());
      }
      // 将当前节点加入节点队列，里面包含这个设备中最终剩余的一些索引项
      addCurrentIndexNodeToQueue(currentIndexNode, measurementMetadataIndexQueue, out);
      // 根据队列生成当前设备在传感器索引层级最终的根节点（此方法解析见下），并将设备-根节点对应的映射加入 deviceMetadataIndexMap 中
      deviceMetadataIndexMap.put(entry.getKey(), generateRootNode(measurementMetadataIndexQueue,
          out, MetadataIndexNodeType.INTERNAL_MEASUREMENT));
    }
```

2. 接下来，判断设备数是否超过 `MAX_DEGREE_OF_INDEX_NODE`，如果未超过则可以直接形成元数据索引树的根节点并返回

```
    if (deviceMetadataIndexMap.size() <= MAX_DEGREE_OF_INDEX_NODE) {
      // 新建一个 MetadataIndexNode，由于它一定不是传感器索引层级的叶子节点，因此其类型为 INTERNAL_MEASUREMENT
      MetadataIndexNode metadataIndexNode = new MetadataIndexNode(
          MetadataIndexNodeType.INTERNAL_MEASUREMENT);
      for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
        // 将设备的元数据对应成一个索引项放入当前索引节点中
        metadataIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
        // 将设备元数据序列化
        entry.getValue().serializeTo(out.wrapAsStream());
      }
      // 设置当前 MetadataIndexNode 的 endOffset 并返回
      metadataIndexNode.setEndOffset(out.getPosition());
      return metadataIndexNode;
    }
```

3. 如果设备数超过 `MAX_DEGREE_OF_INDEX_NODE`，则需要形成元数据索引树的设备索引层级

```
    for (Map.Entry<String, MetadataIndexNode> entry : deviceMetadataIndexMap.entrySet()) {
      // 当建立中间节点的时候，每个节点对应一个索引项 entry，一旦当前节点满了，就将其加入节点队列，此队列用于后面不断形成树的层级结构
      if (currentIndexNode.isFull()) {
        addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadaIndexQueue, out);
        // 此时初始化的新 MetadataIndexNode 节点类型为 LEAF_DEVICE
        currentIndexNode = new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
      }
      // 将设备的元数据对应成一个索引项放入当前索引节点中
      currentIndexNode.addEntry(new MetadataIndexEntry(entry.getKey(), out.getPosition()));
      // 将设备元数据序列化
      entry.getValue().serializeTo(out.wrapAsStream());
    }
    // 将当前节点加入节点队列，里面包含最终剩余的一些索引项
    addCurrentIndexNodeToQueue(currentIndexNode, deviceMetadaIndexQueue, out);
    // 根据队列生成设备索引层级最终的根节点（此方法解析见下）
    deviceMetadataIndexNode = generateRootNode(deviceMetadaIndexQueue,
        out, MetadataIndexNodeType.INTERNAL_DEVICE);
    // 设置当前 MetadataIndexNode 的 endOffset 并返回
    deviceMetadataIndexNode.setEndOffset(out.getPosition());
    return deviceMetadataIndexNode;
```

* MetadataIndexConstructor.generateRootNode

方法的输入包括：
* Queue\<MetadataIndexNode\> metadataIndexNodeQueue，是放有 MetadataIndexNode 的队列
* TsFileOutput out，是包装好的 TsFile output
* MetadataIndexNodeType type，是所形成的树的内部节点的类型，有两种：在传感器索引层级调用时，传入 INTERNAL_MEASUREMENT；在设备索引层级调用时，传入 INTERNAL_DEVICE

该方法需要将队列中的 MetadataIndexNode 形成树级结构，并返回根节点：

```
    int queueSize = metadataIndexNodeQueue.size();
    MetadataIndexNode metadataIndexNode;
    // 根据需要的类型初始化一个 currentIndexNode
    MetadataIndexNode currentIndexNode = new MetadataIndexNode(type);
    // 循环处理队列，直到队列中只有根节点时结束
    while (queueSize != 1) {
      for (int i = 0; i < queueSize; i++) {
        metadataIndexNode = metadataIndexNodeQueue.poll();
        // 当建立中间节点的时候，每个节点对应一个索引项 entry，一旦当前节点满了，就将其加入节点队列
        if (currentIndexNode.isFull()) {
          addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
          currentIndexNode = new MetadataIndexNode(type);
        }
        // 将节点的元数据对应成一个索引项放入当前索引节点中
        currentIndexNode.addEntry(new MetadataIndexEntry(metadataIndexNode.peek().getName(),
            out.getPosition()));
        // 将节点序列化
        metadataIndexNode.serializeTo(out.wrapAsStream());
      }
      // 将当前节点加入节点队列，里面包含最终剩余的一些索引项
      addCurrentIndexNodeToQueue(currentIndexNode, metadataIndexNodeQueue, out);
      currentIndexNode = new MetadataIndexNode(type);
      // 计算当前队列的大小，判断其是否为1
      queueSize = metadataIndexNodeQueue.size();
    }
    // 返回队列中最终剩余的根节点
    return metadataIndexNodeQueue.poll();
```

* MetadataIndexConstructor.addCurrentIndexNodeToQueue

方法的输入包括：
* MetadataIndexNode currentIndexNode，是当前需要加入队列的 MetadataIndexNode
* Queue\<MetadataIndexNode\> metadataIndexNodeQueue，是放有 MetadataIndexNode 的队列
* TsFileOutput out，是包装好的 TsFile output

该方法直接设定当前节点的 endOffset，并将该节点加入队列中：

```
    currentIndexNode.setEndOffset(out.getPosition());
    metadataIndexNodeQueue.add(currentIndexNode);
```
