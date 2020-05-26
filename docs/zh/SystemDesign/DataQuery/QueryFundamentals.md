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

# 查询基础介绍

## 顺序和乱序tsFile文件

在对某一个设备插入数据的过程中，由于插入数据的时间戳的特点会产生顺序和乱序的tsFile文件。如果我们按照时间戳递增的顺序插入数据，那么只会产生顺序文件。顺序数据被写入到磁盘后，一旦新写入的数据时间戳在顺序文件的最大时间戳之前则会产生乱序文件。

IoTDB会将顺序和乱序文件分开存储在data/sequence和data/unsequence文件目录下。在查询过程中也会对顺序和乱序文件中的数据分别进行处理，我们总会使用`QueryResourceManager.java`中的`getQueryDataSource()`方法通过时间序列的全路径得到存储该时间序列的顺序和乱序文件。


## 读取TsFile的一般流程

TsFile 各级结构在前面的[TsFile](../TsFile/TsFile.html)文档中已有介绍，读取一个时间序列的过程需要按照层级各级展开TsFileResource -> TimeseriesMetadata -> ChunkMetadata -> IPageReader -> BatchData。

文件读取的功能方法在
`org.apache.iotdb.db.utils.FileLoaderUtils`

* `loadTimeSeriesMetadata()`用来读取一个TsFileResource对应于某一个时间序列的 TimeseriesMetadata，该方法同时接受一个时间戳的Filter条件来保证该方法返回满足条件的 TimeseriesMetadata，若没有满足条件的 TimeseriesMetadata则返回null。
* `loadChunkMetadataList()`得到这个timeseries所包含的所有ChunkMetadata列表。
* `loadPageReaderList()`可以用来读取一个 ChunkMetadata 对应的 Chunk 所包含的所有page列表，用PageReader来进行访问。

以上在对于时间序列数据的各种读取方法中总会涉及到读取内存和磁盘数据两种情况。

读取内存数据是指读取存在于 Memtable 中但尚未被写入磁盘的数据，例如`loadTimeSeriesMetadata()`中使用`TsFileResource.getTimeSeriesMetadata()`得到一个未被封口的 TimeseriesMetadata。一旦这个 TimeseriesMetadata被刷新到磁盘中之后,我们将只能通过访问磁盘读取到其中的数据。磁盘和内存读取metadata的相关类为 DiskChunkMetadataLoader 和 MemChunkMetadataLoader。

`loadPageReaderList()`读取page数据也是一样，分别通过两个辅助类 MemChunkLoader 和 DiskChunkLoader 进行处理。



## 顺序和乱序文件的数据特点

对于顺序和乱序文件的数据，其数据在文件中的分部特征有所不同。
顺序文件的 TimeseriesMetadata 中所包含的 ChunkMetadata 也是有序的，也就是说如果按照chunkMetadata1, chunkMetadata2的顺序存储，那么将会保证chunkMetadata1.endtime <= chunkMetadata2.startTime。

乱序文件的 TimeseriesMetadata 中所包含的 ChunkMetadata 是无序的，乱序文件中多个 Chunk 所覆盖的数据可能存在重叠，同时也可能与顺序文件中的 Chunk 数据存在重叠。

每个 Chunk 结构内部所包含的 Page 数据总是有序的，不管是从属于顺序文件还是乱序文件。也就是说前一个 Page 的最大时间戳不小于后一个的最小时间戳。因此在查询过程中可以充分利用这种有序性，通过统计信息对 Page 数据进行提前筛选。



## 查询中的数据修改处理

IoTDB的数据删除操作对磁盘数据只记录了 mods 文件，并未真正执行删除逻辑，因此查询时需要考虑数据删除的逻辑。

如果一个文件中有数据被删除了，将删除操作记录到 mods 文件中。记录三列：删除的时间序列，删除范围的最大时间点，删除操作对应的版本。

### 相关类
Modification文件: org.apache.iotdb.db.engine.modification.ModificationFile

删除操作: org.apache.iotdb.db.engine.modification.Modification

### 查询流程处理Modification

对于任意的 TimeseriesMetadata,ChunkMetadata和PageHeader都有相应的modified标记，表示当前的数据块是否存在更改。由于数据删除都是从一个时间节点删除该时间前面的数据，因此如果存在数据删除会导致数据块统计信息中的startTime失效。因此在使用统计信息中的startTime之前必须检查数据块是否包含modification。对于 TimeseriesMetadata，如果删除时间点等于endTime也会导致统计信息中的endTime失效。


![](https://user-images.githubusercontent.com/7240743/78339324-deca5d80-75c6-11ea-8fa8-dbd94232b756.png)

如上图所示，数据修改会对前面提到的TsFile层级数据读取产生影响
* TsFileResource -> TimeseriesMetadata

```
// 只要这个时间序列有对应的删除操作，就标记 TimeseriesMetadata 中的统计信息不可用
FileLoaderUtils.loadTimeseriesMetadata()
```

* TimeseriesMetadata -> List\<ChunkMetadata\>

```
// 对于每个 ChunkMetadata，找到比其 version 大的所有删除操作中最大时间戳, 设置到  ChunkMetadata 的 deleteAt 中，并标记 统计信息不可用
FileLoaderUtils.loadChunkMetadataList()
```

对于以上示例，读取到的 ChunkMetadataList 为

![](https://user-images.githubusercontent.com/7240743/78339335-e427a800-75c6-11ea-815f-16dc5b6ebfa3.png)

* ChunkMetadata -> List\<IPageReader\>

```
// 跳过被完全删除的 Page，将 deleteAt 设置到 PageReader 里，将数据被部分删除的 page 标记统计信息不可用
FileLoaderUtils.loadPageReaderList()
```

* IPageReader -> BatchData

```
// 对于磁盘数据，跳过被删除的和过滤掉的，对于内存数据，跳过被过滤掉的
IPageReader.getAllSatisfiedPageData()
```