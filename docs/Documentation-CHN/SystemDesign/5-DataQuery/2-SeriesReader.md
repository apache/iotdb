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

# 查询基础组件

## 设计原理

IoTDB server 模块共提供 3 种不同形式的针对单个时间序列的读取接口，以支持不同形式的查询。

* 原始数据查询接口，返回 BatchData，可带时间过滤条件或值过滤条件，两种过滤不可同时存在。
* 聚合查询接口 （主要用于聚合查询和降采样查询）
* 按递增时间戳查询对应值的接口（主要用于带值过滤的查询）

## 相关接口

以上三种读取单个时间序列数据的方式对应代码里的三个接口

### org.apache.iotdb.tsfile.read.reader.IBatchReader

#### 主要方法

```
// 判断是否还有 BatchData
boolean hasNextBatch() throws IOException;

// 获得下一个 BatchData，并把游标后移
BatchData nextBatch() throws IOException;
```

#### 使用流程

```
while (batchReader.hasNextBatch()) {
	BatchData batchData = batchReader.nextBatch();
	
	// use batchData to do some work
	...
}
```

### org.apache.iotdb.db.query.reader.series.IAggregateReader

#### 主要方法

```
// 判断是否还有 Chunk
boolean hasNextChunk() throws IOException;

// 判断是否能够使用当前 Chunk 的统计信息
boolean canUseCurrentChunkStatistics();

// 获得当前 Chunk 的统计信息
Statistics currentChunkStatistics();

// 跳过当前 Chunk
void skipCurrentChunk();

// 判断当前Chunk是否还有下一个 Page
boolean hasNextPage() throws IOException;

// 判断能否使用当前 Page 的统计信息
boolean canUseCurrentPageStatistics() throws IOException;

// 获得当前 Page 的统计信息
Statistics currentPageStatistics() throws IOException;

// 跳过当前的 Page
void skipCurrentPage();

// 获得当前 Page 的数据
BatchData nextPage() throws IOException;
```

#### 一般使用流程

```
while (aggregateReader.hasNextChunk()) {
  if (aggregateReader.canUseCurrentChunkStatistics()) {
    Statistics chunkStatistics = aggregateReader.currentChunkStatistics();
    
    // 用 chunk 层的统计信息计算
    ...
    
    aggregateReader.skipCurrentChunk();
    continue;
  }
  
  // 把当前 chunk 中的 page 消耗完
  while (aggregateReader.hasNextPage()) {
	 if (aggregateReader.canUseCurrentPageStatistics()) {
	   // 可以用统计信息
	   Statistics pageStatistic = aggregateReader.currentPageStatistics();
	   
	   // 用 page 层的统计信息计算
	   ...
	  
	   aggregateReader.skipCurrentPage();
	   continue;
	 } else {
	   // 不能用统计信息，需要用数据计算
	   BatchData batchData = aggregateReader.nextOverlappedPage();
	   
	   // 用 batchData 计算
	   ...
	 }
  }
}
```

### org.apache.iotdb.db.query.reader.IReaderByTimestamp

#### 主要方法

``` 
// 得到给定时间戳的值，如果不存在返回 null（要求传入的 timestamp 是递增的）
Object getValueInTimestamp(long timestamp) throws IOException;

// 给定一批递增时间戳的值，返回一批结果（减少方法调用次数）
Object[] getValuesInTimestamps(long[] timestamps) throws IOException;
```

#### 一般使用流程

该接口在带值过滤的查询中被使用，TimeGenerator生成时间戳后，使用该接口获得该时间戳对应的value

```
Object value = readerByTimestamp.getValueInTimestamp(timestamp);

or

Object[] values = readerByTimestamp.getValueInTimestamp(timestamps);
```

## 具体实现类

上述三个接口都有其对应的实现类，由于以上三种查询有共性，我们设计了一个基础的 SeriesReader 工具类，封装了对于一个时间序列读取操作的基本方法，帮助实现以上三种接口。下面首先介绍 SeriesReader 的设计原理，然后再依次介绍三个接口的具体实现。

### org.apache.iotdb.db.query.reader.series.SeriesReader

#### 设计思想

背景知识：TsFile 文件（TsFileResource）解开后可以得到 ChunkMetadata，ChunkMetadata 解开后可以得到一堆 PageReader，PageReader 可以直接返回 BatchData 数据点。

为了支持以上三种接口

数据按照粒度从大到小分成四种：文件，Chunk，Page，相交数据点。在原始数据查询中，最大的数据块返回粒度是一个 page，如果一个 page 和其他 page 由于乱序写入相互覆盖了，就解开成数据点做合并。聚合查询中优先使用 Chunk 的统计信息，其次是 Page 的统计信息，最后是相交数据点。

设计原则是能用粒度大的就不用粒度小的。

首先介绍一下SeriesReader里的几个重要字段

```

/*
 * 文件层
 */
private final List<TsFileResource> seqFileResource;
	顺序文件列表，因为顺序文件本身就保证有序，且时间戳互不重叠，只需使用List进行存储
	
private final PriorityQueue<TsFileResource> unseqFileResource;
	乱序文件列表，因为乱序文件互相之间不保证顺序性，且可能有重叠，为了保证顺序，使用优先队列进行存储
	
/*
 * chunk 层
 * 
 * 三个字段之间数据永远不重复，first 永远是第一个（开始时间最小）
 */
private ChunkMetaData firstChunkMetaData;
	填充 chunk 层时优先填充此字段，保证这个 chunk 具有当前最小开始时间
	
private final List<ChunkMetaData> seqChunkMetadatas;
	顺序文件解开后得到的 ChunkMetaData 存放在此，本身有序且互不重叠，所以使用 List 存储

private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas;
	乱序文件解开后得到的 ChunkMetaData 存放在此，互相之间可能有重叠，为了保证顺序，使用优先队列进行存储
	
/*
 * page 层
 *
 * 两个字段之间数据永远不重复，first 永远是第一个（开始时间最小）
 */ 
private VersionPageReader firstPageReader;
	开始时间最小的 page reader
	
private PriorityQueue<VersionPageReader> cachedPageReaders;
	当前获得的所有 page reader，按照每个 page 的起始时间进行排序
	
/*
 * 相交数据点层
 */ 
private PriorityMergeReader mergeReader;
	本质上是多个带优先级的 page，按时间戳从低到高输出数据点，时间戳相同时，保留优先级高的

/*
 * 相交数据点产出结果的缓存
 */ 
private boolean hasCachedNextOverlappedPage;
	是否缓存了下一个batch
	
private BatchData cachedBatchData;
	缓存的下一个batch的引用
```
	 
下面介绍一下SeriesReader里的重要方法

#### hasNextChunk()

* 主要功能：判断该时间序列还有没有下一个chunk。

* 约束：在调用这个方法前，需要保证 `SeriesReader` 内已经没有 page 和 数据点 层级的数据了，也就是之前解开的 chunk 都消耗完了。

* 实现：如果 `firstChunkMetaData` 不为空，则代表当前已经缓存了第一个 `ChunkMetaData`，且未被使用，直接返回`true`；

	尝试去解开第一个顺序文件和第一个乱序文件，填充 chunk 层。并解开与 `firstChunkMetadata` 相重合的所有文件。

#### isChunkOverlapped()

* 主要功能：判断当前的 chunk 有没有与其他 Chunk 有重叠

* 约束：在调用这个方法前，需要保证 chunk 层已经缓存了 `firstChunkMetadata`，也就是调用了 hasNextChunk() 并且为 true。

* 实现：直接把 `firstChunkMetadata` 与 `seqChunkMetadatas` 和 `unseqChunkMetadatas` 相比较。因为此前已经保证所有和 `firstChunkMetadata` 相交的文件都被解开了。

#### currentChunkStatistics()

返回 `firstChunkMetaData` 的统计信息。

#### skipCurrentChunk()

跳过当前 chunk。只需要将`firstChunkMetaData `置为`null`。

#### hasNextPage()

* 主要功能：判断 SeriesReader 中还有没有已经解开的 page，如果有相交的 page，就构造 `cachedBatchData` 并缓存，否则缓存 `firstPageReader`。

* 实现：如果已经缓存了 `cachedBatchData` 就直接返回。如果有相交数据点，就构造 `cachedBatchData`。如果已经缓存了 `firstPageReader`，就直接返回。

	如果当前的 `firstChunkMetadata` 还没有解开，就解开与之重叠的所有 ChunkMetadata，构造 firstPageReader。
	
	判断，如果 `firstPageReader` 和 `cachedPageReaders` 相交，则构造 `cachedBatchData`，否则直接返回。

#### isPageOverlapped()

* 主要功能：判断当前的 page 有没有与其他 page 有重叠

* 约束：在调用这个方法前，需要保证调用了 hasNextPage() 并且为 true。也就是，有可能缓存了一个相交的 `cachedBatchData`，或者缓存了不相交的 `firstPageReader`。

* 实现：先判断有没有 `cachedBatchData`，如果没有，就说明当前 page 不相交，则 `mergeReader` 里没数据。再判断 `firstPageReader` 是否与 `cachedPageReaders` 中的 page 相交。

#### currentPageStatistics()

返回 `firstPageReader` 的统计信息。

#### skipCurrentPage()

跳过当前Page。只需要将 `firstPageReader` 置为 null。

#### nextPage()

* 主要功能：返回下一个相交或不想交的 page

* 约束：在调用这个方法前，需要保证调用了 hasNextPage() 并且为 true。也就是，有可能缓存了一个相交的 `cachedBatchData`，或者缓存了不相交的 `firstPageReader`。

* 实现：如果 `hasCachedNextOverlappedPage` 为 true，说明缓存了一个相交的 page，直接返回 `cachedBatchData`。否则当前 page 不相交，直接从 firstPageReader 里拿当前 page 的数据。

#### hasNextOverlappedPage()

* 主要功能：内部方法，用来判断当前有没有重叠的数据，并且构造相交的 page 并缓存。

* 实现：如果 `hasCachedNextOverlappedPage` 为 `true`，直接返回 `true`。

	否则，先调用 `tryToPutAllDirectlyOverlappedPageReadersIntoMergeReader()` 方法，将 `cachedPageReaders` 中所有与 `firstPageReader` 有重叠的放进 `mergeReader` 里。`mergeReader` 里维护了一个 `currentLargestEndTime` 变量，每次添加进新的 Reader 时被更新，用以记录当前添加进 `mergeReader` 的 page 的最大结束时间。	
	然后先从`mergeReader`里取出当前最大的结束时间，作为第一批数据的结束时间，记为`currentPageEndTime`。接着去遍历`mergeReader`，直到当前的时间戳大于`currentPageEndTime`。
	
	每从 mergeReader 移出一个点前，我们先要判断是否有与当前时间戳重叠的file或者chunk或者page（这里之所以还要再做一次判断，是因为，比如当前page是1-30，和他直接相交的page是20-50，还有一个page是40-60，每取一个点判断一次是想把40-60解开），如果有，解开相应的file或者chunk或者page，并将其放入`mergeReader`。完成重叠的判断后，从`mergeReader`中取出相应数据。

	完成迭代后将获得数据缓存在 `cachedBatchData` 中，并将 `hasCachedNextOverlappedPage` 置为 `true`。

#### nextOverlappedPage()

将缓存的`cachedBatchData`返回，并将`hasCachedNextOverlappedPage`置为`false`。

### org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader

`SeriesRawDataBatchReader`实现了`IBatchReader`。

其方法`hasNextBatch()`的核心判断流程是

```
// 有缓存了 batch，直接返回
if (hasCachedBatchData) {
  return true;
}

/*
 * 如果 SeriesReader 里还有 page，返回 page
 */
if (readPageData()) {
  hasCachedBatchData = true;
  return true;
}

/*
 * 如果有 chunk，并且有 page，返回 page
 */
while (seriesReader.hasNextChunk()) {
  if (readPageData()) {
    hasCachedBatchData = true;
    return true;
  }
}
return hasCachedBatchData;
```

### org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp

`SeriesReaderByTimestamp` 实现了 `IReaderByTimestamp`。

设计思想：当给一个时间戳要查询值时，这个时间戳可以转化为一个 time >= x 的过滤条件。不断更新这个过滤条件，并且跳过不满足的文件，chunk 和 page。

实现方式：

```
/*
 * 优先判断下一个 page 有没有当前所查时间，能跳过就跳过
 */
if (readPageData(timestamp)) {
  return true;
}

/*
 * 判断下一个 chunk 有没有当前所查时间，能跳过就跳过
 */
while (seriesReader.hasNextChunk()) {
  Statistics statistics = seriesReader.currentChunkStatistics();
  if (!satisfyTimeFilter(statistics)) {
    seriesReader.skipCurrentChunk();
    continue;
  }
  /*
   * chunk 不能跳过，继续到 chunk 里检查 page
   */
  if (readPageData(timestamp)) {
    return true;
  }
}
return false;
```

### org.apache.iotdb.db.query.reader.series.SeriesAggregateReader

`SeriesAggregateReader` 实现了 `IAggregateReader`

`IAggregateReader`的大部分接口方法都在`SeriesReader`有对应实现，除了`canUseCurrentChunkStatistics()`和`canUseCurrentPageStatistics()`两个方法。

#### canUseCurrentChunkStatistics()

设计思想：可以用统计信息的条件是当前 chunk 不重叠，并且满足过滤条件。

先调用`SeriesReader`的`currentChunkStatistics()`方法，获得当前chunk的统计信息，再调用`SeriesReader`的`isChunkOverlapped()`方法判断当前chunk是否重叠，如果当前chunk不重叠，且其统计信息满足过滤条件，则返回`true`，否则返回`false`。

#### canUseCurrentPageStatistics()

设计思想：可以用统计信息的条件是当前 page 不重叠，并且满足过滤条件。

先调用`SeriesReader`的 `currentPageStatistics()` 方法，获得当前page的统计信息，再调用`SeriesReader` 的 `isPageOverlapped()` 方法判断当前 page 是否重叠，如果当前page不重叠，且其统计信息满足过滤条件，则返回`true`，否则返回`false`。