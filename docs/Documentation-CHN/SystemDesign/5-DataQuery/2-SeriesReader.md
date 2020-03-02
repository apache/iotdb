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

IoTDB server 模块共提供 4 种不同形式的针对单个时间序列的读取接口，以支持不同形式的查询。

* 按批遍历形式，返回 BatchData（主要用于不带值过滤的原始数据查询）
* 聚合查询接口 （主要用于聚合查询和降采样查询）
* 单点遍历形式，返回 TimeValuePair （主要用于带值过滤的查询）
* 按递增时间戳查询对应值（主要用于带值过滤的查询）

## 相关接口

以上四种读取单个时间序列数据的方式对应代码里的四个接口

### org.apache.iotdb.tsfile.read.reader.IPointReader

#### 主要方法

```
// 判断是否还有数据点
boolean hasNextTimeValuePair() throws IOException;

// 获得当前数据点，并把游标后移
TimeValuePair nextTimeValuePair() throws IOException;

// 获得当前数据点，不移动游标
TimeValuePair currentTimeValuePair() throws IOException;
```

#### 一般使用流程

```
while (pointReader.hasNextTimeValuePair()) {
	TimeValuePair timeValuePair = pointReader.currentTimeValuePair();
	
	// use current timeValuePair to do some work
	...
	
	// get next timeValuePair
	timeValuePair = mergeReader.nextTimeValuePair();
}
```

### org.apache.iotdb.tsfile.read.reader.IBatchReader

#### 主要方法

```
// 判断是否还有BatchData
boolean hasNextBatch() throws IOException;

// 获得下一个BatchData，并把游标后移
BatchData nextBatch() throws IOException;
```

#### 一般使用流程

```
while (batchReader.hasNextBatch()) {
	BatchData batchData = batchReader.nextBatch();
	
	// use batchData to do some work
	...
}
```

### org.apache.iotdb.db.query.reader.IReaderByTimestamp

#### 主要方法

``` 
// 得到给定时间戳的值，如果不存在返回null（要求传入的 timestamp 是递增的）
Object getValueInTimestamp(long timestamp) throws IOException;
```

#### 一般使用流程

该接口在带值过滤的查询中被使用，TimeGenerator生成时间戳后，使用该接口获得该时间戳对应的value

```
while (timeGenerator.hasNext()) {
	long timestamp = timeGenerator.next();
	Object value = readerByTimestamp.getValueInTimestamp(timestamp);
}
```

### org.apache.iotdb.db.query.reader.seriesRelated.IAggregateReader

#### 主要方法

```
// 判断是否还有Chunk
boolean hasNextChunk() throws IOException;

// 判断是否能够使用当前Chunk的统计信息
boolean canUseCurrentChunkStatistics();

// 获得当前Chunk的统计信息
Statistics currentChunkStatistics();

// 跳过当前Chunk
void skipCurrentChunk();

// 判断当前Chunk是否还有下一个Page
boolean hasNextPage() throws IOException;

// 判断能否使用当前Page的统计信息
boolean canUseCurrentPageStatistics() throws IOException;

// 获得当前Page的统计信息
Statistics currentPageStatistics() throws IOException;

// 跳过当前的Page
void skipCurrentPage();

// 判断是否有下一个重叠的Page
boolean hasNextOverlappedPage() throws IOException;

// 获得当前重叠Page的数据
BatchData nextOverlappedPage() throws IOException;
```

#### 一般使用流程

```
while (aggregateReader.hasNextChunk()) {
  if (aggregateReader.canUseCurrentChunkStatistics()) {
    Statistics chunkStatistics = aggregateReader.currentChunkStatistics();
    
    // do some aggregate calculation using chunk statistics
    ...
    
    aggregateReader.skipCurrentChunk();
    continue;
  }
	  
  while (aggregateReader.hasNextPage()) {
	 if (aggregateReader.canUseCurrentPageStatistics()) {
	   Statistics pageStatistic = aggregateReader.currentPageStatistics();
	   
	   // do some aggregate calculation using page statistics
      ...
	   
	   aggregateReader.skipCurrentPage();
	   continue;
	 }
	 
	 // 遍历所有重叠的page
	 while (aggregateReader.hasNextOverlappedPage()) {
	   BatchData batchData = aggregateReader.nextOverlappedPage();
	   
	   // do some aggregate calculation using batch data
      ...
	 }
  }
}
```

## 具体实现类

上述四个接口都有其对应的实现类，这四个实现类的内部都有一个 SeriesReader 对象，SeriesReader是一个基础的工具类，
封装了对于一个时间序列读取操作的基本方法。所以，下面先介绍一下SeriesReader的实现，然后再依次介绍四个接口实现类的具体实现。

### org.apache.iotdb.db.query.reader.seriesRelated.SeriesReader

首先介绍一下SeriesReader里的几个重要字段

* private final List<TsFileResource> seqFileResource;

	顺序文件列表，因为顺序文件本身就保证有序，且时间戳互不重叠，只需使用List进行存储
  
* private final PriorityQueue<TsFileResource> unseqFileResource;
	
	乱序文件列表，因为乱序文件互相之间不保证顺序性，且可能有重叠，为了保证顺序，使用优先队列进行存储

* private final List<ChunkMetaData> seqChunkMetadatas = new LinkedList<>();

	顺序文件解开后得到的ChunkMetaData列表，本身有序且互不重叠，所以使用List存储
  
* private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas =
      new PriorityQueue<>(Comparator.comparingLong(ChunkMetaData::getStartTime));
      
   乱序文件解开后得到的ChunkMetaData列表，互相之间可能有重叠，为了保证顺序，使用优先队列进行存储

* private boolean hasCachedFirstChunkMetadata;

	是否缓存了第一个chunk meta data
	
* private ChunkMetaData firstChunkMetaData;

	当前第一个chunk meta data的引用

* private PriorityQueue<VersionPair<IPageReader>> overlappedPageReaders =
      new PriorityQueue<>(
          Comparator.comparingLong(pageReader -> 				pageReader.data.getStatistics().getStartTime()));
   
    所有重叠page的reader，按照每个page的起始时间进行排序
    
* private PriorityMergeReader mergeReader = new PriorityMergeReader();

	 PriorityMergeReader可以接收多个数据源，且根据其优先级输出优先级最高的数据点

* private boolean hasCachedNextBatch;

	 是否缓存了下一个batch
	 
* private BatchData cachedBatchData;

	 缓存的下一个batch的引用
	 
下面介绍一下SeriesReader里的重要方法

#### hasNextChunk()

这个方法判断该时间序列还有没有下一个chunk。

如果`hasCachedFirstChunkMetadata`为`true`，则代表当前已经缓存了第一个`ChunkMetaData`，且该`ChunkMetaData`并未被使用，直接返回`true`；若`hasCachedFirstChunkMetadata`为`false`，我们调用`tryToInitFirstChunk()`，尝试去解开顺序文件和乱序文件，并从中选出开始时间最小的chunk meta data赋值给`firstChunkMetaData`，并将`hasCachedFirstChunkMetadata`置为`true`。

#### tryToInitFirstChunk()

这个方法尝试去初始化当前的chunk meta data。

首先调用`tryToFillChunkMetadatas()`去尝试解开顺序和乱序文件，并填充`seqChunkMetadatas`和`unseqChunkMetadatas`，之后再从这二者里选出开始时间最小的。如果只有顺序的，直接取出`seqChunkMetadatas`的第一个；如果只有乱序的，直接取出`unseqChunkMetadatas`的第一个，如果既有顺序的又有乱序的，比较两者第一个的开始时间大小，取较小者取出。

#### tryToFillChunkMetadatas()

这个方法尝试解开顺序和乱序文件。

因为可能一个时间序列对应很多个文件，所以一下子解开全部的文件，可能会导致OOM，所以我们推迟解开文件的时间至其被需要时。

因为顺序文件之间相互不重叠，所以，对于顺序文件，我们只需要保证`seqChunkMetadatas`不为空即可，每次检测到`seqChunkMetadatas`为空后，调用`loadSatisfiedChunkMetadatas(TsFileResource)`方法去解开`seqFileResource`的第一个顺序文件。

而乱序文件之间可能会相互重叠，仅仅解开第一个乱序文件是不够的，还需要解开与`unseqChunkMetadatas`中第一个chunk meta data时间有重叠的所有乱序文件。

#### isChunkOverlapped()

这个方法判断当前的chunk有没有其他与之重叠的chunk存在。

如果`mergeReader`里仍然有数据，或者`seqChunkMetadatas`里有与`firstChunkMetaData`时间重叠的，或者`unseqChunkMetadatas`里有与`firstChunkMetaData`时间重叠的，则返回`true`；反之，返回`false`。

#### currentChunkStatistics()

返回`firstChunkMetaData`的统计信息。

#### skipCurrentChunk()

跳过当前chunk。只需要将`hasCachedFirstChunkMetadata`置为`false`，`firstChunkMetaData`置为`null`即可。

#### hasNextPage()

这个方法判断是否有下一个Page，一般在`firstChunkMetaData`不可直接使用时，继续解成Page。

首先调用`fillOverlappedPageReaders()`去将`firstChunkMetaData`解开为`PageReader`，解开的`PageReader`都放进`overlappedPageReaders`里。并将`hasCachedFirstChunkMetadata`置为`false`，`firstChunkMetaData`置为`null`。若`overlappedPageReaders`为空则返回`false`，若不为空，返回`true`。

#### isPageOverlapped()

这个方法判断当前的Page有没有其他与之重叠的Page存在。

如果`mergeReader`里仍然有数据，或者`seqChunkMetadatas`里有与`overlappedPageReaders`里第一个`PageReader`时间重叠的，或者`unseqChunkMetadatas`里有与`overlappedPageReaders`里第一个`PageReader`时间重叠的，则返回`true`；反之，返回`false`。

#### nextPage()

须与`isPageOverlapped()`方法搭配使用。

当`overlappedPageReaders`里第一个Page没有与之重叠的其他Page时，直接获得`overlappedPageReaders`的第一个Page里符合过滤条件的所有data。

#### currentPageStatistics()

返回`overlappedPageReaders`里第一个Page的统计信息。

#### skipCurrentPage()

跳过当前Page。只需要将`overlappedPageReaders`里第一个PageReader删掉即可。

#### hasNextOverlappedPage()

这个方法判断当前还有没有重叠的Page。

如果`hasCachedNextBatch`为`true`，直接返回`true`。

否则，先调用`putAllDirectlyOverlappedPageReadersIntoMergeReader()`方法，将所有与`overlappedPageReaders`第一个Page有重叠的PageReader放进`mergeReader`里。`mergeReader`里维护了一个`currentLargestEndTime`变量，每次add进新的Reader时被更新，用以记录当前添加进`mergeReader`的最大的结束时间。

然后先从`mergeReader`里取出当前最大的结束时间，作为此次所要返回的batch的结束时间，记为`currentPageEndTime`。接着去遍历`mergeReader`，直到当前的时间戳大于`currentPageEndTime`。

在迭代的过程里，我们也要判断是否有与当前时间戳重叠的file或者chunk或者page（这里之所以还要再做一次判断，是因为，比如当前page是1-30，和他直接相交的page是20-50，还有一个page是40-60，每取一个点判断一次是想把40-60解开），如果有，解开相应的file或者chunk或者page，并将其放入`mergeReader`。完成重叠的判断后，从`mergeReader`中取出相应数据。

完成迭代后将获得数据缓存在`cachedBatchData`中，并将`hasCachedNextBatch`置为`true`。

#### nextOverlappedPage()

将缓存的`cachedBatchData`返回，并将`hasCachedNextBatch`置为`false`。

### org.apache.iotdb.db.query.reader.seriesRelated.SeriesRawDataPointReader

`SeriesRawDataPointReader`实现了`IPointReader`。

其方法`hasNextTimeValuePair()`的核心判断流程是去组合调用SeriesReader的三个方法

```
while (seriesReader.hasNextChunk()) {
  while (seriesReader.hasNextPage()) {
    if (seriesReader.hasNextOverlappedPage()) {
      return true;
    }
  }
}
return false;
```

### org.apache.iotdb.db.query.reader.seriesRelated.SeriesRawDataBatchReader

`SeriesRawDataBatchReader`实现了`IBatchReader`。

其方法`hasNextBatch()`的核心判断流程是

```
// 判断有没有下一个chunk
while (seriesReader.hasNextChunk()) {
  // 判断有没有下一个page
  while (seriesReader.hasNextPage()) {
    // 如果当前page没有重叠
    if (!seriesReader.isPageOverlapped()) {
      // 直接调用nextPage()方法返回一个batch
      batchData = seriesReader.nextPage();
      hasCachedBatchData = true;
      return true;
    }
    // 如果当前还有下一个重叠的Page
    if (seriesReader.hasNextOverlappedPage()) {
      // 调用nextOverlappedPage()方法返回一个batch
      batchData = seriesReader.nextOverlappedPage();
      hasCachedBatchData = true;
      return true;
    }
  }
}
return false;
```

### org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp

`SeriesReaderByTimestamp`实现了`IReaderByTimestamp`。

其方法getValueInTimestamp()的核心流程是找到下一个满足条件的batch data，然后调`BatchData`的`getValueInTimestamp(long)`方法获得相应值，而找下一个batch data的流程是

```
// 判断是否有下一个chunk
while (seriesReader.hasNextChunk()) {
  // 如果当前chunk不满足过滤条件，直接跳过当前chunk，进入下一个循环
  if (!satisfyTimeFilter(seriesReader.currentChunkStatistics())) {
    seriesReader.skipCurrentChunk();
    continue;
  }
  // 判断是否有下一个page
  while (seriesReader.hasNextPage()) {
    // 如果当前page不满足过滤条件，直接跳过当前page，进入下一个循环
    if (!satisfyTimeFilter(seriesReader.currentPageStatistics())) {
      seriesReader.skipCurrentPage();
      continue;
    }
    // 如果当前的page没有重叠
    if (!seriesReader.isPageOverlapped()) {
      // 直接调用nextPage()方法获得下一个batch
      batchData = seriesReader.nextPage();
    } 
    // 如果当前的page有重叠
    else {
      // 调用nextOverlappedPage()方法获得下一个batch
      batchData = seriesReader.nextOverlappedPage();
    }
    // 如果该batch满足要求，则返回true，否则继续搜索下一个batch
    if (batchData.getTimeByIndex(batchData.length() - 1) >= timestamp) {
      return true;
    }
  }
}
return false;
```

### org.apache.iotdb.db.query.reader.seriesRelated.SeriesAggregateReader

`SeriesAggregateReader`实现了`IAggregateReader`

`IAggregateReader`的大部分接口方法都在`SeriesReader`有对应实现，除了`canUseCurrentChunkStatistics()`和`canUseCurrentPageStatistics()`两个方法。

#### canUseCurrentChunkStatistics()

先调用`SeriesReader`的`currentChunkStatistics()`方法，获得当前chunk的统计信息，再调用`SeriesReader`的`isChunkOverlapped()`方法判断当前chunk是否重叠，如果当前chunk不重叠，且其统计信息满足过滤条件，则返回`true`，否则返回`false`。

#### canUseCurrentPageStatistics()

先调用`SeriesReader`的`currentPageStatistics()`方法，获得当前page的统计信息，再调用`SeriesReader`的`isPageOverlapped()`方法判断当前page是否重叠，如果当前page不重叠，且其统计信息满足过滤条件，则返回`true`，否则返回`false`。
