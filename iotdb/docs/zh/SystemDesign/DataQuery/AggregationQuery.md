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

# 聚合查询

聚合查询的主要逻辑在 AggregateExecutor

* org.apache.iotdb.db.query.executor.AggregationExecutor

## 不带值过滤条件的聚合查询

对于不带值过滤条件的聚合查询，通过 `executeWithoutValueFilter()` 方法获得结果并构建 dataSet。首先使用 `mergeSameSeries()` 方法将对于相同时间序列的聚合查询合并，例如：如果需要计算 count(s1), sum(s2), count(s3), sum(s1)，即需要计算 s1 的两个聚合值，那么将会得到 pathToAggrIndexesMap 结果为：s1 -> 0, 3; s2 -> 1; s3 -> 2。

那么将会得到 `pathToAggrIndexesMap`，其中每一个 entry 都是一个 series 的聚合查询，因此可以通过调用 `groupAggregationsBySeries()` 方法计算出其聚合值 `aggregateResults`。在最后创建结果集之前，需要将其顺序还原为用户查询的顺序。最后使用 `constructDataSet()` 方法创建结果集并返回。

下面详细讲解 `groupAggregationsBySeries()` 方法。首先创建一个 `IAggregateReader`：
```
IAggregateReader seriesReader = new SeriesAggregateReader(
        pathToAggrIndexes.getKey(), tsDataType, context, QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context, timeFilter), timeFilter, null);
```

对于每一个 entry（即 series），首先为其每一种聚合查询创建一个聚合结果 `AggregateResult`，同时维护一个布尔值列表 `isCalculatedList`，对应每一个 `AggregateResult`是否已经计算完成，并记录需要剩余计算的聚合函数数目 `remainingToCalculate`。布尔值列表和这个计数值将会使得某些聚合函数（如 `FIRST_VALUE`）在获得结果后，不需要再继续进行整个循环过程。

接下来，按照 5.2 节所介绍的 `aggregateReader` 使用方法，更新 `AggregateResult`：

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
	 } else {
	 	BatchData batchData = aggregateReader.nextPage();
	 	// do some aggregate calculation using batch data
      ...
	 }	 
  }
}
```

需要注意的是，在对于每一个 result 进行更新之前，需要首先判断其是否已经被计算完（利用 `isCalculatedList` 列表）；每一次更新后，调用 `isCalculatedAggregationResult()` 方法同时更新列表中的布尔值。如果列表中所有值均为 true，即 `remainingToCalculate` 值为 0，证明所有聚合函数结果均已计算完，可以返回。
```
if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
  AggregateResult aggregateResult = aggregateResultList.get(i);
  ... // 更新
  if (aggregateResult.isCalculatedAggregationResult()) {
    isCalculatedList.set(i, true);
    remainingToCalculate--;
    if (remainingToCalculate == 0) {
      return aggregateResultList;
    }
  }
}
```

在使用 `overlapedPageData` 进行更新时，由于获得每一个聚合函数结果都会遍历这个 batchData，因此需要调用 `resetBatchData()` 方法将指针指向其开始位置，使得下一个函数可以遍历。

## 带值过滤条件的聚合查询
对于带值过滤条件的聚合查询，通过 `executeWithoutValueFilter()` 方法获得结果并构建 dataSet。首先根据表达式创建 `timestampGenerator`，然后为每一个时间序列创建一个 `SeriesReaderByTimestamp`，放到 `readersOfSelectedSeries`列表中；为每一个查询创建一个聚合结果 `AggregateResult`，放到 `aggregateResults`列表中。

初始化完成后，调用 `aggregateWithValueFilter()` 方法更新结果：
```
while (timestampGenerator.hasNext()) {
  // 生成 timestamps
  long[] timeArray = new long[aggregateFetchSize];
  int timeArrayLength = 0;
  for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
    if (!timestampGenerator.hasNext()) {
      break;
    }
    timeArray[timeArrayLength++] = timestampGenerator.next();
  }

  // 利用 timestamps 计算聚合结果
  for (int i = 0; i < readersOfSelectedSeries.size(); i++) {
    aggregateResults.get(i).updateResultUsingTimestamps(timeArray, timeArrayLength,
      readersOfSelectedSeries.get(i));
    }
  }
```

## 使用 Level 来统计点数

对于 count 聚合查询，我们也可以使用 level 关键字来进一步汇总点数。

这个逻辑在 `AggregationExecutor`类里。

1. 首先，把所有涉及到的时序按 level 来进行汇集，最后的路径。
    > 例如把 root.sg1.d1.s0,root.sg1.d2.s1 按 level=1 汇集成 root.sg1。

2. 然后调用上述的聚合逻辑求出所有时序的总点数信息，这个会返回 RowRecord 数据结构。

3. 最后，把聚合查询返回的 RowRecord 按上述的 final paths，进行累加，组合成新的 RowRecord。

    > 例如，把《root.sg1.d1.s0,3》，《root.sg1.d2.s1，4》聚合成《root.sg1，7》

> 注意：
> 1. 这里只支持 count 操作
> 2. root 的层级 level=0