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

# 降采样查询

* org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet

降采样查询的结果集都会继承 `GroupByEngineDataSet`，该类包含如下字段：
* protected long queryId
* private long interval
* private long slidingStep

以下两个字段针对整个查询，时间段为左闭右开，即 `[startTime, endTime)`：
* private long startTime
* private long endTime

以下字段针对当前分段，时间段为左闭右开，即 `[curStartTime, curEndTime)`
* protected long curStartTime;
* protected long curEndTime;
* private int usedIndex;
* protected boolean hasCachedTimeInterval;


`GroupByEngineDataSet` 的核心方法很容易，首先根据是否有缓存的时间段判断是否有下一分段，有则返回 `true`；如果没有就计算分段开始时间，将 `usedIndex` 增加1。如果分段开始时间已经超过了查询结束时间，返回 `false`，否则计算查询结束时间，将 `hasCachedTimeInterval` 置为`true`，并返回 `true`：
```
protected boolean hasNextWithoutConstraint() {
  if (hasCachedTimeInterval) {
    return true;
  }

  curStartTime = usedIndex * slidingStep + startTime;
  usedIndex++;
  if (curStartTime < endTime) {
    hasCachedTimeInterval = true;
    curEndTime = Math.min(curStartTime + interval, endTime);
    return true;
  } else {
    return false;
  }
}
```

## 不带值过滤条件的降采样查询

不带值过滤条件的降采样查询逻辑主要在 `GroupByWithoutValueFilterDataSet` 类中，该类继承了 `GroupByEngineDataSet`。

该类有如下关键字段：
* private Map<Path, List<Integer>> pathToAggrIndexesMap 类似聚合查询中的 `pathToAggrIndexesMap`，其中每一个 entry 都是一个 series 的查询
* private Map<Path, IAggregateReader> aggregateReaders 查询的序列与其 reader 的对应映射
* private List<BatchData> cachedBatchDataList 记录缓存的 batchData 列表
* private Filter timeFilter
* private GroupByPlan groupByPlan
  
首先，在初始化 `initGroupBy()` 方法中，根据表达式计算出 `timeFilter`，并通过 `path` 计算出 `pathToAggrIndexesMap` 和 `aggregateReaders`。`aggregateReaders` 列表中的每一个 `SeriesAggregateReader` 用于读取一个时间序列。

`nextWithoutConstraint()` 方法通过调用 `nextIntervalAggregation()` 方法计算出其聚合值 `aggregateResults`。在将结果集转化为RowRecord之前，需要将其顺序还原为用户查询的顺序。以下方法用于将结果列表转化为 RowRecord，需要注意列表中没有结果时， RowRecord 中添加类型为 `null` 的 `Field`：
```
if (aggregateResultList.length == 0) {
  record.addField(new Field(null));
} else {
  for (AggregateResult res : aggregateResultList) {
    record.addField(res.getResult(), res.getDataType());
  }
}
```

下面详细讲解 `nextIntervalAggregation()` 方法。类似5.4节中聚合查询的流程中 `groupAggregationsBySeries()` 方法，对于每一个 entry（即series），首先为其每一种聚合查询创建一个聚合结果 `AggregateResult`，同时维护一个布尔值列表 `isCalculatedList`，对应每一个 `AggregateResult`是否已经计算完成，并记录需要剩余计算的聚合函数数目 `remainingToCalculate`。布尔值列表和这个计数值将会使得某些聚合函数（如 `FIRST_VALUE`）在获得结果后，不需要再继续进行整个循环过程。

根据 `aggregateReaders` 获得所查询的时间序列路径对应的 `IAggregateReader`，接下来，按照5.2节所介绍的 `aggregateReader` 使用方法，更新 `AggregateResult`：

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

`calcBatchData()` 方法用于根据 batchData 计算聚合结果。在 `batchData.hasCurrent() && batchData.currentTime() < curStartTime)` 的条件下，需要不断调用 `batchData.next()` 来得到符合要求的 batchData 段。更新 `AggregateResult` 后，由于获得每一个聚合函数结果都会遍历这个 batchData，因此需要调用 `resetBatchData()` 方法将指针指向其开始位置，使得下一个函数可以遍历。

需要注意的是，在对于每一个result进行更新之前，需要首先判断其是否已经被计算完（利用 `isCalculatedList` 列表）；每一次更新后，调用 `isCalculatedAggregationResult()` 方法同时更新列表中的布尔值。如果列表中所有值均为true，即 `remainingToCalculate` 值为0，证明所有聚合函数结果均已计算完，可以返回。
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

类似地，`isEndCal()` 方法用于判断 batchData 是不是已经计算完，如果已经计算完，也需要更新 `isCalculatedList` 列表和 `remainingToCalculate` 值：
```
private boolean isEndCalc(AggregateResult function, BatchData lastBatch) {
  return (lastBatch != null && lastBatch.hasCurrent() && lastBatch.currentTime() >= curEndTime)
    || function.isCalculatedAggregationResult();
  }
```

## 带值过滤条件的聚合查询
带值过滤条件的降采样查询逻辑主要在 `GroupByWithValueFilterDataSet` 类中，该类继承了 `GroupByEngineDataSet`。

该类有如下关键字段：
* private List<IReaderByTimestamp> allDataReaderList
* private GroupByPlan groupByPlan
* private TimeGenerator timestampGenerator
* private long timestamp 用于为下一个 group by 分区缓存 timestamp
* private boolean hasCachedTimestamp 用于判断是否有为下一个 group by 分区缓存 timestamp
* private int timeStampFetchSize 是 group by 计算 batch 的大小

首先，在初始化 `initGroupBy()` 方法中，根据表达式创建 `timestampGenerator`；然后为每一个时间序列创建一个 `SeriesReaderByTimestamp`，放到 `allDataReaderList`列表中

初始化完成后，调用 `nextWithoutConstraint()` 方法更新结果。如果有为下一个 group by 分区缓存 timestamp，且时间符合要求，则将其加入 `timestampArray` 中，否则直接返回 `aggregateResultList` 结果；在没有为下一个 group by 分区缓存 timestamp 的情况下，使用 `timestampGenerator` 进行遍历：
```
while (timestampGenerator.hasNext()) {
  // 调用 constructTimeArrayForOneCal() 方法，得到 timestamp 列表
  timeArrayLength = constructTimeArrayForOneCal(timestampArray, timeArrayLength);

  // 调用 updateResultUsingTimestamps() 方法，使用 timestamp 列表计算聚合结果
  for (int i = 0; i < paths.size(); i++) {
    aggregateResultList.get(i).updateResultUsingTimestamps(
        timestampArray, timeArrayLength, allDataReaderList.get(i));
  }

  timeArrayLength = 0;
  // 判断是否到结束
  if (timestamp >= curEndTime) {
    hasCachedTimestamp = true;
    break;
  }
}
```
其中的 `constructTimeArrayForOneCal()` 方法遍历 timestampGenerator 构建 timestamp 列表：
```
for (int cnt = 1; cnt < timeStampFetchSize && timestampGenerator.hasNext(); cnt++) {
  timestamp = timestampGenerator.next();
  if (timestamp < curEndTime) {
    timestampArray[timeArrayLength++] = timestamp;
  } else {
    hasCachedTimestamp = true;
    break;
  }
}
```