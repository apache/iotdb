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
* private Map<Path, GroupByExecutor> pathExecutors 针对于相同 `Path` 的聚合函数进行归类，并封装成 `GroupByExecutor` ,
`GroupByExecutor` 封装了每个 `Path` 的数据计算逻辑和方法，在后面介绍

* private TimeRange timeRange 将每次计算的时间区间封装成对象，用于判断 `Statistics` 是否可以直接参与计算
* private Filter timeFilter   将用户定义的查询区间生成为 `Filter` 对象，用来过滤可用的`文件`、`chunk`、`page`
  

首先，在初始化 `initGroupBy()` 方法中，根据表达式计算出 `timeFilter`，并为每个 `path` 生成 `GroupByExecutor` 。

`nextWithoutConstraint()` 方法通过调用 `GroupByExecutor.calcResult()` 方法计算出每个 `Path` 内的所有聚合方法的聚合值 `aggregateResults`。
以下方法用于将结果列表转化为 RowRecord，需要注意列表中没有结果时， RowRecord 中添加 `null`：
```
for (AggregateResult res : fields) {
  if (res == null) {
    record.addField(null);
    continue;
  }
  record.addField(res.getResult(), res.getResultDataType());
}
```


### GroupByExecutor
封装了相同 path 下的所有聚合函数的计算方法，该类有如下关键字段：
* private IAggregateReader reader 读取当前 `Path` 数据用到的 `SeriesAggregateReader`
* private BatchData preCachedData 每次从 `Reader` 读取的数据是一批，很有可能会超过当前的时间段，那么这个 `BatchData` 就会被缓存留给下一次使用
* private List<Pair<AggregateResult, Integer>> results 存储了当前 `Path` 里所有的聚合方法，
例如：`select count(a),sum(a),avg(b)` ，`count` 和 `sum` 方法就被存到一起。
右侧的 `Integer` 用于结果集转化为RowRecord之前，需要将其顺序还原为用户查询的顺序。

#### 主要方法

```
//从 reader 中读取数据，并计算，该类的主方法。
private List<Pair<AggregateResult, Integer>> calcResult() throws IOException, QueryProcessException;

//添加当前 path 的聚合操作
private void addAggregateResult(AggregateResult aggrResult, int index);

//判断当前 path 是否已经完成了所有的聚合计算
private boolean isEndCalc();

//从上次计算没有使用完缓存的 BatchData 中计算结果
private boolean calcFromCacheData() throws IOException;

//使用 BatchData 计算
private void calcFromBatch(BatchData batchData) throws IOException;

//使用 Page 或 Chunk 的 Statistics 信息直接计算结果
private void calcFromStatistics(Statistics statistics) throws QueryProcessException;

//清空所有计算结果
private void resetAggregateResults();

//遍历并计算 page 中的数据
private boolean readAndCalcFromPage() throws IOException, QueryProcessException;

```

`GroupByExecutor` 中因为相同 `path` 的不同聚合函数使用的数据是相同的，所以在入口方法 `calcResult` 中负责读取该 `Path` 的所有数据,
取出来的数据再调用 `calcFromBatch` 方法完成遍历所有聚合函数对 `BatchData` 的计算。

`calcResult` 方法返回当前 Path 下的所有AggregateResult，以及当前聚合值在用户查询顺序里的位置，其主要逻辑：

```
//把上次遗留的数据先做计算，如果能直接获得结果就结束计算
if (calcFromCacheData()) {
    return results;
}

//因为一个chunk是包含多个page的，那么必须先使用完当前chunk的page，再打开下一个chunk
if (readAndCalcFromPage()) {
    return results;
}

//遗留的数据如果计算完了就要打开新的chunk继续计算
while (reader.hasNextChunk()) {
    Statistics chunkStatistics = reader.currentChunkStatistics();
      // 判断能否使用 Statistics，并执行计算
       ....
      // 跳过当前chunk
      reader.skipCurrentChunk();
      // 如果已经获取到了所有结果就结束计算
      if (isEndCalc()) {
        return true;
      }
      continue;
    }
    //如果不能使用 chunkStatistics 就需要使用page数据计算
    if (readAndCalcFromPage()) {
      return results;
    }
}
```

`readAndCalcFromPage` 方法是从当前打开的chunk中获取page的数据，并计算聚合结果。当完成所有计算时返回 true，否则返回 false。主要逻辑：

```
while (reader.hasNextPage()) {
    Statistics pageStatistics = reader.currentPageStatistics();
    //只有page与其它page不相交时，才能使用 pageStatistics
    if (pageStatistics != null) {
        // 判断能否使用 Statistics，并执行计算
        ....
        // 跳过当前page
        reader.skipCurrentPage();
        // 如果已经获取到了所有结果就结束计算
        if (isEndCalc()) {
          return true;
        }
        continue;
      }
    }
    // 不能使用 Statistics 时，只能取出所有数据进行计算
    BatchData batchData = reader.nextPage();
    if (batchData == null || !batchData.hasCurrent()) {
      continue;
    }
    // 如果刚打开的page就超过时间范围，缓存取出来的数据并直接结束计算
    if (batchData.currentTime() >= curEndTime) {
      preCachedData = batchData;
      return true;
    }
    //执行计算
    calcFromBatch(batchData);
    ...
}

```

`calcFromBatch` 方法是对于取出的BatchData数据，遍历所有聚合函数进行计算，主要逻辑为：

```
for (Pair<AggregateResult, Integer> result : results) {
    //如果某个函数已经完成了计算，就不在进行计算了，比如最小值这种计算
    if (result.left.isCalculatedAggregationResult()) {
      continue;
    }
    // 执行计算
    ....
}
//判断当前的 batchdata 里的数据是否还能被下次使用，如果能则加到缓存中
if (batchData.getMaxTimestamp() >= curEndTime) {
    preCachedData = batchData;
}
```

## 带值过滤条件的聚合查询
带值过滤条件的降采样查询逻辑主要在 `GroupByWithValueFilterDataSet` 类中，该类继承了 `GroupByEngineDataSet`。

该类有如下关键字段：
* ```
  private List<IReaderByTimestamp> allDataReaderList
  ```

* private GroupByPlan groupByTimePlan

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

## 使用Level来汇总降采样的总点数

降采样后，我们也可以使用level关键字来进一步汇总点数。

这个逻辑在 `GroupByTimeDataSet`类里。

1. 首先，把所有涉及到的时序按level来进行汇集，最后的路径。
    > 例如把root.sg1.d1.s0,root.sg1.d2.s1按level=1汇集成root.sg1。

2. 然后调用上述的降采样逻辑求出所有时序的总点数信息，这个会返回RowRecord数据结构。

3. 最后，把降采样返回的RowRecord按上述的final paths，进行累加，组合成新的RowRecord。

    > 例如，把《root.sg1.d1.s0,3》，《root.sg1.d2.s1，4》聚合成《root.sg1，7》


> 注意:
> 1. 这里只支持count操作
> 2. root的层级level=0