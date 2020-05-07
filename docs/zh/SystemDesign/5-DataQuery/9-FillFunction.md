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

# 空值填充

空值填充的主要逻辑在 FillQueryExecutor

* org.apache.iotdb.db.query.executor.FillQueryExecutor

IoTDB 中支持两种填充方式，Previous填充和Linear填充。

## Previous填充

Previous填充是使用离查询时间戳最近且小于等于查询时间戳的值来进行填充的一种方式，下图展示了一种可能的填充情况。
```
                                     |
+-------------------------+          |
|     seq files           |          |
+-------------------------+          |
                                     |
            +---------------------------+
            |     unseq files        |  |
            +---------------------------+ 
                                     |
                                     |
                                 queryTime
```

## 设计原理

在实际生产场景中，可能需要在大量的顺序和乱序TsFile中查找可用的填充值，因此解开大量TsFile文件会成为性能瓶颈。我们设计的思路在于尽可能的减少需要解开tsFile文件的个数，从而加速找到填充值这一过程。

## 顺序文件查找"最近点"

首先在顺序文件中查询满足条件的"最近点"。由于所有顺序文件及其所有包含的chunk数据都已经按照顺序排列，因此这个"最近点"可以通过`retrieveValidLastPointFromSeqFiles()`方法很容易找到。

这个找到的"最近点"是可以成为Previous填充的最终值的，因此它在接下来的搜索中能够被用来作为下界来寻找其他更好的的填充值。接下来只有那些时间戳大于这个下界的点才是Previous填充的结果值。

下例中的unseq 文件由于其最大值没有超过"最近点"，因此不需要再进一步搜索。
```
                        last point
                              |       |
            +---------------------------+
            |   seq file      |       | |
            +---------------------------+ 
  +-------------------------+ |       |
  |      unseq file         | |       |
  +-------------------------+ |       |
                              |       |
                                      |
                                queryTime
```

## 筛选乱序文件

筛选并解开乱序文件的逻辑在成员函数`UnpackOverlappedUnseqFiles(long lBoundTime)`中，该方法接受一个下界参数，将满足要求的乱序文件解开且把它们的TimeseriesMetadata结构存入`unseqTimeseriesMetadataList`中。

`UnpackOverlappedUnseqFiles(long lBoundTime)`方法首先用 `lBoundTime`排除掉不满足要求的乱序文件，剩下被筛选后的乱序文件都一定会包含`queryTime`时间戳，即startTime <= queryTime and queryTime <= endTime。如果乱序文件的start time大于`lBoundTime`，更新`lBoundTime`。

下图中，case 1中的`lBoundTime`将会被更新，而case 2中的则不会被更新。
```
case1           case2
  |              |                     |
  |          +---------------------------+
  |          |   |    unseq file       | |
  |          +---------------------------+ 
  |              |                     |
  |              |                     |
lBoundTime   lBoundTime            queryTime
```

```
while (!unseqFileResource.isEmpty()) {
  // The very end time of unseq files is smaller than lBoundTime,
  // then skip all the rest unseq files
  if (unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice()) < lBoundTime) {
    return;
  }
  TimeseriesMetadata timeseriesMetadata =
      FileLoaderUtils.loadTimeSeriesMetadata(
          unseqFileResource.poll(), seriesPath, context, timeFilter, allSensors);
  if (timeseriesMetadata != null && timeseriesMetadata.getStatistics().canUseStatistics()
      && lBoundTime <= timeseriesMetadata.getStatistics().getEndTime()) {
    lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getStartTime());
    unseqTimeseriesMetadataList.add(timeseriesMetadata);
    break;
  }
}
```

接下来我们可以使用得到的Timeseries Metadata来查找所有其他重叠的乱序文件。需要注意的依旧是当一个重叠的乱序文件被找到后，需要按情况更新`lBoundTime`。

```
while (!unseqFileResource.isEmpty()
        && (lBoundTime <= unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice()))) {
  TimeseriesMetadata timeseriesMetadata =
      FileLoaderUtils.loadTimeSeriesMetadata(
          unseqFileResource.poll(), seriesPath, context, timeFilter, allSensors);
  unseqTimeseriesMetadataList.add(timeseriesMetadata);
  // update lBoundTime if current unseq timeseriesMetadata's last point is a valid result
  if (timeseriesMetadata.getStatistics().canUseStatistics()
      && endtimeContainedByTimeFilter(timeseriesMetadata.getStatistics())) {
    lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getEndTime());
  }
}
```

## 组合最终结果

最终结果在方法`getFillResult()` 中生成。
```
public TimeValuePair getFillResult() throws IOException {
    TimeValuePair lastPointResult = retrieveValidLastPointFromSeqFiles();
    UnpackOverlappedUnseqFiles(lastPointResult.getTimestamp());
    
    long lastVersion = 0;
    PriorityQueue<ChunkMetadata> sortedChunkMetatdataList = sortUnseqChunkMetadatasByEndtime();
    while (!sortedChunkMetatdataList.isEmpty()
        && lastPointResult.getTimestamp() <= sortedChunkMetatdataList.peek().getEndTime()) {
      ChunkMetadata chunkMetadata = sortedChunkMetatdataList.poll();
      TimeValuePair lastChunkPoint = getChunkLastPoint(chunkMetadata);
      if (shouldUpdate(lastPointResult.getTimestamp(), lastVersion,
          lastChunkPoint.getTimestamp(), chunkMetadata.getVersion())) {
        lastPointResult = lastChunkPoint;
        lastVersion = chunkMetadata.getVersion();
      }
    }
    return lastPointResult;
}
```

# Linear 填充

对于T时间的 Linear Fill 线性填充值是由该时间序列的两个相关值做线性拟合得到的：T之前的最近时间戳对应的值，T之后的最早时间戳对应的值。
基于这种特点，线性填充只能被应用于数字类型如：int, double, float。

## 计算前时间值
前时间值使用与 Previous Fill 中相同方式计算.

## 计算后时间值
后时间值使用聚合运算中的"MIN_TIME"和"FIRST_VALUE"，分别计算出T时间之后最近的时间戳和对应的值，并组成time-value对返回。