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

# Fill Function

The main logic of Fill function is in FillQueryExecutor

* org.apache.iotdb.db.query.executor.FillQueryExecutor

Two fill functions are support in IoTDB, Previous Fill and Linear Fill. 

# Previous Fill

The target of Previous Fill is to find out the time-value that has the largest timestamp, which is also smaller or equal to `queryTime`. The graph below displays one scenario of the function.
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

## Design principle
In the real scenarios, the previous fill result may exist in a single page in tons of TsFiles. To accelerate the search procedure, **the key point** is to filter as many tsFiles out as possible.

The main logic of TsFile filtering work is in `UnpackOverlappedUnseqFiles()`.

## Find the "lower bound" from all the sequential files
We first find out the "last point" of sequential files that satisfies fill queryTime. Since all the chunk data in sequential files are in decent order, this last point can be easily obtained by `retrieveValidLastPointFromSeqFiles()` function.

After the above "last point " is found via `retrieveValidLastPointFromSeqFiles()`, the timestamp of this last point can be treated as a lower bound in the following searching. Only those points whose timestamps exceeds this lower bound are candidates to become the final result. 

In the below example, the unseq file is not qualified to be processed as its end time is before "last point".

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

## Filter unseq files with lower bound
The selecting of unseq files is in method `UnpackOverlappedUnseqFiles(long lBoundTime)`. All the unseq files are filtered by the parameter `lBoundTime`, and unpacked into Timeseries Metadata. We cache all the `TimeseriesMetadata` into `unseqTimeseriesMetadataList`. 

Firstly the method selects the last unseq file that satisfies timeFilter. Its end time must be larger than `lBoundTime`, otherwise this file will be skipped. It is guaranteed that the rest files will cross the queryTime line, that is startTime <= queryTime and queryTime <= endTime. Then we can update the `lBoundTime` with the start time of the unpacked `TimeseriesMetadata`.

In the below graph, the `lBoundTime` is updated with start time of unseq file in case1, and not updated in case2.
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

Next, the selected Timeseries Metadata could be used to find all the other overlapped unseq files. It should be noted that each time when a overlapped unseq file is added, the `lBoundTime` could be updated if the end time of this file satisfies `queryTime`

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

## Compose the Previous Fill result

The method `getFillResult()` will search among the sequential and un-sequential file to find the result. When searching in a chunk or a page, the statistic data should be utilized first.

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

# Linear Fill

The result of Linear Fill functions at timestamp "T" is calculated by performing a linear fitting method on two timeseries values, one is at the closest timestamp before T, and the other is at the closest timestamp after T. Linear Fill function calculation only supports numeric types including int, double and float.

## Calculating before timestamp value
Before timestamp value is calculated in the same way with Previous Fill.

## Calculating after timestamp value
For after timestamp value, the time-value pair is generated by two aggregation querys "MIN_TIME" and "FIRST_VALUE". `AggregationExecutor.aggregateOneSeries()` is used to calculate these two aggregation results and compose a time-value pair.
