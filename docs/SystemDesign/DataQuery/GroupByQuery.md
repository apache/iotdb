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

# Downsampling query

* org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet

The result set of the downsampling query will inherit `GroupByEngineDataSet`, this class contains the following fields:
* protected long queryId
* private long interval
* private long slidingStep

The following two fields are for the entire query, and the time period is left closed and right open, which is `[startTime, endTime)`:
* private long startTime
* private long endTime

The following fields are for the current segment, and the time period is left closed and right open, which is `[startTime, endTime)`:

* protected long curStartTime;
* protected long curEndTime;
* private int usedIndex;
* protected boolean hasCachedTimeInterval;


The core method of `GroupByEngineDataSet` is very easy. First, determine if there is a next segment based on whether there is a cached time period, and return `true`; if not, calculate the segmentation start time and increase `usedIndex` by 1.  If the segment start time has exceeded the query end time, return `false`; otherwise, calculate the query end time, set `hasCachedTimeInterval` to `true`, and return` true`:
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

## Downsampling query without value filter

The downsampling query logic without value filter is mainly in the `GroupByWithoutValueFilterDataSet` class, which inherits` GroupByEngineDataSet`.


This class has the following key fields:
* private Map <Path, GroupByExecutor> pathExecutors classifies aggregate functions for the same `Path` and encapsulates them as` GroupByExecutor`,
  `GroupByExecutor` encapsulates the data calculation logic and method of each Path, which will be described later

* private TimeRange timeRange encapsulates the time interval of each calculation into an object, which is used to determine whether Statistics can directly participate in the calculation
* private Filter timeFilter Generates a user-defined query interval as a Filter object, which is used to filter the available files, chunks, and pages.


First, in the initialization `initGroupBy()` method, the `timeFilter` is calculated based on the expression, and `GroupByExecutor` is generated for each `path`.

First, in the initialization `initGroupBy()` method, the `timeFilter` is calculated based on the expression, and `GroupByExecutor` is generated for each `path`.
The following method is used to convert the result list into a RowRecord. Note that when there are no results in the list, add `null` to the RowRecord:

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
Encapsulating the calculation method of all aggregate functions under the same path, this class has the following key fields:
* private IAggregateReader reader  the `SeriesAggregateReader` used to read the current `Path` data
* private BatchData preCachedData Every time the data read from `Reader` is a batch, and it is likely to exceed the current time period. This `BatchData` will be cached for next use
* private List<Pair<AggregateResult, Integer>> results stores all aggregation methods in the current `Path`, for example: `select count(a), sum(a), avg(b)`, `count` and `sum` can be stored together.
     The `Integer` on the right is used to convert the result set to the order of the user query before converting it to RowRecord.

#### Main method

```
//Read data from the reader and calculate the main method of this class.
private List<Pair<AggregateResult, Integer>> calcResult() throws IOException, QueryProcessException;

//Add aggregation operation for current path
private void addAggregateResult(AggregateResult aggrResult, int index);

//Determine whether the current path has completed all aggregation calculations
private boolean isEndCalc();

//Calculate results from BatchData that did not run out of cache last calculation
private boolean calcFromCacheData() throws IOException;

//Calculation using BatchData
private void calcFromBatch(BatchData batchData) throws IOException;

//Calculate results directly using Page or Chunk's Statistics
private void calcFromStatistics(Statistics statistics) throws QueryProcessException;

//Clear all calculation results
private void resetAggregateResults();

//Iterate through and calculate the data in the page
private boolean readAndCalcFromPage() throws IOException, QueryProcessException;

```

In `GroupByExecutor`, because different aggregate functions of the same path use the same data, the entry method `calcResult` is responsible for reading all the data of the `Path`.
The retrieved data then calls the `calcFromBatch` method to complete the calculation of `BatchData` through all the aggregate functions.

The `calcResult` method returns all AggregateResult under the current Path and the position of the current aggregated value in the user query order. Its main logic is:

```
//Calculate the data left over from the last time, and end the calculation if you can get the results directly
if (calcFromCacheData()) {
    return results;
}

//Because a chunk contains multiple pages, the page of the current chunk must be used up before the next chunk is opened.
if (readAndCalcFromPage()) {
    return results;
}

//If the remaining data is calculated, open a new chunk to continue the calculation.
while (reader.hasNextChunk()) {
    Statistics chunkStatistics = reader.currentChunkStatistics();
      // Determine if Statistics is available and perform calculations
       ....
      // Skip current chunk
      reader.skipCurrentChunk();
      // End calculation if all results have been obtained
      if (isEndCalc()) {
        return true;
      }
      continue;
    }
    //If you cannot use chunkStatistics, you need to use page data to calculate
    if (readAndCalcFromPage()) {
      return results;
    }
}
```

The `readAndCalcFromPage` method is to obtain the page data from the currently opened chunk and calculate the aggregate result.  Returns true when all calculations are completed, otherwise returns false.  The main logic:

```
while (reader.hasNextPage()) {
    Statistics pageStatistics = reader.currentPageStatistics();
    //PageStatistics can only be used if the page does not intersect with other pages
    if (pageStatistics != null) {
        // Determine if Statistics is available and perform calculations
        ....
        // Skip current page
        reader.skipCurrentPage();
        // End calculation if all results have been obtained
        if (isEndCalc()) {
          return true;
        }
        continue;
      }
    }
    // When Statistics is not available, you can only fetch all data for calculation
    BatchData batchData = reader.nextPage();
    if (batchData == null || !batchData.hasCurrent()) {
      continue;
    }
    // If the page just opened exceeds the time range, the data retrieved is cached and the calculation is directly ended.
    if (batchData.currentTime() >= curEndTime) {
      preCachedData = batchData;
      return true;
    }
    //Perform calculations
    calcFromBatch(batchData);
    ...
}

```

The `calcFromBatch` method is to traverse all the aggregate functions to calculate the retrieved BatchData. The main logic is:

```
for (Pair<AggregateResult, Integer> result : results) {
    //If a function has already been calculated, it will not be calculated, such as the minimum calculation.
    if (result.left.isCalculatedAggregationResult()) {
      continue;
    }
    // Perform calculations
    ....
}
//Determine if the data in the current batchdata can still be used next time, if it can be added to the cache
if (batchData.getMaxTimestamp() >= curEndTime) {
    preCachedData = batchData;
}
```

## Aggregated query with value filter
The downsampling query logic with value filtering conditions is mainly in the `GroupByWithValueFilterDataSet` class, which inherits `GroupByEngineDataSet`.

This class has the following key fields:
* private List\<IReaderByTimestamp\> allDataReaderList
* private GroupByPlan groupByTimePlan
* private TimeGenerator timestampGenerator
* private long timestamp is used to cache timestamp for the next group by partition
* private boolean hasCachedTimestamp used to determine whether there is a timestamp cache for the next group by partition
* private int timeStampFetchSize is the size of the group by calculating the batch

First, in the initialization ``initGroupBy ()``method, create a `timestampGenerator` based on the expression; then create a `SeriesReaderByTimestamp` for each time series and place it in the `allDataReaderList` list. After initialization is complete, call the `nextWithoutConstraint ()`method to update the result.  If timestamp is cached for the next group by partition and the time meets the requirements, add it to `timestampArray`, otherwise return the `aggregateResultList` result directly; if timestamp is not cached for the next group by partition, use `timestampGenerator` to traverse:

```
while (timestampGenerator.hasNext()) {
  // Call constructTimeArrayForOneCal () method to get a list of timestamp
  timeArrayLength = constructTimeArrayForOneCal(timestampArray, timeArrayLength);

  // Call the updateResultUsingTimestamps () method to calculate the aggregate result using the timestamp list
  for (int i = 0; i < paths.size(); i++) {
    aggregateResultList.get(i).updateResultUsingTimestamps(
        timestampArray, timeArrayLength, allDataReaderList.get(i));
  }

  timeArrayLength = 0;
  // Determine if it is over
  if (timestamp >= curEndTime) {
    hasCachedTimestamp = true;
    break;
  }
}
```

The `constructTimeArrayForOneCal ()`method traverses timestampGenerator to build a list of timestamps:

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


## Aggregated query with level

After down-frequency query, we could also to count the total number of points of

each node at the given level in current Metadata Tree.

The logic is in the `GroupByTimeDataSet` class.

1. In the beginning, get the final paths group by level and the origin path index to final path.
    > For example, we could get final path `root.sg1` by `root.sg1.d1.s0,root.sg1.d2.s1` and `level=1`.

2. Then, get the down-frequency query result: RowRecord.

3. Finally, merge each RowRecord to NewRecord, which has fields like <final path, count>.

    > For example, we will get new RowRecord `<root.sg1,7>` by `<root.sg1.d1.s0, 3>, <root.sg1.d2.s1, 4>` and level=1.


> Attention:
> 1. only support count aggregation
> 2. root's level == 0