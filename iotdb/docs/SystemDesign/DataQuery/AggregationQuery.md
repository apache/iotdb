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

# Aggregation query

The main logic of the aggregation query is in AggregateExecutor

* org.apache.iotdb.db.query.executor.AggregationExecutor

## Aggregation query without value filter

For aggregate queries without value filters, the results are obtained by the `executeWithoutValueFilter()` method and a dataSet is constructed. First use the `mergeSameSeries()` method to merge aggregate queries for the same time series. For example: if you need to calculate count(s1), sum(s2), count(s3), sum(s1), you need to calculate two aggregation values of s1, then the pathToAggrIndexesMap result will be: s1-> 0, 3; s2-> 1; s3-> 2.

Then you will get `pathToAggrIndexesMap`, where each entry is an aggregate query of series, so you can calculate its aggregate value `aggregateResults` by calling the `groupAggregationsBySeries()` method.  Before you finally create the result set, you need to restore its order to the order of the user query.  Finally use the `constructDataSet()` method to create a result set and return it.

The `groupAggregationsBySeries ()` method is explained in detail below.  First create an `IAggregateReader`:
```
IAggregateReader seriesReader = new SeriesAggregateReader(
        pathToAggrIndexes.getKey(), tsDataType, context, QueryResourceManager.getInstance()
        .getQueryDataSource(seriesPath, context, timeFilter), timeFilter, null);
```

For each entry (that is, series), first create an aggregate result `AggregateResult` for each aggregate query. Maintain a boolean list `isCalculatedList`, corresponding to whether each `AggregateResult` has been calculated. Record the remaining number of functions to be calculated in `remainingToCalculate`.  The list of boolean values and this count value will make some aggregate functions (such as `FIRST_VALUE`) not need to continue the entire loop process after obtaining the result.

Next, update `AggregateResult` according to the usage method of `aggregateReader` introduced in Section 5.2:

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

It should be noted that before updating each result, you need to first determine whether it has been calculated (using the isCalculatedList list); after each update, call the isCalculatedAggregationResult () method to update the boolean values in the list  .  If all values in the list are true, that is, the value of `remainingToCalculate` is 0, it proves that all aggregate function results have been calculated and can be returned.
```
if (Boolean.FALSE.equals(isCalculatedList.get(i))) {
  AggregateResult aggregateResult = aggregateResultList.get(i);
  ... // update
  if (aggregateResult.isCalculatedAggregationResult()) {
    isCalculatedList.set(i, true);
    remainingToCalculate--;
    if (remainingToCalculate == 0) {
      return aggregateResultList;
    }
  }
}
```

When using `overlapedPageData` to update, since each batch function result will traverse this batchData, you need to call the` resetBatchData () `method to point the pointer to its starting position, so that the next function can traverse.

## Aggregated query with value filter
For an aggregate query with a value filter, obtain the results through the `executeWithoutValueFilter()` method and build a dataSet.  First create a `timestampGenerator` based on the expression, then create a `SeriesReaderByTimestamp` for each time series and place it in the `readersOfSelectedSeries` list; create an aggregate result for each query as `AggregateResult`, and place it in the `aggregateResults` list.

After initialization is complete, call the `aggregateWithValueFilter()` method to update the result:
```
while (timestampGenerator.hasNext()) {
  // Generate timestamps
  long[] timeArray = new long[aggregateFetchSize];
  int timeArrayLength = 0;
  for (int cnt = 0; cnt < aggregateFetchSize; cnt++) {
    if (!timestampGenerator.hasNext()) {
      break;
    }
    timeArray[timeArrayLength++] = timestampGenerator.next();
  }

  // Calculate aggregate results using timestamps
  for (int i = 0; i < readersOfSelectedSeries.size(); i++) {
    aggregateResults.get(i).updateResultUsingTimestamps(timeArray, timeArrayLength,
      readersOfSelectedSeries.get(i));
    }
  }
```

## Aggregated query with level

After aggregated query, we could also to count the total number of points of

each node at the given level in current Metadata Tree.

The logic is in the `AggregationExecutor` class.

1. In the beginning, get the final paths group by level and the origin path index to final path.
    > For example, we could get final path `root.sg1` by `root.sg1.d1.s0,root.sg1.d2.s1` and `level=1`.

2. Then, get the aggregated query result: RowRecord.

3. Finally, merge each RowRecord to NewRecord, which has fields like <final path, count>.

    > For example, we will get new RowRecord `<root.sg1,7>` by `<root.sg1.d1.s0, 3>, <root.sg1.d2.s1, 4>` and level=1.


> Attention:
> 1. only support count aggregation
> 2. root's level == 0