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

# Last query

The main logic of Last query is in LastQueryExecutor

* org.apache.iotdb.db.query.executor.LastQueryExecutor

The Last query executes the `calculateLastPairForOneSeries` method for each specified time series.

## Read MNode cache data

We add a Last data cache to the MNode structure corresponding to the time series that needs to be queried.

`calculateLastPairForOneSeries` method For the last query of a certain time series, first try to read the cached data in the MNode.

```
try {
  node = IoTDB.metaManager.getDeviceNodeWithAutoCreateStorageGroup(seriesPath.toString());
} catch (MetadataException e) {
  throw new QueryProcessException(e);
}
if (((LeafMNode) node).getCachedLast() != null) {
  return ((LeafMNode) node).getCachedLast();
}
```
If it is found that the cache has not been written, execute the following standard query process to read the TsFile data.

## Last standard query process

Last standard query process needs to scan sequential files and unsequential files in a reverse order to get query result, and finally write the query result back to the MNode cache.  In the algorithm, sequential files and  unsequential  files are processed separately.
- The sequential files are sorted by its writing time, so use the `loadTimeSeriesMetadata()` method directly to get the last valid ` TimeseriesMetadata`. If the statistics of `TimeseriesMetadata` is available, the Last pair could be returned directly, otherwise we need to call `loadChunkMetadataList()` to get the last `ChunkMetadata` structure and obtain the Last time-value pair via the statistical data of `ChunkMetadata`.
    ```
    for (int i = seqFileResources.size() - 1; i >= 0; i--) {
        TimeseriesMetadata timeseriesMetadata = FileLoaderUtils.loadTimeSeriesMetadata(
                seqFileResources.get(i), seriesPath, context, null, sensors);
        if (timeseriesMetadata != null) {
          if (!timeseriesMetadata.isModified()) {
            Statistics timeseriesMetadataStats = timeseriesMetadata.getStatistics();
            resultPair = constructLastPair(
                    timeseriesMetadataStats.getEndTime(),
                    timeseriesMetadataStats.getLastValue(),
                    tsDataType);
            break;
          } else {
            List<ChunkMetadata> chunkMetadataList = timeseriesMetadata.loadChunkMetadataList();
            if (!chunkMetadataList.isEmpty()) {
              ChunkMetadata lastChunkMetaData = chunkMetadataList.get(chunkMetadataList.size() - 1);
              Statistics chunkStatistics = lastChunkMetaData.getStatistics();
              resultPair =
                  constructLastPair(
                      chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
              break;
            }
          }
        }
      }
    ```
- For unsequential files, we need to traverse all valid `TimeseriesMetadata` structures and keep updating the current Last timestamp to find the biggest timestamp. It should be noted that when multiple `ChunkMetadata` have the same timestamp, we take the data in` ChunkMatadata` with the largest `version` value as the result of Last.

    ```
    long version = 0;
    for (TsFileResource resource : unseqFileResources) {
      if (resource.getEndTime(seriesPath.getDevice()) < resultPair.getTimestamp()) {
        continue;
      }
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(resource, seriesPath, context, null, sensors);
      if (timeseriesMetadata != null) {
        for (ChunkMetadata chunkMetaData : timeseriesMetadata.loadChunkMetadataList()) {
          if (chunkMetaData.getEndTime() > resultPair.getTimestamp()
              || (chunkMetaData.getEndTime() == resultPair.getTimestamp()
              && chunkMetaData.getVersion() > version)) {
            Statistics chunkStatistics = chunkMetaData.getStatistics();
            resultPair =
                constructLastPair(
                    chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
            version = chunkMetaData.getVersion();
          }
        }
      }
    }
    ```
 - Finally write the query results to the MNode's Last cache
    ```
    ((LeafMNode) node).updateCachedLast(resultPair, false, Long.MIN_VALUE);
    ```

## Last cache update strategy

The last cache update logic is located in the `UpdateCachedLast` method of` LeafMNode`. Here, two additional parameters `highPriorityUpdate` and` latestFlushTime` are introduced.  `highPriorityUpdate` is used to indicate whether this update is high priority. Cache updates caused by new data writing are considered high priority updates, and the update cache defaults to low priority updates when querying.  `latestFlushTime` is used to record the maximum timestamp of data that has been currently written back to disk.

The cache update strategy is as follows:

1. When there is no record in the cache, the query results are written directly into the cache for the last data that is queried.
2. When there is no record in the cache, if the latest data written is a timestamp greater than or equal to `latestFlushTime`, the written data is written to the cache.
3. When there are records in the cache, the timestamp of the query or written data is compared with the timestamp in the current cache.  The written data has a high priority, and the cache is updated when the timestamp is not less than the cache record; the data that is queried has a lower priority and must be greater than the timestamp of the cache record to update the cache.

The specific code is as follows
```
public synchronized void updateCachedLast(
  TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) return;
    
    if (cachedLastValuePair == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        cachedLastValuePair =
            new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > cachedLastValuePair.getTimestamp()
        || (timeValuePair.getTimestamp() == cachedLastValuePair.getTimestamp()
            && highPriorityUpdate)) {
      cachedLastValuePair.setTimestamp(timeValuePair.getTimestamp());
      cachedLastValuePair.setValue(timeValuePair.getValue());
    }
}
```