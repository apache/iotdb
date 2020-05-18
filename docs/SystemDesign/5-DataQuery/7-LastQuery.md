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

## Read Last cache data in LastCacheManager

All the Last data are cached in a global structure instance: `LastCacheManager`.

The method `calculateLastPairForOneSeries` starts with reading the cached data via LastCacheManager.

```
TimeValuePair cachedLast = LastCacheManager.getInstance().get(seriesPath.getFullPath());
if (cachedLast != null && cachedLast.getValue() != null) {
  return cachedLast;
}
```
If it is found that the cache has not been written, execute the following standard query process to read the TsFile data.

## Last standard query process

Last standard query process needs to traverse all sequential files and unsequential files to get query results, and finally write the query results back to the MNode cache.  In the algorithm, sequential files and  unsequential  files are processed separately.
- The sequential file is sorted by its writing time, so use the `loadChunkMetadataFromTsFileResource` method directly to get the last` ChunkMetadata`, and get the maximum timestamp and corresponding value through the statistical data of `ChunkMetadata`.
    ```
    if (!seqFileResources.isEmpty()) {
      List<ChunkMetaData> chunkMetadata =
          FileLoaderUtils.loadChunkMetadataFromTsFileResource(
              seqFileResources.get(seqFileResources.size() - 1), seriesPath, context);
      if (!chunkMetadata.isEmpty()) {
        ChunkMetaData lastChunkMetaData = chunkMetadata.get(chunkMetadata.size() - 1);
        Statistics chunkStatistics = lastChunkMetaData.getStatistics();
        resultPair =
            constructLastPair(
                chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
      }
    }
    ```
- Unsequential files need to traverse all `ChunkMetadata` structures to get the maximum timestamp data.  It should be noted that when multiple `ChunkMetadata` have the same timestamp, we take the data in` ChunkMatadata` with the largest `version` value as the result of Last.

    ```
    long version = 0;
    for (TsFileResource resource : unseqFileResources) {
      if (resource.getEndTimeMap().get(seriesPath.getDevice()) < resultPair.getTimestamp()) {
        break;
      }
      List<ChunkMetaData> chunkMetadata =
          FileLoaderUtils.loadChunkMetadataFromTsFileResource(resource, seriesPath, context);
      for (ChunkMetaData chunkMetaData : chunkMetadata) {
        if (chunkMetaData.getEndTime() == resultPair.getTimestamp()
            && chunkMetaData.getVersion() > version) {
          Statistics chunkStatistics = chunkMetaData.getStatistics();
          resultPair =
              constructLastPair(
                  chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
          version = chunkMetaData.getVersion();
        }
      }
    }
    ```
 - Finally write the query results to the MNode's Last cache
    ```
    ((LeafMNode) node).updateCachedLast(resultPair, false, Long.MIN_VALUE);
    ```

## Last cache update strategy

 When inserting new records in `StorageGroupProcessor.java`, `latestFlushTime` is used to maintain the maximum timestamp of data that has been currently written back to disk.
 
 - When the cache is empty, if the timestamp of the latest record is smaller than `latestFlushTime`, do not update the cache as it is not a valid last pair.

The last cache update logic is located in the `put()` method of `StorageGroupLastCache`. An additional parameters `highPriorityUpdate` is introduced.  `highPriorityUpdate` is used to indicate whether this update is high priority. Cache updates caused by new data writing are considered high priority updates, and the update cache defaults to low priority updates when querying. 

 - When there is no record in the cache, the query results are written directly into the cache for the last data that is queried.
 - When there are records in the cache, the timestamp of the query or written data is compared with the timestamp in the current cache.  The written data has a high priority, and the cache is updated when the timestamp is not less than the cache record; the data that is queried has a lower priority and must be greater than the timestamp of the cache record to update the cache.

The specific code is as follows
```
void put(String key, TimeValuePair timeValuePair, boolean highPriorityUpdate) {
  if (timeValuePair == null || timeValuePair.getValue() == null) return;

  try {
    lock.writeLock().lock();
    if (lastCache.containsKey(key)) {
      TimeValuePair cachedPair = lastCache.get(key);
      if (timeValuePair.getTimestamp() > cachedPair.getTimestamp()
          || (timeValuePair.getTimestamp() == cachedPair.getTimestamp()
          && highPriorityUpdate)) {
        cachedPair.setTimestamp(timeValuePair.getTimestamp());
        cachedPair.setValue(timeValuePair.getValue());
      }
    } else {
      TimeValuePair cachedPair =
          new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
      lastCache.put(key, cachedPair);
    }
  } finally {
    lock.writeLock().unlock();
  }
}
```