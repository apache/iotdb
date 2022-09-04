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

# 最近时间戳 Last 查询

Last 查询的主要逻辑在 LastQueryExecutor

* org.apache.iotdb.db.query.executor.LastQueryExecutor

Last 查询对每个指定的时间序列执行`calculateLastPairForOneSeries`方法。

## 读取 MNode 缓存数据

我们在需要查询的时间序列所对应的 MNode 结构中添加 Last 数据缓存。`calculateLastPairForOneSeries`方法对于某个时间序列的 Last 查询，首先尝试读取 MNode 中的缓存数据。
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
如果发现缓存没有被写入过，则执行下面的标准查询流程读取 TsFile 数据。

## Last 标准查询流程

Last 标准查询流程需要以倒序方式扫描顺序文件和乱序文件，一旦得到满足要求的 Last 查询结果就将其写回到 MNode 缓存中并返回。算法中对顺序文件和乱序文件分别进行处理。
- 顺序文件由于是按照写入时间已经排好序，因此直接使用`loadTimeSeriesMetadata()`方法取出最后一个不为空的`TimeseriesMetadata`。若`TimeseriesMetadata`的统计数据可用即可直接得到 Last 时间戳和对应的值；如不可用则需要使用`loadChunkMetadataList()`方法得到下一层的最后一个`ChunkMetadata`，通过统计数据得到 Last 结果。
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
- 对于乱序文件，需要遍历所有不为空的`TimeseriesMetadata`结构并更新当前最大时间戳的 Last 数据，直到扫描完所有乱序文件为止。需要注意的是当多个`ChunkMetadata`拥有相同的最大时间戳时，我们取`version`值最大的`ChunkMatadata`中的数据作为 Last 的结果。

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
    }
    ```
 - 最后将查询结果写入到 MNode 的 Last 缓存
    ```
    ((LeafMNode) node).updateCachedLast(resultPair, false, Long.MIN_VALUE);
    ```

## Last 缓存更新策略

Last 缓存更新的逻辑位于`LeafMNode`的`updateCachedLast`方法内，这里引入两个额外的参数`highPriorityUpdate`和`latestFlushTime`。`highPriorityUpdate`用来表示本次更新是否是高优先级的，新数据写入而导致的缓存更新都被认为是高优先级更新，而查询时更新缓存默认为低优先级更新。`latestFlushTime`用来记录当前已被写回到磁盘的数据的最大时间戳。

缓存更新的策略如下：

1. 当缓存中没有记录时，对于查询到的 Last 数据，将查询的结果直接写入到缓存中。
2. 当缓存中没有记录时，对于写入的最新数据如果时间戳大于或等于`latestFlushTime`，则将写入的数据写入到缓存中。
3. 当缓存中已有记录时，根据查询或写入的数据时间戳与当前缓存中时间戳作对比。写入的数据具有高优先级，时间戳不小于缓存记录则更新缓存；查询出的数据低优先级，必须大于缓存记录的时间戳才更新缓存。

具体代码如下
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