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

# Query Fundamentals

This chapter introduces some basic concepts, terms and things to pay attention in IoTDB Query design. 
Designers and developers who hope to start with IoTDB query design may find this guide helpful, as some concepts will be treated as common sense and not explained in detail in the following chapters. 

## Sequential and un-sequential TsFiles

IoTDB uses TsFile as its data storage format. Sequential and un-sequential TsFiles are generated separately in terms of their different data insert patterns.
Basically when time series data are written in strict ascending time order, only sequential TsFiles will be formed. 
After these sequential TsFiles are flushed onto disk, the current maximum timestamp of these sequential data will be recorded, and all the timeseries data with timestamps less than this maximum timestamp will be kept in un-sequential files.

IoTDB stores sequential and un-sequential TsFiles separately under `data/sequence` and `data/unsequence` directory. These files can be uniformly accessed via `getQueryDataSource()` in `QueryResourceManager.java`, by giving a full path of the timeseries.

It should be noted that, in the following query documents, we tend to use `seq file` to represent Sequential files for short. For un-sequential ones we use `unseq file`. Sometimes `unordered file` or `out-of-order file` are also used as aliases of un-sequential files.

## General process of reading TsFile and other memory data

The multi-level structure of TsFile is introduced in [TsFile](../TsFile/TsFile.md). 
For each timeseries, we always follow the query routine across 5 levels: TsFileResource -> TimeseriesMetadata -> ChunkMetadata -> IPageReader -> BatchData

The function methods for file reading are in `org.apache.iotdb.db.utils.FileLoaderUtils`

* `loadTimeSeriesMetadata()` reads the TimeseriesMetadata of a timeseries in a TsFileResource. If a time filter is set for this method, only those TimeseriesMetadata that satisfies this filter will be returned. `loadTimeSeriesMetadata()` returns null otherwise.
* `loadChunkMetadataList()` can load a ChunkMetadata list for a TimeseriesMetadata。
* `loadPageReaderList()` loads a page list contained in chunkMetadata，and can be accessed with `PageReader`。

The implementation of the above methods must consider two cases of reading: 
1. Reading memory data
2. Reading disk data.

Memory data reading means to read data cached in "Memtable" which is not yet flushed into disk storage.
In `loadTimeSeriesMetadata()`, it obtains an unsealed TimeseriesMetadata using `TsFileResource.getTimeSeriesMetadata()`.
We call it unsealed because users may be still writing data into this Timeseries, and it will remain unsealed until IoTDB flushes it into disk.

`DiskChunkMetadataLoader` and `MemChunkMetadataLoader` provide access to read disk and memory chunk metadata.

It is almost the same in `loadPageReaderList()` to read Page data. 
`MemChunkLoader` and `DiskChunkLoader` support for memory page loading and disk page loading. 

## Data characteristics of sequential and unsequential files.
For data in sequential and unsequential files, the partial characteristics of the data in the file are different.
Timeseries data in seq files is in "overall" ascending order. Specifically, all the ChunkMetadata in a timeseries stored in seq files are in the right order.
Therefore, if we have `ChunkMetadata1` and `ChunkMetadata2` kept in a seq file, it is guaranteed that 
```
chunkMetadata1.endtime <= chunkMetadata2.startTime.
```

While it is different that the ChunkMetadatas are stored unordered in unseq files. Some chunks might be positioned in the right order but most of them are overlapped with each other. There might be overlapping between seq file chunks and unseq file chunks as well.

The page data in a single chunk is always sequential, no matter it is stored in seq files or unseq files. 
That means, two orderings within pages are guaranteed:
* Timestamps of data in a single page are in ascending order.
* Different page timestamps are in ascending order. e.g. Page1.maxTime <= Page2.minTime.

This certain ordering will be fully utilized to accelerate query process within our design.

## Data modification processing in the query

Data deletion in IoTDB records a series of mods file for disk data. The data is not really deleted, so we need to consider the existence of modifications in query.
 If any data is deleted from a file, record the deletion to the mods file. Record three columns: the time series of the deletion, the maximum time point of the deletion range, and the version corresponding to the deletion operation.
### Related class

Modification file: org.apache.iotdb.db.engine.modification.ModificationFile

Deletion operation: org.apache.iotdb.db.engine.modification.Modification

Deletion interval: org.apache.iotdb.tsfile.read.common.TimeRange

### Modification File
Data deletion in IoTDB is accomplished by writing Modification files for related TsFiles.

In IoTDB version 0.11.0, the deletion format in Modification file has been changed. Now each line contains a start time and end time representing a delete range for a timeseries path. 
For Modification files generated in past version of IoTDB with only a "deleteAt" timestamp, they could still be recognized, interpreting the "deleteAt" field as end time.

### TimeRange  
Correspondingly, TimeRange is the medium that deletions exist within memory.

All deletion TimeRanges are both left-close and right-close intervals. We use Long.MIN_VALUE and Long.MAX_VALUE to refer to infinity and negative infinity timestamp.

### Query processing with deletion ranges
When querying a TVList, the TimeRanges are sorted and merged before a TVList tries to access them. 
For example, we have [1,10], [5,12], [15,20], [16,21] in the original list, then they will be preprocessed to [1,12] and [15,21].
For cases when there are a large number of deletion operations, it would be helpful to exclude deleted data.

More specifically, since the TVList stores ordered timestamp data, using a sorted TimeRange list is easy to filter out deleted data.
We use a cursor to mark which TimeRange in the list is currently in use. Intervals before the current one are no longer needed to be traversed.
```
private boolean isPointDeleted(long timestamp) {
  while (deletionList != null && deleteCursor < deletionList.size()) {
    if (deletionList.get(deleteCursor).contains(timestamp)) {
      return true;
    } else if (deletionList.get(deleteCursor).getMax() < timestamp) {
      deleteCursor++;
    } else {
      return false;
    }
  }
  return false;
}
```


### Query with Modifications

For any TsFile data units, their metadata structures including TimeseriesMetadata, ChunkMetadata and PageHeader use a `modified` flag to indicate whether this data unit is modified or not.
Upon setting this `modified` flag to "true", the integrity of this data unit is supposed to be damaged and some statistics turns invalid. 

![](https://user-images.githubusercontent.com/59866276/87266560-27fc4880-c4f8-11ea-9c8f-6794a9c599cb.jpg)

Modifications affects timeseries reading process in the 5 levels mentioned before:
* TsFileResource -> TimeseriesMetadata

```
// Set the statistics in TimeseriesMetadata unusable if the timeseries contains deletion operations 
FileLoaderUtils.loadTimeseriesMetadata()
```

* TimeseriesMetadata -> List\<ChunkMetadata\>

```
// For each ChunkMetadata, find the largest timestamp in all deletion operations whose version is larger than it. Set deleted time to ChunkMetadata. 
// set the statistics in ChunkMetadata unusable if it is affected by deletion
FileLoaderUtils.loadChunkMetadataList()
```

E.g., the got ChunkMetadatas are:
![](https://user-images.githubusercontent.com/59866276/87266976-0b144500-c4f9-11ea-95b3-15d60d2b7416.jpg)
* ChunkMetadata -> List\<IPageReader\>

```
// Skip the fully deleted page, set deleteAt into PageReader，Set the page statistics unusable if it is affected by deletion
FileLoaderUtils.loadPageReaderList()
```

* IPageReader -> BatchData

```
// For disk page, skip the data points that be deleted and filtered out. For memory data, skip data points be filtered out.
IPageReader.getAllSatisfiedPageData()
```
