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

## General query process

The multi-level structure of TsFile is introduced in [1-TsFile](/#/SystemDesign/progress/chap1/sec1). 
For each timeseries, we always follow the query routine across 5 levels: TsFileResource -> TimeseriesMetadata -> ChunkMetadata -> IPageReader -> BatchData

The file access utility methods are in `org.apache.iotdb.db.utils.FileLoaderUtils`

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

## Data orderings in TsFiles

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

## Modifications in query

Data deletion in IoTDB records a series of mods file for disk data. The data is not really deleted, so we need to consider the existence of modifications in query.

For any TsFile data units, their metadata structures including TimeseriesMetadata, ChunkMetadata and PageHeader use a `modified` flag to indicate whether this data unit is modified or not.
Upon setting this `modified` flag to "true", the integrity of this data unit is supposed to be damaged and some statistics turns invalid. In the following document chapters, sometimes we have to check this flag before using statistics data.