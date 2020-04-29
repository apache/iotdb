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

# Query basic components

## Design principle

The IoTDB server module provides a total of 3 different forms of reading interfaces for a single time series to support different forms of queries.

* The raw data query interface returns BatchData with time or value filters. Both filters cannot exist at the same time.
* Aggregation query interface (mainly used for aggregation query and downsampling query)
* Interface for querying corresponding values by increasing timestamp (mainly used for queries with value filtering)

## Related interfaces

The above three ways to read a single time series data correspond to the three interfaces in the code.

### org.apache.iotdb.tsfile.read.reader.IBatchReader

#### Main method

```
// Determine if there is still BatchData
boolean hasNextBatch() throws IOException;

// Get the next BatchData and move the cursor back
BatchData nextBatch() throws IOException;
```

#### use process

```
while (batchReader.hasNextBatch()) {
	BatchData batchData = batchReader.nextBatch();
	
	// use batchData to do some work
	...
}
```

### org.apache.iotdb.db.query.reader.series.IAggregateReader

#### Main method

```
// Determine if there is still Chunk
boolean hasNextChunk() throws IOException;

// Determine if you can use the current Chunk statistics
boolean canUseCurrentChunkStatistics();

// Get statistics for the current Chunk
Statistics currentChunkStatistics();

// Skip the current Chunk
void skipCurrentChunk();

// Determine if the current Chunk has a next Page
boolean hasNextPage() throws IOException;

// Determine if the statistics of the current Page can be used
boolean canUseCurrentPageStatistics() throws IOException;

// Get statistics for the current Page
Statistics currentPageStatistics() throws IOException;

// Skip the current Page
void skipCurrentPage();

// Get data for the current Page
BatchData nextPage() throws IOException;
```

#### General use process

```
while (aggregateReader.hasNextChunk()) {
  if (aggregateReader.canUseCurrentChunkStatistics()) {
    Statistics chunkStatistics = aggregateReader.currentChunkStatistics();
    
    // Calculate with the statistics of the chunk layer
    ...
    
    aggregateReader.skipCurrentChunk();
    continue;
  }
  
  // Run out of pages in the current chunk
  while (aggregateReader.hasNextPage()) {
	 if (aggregateReader.canUseCurrentPageStatistics()) {
	   // Can use statistics
	   Statistics pageStatistic = aggregateReader.currentPageStatistics();
	   
	   // Calculate with page-level statistics
	   ...
	  
	   aggregateReader.skipCurrentPage();
	   continue;
	 } else {
	   // Can't use statistics, need to calculate with data
	   BatchData batchData = aggregateReader.nextOverlappedPage();
	   
	   // Calculate with batchData
	   ...
	 }
  }
}
```

### org.apache.iotdb.db.query.reader.IReaderByTimestamp

#### Main method

``` 
// Get the value of the given timestamp, or null if none exists (requires that the //timestamp passed in is incremented)
Object getValueInTimestamp(long timestamp) throws IOException;

// Given a batch of timestamp values, return a batch of results (reduce the number //of method calls)
Object[] getValuesInTimestamps(long[] timestamps) throws IOException;
```

#### General use process

This interface is used in queries with value filtering. After TimeGenerator generates a timestamp, use this interface to obtain the value corresponding to the timestamp.

```
Object value = readerByTimestamp.getValueInTimestamp(timestamp);

or

Object[] values = readerByTimestamp.getValueInTimestamp(timestamps);
```

## Concrete implementation class

The above three interfaces all have their corresponding implementation classes. As the above three queries have similarities, we have designed a basic SeriesReader tool class that encapsulates the basic methods for a time series read operation to help implement the above three interfaces. The following first introduces the design principle of the SeriesReader, and then introduces the specific implementation of the three interfaces in turn.

### org.apache.iotdb.db.query.reader.series.SeriesReader

#### Design ideas

Background knowledge: TsFile source (TsFilesource) can be unpacked to get ChunkMetadata, ChunkMetadata can be unpacked to get a bunch of PageReader, PageReader can directly return BatchData data points.

To support the above three interfaces

The data is divided into four types according to the granularity: file, chunk, page, and intersecting data points. In the original data query, the largest data block return granularity is a page. If a page and other pages cover each other due to out-of-order writing, they are unraveled into data points for merging. Aggregate queries use Chunk's statistics first, followed by Page's statistics, and finally intersecting data points.

The design principle is to use the larger granularity instead of the smaller granularity.

First introduce some important fields in SeriesReader

```

/*
 * File layer
 */
private final List<TsFileResource> seqFileResource;
	Sequential file list, because the sequential file itself is guaranteed to be ordered, and the timestamps do not overlap each other, just use List to store
	
private final PriorityQueue<TsFileResource> unseqFileResource;
	Out-of-order file list, because out-of-order files do not guarantee order between each other, and may overlap
	
/*
 * chunk layer
 * 
 * The data between the three fields is never duplicated, and first is always the first (minimum start time)
 */
private ChunkMetaData firstChunkMetaData;
	This field is filled first when filling the chunk layer to ensure that this chunk has the current minimum start time
	
private final List<ChunkMetaData> seqChunkMetadatas;
	The ChunkMetaData obtained after the sequential files are unpacked is stored here. It is ordered and does not overlap with each other, so the List is used for storage.

private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas;
	ChunkMetaData obtained after unordered files are stored is stored here, there may be overlap between each other, in order to ensure order, priority queue is used for storage
	
/*
 * page layer
 *
 * The data between the two fields is never duplicated, and first is always the first (minimum start time)
 */ 
private VersionPageReader firstPageReader;
	Page reader with the smallest start time
	
private PriorityQueue<VersionPageReader> cachedPageReaders;
	All page readers currently acquired, sorted by the start time of each page
	
/*
 * Intersecting data point layer
 */ 
private PriorityMergeReader mergeReader;
	Essentially, there are multiple pages with priority, and the data points are output from low to high according to the timestamp. When the timestamps are the same, the high priority page is retained.

/*
 * Caching of results from intersecting data points
 */ 
private boolean hasCachedNextOverlappedPage;
	Whether the next batch is cached
	
private BatchData cachedBatchData;
	Cached reference to the next batch
```

The following describes the important methods in SeriesReader

#### hasNextChunk()

* Main function: determine whether the time series has the next chunk.

* Constraint: Before calling this method, you need to ensure that there is no page and data point level data in the `SeriesReader` , that is, all the previously unlocked chunks have been consumed. .

* Implementation: If `firstChunkMetaData` is not empty, it means that the first` ChunkMetaData` is currently cached and not used, and returns `true` directly;

  Try to untie the first sequential file and the first out-of-order file to fill the chunk layer. And unpack all files that coincide with `firstChunkMetadata`.

#### isChunkOverlapped()

* Main function: determine whether the current chunk overlaps with other Chunk

* Constraint: Before calling this method, make sure that the chunk layer has cached `firstChunkMetadata`, that is, hasNextChunk () is called and is true.

* Implementation: Compare `firstChunkMetadata` with` seqChunkMetadatas` and `unseqChunkMetadatas` directly. Because it has been guaranteed that all files that intersect with `firstChunkMetadata` will be unzipped.

#### currentChunkStatistics()

Returns statistics for `firstChunkMetaData`.

#### skipCurrentChunk()

Skip the current chunk. Just set `firstChunkMetaData` to` null`.

#### hasNextPage()

* Main function: determine whether there are already unwrapped pages in the SeriesReader. If there are intersecting pages, construct `cachedBatchData` and cache, otherwise cache` firstPageReader`.

* Implementation: If `cachedBatchData` is already cached, return directly. If there are intersecting data points, a `cachedBatchData` is constructed. If `firstPageReader` is already cached, return directly.

	If the current `firstChunkMetadata` has not been solved, then all the ChunkMetadata which overlaps with it are constructed to construct the firstPageReader.
	
	Determine if `firstPageReader` and` cachedPageReaders` intersect, then construct `cachedBatchData`, otherwise return directly.

#### isPageOverlapped()

* Main function: determine whether the current page overlaps with other pages

* Constraint: Before calling this method, you need to ensure that hasNextPage () is called and is true. That is, it is possible to cache an intersecting `cachedBatchData` or an disjoint` firstPageReader`.

* Implementation: First determine if there is `cachedBatchData`, if not, it means that the current page does not intersect, then there is no data in` mergeReader`. Then determine whether `firstPageReader` intersects with page in` cachedPageReaders`.

#### currentPageStatistics()

Returns statistics for `firstPageReader`.

#### skipCurrentPage()

Skip the current Page. Just set `firstPageReader` to null.

#### nextPage()

* Main function: return to the next intersecting or unwanted page

* Constraint: Before calling this method, you need to ensure that hasNextPage () is called and is true. That is, it is possible to cache an intersecting `cachedBatchData` or an disjoint` firstPageReader`.

* Implementation: If `hasCachedNextOverlappedPage` is true, it means that an intersecting page is cached, and` cachedBatchData` is returned directly. Otherwise, the current page does not intersect, and the data of the current page is taken directly from firstPageReader.

#### hasNextOverlappedPage()

* Main function: internal method, used to determine whether there is currently overlapping data, and construct intersecting pages and cache them.

* Implementation: If `hasCachedNextOverlappedPage` is` true`, return `true` directly.

	Otherwise, first call the `tryToPutAllDirectlyOverlappedPageReadersIntoMergeReader ()` method, and put all of the cachedPageReaders that overlap with the firstPageReader into the mergeReader. `mergeReader` maintains a` currentLargestEndTime` variable, which is updated each time a new Reader is added to record the maximum end time of the page currently added to `mergeReader`.
	Then first take out the current maximum end time from `mergeReader`, as the end time of the first batch of data, record it as` currentPageEndTime`. Then go through `mergeReader` until the current timestamp is greater than` currentPageEndTime`.
	
	Before moving a point from mergeReader, we must first determine whether there is a file, chunk, or page that overlaps with the current timestamp. (The reason for this is to make another judgment here because, for example, the current page is 1-30, and he directly The intersecting pages are 20-50, and there is another page 40-60. Every time you take a point, you want to unlock 40-60. If so, unpack the corresponding file or chunk or page and put it in Enter `mergeReader`. After the overlap judgment is completed, the corresponding data is taken from `mergeReader`.

	After completing the iteration, the data will be cached in `cachedBatchData`, and` hasCachedNextOverlappedPage` will be set to `true`.

#### nextOverlappedPage()

Return cached `cachedBatchData` and set` hasCachedNextOverlappedPage` to `false`.

### org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader

`SeriesRawDataBatchReader` implements` IBatchReader`.

The core judgment flow of its method `hasNextBatch ()` is

```
// There are cached batches, return directly
if (hasCachedBatchData) {
  return true;
}

/*
 * If there are still pages in the SeriesReader, return to page
 */
if (readPageData()) {
  hasCachedBatchData = true;
  return true;
}

/*
 * If there is a chunk and a page, return page
 */
while (seriesReader.hasNextChunk()) {
  if (readPageData()) {
    hasCachedBatchData = true;
    return true;
  }
}
return hasCachedBatchData;
```

### org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp

`SeriesReaderByTimestamp` implements `IReaderByTimestamp`.

Design idea: When a time stamp is used to query the value, this time stamp can be converted into a filter condition with time> = x. Keep updating this filter, and skip files, chunks and pages that don't meet.

Method to realize:

```
/*
 * Prioritize whether the next page is currently checked, skip it if you can
 */
if (readPageData(timestamp)) {
  return true;
}

/*
 * Determine if the next chunk has the current search time, skip it if it can
 */
while (seriesReader.hasNextChunk()) {
  Statistics statistics = seriesReader.currentChunkStatistics();
  if (!satisfyTimeFilter(statistics)) {
    seriesReader.skipCurrentChunk();
    continue;
  }
  /*
   * The chunk cannot be skipped, continue to check the page in the chunk
   */
  if (readPageData(timestamp)) {
    return true;
  }
}
return false;
```

### org.apache.iotdb.db.query.reader.series.SeriesAggregateReader

`SeriesAggregateReader`implements `IAggregateReader`

Most interface methods of `IAggregateReader` have corresponding implementations in` SeriesReader`, except for `canUseCurrentChunkStatistics ()` and `canUseCurrentPageStatistics ()`.

#### canUseCurrentChunkStatistics()

Design Idea: The conditions under which the statistical information can be used are that the current chunks do not overlap and meet the filtering conditions.

First call the `CurrentChunkStatistics ()` method of `SeriesReader` to obtain the statistics of the current chunk, then call the` isChunkOverlapped () `method of` SeriesReader` to determine whether the current chunks overlap. If the current chunks do not overlap and their statistics meet the filtering If true, return `true`, otherwise return` false`.

#### canUseCurrentPageStatistics()

Design idea: The conditions under which the statistical information can be used are that the current pages do not overlap and meet the filter conditions.

First call the `CurrentPageStatistics ()` method of `SeriesReader` to obtain the statistical information of the current page, and then call the` isPageOverlapped () `method of` SeriesReader` to determine whether the current pages overlap. If the current pages do not overlap, and their statistics meet the filtering If true, return `true`, otherwise return` false`.