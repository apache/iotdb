/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.series.SeriesReader.VersionPageReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.*;

public class InvertedSeriesReader {

  private final Path seriesPath;
  private final TSDataType dataType;
  private final QueryContext context;

  /*
   * There is at most one is not null between timeFilter and valueFilter
   *
   * timeFilter is pushed down to all pages (seq, unseq) without correctness problem
   *
   * valueFilter is pushed down to non-overlapped page only
   */
  private final Filter timeFilter;
  private final Filter valueFilter;

  /*
   * file cache
   */
  private final List<TsFileResource> seqFileResource;
  private final PriorityQueue<TsFileResource> unseqFileResource;

  /*
   * chunk cache
   */
  private ChunkMetaData firstChunkMetaData;
  private ChunkMetaData lastSeqChunkMetadata;
  private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas =
      new PriorityQueue<>(Comparator.comparingLong(ChunkMetaData::getStartTime));

  /*
   * page cache
   */
  private VersionPageReader firstPageReader;
  private PriorityQueue<VersionPageReader> cachedPageReaders =
      new PriorityQueue<>(
          Comparator.comparingLong(VersionPageReader::getStartTime));

  /*
   * point cache
   */
  private PriorityMergeReader mergeReader = new PriorityMergeReader();

  /*
   * result cache
   */
  private boolean hasCachedNextOverlappedPage;
  private BatchData cachedBatchData;

  public InvertedSeriesReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter, TsFileFilter fileFilter) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.context = context;
    QueryUtils.filterQueryDataSource(dataSource, fileFilter);
    this.seqFileResource = dataSource.getSeqResources();
    this.unseqFileResource = sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
  }

  public boolean hasNextChunk() throws IOException {

    if (!cachedPageReaders.isEmpty() || firstPageReader != null || mergeReader
        .hasNextTimeValuePair()) {
      throw new IOException("all cached pages should be consumed first");
    }

    if (firstChunkMetaData != null) {
      return true;
    }

    // init first chunk metadata whose startTime is minimum
    tryToUnpackAllOverlappedFilesToChunkMetadatas();

    return firstChunkMetaData != null;
  }

  public Statistics currentChunkStatistics() {
    return firstChunkMetaData.getStatistics();
  }

  public boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics currentPageStatistics = currentPageStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !isChunkOverlapped() && containedByTimeFilter(currentPageStatistics);
  }

  private boolean isChunkOverlapped() throws IOException {
    if (firstChunkMetaData == null) {
      throw new IOException("no first chunk");
    }

    Statistics chunkStatistics = firstChunkMetaData.getStatistics();
    return lastSeqChunkMetadata != null
        && chunkStatistics.getEndTime() >= lastSeqChunkMetadata.getStartTime()
        || !unseqChunkMetadatas.isEmpty()
        && chunkStatistics.getEndTime() >= unseqChunkMetadatas.peek().getStartTime();
  }

  public void skipCurrentChunk() {
    firstChunkMetaData = null;
  }

  /**
   * This method should be called after hasNextChunk() until no next page,
   * make sure that all overlapped pages are consumed
   */
  public boolean hasNextPage() throws IOException {

    /*
     * has overlapped data before
     */
    if (hasCachedNextOverlappedPage) {
      return true;
    } else if (mergeReader.hasNextTimeValuePair()) {
      if (hasNextOverlappedPage()) {
        cachedBatchData = nextOverlappedPage();
        if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
          hasCachedNextOverlappedPage = true;
          return true;
        }
      }
    }

    if (firstPageReader != null) {
      return true;
    }

    /*
     * construct first page reader
     */
    if (firstChunkMetaData != null) {
      /*
       * try to unpack all overlapped ChunkMetadata to cachedPageReaders
       */
      unpackAllOverlappedChunkMetadataToCachedPageReaders(firstChunkMetaData.getStartTime());
    } else {
      /*
       * first chunk metadata is already unpacked, consume cached pages
       */
      if (!cachedPageReaders.isEmpty()) {
        firstPageReader = cachedPageReaders.poll();
      }
    }

    if (firstPageReader != null && !cachedPageReaders.isEmpty() &&
        firstPageReader.getStartTime() <= cachedPageReaders.peek().getEndTime()) {
      /*
       * next page is overlapped, read overlapped data and cache it
       */
      if (hasNextOverlappedPage()) {
        cachedBatchData = nextOverlappedPage();
        if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
          hasCachedNextOverlappedPage = true;
          return true;
        }
      }
    }

    return firstPageReader != null;
  }


  private void unpackAllOverlappedChunkMetadataToCachedPageReaders(long startTime)
      throws IOException {
    if (lastSeqChunkMetadata != null && startTime <= lastSeqChunkMetadata.getEndTime()) {
      unpackOneChunkMetaData(lastSeqChunkMetadata);
      lastSeqChunkMetadata = null;
    }
    while (!unseqChunkMetadatas.isEmpty() && startTime <= unseqChunkMetadatas.peek().getEndTime()) {
      unpackOneChunkMetaData(unseqChunkMetadatas.poll());
    }

    if (firstChunkMetaData != null && startTime <= firstChunkMetaData.getEndTime()) {
      unpackOneChunkMetaData(firstChunkMetaData);
      firstChunkMetaData = null;
    }

    if (firstPageReader == null && !cachedPageReaders.isEmpty()) {
      firstPageReader = cachedPageReaders.poll();
    }
  }

  private void unpackOneChunkMetaData(ChunkMetaData chunkMetaData) throws IOException {
    initChunkReader(chunkMetaData)
        .getPageReaderList()
        .forEach(
            pageReader ->
                cachedPageReaders.add(
                    new VersionPageReader(chunkMetaData.getVersion(), pageReader)));
  }

  /**
   * This method should be called after calling hasNextPage.
   *
   * hasNextPage may cache firstPageReader if it is not overlapped
   * or cached a BatchData if the first page is overlapped
   *
   */
  boolean isPageOverlapped() throws IOException {

    /*
     * has an overlapped page
     */
    if (hasCachedNextOverlappedPage) {
      return true;
    }

    /*
     * has a non-overlapped page in firstPageReader
     */
    if (mergeReader.hasNextTimeValuePair()) {
      throw new IOException("overlapped data should be consumed first");
    }

    Statistics firstPageStatistics = firstPageReader.getStatistics();

    return !cachedPageReaders.isEmpty() &&
        firstPageStatistics.getEndTime() >= cachedPageReaders.peek().getStartTime();
  }

  Statistics currentPageStatistics() {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getStatistics();
  }

  void skipCurrentPage() {
    firstPageReader = null;
  }

  /**
   * This method should only be used when the method isPageOverlapped() return true.
   */
  public BatchData nextPage() throws IOException {

    if (!hasNextPage()) {
      throw new IOException("no next page, neither non-overlapped nor overlapped");
    }

    if (hasCachedNextOverlappedPage) {
      hasCachedNextOverlappedPage = false;
      return cachedBatchData;
    } else {

      /*
       * next page is not overlapped, push down value filter if it exists
       */
      if (valueFilter != null) {
        firstPageReader.setFilter(valueFilter);
      }
      BatchData batchData = firstPageReader.getAllSatisfiedPageData();
      firstPageReader = null;

      return batchData;
    }
  }

  /**
   * read overlapped data till currentLargestEndTime in mergeReader,
   * if current batch does not contain data, read till next currentLargestEndTime again
   */
  private boolean hasNextOverlappedPage() throws IOException {

    if (hasCachedNextOverlappedPage) {
      return true;
    }

    tryToPutAllDirectlyOverlappedPageReadersIntoMergeReader();

    while (true) {

      if (mergeReader.hasNextTimeValuePair()) {

        cachedBatchData = new BatchData(dataType);
        long currentPageEndTime = mergeReader.getCurrentLargestEndTime();

        while (mergeReader.hasNextTimeValuePair()) {

          /*
           * get current first point in mergeReader, this maybe overlapped latter
           */
          TimeValuePair timeValuePair = mergeReader.currentTimeValuePair();

          if (timeValuePair.getTimestamp() > currentPageEndTime) {
            break;
          }

          unpackAllOverlappedTsFilesToChunkMetadatas(timeValuePair.getTimestamp());
          unpackAllOverlappedChunkMetadataToCachedPageReaders(timeValuePair.getTimestamp());
          unpackAllOverlappedCachedPageReadersToMergeReader(timeValuePair.getTimestamp());

          /*
           * get the latest first point in mergeReader
           */
          timeValuePair = mergeReader.nextTimeValuePair();

          if (valueFilter == null || valueFilter
              .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            cachedBatchData.putAnObject(
                timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
          }

        }
        hasCachedNextOverlappedPage = cachedBatchData.hasCurrent();
        /*
         * if current overlapped page has valid data, return, otherwise read next overlapped page
         */
        if (hasCachedNextOverlappedPage) {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  private void tryToPutAllDirectlyOverlappedPageReadersIntoMergeReader() throws IOException {

    /*
     * no cached page readers
     */
    if (firstPageReader == null && cachedPageReaders.isEmpty()) {
      return;
    }

    /*
     * init firstPageReader
     */
    if (firstPageReader == null) {
      firstPageReader = cachedPageReaders.poll();
    }

    long currentPageEndTime;
    if (mergeReader.hasNextTimeValuePair()) {
      currentPageEndTime = mergeReader.getCurrentLargestEndTime();
    } else {
      // put the first page into merge reader
      currentPageEndTime = firstPageReader.getEndTime();
    }

    /*
     * put all currently directly overlapped page reader to merge reader
     */
    unpackAllOverlappedCachedPageReadersToMergeReader(currentPageEndTime);
  }

  private void unpackAllOverlappedCachedPageReadersToMergeReader(long startTime) throws IOException {
    while (!cachedPageReaders.isEmpty() && startTime <= cachedPageReaders.peek().data
        .getStatistics().getEndTime()) {
      putPageReaderToMergeReader(cachedPageReaders.poll());
    }
    if (firstPageReader != null && startTime <= firstPageReader.getEndTime()) {
      putPageReaderToMergeReader(firstPageReader);
      firstPageReader = null;
    }
  }

  private void putPageReaderToMergeReader(VersionPageReader pageReader) throws IOException {
    mergeReader.addReader(
        pageReader.getAllSatisfiedPageData().getBatchDataIterator(),
        pageReader.version, pageReader.getEndTime());
  }

  private BatchData nextOverlappedPage() throws IOException {
    if (hasCachedNextOverlappedPage || hasNextOverlappedPage()) {
      hasCachedNextOverlappedPage = false;
      return cachedBatchData;
    }
    throw new IOException("No more batch data");
  }

  private IChunkReader initChunkReader(ChunkMetaData metaData) throws IOException {
    if (metaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = metaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
    } else {
      Chunk chunk = chunkLoader.getChunk(metaData);
      chunkReader = new ChunkReader(chunk, timeFilter);
      chunkReader.hasNextSatisfiedPage();
    }
    return chunkReader;
  }


  private PriorityQueue<TsFileResource> sortUnSeqFileResourcesInDecendingOrder(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Map<String, Long> startTimeMap = o1.getEndTimeMap();
              Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
              Map<String, Long> startTimeMap2 = o2.getEndTimeMap();
              Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

              return Long.compare(minTimeOfO2, minTimeOfO1);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }


  /**
   * unpack all overlapped seq/unseq files and find the first chunk metadata
   * <p>
   * Because there may be too many files in the scenario used by the user, we cannot open all the
   * chunks at once, which may cause OOM, so we can only unpack one file at a time when needed. This
   * approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  private void tryToUnpackAllOverlappedFilesToChunkMetadatas() throws IOException {

    /*
     * Load the last sequence chunkMetadata
     */
    if (lastSeqChunkMetadata == null && !seqFileResource.isEmpty()) {
      for (int index = seqFileResource.size() - 1; index >= 0; index--) {
        TsFileResource resource = seqFileResource.remove(index);
        List<ChunkMetaData> chunkMetadata = FileLoaderUtils.loadChunkMetadataFromTsFileResource(
                resource, seriesPath, context);
        if (!chunkMetadata.isEmpty()) {
          lastSeqChunkMetadata = chunkMetadata.get(chunkMetadata.size() - 1);
          break;
        }
      }
    }

    /*
     * Load unseq chunkMetadatas that is overlapped with lastSeqChunkMetadata until it is not empty
     */
    while (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()
        && (lastSeqChunkMetadata == null || (lastSeqChunkMetadata.getStartTime()
        <= unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice())))) {
      unseqChunkMetadatas.addAll(FileLoaderUtils
          .loadChunkMetadataFromTsFileResource(unseqFileResource.poll(), seriesPath, context,
              timeFilter));
    }

    /*
     * find first chunk metadata
     */
    if (lastSeqChunkMetadata != null && unseqChunkMetadatas.isEmpty()) {
      // only has seq
      firstChunkMetaData = lastSeqChunkMetadata;
      lastSeqChunkMetadata = null;
    } else if (lastSeqChunkMetadata == null && !unseqChunkMetadatas.isEmpty()) {
      // only has unseq
      firstChunkMetaData = unseqChunkMetadatas.poll();
    } else if (lastSeqChunkMetadata != null) {
      // has seq and unseq
      if (lastSeqChunkMetadata.getEndTime() >= unseqChunkMetadatas.peek().getEndTime()) {
        firstChunkMetaData = lastSeqChunkMetadata;
        lastSeqChunkMetadata = null;
      } else {
        firstChunkMetaData = unseqChunkMetadatas.poll();
      }
    }

    /*
     * unpack all directly overlapped seq/unseq files with first chunk metadata
     */
    if (firstChunkMetaData != null) {
      unpackAllOverlappedTsFilesToChunkMetadatas(firstChunkMetaData.getStartTime());
    }
  }

  private void unpackAllOverlappedTsFilesToChunkMetadatas(long startTime) throws IOException {
    /*
     * only need to unpack unsequence files here.
     */
    while (!unseqFileResource.isEmpty() && startTime <=
        unseqFileResource.peek().getEndTimeMap().get(seriesPath.getDevice())) {
      unseqChunkMetadatas.addAll(FileLoaderUtils
          .loadChunkMetadataFromTsFileResource(unseqFileResource.poll(), seriesPath, context,
              timeFilter));
    }
  }

  private boolean containedByTimeFilter(Statistics statistics) {
    return timeFilter == null
        || timeFilter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  void setTimeFilter(long timestamp) {
    ((UnaryFilter) timeFilter).setValue(timestamp);
  }

  Filter getTimeFilter() {
    return timeFilter;
  }
}