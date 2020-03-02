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

import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeriesReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SeriesReader.class);

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
  private final List<ChunkMetaData> seqChunkMetadatas = new LinkedList<>();
  private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas =
      new PriorityQueue<>(Comparator.comparingLong(ChunkMetaData::getStartTime));

  /*
   * page cache
   */
  private VersionPair<IPageReader> firstPageReader;
  private PriorityQueue<VersionPair<IPageReader>> cachedPageReaders =
      new PriorityQueue<>(
          Comparator.comparingLong(pageReader -> pageReader.data.getStatistics().getStartTime()));

  /*
   * point cache
   */
  private PriorityMergeReader mergeReader = new PriorityMergeReader();

  /*
   * result cache
   */
  private boolean hasCachedNextOverlappedPage;
  private BatchData cachedBatchData;

  public SeriesReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter, TsFileFilter fileFilter) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.context = context;
    QueryUtils.filterQueryDataSource(dataSource, fileFilter);
    this.seqFileResource = dataSource.getSeqResources();
    this.unseqFileResource = sortUnSeqFileResources(dataSource.getUnseqResources());
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
  }

  @TestOnly
  public SeriesReader(Path seriesPath, TSDataType dataType, QueryContext context,
      List<TsFileResource> seqFileResource, List<TsFileResource> unseqFileResource,
      Filter timeFilter, Filter valueFilter) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.context = context;
    this.seqFileResource = seqFileResource;
    this.unseqFileResource = sortUnSeqFileResources(unseqFileResource);
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
  }


  public boolean hasNextChunk() throws IOException {

    if (!cachedPageReaders.isEmpty() || firstPageReader != null || mergeReader
        .hasNextTimeValuePair()) {
      throw new IOException("all cached pages should be consumed first");
    }

    // current chunk not consume
    if (firstChunkMetaData != null) {
      return true;
    }

    // init first chunk metadata whose startTime is minimum
    tryToUnpackAllOverlappedFilesToChunkMetadatas();

    return firstChunkMetaData != null;
  }


  public boolean isChunkOverlapped() {
    Statistics chunkStatistics = firstChunkMetaData.getStatistics();
    return !seqChunkMetadatas.isEmpty()
        && chunkStatistics.getEndTime() >= seqChunkMetadatas.get(0).getStartTime()
        || !unseqChunkMetadatas.isEmpty()
        && chunkStatistics.getEndTime() >= unseqChunkMetadatas.peek().getStartTime();
  }

  public Statistics currentChunkStatistics() {
    return firstChunkMetaData.getStatistics();
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
      while (hasNextOverlappedPage()) {
        cachedBatchData = nextOverlappedPage();
        if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
          return hasCachedNextOverlappedPage = true;
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
      unpackAllOverlappedChunkMetadataToCachedPageReaders(firstChunkMetaData.getEndTime());
    } else {
      /*
       * first chunk metadata is already unpacked, consume cached pages
       */
      if (!cachedPageReaders.isEmpty()) {
        firstPageReader = cachedPageReaders.poll();
      }
    }

    if (firstPageReader != null && isPageOverlapped()) {
      /*
       * next page is overlapped, read data and cache
       */
      while (hasNextOverlappedPage()) {
        cachedBatchData = nextOverlappedPage();
        if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
          return hasCachedNextOverlappedPage = true;
        }
      }
    }

    return firstPageReader != null;
  }


  private void unpackAllOverlappedChunkMetadataToCachedPageReaders(long endTime)
      throws IOException {
    while (!seqChunkMetadatas.isEmpty() && endTime >= seqChunkMetadatas.get(0).getStartTime()) {
      unpackOneChunkMetaData(seqChunkMetadatas.remove(0));
    }
    while (!unseqChunkMetadatas.isEmpty() && endTime >= unseqChunkMetadatas.peek().getStartTime()) {
      unpackOneChunkMetaData(unseqChunkMetadatas.poll());
    }

    if (firstChunkMetaData != null && endTime >= firstChunkMetaData.getStartTime()) {
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
                    new VersionPair(chunkMetaData.getVersion(), pageReader)));
  }

  /**
   * This method should be called after calling hasNextPage.
   */
  protected boolean isPageOverlapped() {
    if (hasCachedNextOverlappedPage || mergeReader.hasNextTimeValuePair()) {
      return true;
    }

    Statistics firstPageStatistics = firstPageReader.data.getStatistics();

    return !cachedPageReaders.isEmpty() &&
        firstPageStatistics.getEndTime() >= cachedPageReaders.peek().data.getStatistics()
            .getStartTime();
  }

  /**
   * This method should only be used when the method isPageOverlapped() return true.
   */
  protected BatchData nextPage() throws IOException {

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
        firstPageReader.data.setFilter(valueFilter);
      }
      BatchData batchData = firstPageReader.data.getAllSatisfiedPageData();
      firstPageReader = null;

      return batchData;
    }
  }

  public Statistics currentPageStatistics() {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.data.getStatistics();
  }

  public void skipCurrentPage() {
    firstPageReader = null;
  }

  /**
   * This method should be called after hasNextChunk and hasNextPage methods.
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

          TimeValuePair timeValuePair = mergeReader.currentTimeValuePair();

          if (timeValuePair.getTimestamp() == 100492) {
            System.out.println("@+++++<<<<< series: " + seriesPath.getFullPath() + " point: " + timeValuePair);
          }

          if (timeValuePair.getTimestamp() > currentPageEndTime) {
            break;
          }

          unpackAllOverlappedTsFilesToChunkMetadatas(timeValuePair.getTimestamp());
          unpackAllOverlappedChunkMetadataToCachedPageReaders(timeValuePair.getTimestamp());
          unpackAllOverlappedCachedPageReadersToMergeReader(timeValuePair.getTimestamp());

          timeValuePair = mergeReader.nextTimeValuePair();

          if (valueFilter == null || valueFilter
              .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            cachedBatchData.putAnObject(
                timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
          }

//          mergeReader.nextTimeValuePair();

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
      currentPageEndTime = firstPageReader.data.getStatistics().getEndTime();
    }

    /*
     * put all currently directly overlapped page reader to merge reader
     */
    unpackAllOverlappedCachedPageReadersToMergeReader(currentPageEndTime);
  }

  private void unpackAllOverlappedCachedPageReadersToMergeReader(long endTime) throws IOException {
    while (!cachedPageReaders.isEmpty() && endTime >= cachedPageReaders.peek().data
        .getStatistics().getStartTime()) {
      putPageReaderToMergeReader(cachedPageReaders.poll());
    }
    if (firstPageReader != null && endTime >= firstPageReader.data.getStatistics().getStartTime()) {
      putPageReaderToMergeReader(firstPageReader);
      firstPageReader = null;
    }
  }

  private void putPageReaderToMergeReader(VersionPair<IPageReader> pageReader) throws IOException {
    if (pageReader.data.getStatistics().getStartTime() <= 100492
        && pageReader.data.getStatistics().getEndTime() >= 100492) {
      LOGGER.warn("@+++++<<<<: versino: {}, page: {}, ", pageReader.version,
          pageReader.data.getStatistics());
      LOGGER.warn("@+++++<<<<: merge reader: {}", mergeReader.hashCode());
      new IOException("@++++<<<").printStackTrace();
    }

    mergeReader.addReader(
        pageReader.data.getAllSatisfiedPageData().getBatchDataIterator(),
        pageReader.version, pageReader.data.getStatistics().getEndTime());
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

  private List<ChunkMetaData> loadSatisfiedChunkMetadatas(TsFileResource resource)
      throws IOException {
    List<ChunkMetaData> currentChunkMetaDataList;
    if (resource == null) {
      return new ArrayList<>();
    }
    if (resource.isClosed()) {
      currentChunkMetaDataList = DeviceMetaDataCache.getInstance().get(resource, seriesPath);
    } else {
      currentChunkMetaDataList = resource.getChunkMetaDataList();
    }
    List<Modification> pathModifications =
        context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(currentChunkMetaDataList, pathModifications);
    }

    for (ChunkMetaData chunkMetaData : currentChunkMetaDataList) {
      TsFileSequenceReader tsFileSequenceReader = FileReaderManager.getInstance()
          .get(resource, resource.isClosed());
      chunkMetaData.setChunkLoader(new DiskChunkLoader(tsFileSequenceReader));
    }
    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          currentChunkMetaDataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }

    /*
     * remove empty and not satisfied ChunkMetaData
     */
    currentChunkMetaDataList.removeIf(chunkMetaData -> (timeFilter != null && !timeFilter
        .satisfyStartEndTime(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
        || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
    return currentChunkMetaDataList;
  }

  private PriorityQueue<TsFileResource> sortUnSeqFileResources(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Map<String, Long> startTimeMap = o1.getStartTimeMap();
              Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
              Map<String, Long> startTimeMap2 = o2.getStartTimeMap();
              Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

              return Long.compare(minTimeOfO1, minTimeOfO2);
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
     * Fill sequence chunkMetadatas until it is not empty
     */
    while (seqChunkMetadatas.isEmpty() && !seqFileResource.isEmpty()) {
      seqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(seqFileResource.remove(0)));
    }

    /*
     * Fill unsequence chunkMetadatas until it is not empty
     */
    while (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()) {
      unseqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(unseqFileResource.poll()));
    }

    /*
     * find first chunk metadata
     */
    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
      // only has seq
      firstChunkMetaData = seqChunkMetadatas.remove(0);
    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
      // only has unseq
      firstChunkMetaData = unseqChunkMetadatas.poll();
    } else if (!seqChunkMetadatas.isEmpty()) {
      // has seq and unseq
      if (seqChunkMetadatas.get(0).getStartTime() <= unseqChunkMetadatas.peek().getStartTime()) {
        firstChunkMetaData = seqChunkMetadatas.remove(0);
      } else {
        firstChunkMetaData = unseqChunkMetadatas.poll();
      }
    }

    /*
     * unpack all directly overlapped seq/unseq files with first chunk metadata
     */
    if (firstChunkMetaData != null) {
      unpackAllOverlappedTsFilesToChunkMetadatas(firstChunkMetaData.getEndTime());
    }
  }

  private void unpackAllOverlappedTsFilesToChunkMetadatas(long endTime) throws IOException {
    while (!unseqFileResource.isEmpty() && endTime >=
        unseqFileResource.peek().getStartTimeMap().get(seriesPath.getDevice())) {
      unseqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(unseqFileResource.poll()));
    }
    while (!seqFileResource.isEmpty() && endTime >=
        seqFileResource.get(0).getStartTimeMap().get(seriesPath.getDevice())) {
      seqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(seqFileResource.remove(0)));
    }
  }

  public void setTimeFilter(long timestamp) {
    ((UnaryFilter) timeFilter).setValue(timestamp);
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  private class VersionPair<T> {

    protected long version;
    protected T data;

    public VersionPair(long version, T data) {
      this.version = version;
      this.data = data;
    }
  }
}
