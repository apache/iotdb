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

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

public class SeriesReader {

  private final Path seriesPath;

  // all the sensors in this device;
  private final Set<String> allSensors;
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
  private final List<TsFileResource> unseqFileResource;

  /*
   * TimeSeriesMetadata cache
   */
  private TimeseriesMetadata firstTimeSeriesMetadata;
  private final List<TimeseriesMetadata> seqTimeSeriesMetadata = new LinkedList<>();
  private final PriorityQueue<TimeseriesMetadata> unSeqTimeSeriesMetadata =
      new PriorityQueue<>(Comparator.comparingLong(
          timeSeriesMetadata -> timeSeriesMetadata.getStatistics().getStartTime()));

  /*
   * chunk cache
   */
  private ChunkMetadata firstChunkMetadata;
  private final PriorityQueue<ChunkMetadata> cachedChunkMetadata =
      new PriorityQueue<>(Comparator.comparingLong(ChunkMetadata::getStartTime));

  /*
   * page cache
   */
  private VersionPageReader firstPageReader;
  private PriorityQueue<VersionPageReader> cachedPageReaders =
      new PriorityQueue<>(Comparator.comparingLong(VersionPageReader::getStartTime));

  /*
   * point cache
   */
  private PriorityMergeReader mergeReader = new PriorityMergeReader();

  /*
   * result cache
   */
  private boolean hasCachedNextOverlappedPage;
  private BatchData cachedBatchData;

  public SeriesReader(Path seriesPath, Set<String> allSensors, TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter, TsFileFilter fileFilter) {
    this.seriesPath = seriesPath;
    this.allSensors = allSensors;
    this.dataType = dataType;
    this.context = context;
    QueryUtils.filterQueryDataSource(dataSource, fileFilter);
    this.seqFileResource = new LinkedList<>(dataSource.getSeqResources());
    this.unseqFileResource = sortUnSeqFileResources(dataSource.getUnseqResources());
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
  }

  @TestOnly
  SeriesReader(Path seriesPath, Set<String> allSensors, TSDataType dataType, QueryContext context,
      List<TsFileResource> seqFileResource, List<TsFileResource> unseqFileResource,
      Filter timeFilter, Filter valueFilter) {
    this.seriesPath = seriesPath;
    this.allSensors = allSensors;
    this.dataType = dataType;
    this.context = context;
    this.seqFileResource = new LinkedList<>(seqFileResource);
    this.unseqFileResource = sortUnSeqFileResources(unseqFileResource);
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
  }

  public boolean isEmpty() {
    return seqFileResource.isEmpty() && unseqFileResource.isEmpty();
  }

  boolean hasNextFile() throws IOException {

    if (!cachedPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IOException(
          "all cached pages should be consumed first cachedPageReaders.isEmpty() is "
              + cachedPageReaders.isEmpty()
              + " firstPageReader != null is "
              + (firstPageReader != null)
              + " mergeReader.hasNextTimeValuePair() = "
              + mergeReader.hasNextTimeValuePair());
    }

    if (firstChunkMetadata != null || !cachedChunkMetadata.isEmpty()) {
      throw new IOException("all cached chunks should be consumed first");
    }

    if (firstTimeSeriesMetadata != null) {
      return true;
    }

    // init first time series metadata whose startTime is minimum
    tryToUnpackAllOverlappedFilesToTimeSeriesMetadata();

    return firstTimeSeriesMetadata != null;
  }

  boolean isFileOverlapped() throws IOException {
    if (firstTimeSeriesMetadata == null) {
      throw new IOException("no first file");
    }

    Statistics fileStatistics = firstTimeSeriesMetadata.getStatistics();
    return !seqTimeSeriesMetadata.isEmpty()
        && fileStatistics.getEndTime()
        >= seqTimeSeriesMetadata.get(0).getStatistics().getStartTime()
        || !unSeqTimeSeriesMetadata.isEmpty()
        && fileStatistics.getEndTime()
        >= unSeqTimeSeriesMetadata.peek().getStatistics().getStartTime();
  }

  Statistics currentFileStatistics() {
    return firstTimeSeriesMetadata.getStatistics();
  }

  boolean currentFileModified() throws IOException {
    if (firstTimeSeriesMetadata == null) {
      throw new IOException("no first file");
    }
    return firstTimeSeriesMetadata.isModified();
  }

  void skipCurrentFile() {
    firstTimeSeriesMetadata = null;
  }

  /**
   * This method should be called after hasNextFile() until no next chunk, make sure that all
   * overlapped chunks are consumed
   */
  boolean hasNextChunk() throws IOException {
    if (!cachedPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IOException(
          "all cached pages should be consumed first cachedPageReaders.isEmpty() is "
              + cachedPageReaders.isEmpty()
              + " firstPageReader != null is "
              + (firstPageReader != null)
              + " mergeReader.hasNextTimeValuePair() = "
              + mergeReader.hasNextTimeValuePair());
    }

    if (firstChunkMetadata != null) {
      return true;
    }

    /*
     * construct first chunk metadata
     */
    if (firstTimeSeriesMetadata != null) {
      /*
       * try to unpack all overlapped TimeSeriesMetadata to cachedChunkMetadata
       */
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(
          firstTimeSeriesMetadata.getStatistics().getEndTime());
      unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
          firstTimeSeriesMetadata.getStatistics().getEndTime(), true);
    } else {
      /*
       * first time series metadata is already unpacked, consume cached ChunkMetadata
       */
      if (!cachedChunkMetadata.isEmpty()) {
        firstChunkMetadata = cachedChunkMetadata.poll();
        unpackAllOverlappedTsFilesToTimeSeriesMetadata(firstChunkMetadata.getEndTime());
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
            firstChunkMetadata.getEndTime(), false);
      }
    }

    return firstChunkMetadata != null;
  }

  private void unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
      long endTime, boolean init) throws IOException {
    while (!seqTimeSeriesMetadata.isEmpty()
        && endTime >= seqTimeSeriesMetadata.get(0).getStatistics().getStartTime()) {
      unpackOneTimeSeriesMetadata(seqTimeSeriesMetadata.remove(0));
    }
    while (!unSeqTimeSeriesMetadata.isEmpty()
        && endTime >= unSeqTimeSeriesMetadata.peek().getStatistics().getStartTime()) {
      unpackOneTimeSeriesMetadata(unSeqTimeSeriesMetadata.poll());
    }

    if (firstTimeSeriesMetadata != null
        && endTime >= firstTimeSeriesMetadata.getStatistics().getStartTime()) {
      unpackOneTimeSeriesMetadata(firstTimeSeriesMetadata);
      firstTimeSeriesMetadata = null;
    }

    if (init && firstChunkMetadata == null && !cachedChunkMetadata.isEmpty()) {
      firstChunkMetadata = cachedChunkMetadata.poll();
    }
  }

  private void unpackOneTimeSeriesMetadata(TimeseriesMetadata timeSeriesMetadata)
      throws IOException {
    List<ChunkMetadata> chunkMetadataList = FileLoaderUtils
        .loadChunkMetadataList(timeSeriesMetadata);
    // try to calculate the total number of chunk and time-value points in chunk
    if (IoTDBDescriptor.getInstance().getConfig().isEnablePerformanceTracing()) {
      QueryResourceManager queryResourceManager = QueryResourceManager.getInstance();
      queryResourceManager.getChunkNumMap()
          .compute(context.getQueryId(),
              (k, v) -> v == null ? chunkMetadataList.size() : v + chunkMetadataList.size());

      long totalChunkSize = chunkMetadataList.stream()
          .mapToLong(chunkMetadata -> chunkMetadata.getStatistics().getCount()).sum();
      queryResourceManager.getChunkSizeMap()
          .compute(context.getQueryId(), (k, v) -> v == null ? totalChunkSize : v + totalChunkSize);
    }

    cachedChunkMetadata.addAll(chunkMetadataList);
  }

  boolean isChunkOverlapped() throws IOException {
    if (firstChunkMetadata == null) {
      throw new IOException("no first chunk");
    }

    Statistics chunkStatistics = firstChunkMetadata.getStatistics();
    return !cachedChunkMetadata.isEmpty()
        && chunkStatistics.getEndTime() >= cachedChunkMetadata.peek().getStartTime();
  }

  Statistics currentChunkStatistics() {
    return firstChunkMetadata.getStatistics();
  }

  boolean currentChunkModified() throws IOException {
    if (firstChunkMetadata == null) {
      throw new IOException("no first chunk");
    }
    return firstChunkMetadata.isModified();
  }

  void skipCurrentChunk() {
    firstChunkMetadata = null;
  }

  /**
   * This method should be called after hasNextChunk() until no next page, make sure that all
   * overlapped pages are consumed
   */
  boolean hasNextPage() throws IOException {

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
    if (firstChunkMetadata != null) {
      /*
       * try to unpack all overlapped ChunkMetadata to cachedPageReaders
       */
      unpackAllOverlappedChunkMetadataToCachedPageReaders(firstChunkMetadata.getEndTime(), true);
    } else {
      /*
       * first chunk metadata is already unpacked, consume cached pages
       */
      if (!cachedPageReaders.isEmpty()) {
        firstPageReader = cachedPageReaders.poll();
        long endTime = firstPageReader.getEndTime();
        unpackAllOverlappedTsFilesToTimeSeriesMetadata(endTime);
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(endTime, false);
        unpackAllOverlappedChunkMetadataToCachedPageReaders(endTime, false);
      }
    }

    if (firstPageReader != null
        && !cachedPageReaders.isEmpty()
        && firstPageReader.getEndTime() >= cachedPageReaders.peek().getStartTime()) {
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

    // make sure firstPageReader won't be null while cachedPageReaders has more cached page readers
    while (firstPageReader == null && !cachedPageReaders.isEmpty()) {
      firstPageReader = cachedPageReaders.poll();
      if (!cachedPageReaders.isEmpty()
          && firstPageReader.getEndTime() >= cachedPageReaders.peek().getStartTime()) {
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
    }
    return firstPageReader != null;
  }

  private void unpackAllOverlappedChunkMetadataToCachedPageReaders(long endTime, boolean init)
      throws IOException {
    while (!cachedChunkMetadata.isEmpty() && endTime >= cachedChunkMetadata.peek().getStartTime()) {
      unpackOneChunkMetaData(cachedChunkMetadata.poll());
    }
    if (firstChunkMetadata != null && endTime >= firstChunkMetadata.getStartTime()) {
      unpackOneChunkMetaData(firstChunkMetadata);
      firstChunkMetadata = null;
    }
    if (init && firstPageReader == null && !cachedPageReaders.isEmpty()) {
      firstPageReader = cachedPageReaders.poll();
    }
  }

  private void unpackOneChunkMetaData(ChunkMetadata chunkMetaData) throws IOException {
    FileLoaderUtils.loadPageReaderList(chunkMetaData, timeFilter)
        .forEach(
            pageReader ->
                cachedPageReaders.add(
                    new VersionPageReader(chunkMetaData.getVersion(), pageReader)));
  }

  /**
   * This method should be called after calling hasNextPage.
   *
   * <p>hasNextPage may cache firstPageReader if it is not overlapped or cached a BatchData if the
   * first page is overlapped
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

    return !cachedPageReaders.isEmpty()
        && firstPageStatistics.getEndTime() >= cachedPageReaders.peek().getStartTime();
  }

  Statistics currentPageStatistics() {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getStatistics();
  }

  boolean currentPageModified() throws IOException {
    if (firstPageReader == null) {
      throw new IOException("no first page");
    }
    return firstPageReader.isModified();
  }

  void skipCurrentPage() {
    firstPageReader = null;
  }

  /**
   * This method should only be used when the method isPageOverlapped() return true.
   */
  BatchData nextPage() throws IOException {

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
   * read overlapped data till currentLargestEndTime in mergeReader, if current batch does not
   * contain data, read till next currentLargestEndTime again
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

          unpackAllOverlappedTsFilesToTimeSeriesMetadata(timeValuePair.getTimestamp());
          unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
              timeValuePair.getTimestamp(), false);
          unpackAllOverlappedChunkMetadataToCachedPageReaders(timeValuePair.getTimestamp(), false);
          unpackAllOverlappedCachedPageReadersToMergeReader(timeValuePair.getTimestamp());

          /*
           * get the latest first point in mergeReader
           */
          timeValuePair = mergeReader.nextTimeValuePair();

          if (valueFilter == null
              || valueFilter.satisfy(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
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

  private void unpackAllOverlappedCachedPageReadersToMergeReader(long endTime) throws IOException {
    while (!cachedPageReaders.isEmpty()
        && endTime >= cachedPageReaders.peek().data.getStatistics().getStartTime()) {
      putPageReaderToMergeReader(cachedPageReaders.poll());
    }
    if (firstPageReader != null && endTime >= firstPageReader.getStartTime()) {
      putPageReaderToMergeReader(firstPageReader);
      firstPageReader = null;
    }
  }

  private void putPageReaderToMergeReader(VersionPageReader pageReader) throws IOException {
    mergeReader.addReader(
        pageReader.getAllSatisfiedPageData().getBatchDataIterator(),
        pageReader.version,
        pageReader.getEndTime());
  }

  private BatchData nextOverlappedPage() throws IOException {
    if (hasCachedNextOverlappedPage || hasNextOverlappedPage()) {
      hasCachedNextOverlappedPage = false;
      return cachedBatchData;
    }
    throw new IOException("No more batch data");
  }

  private LinkedList<TsFileResource> sortUnSeqFileResources(List<TsFileResource> tsFileResources) {
    return tsFileResources.stream()
        .sorted(
            Comparator.comparingLong(
                tsFileResource -> tsFileResource.getStartTime(seriesPath.getDevice())))
        .collect(Collectors.toCollection(LinkedList::new));
  }

  /**
   * unpack all overlapped seq/unseq files and find the first TimeSeriesMetadata
   *
   * <p>Because there may be too many files in the scenario used by the user, we cannot open all
   * the chunks at once, which may cause OOM, so we can only unpack one file at a time when needed.
   * This approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  private void tryToUnpackAllOverlappedFilesToTimeSeriesMetadata() throws IOException {
    /*
     * Fill sequence TimeSeriesMetadata List until it is not empty
     */
    while (seqTimeSeriesMetadata.isEmpty() && !seqFileResource.isEmpty()) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              seqFileResource.remove(0), seriesPath, context, getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        seqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }

    /*
     * Fill unSequence TimeSeriesMetadata Priority Queue until it is not empty
     */
    while (unSeqTimeSeriesMetadata.isEmpty() && !unseqFileResource.isEmpty()) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              unseqFileResource.remove(0), seriesPath, context, getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        timeseriesMetadata.setModified(true);
        unSeqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }

    /*
     * find end time of the first TimeSeriesMetadata
     */
    long endTime = -1L;
    if (!seqTimeSeriesMetadata.isEmpty() && unSeqTimeSeriesMetadata.isEmpty()) {
      // only has seq
      endTime = seqTimeSeriesMetadata.get(0).getStatistics().getEndTime();
    } else if (seqTimeSeriesMetadata.isEmpty() && !unSeqTimeSeriesMetadata.isEmpty()) {
      // only has unseq
      endTime = unSeqTimeSeriesMetadata.peek().getStatistics().getEndTime();
    } else if (!seqTimeSeriesMetadata.isEmpty()) {
      // has seq and unseq
      if (seqTimeSeriesMetadata.get(0).getStatistics().getStartTime()
          <= unSeqTimeSeriesMetadata.peek().getStatistics().getStartTime()) {
        endTime = seqTimeSeriesMetadata.get(0).getStatistics().getEndTime();
      } else {
        endTime = unSeqTimeSeriesMetadata.peek().getStatistics().getEndTime();
      }
    }

    /*
     * unpack all directly overlapped seq/unseq files with first TimeSeriesMetadata
     */
    if (endTime != -1) {
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(endTime);
    }

    /*
     * update the first TimeSeriesMetadata
     */
    if (!seqTimeSeriesMetadata.isEmpty() && unSeqTimeSeriesMetadata.isEmpty()) {
      // only has seq
      firstTimeSeriesMetadata = seqTimeSeriesMetadata.remove(0);
    } else if (seqTimeSeriesMetadata.isEmpty() && !unSeqTimeSeriesMetadata.isEmpty()) {
      // only has unseq
      firstTimeSeriesMetadata = unSeqTimeSeriesMetadata.poll();
    } else if (!seqTimeSeriesMetadata.isEmpty()) {
      // has seq and unseq
      // has seq and unseq
      if (seqTimeSeriesMetadata.get(0).getStatistics().getStartTime()
          <= unSeqTimeSeriesMetadata.peek().getStatistics().getStartTime()) {
        firstTimeSeriesMetadata = seqTimeSeriesMetadata.remove(0);
      } else {
        firstTimeSeriesMetadata = unSeqTimeSeriesMetadata.poll();
      }
    }
  }

  private void unpackAllOverlappedTsFilesToTimeSeriesMetadata(long endTime) throws IOException {
    while (!unseqFileResource.isEmpty()
        && endTime >= unseqFileResource.get(0).getStartTime(seriesPath.getDevice())) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              unseqFileResource.remove(0), seriesPath, context, getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        timeseriesMetadata.setModified(true);
        unSeqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }
    while (!seqFileResource.isEmpty()
        && endTime >= seqFileResource.get(0).getStartTime(seriesPath.getDevice())) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              seqFileResource.remove(0), seriesPath, context, getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        seqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }
  }

  private Filter getAnyFilter() {
    return timeFilter != null ? timeFilter : valueFilter;
  }

  void setTimeFilter(long timestamp) {
    ((UnaryFilter) timeFilter).setValue(timestamp);
  }

  Filter getTimeFilter() {
    return timeFilter;
  }

  private class VersionPageReader {

    protected long version;
    protected IPageReader data;

    VersionPageReader(long version, IPageReader data) {
      this.version = version;
      this.data = data;
    }

    Statistics getStatistics() {
      return data.getStatistics();
    }

    long getStartTime() {
      return data.getStatistics().getStartTime();
    }

    long getEndTime() {
      return data.getStatistics().getEndTime();
    }

    BatchData getAllSatisfiedPageData() throws IOException {
      return data.getAllSatisfiedPageData();
    }

    void setFilter(Filter filter) {
      data.setFilter(filter);
    }

    boolean isModified() {
      return data.isModified();
    }
  }
}
