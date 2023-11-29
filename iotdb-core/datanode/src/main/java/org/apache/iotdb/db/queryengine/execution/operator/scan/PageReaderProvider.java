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

package org.apache.iotdb.db.queryengine.execution.operator.scan;

import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.DescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.PriorityMergeReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.execution.operator.scan.SeriesScanUtil.SERIES_SCAN_COST_METRIC_SET;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED;

public class PageReaderProvider {

  // page cache
  private SeriesScanUtil.VersionPageReader firstPageReader;
  private final List<SeriesScanUtil.VersionPageReader> seqPageReaders;
  private final PriorityQueue<SeriesScanUtil.VersionPageReader> unSeqPageReaders;

  // point cache
  private final PriorityMergeReader mergeReader;

  // result cache
  private boolean hasCachedNextOverlappedPage;
  private TsBlock cachedTsBlock;

  private ChunkMetadataProvider sourceProvider;
  private SeriesScanUtil.TimeOrderUtils orderUtils;

  public PageReaderProvider(
      ChunkMetadataProvider sourceProvider, SeriesScanUtil.TimeOrderUtils orderUtils) {
    this.sourceProvider = sourceProvider;
    this.seqPageReaders = new LinkedList<>();
    this.unSeqPageReaders =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                versionPageReader -> orderUtils.getOrderTime(versionPageReader.getStatistics())));

    if (orderUtils.getAscending()) {
      this.mergeReader = new PriorityMergeReader();
    } else {
      this.mergeReader = new DescPriorityMergeReader();
    }

    this.orderUtils = orderUtils;
  }

  public boolean isCachedPageReaderConsumed() {
    return unSeqPageReaders.isEmpty()
        && firstPageReader == null
        && !mergeReader.hasNextTimeValuePair();
  }

  public boolean hasCurrentPage() {
    return firstPageReader != null;
  }

  public boolean hasNext() {
    return sourceProvider.hasCurrentChunk()
        || !seqPageReaders.isEmpty()
        || !unSeqPageReaders.isEmpty();
  }

  public boolean currentPageOverlapped() throws IOException {
    // has an overlapped page
    if (hasCachedNextOverlappedPage) {
      return true;
    }

    // has a non-overlapped page in firstPageReader
    checkState(!firstPageOverlappedWithMergeReader(), "overlapped data should be consumed first");

    Statistics firstPageStatistics = firstPageReader.getStatistics();
    return !unSeqPageReaders.isEmpty()
        && orderUtils.isOverlapped(firstPageStatistics, unSeqPageReaders.peek().getStatistics());
  }

  public void peekCurrentPage() throws IOException {
    if (sourceProvider.hasCurrentChunk()) {
      // try to unpack all overlapped ChunkMetadata to cachedPageReaders
      unpackAllOverlappedChunkMetadataToPageReaders(
          orderUtils.getOverlapCheckTime(sourceProvider.getCurrentChunk().getStatistics()), true);
    } else {
      // first chunk metadata is already unpacked, consume cached pages
      initFirstPageReader();
    }
  }

  private void unpackAllOverlappedChunkMetadataToPageReaders(long endpointTime, boolean init)
      throws IOException {
    if (sourceProvider.hasCurrentChunk()
        && orderUtils.isOverlapped(endpointTime, firstChunkMetadata.getStatistics())) {
      unpackOneChunkMetaData(firstChunkMetadata);
      sourceProvider.skipCurrentChunk();
    }
    // In case unpacking too many sequence chunks
    boolean hasMeetSeq = false;
    while (!cachedChunkMetadata.isEmpty()
        && orderUtils.isOverlapped(endpointTime, cachedChunkMetadata.peek().getStatistics())) {
      if (cachedChunkMetadata.peek().isSeq() && hasMeetSeq) {
        break;
      } else if (cachedChunkMetadata.peek().isSeq()) {
        hasMeetSeq = true;
      }
      unpackOneChunkMetaData(cachedChunkMetadata.poll());
    }
    if (init
        && firstPageReader == null
        && (!seqPageReaders.isEmpty() || !unSeqPageReaders.isEmpty())) {
      initFirstPageReader();
    }
  }

  private void unpackOneChunkMetaData(IChunkMetadata chunkMetaData) throws IOException {
    List<IPageReader> pageReaderList =
        FileLoaderUtils.loadPageReaderList(chunkMetaData, getGlobalTimeFilter());

    // init TsBlockBuilder for each page reader
    pageReaderList.forEach(p -> p.initTsBlockBuilder(getTsDataTypeList()));

    if (chunkMetaData.isSeq()) {
      if (orderUtils.getAscending()) {
        for (IPageReader iPageReader : pageReaderList) {
          seqPageReaders.add(
              new SeriesScanUtil.VersionPageReader(
                  chunkMetaData.getVersion(),
                  chunkMetaData.getOffsetOfChunkHeader(),
                  iPageReader,
                  true));
        }
      } else {
        for (int i = pageReaderList.size() - 1; i >= 0; i--) {
          seqPageReaders.add(
              new SeriesScanUtil.VersionPageReader(
                  chunkMetaData.getVersion(),
                  chunkMetaData.getOffsetOfChunkHeader(),
                  pageReaderList.get(i),
                  true));
        }
      }
    } else {
      pageReaderList.forEach(
          pageReader ->
              unSeqPageReaders.add(
                  new SeriesScanUtil.VersionPageReader(
                      chunkMetaData.getVersion(),
                      chunkMetaData.getOffsetOfChunkHeader(),
                      pageReader,
                      false)));
    }
  }

  private void initFirstPageReader() throws IOException {
    while (firstPageReader == null) {
      SeriesScanUtil.VersionPageReader candidatePageReader =
          getCandidateFirstPageReaderFromCachedReaders();
      if (candidatePageReader == null) {
        break;
      }

      // unpack overlapped page using current page reader
      long overlapCheckTime = orderUtils.getOverlapCheckTime(candidatePageReader.getStatistics());
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(overlapCheckTime);
      unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(overlapCheckTime);
      unpackAllOverlappedChunkMetadataToPageReaders(overlapCheckTime, false);

      // this page after unpacking must be the first page
      if (candidatePageReader.equals(getCandidateFirstPageReaderFromCachedReaders())) {
        // set first page reader
        firstPageReader = candidatePageReader;

        // update seq and unseq page readers
        if (!seqPageReaders.isEmpty() && candidatePageReader.equals(seqPageReaders.get(0))) {
          seqPageReaders.remove(0);
        } else if (!unSeqPageReaders.isEmpty()
            && candidatePageReader.equals(unSeqPageReaders.peek())) {
          unSeqPageReaders.poll();
        }
      }
    }
  }

  // We use get() and peek() here in case it's not the first page reader before unpacking
  private SeriesScanUtil.VersionPageReader getCandidateFirstPageReaderFromCachedReaders() {
    SeriesScanUtil.VersionPageReader candidatePageReader = null;
    if (!seqPageReaders.isEmpty() && !unSeqPageReaders.isEmpty()) {
      if (orderUtils.isTakeSeqAsFirst(
          seqPageReaders.get(0).getStatistics(), unSeqPageReaders.peek().getStatistics())) {
        candidatePageReader = seqPageReaders.get(0);
      } else {
        candidatePageReader = unSeqPageReaders.peek();
      }
    } else if (!seqPageReaders.isEmpty()) {
      candidatePageReader = seqPageReaders.get(0);
    } else if (!unSeqPageReaders.isEmpty()) {
      candidatePageReader = unSeqPageReaders.peek();
    }
    return candidatePageReader;
  }

  public boolean isExistOverlappedPage() throws IOException {
    if (firstPageOverlapped() && hasNextOverlappedPage()) {
      // next page is overlapped, read overlapped data and cache it
      cachedTsBlock = nextOverlappedPage();
      if (cachedTsBlock != null && !cachedTsBlock.isEmpty()) {
        hasCachedNextOverlappedPage = true;
        return true;
      }
    }
    return false;
  }

  private boolean firstPageOverlapped() throws IOException {
    if (firstPageReader == null) {
      return false;
    }

    long endpointTime = orderUtils.getOverlapCheckTime(firstPageReader.getStatistics());
    unpackAllOverlappedTsFilesToTimeSeriesMetadata(endpointTime);
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(endpointTime);
    unpackAllOverlappedChunkMetadataToPageReaders(endpointTime, false);

    return (!seqPageReaders.isEmpty()
            && orderUtils.isOverlapped(
                firstPageReader.getStatistics(), seqPageReaders.get(0).getStatistics()))
        || (!unSeqPageReaders.isEmpty()
                && orderUtils.isOverlapped(
                    firstPageReader.getStatistics(), unSeqPageReaders.peek().getStatistics())
            || (mergeReader.hasNextTimeValuePair()
                && orderUtils.isOverlapped(
                    mergeReader.currentTimeValuePair().getTimestamp(),
                    firstPageReader.getStatistics())));
  }

  public boolean hasCachedNextOverlappedPage() {
    return hasCachedNextOverlappedPage;
  }

  private boolean firstPageOverlappedWithMergeReader() throws IOException {
    return mergeReader.hasNextTimeValuePair()
        && ((orderUtils.getAscending()
                && mergeReader.currentTimeValuePair().getTimestamp()
                    <= firstPageReader.getStatistics().getEndTime())
            || (!orderUtils.getAscending()
                && mergeReader.currentTimeValuePair().getTimestamp()
                    >= firstPageReader.getStatistics().getStartTime()));
  }

  /**
   * read overlapped data till currentLargestEndTime in mergeReader, if current batch does not
   * contain data, read till next currentLargestEndTime again
   */
  private boolean hasNextOverlappedPage() throws IOException {
    long startTime = System.nanoTime();
    try {
      if (hasCachedNextOverlappedPage) {
        return true;
      }

      tryToPutAllDirectlyOverlappedUnseqPageReadersIntoMergeReader();

      while (true) {
        if (!mergeReader.hasNextTimeValuePair()) {
          return false;
        }

        // may has overlapped data
        if (processOverlappedRecord()) {
          return true;
        }
      }
    } finally {
      recordMergeReaderCost(System.nanoTime() - startTime);
    }
  }

  private long currentPageEndPointTime;

  private boolean processOverlappedRecord() throws IOException {
    TsBlockBuilder builder = new TsBlockBuilder(getTsDataTypeList());
    currentPageEndPointTime = mergeReader.getCurrentReadStopTime();

    while (mergeReader.hasNextTimeValuePair()) {

      // get current first point in mergeReader, this maybe overlapped later
      TimeValuePair timeValuePair = mergeReader.currentTimeValuePair();

      if (isExcessEndpoint(timeValuePair, builder)) {
        return endMergeReaderScan(builder);
      }

      // unpack all overlapped data for the first timeValuePair
      unpackAllOverlappedData(timeValuePair);

      // update if there are unpacked unSeqPageReaders
      timeValuePair = mergeReader.currentTimeValuePair();

      // from now, the unsequence reader is all unpacked, so we don't need to consider it

      // if current timeValuePair excesses the first page reader's end time, we just use the
      // cached data
      if (isTimeValuePairBeyondFirstPageReader(timeValuePair)) {
        return endMergeReaderScan(builder);
      }
      if (isTimeValuePairOverlappingWithFirstPageReader(timeValuePair)) {
        // current timeValuePair is overlapped with firstPageReader, add it to merged reader
        // and update endTime to the max end time
        updateMergeReaderByPageReader(firstPageReader);
        firstPageReader = null;
      }

      timeValuePair = mergeReader.currentTimeValuePair();

      if (isTimeValuePairBeyondSeqPageReader(timeValuePair)) {
        return endMergeReaderScan(builder);
      }
      if (isTimeValuePairOverlappingWithFirstSeqPageReader(timeValuePair)) {
        updateMergeReaderByPageReader(seqPageReaders.remove(0));
      }

      // get the latest first point in mergeReader
      timeValuePair = mergeReader.nextTimeValuePair();
      if (processPaginationAndFilter(timeValuePair, builder)) {
        return endMergeReaderScan(builder);
      }
    }

    return endMergeReaderScan(builder);
  }

  private boolean isExcessEndpoint(TimeValuePair timeValuePair, TsBlockBuilder builder) {
    // when the merged point excesses the currentPageEndPointTime, we have read all overlapped
    // data before currentPageEndPointTime
    // 1. has cached batch data, we don't need to read more data, just use the cached data later
    // 2. has first page reader, which means first page reader last endTime <
    // currentTimeValuePair.getTimestamp(),
    // we could just use the first page reader later
    // 3. sequence page reader is not empty, which means first page reader last endTime <
    // currentTimeValuePair.getTimestamp(),
    // we could use the first sequence page reader later
    return orderUtils.isExcessEndpoint(timeValuePair.getTimestamp(), currentPageEndPointTime)
        && (!builder.isEmpty() || firstPageReader != null || !seqPageReaders.isEmpty());
  }

  private boolean processPaginationAndFilter(TimeValuePair timeValuePair, TsBlockBuilder builder) {
    if (!timeValuePairSatisfyFilter(timeValuePair)) {
      return false;
    }
    if (paginationController.hasCurOffset()) {
      paginationController.consumeOffset();
      return false;
    }
    if (paginationController.hasCurLimit()) {
      addTimeValuePairToResult(timeValuePair, builder);
      paginationController.consumeLimit();
      return false;
    } else {
      return true;
    }
  }

  private void unpackAllOverlappedData(TimeValuePair timeValuePair) throws IOException {
    unpackAllOverlappedTsFilesToTimeSeriesMetadata(timeValuePair.getTimestamp());
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(timeValuePair.getTimestamp());
    unpackAllOverlappedChunkMetadataToPageReaders(timeValuePair.getTimestamp(), false);
    unpackAllOverlappedUnseqPageReadersToMergeReader(timeValuePair.getTimestamp());
  }

  private boolean endMergeReaderScan(TsBlockBuilder builder) {
    // if current overlapped page has valid data, return, otherwise read next overlapped page
    hasCachedNextOverlappedPage = !builder.isEmpty();
    cachedTsBlock = builder.build();
    return hasCachedNextOverlappedPage;
  }

  private boolean isTimeValuePairBeyondFirstPageReader(TimeValuePair timeValuePair) {
    if (firstPageReader == null) {
      return false;
    }
    return (orderUtils.getAscending()
            && timeValuePair.getTimestamp() > firstPageReader.getStatistics().getEndTime())
        || (!orderUtils.getAscending()
            && timeValuePair.getTimestamp() < firstPageReader.getStatistics().getStartTime());
  }

  private boolean isTimeValuePairOverlappingWithFirstPageReader(TimeValuePair timeValuePair) {
    if (firstPageReader == null) {
      return false;
    }
    return orderUtils.isOverlapped(timeValuePair.getTimestamp(), firstPageReader.getStatistics());
  }

  private boolean isTimeValuePairBeyondSeqPageReader(TimeValuePair timeValuePair) {
    if (seqPageReaders.isEmpty()) {
      return false;
    }
    return (orderUtils.getAscending()
            && timeValuePair.getTimestamp() > seqPageReaders.get(0).getStatistics().getEndTime())
        || (!orderUtils.getAscending()
            && timeValuePair.getTimestamp() < seqPageReaders.get(0).getStatistics().getStartTime());
  }

  private boolean isTimeValuePairOverlappingWithFirstSeqPageReader(TimeValuePair timeValuePair) {
    if (seqPageReaders.isEmpty()) {
      return false;
    }
    return orderUtils.isOverlapped(
        timeValuePair.getTimestamp(), seqPageReaders.get(0).getStatistics());
  }

  private void updateMergeReaderByPageReader(SeriesScanUtil.VersionPageReader pageReader)
      throws IOException {
    mergeReader.addReader(
        getPointReader(pageReader.getAllSatisfiedPageData(orderUtils.getAscending())),
        pageReader.version,
        orderUtils.getOverlapCheckTime(pageReader.getStatistics()));
    currentPageEndPointTime = updateEndPointTime(currentPageEndPointTime, pageReader);
  }

  private boolean timeValuePairSatisfyFilter(TimeValuePair timeValuePair) {
    Filter queryFilter = scanOptions.getPushDownFilter();
    if (queryFilter != null) {
      Object valueForFilter = timeValuePair.getValue().getValue();

      if (timeValuePair.getValue().getDataType() == TSDataType.VECTOR) {
        for (TsPrimitiveType tsPrimitiveType : timeValuePair.getValue().getVector()) {
          if (tsPrimitiveType != null) {
            valueForFilter = tsPrimitiveType.getValue();
            break;
          }
        }
      }

      return queryFilter.satisfy(timeValuePair.getTimestamp(), valueForFilter);
    }
    return true;
  }

  private void addTimeValuePairToResult(TimeValuePair timeValuePair, TsBlockBuilder builder) {
    builder.getTimeColumnBuilder().writeLong(timeValuePair.getTimestamp());
    switch (dataType) {
      case BOOLEAN:
        builder.getColumnBuilder(0).writeBoolean(timeValuePair.getValue().getBoolean());
        break;
      case INT32:
        builder.getColumnBuilder(0).writeInt(timeValuePair.getValue().getInt());
        break;
      case INT64:
        builder.getColumnBuilder(0).writeLong(timeValuePair.getValue().getLong());
        break;
      case FLOAT:
        builder.getColumnBuilder(0).writeFloat(timeValuePair.getValue().getFloat());
        break;
      case DOUBLE:
        builder.getColumnBuilder(0).writeDouble(timeValuePair.getValue().getDouble());
        break;
      case TEXT:
        builder.getColumnBuilder(0).writeBinary(timeValuePair.getValue().getBinary());
        break;
      case VECTOR:
        TsPrimitiveType[] values = timeValuePair.getValue().getVector();
        for (int i = 0; i < values.length; i++) {
          if (values[i] == null) {
            builder.getColumnBuilder(i).appendNull();
          } else {
            builder.getColumnBuilder(i).writeTsPrimitiveType(values[i]);
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
    builder.declarePosition();
  }

  private void recordMergeReaderCost(long duration) {
    SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
        isAligned
            ? BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED
            : BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED,
        duration);
  }

  private long updateEndPointTime(
      long currentPageEndPointTime, SeriesScanUtil.VersionPageReader pageReader) {
    if (orderUtils.getAscending()) {
      return Math.min(currentPageEndPointTime, pageReader.getStatistics().getEndTime());
    } else {
      return Math.max(currentPageEndPointTime, pageReader.getStatistics().getStartTime());
    }
  }

  private void tryToPutAllDirectlyOverlappedUnseqPageReadersIntoMergeReader() throws IOException {

    /*
     * no cached page readers
     */
    if (firstPageReader == null && unSeqPageReaders.isEmpty() && seqPageReaders.isEmpty()) {
      return;
    }

    /*
     * init firstPageReader
     */
    if (firstPageReader == null) {
      initFirstPageReader();
    }

    long currentPageEndpointTime;
    if (mergeReader.hasNextTimeValuePair()) {
      currentPageEndpointTime = mergeReader.getCurrentReadStopTime();
    } else {
      currentPageEndpointTime = orderUtils.getOverlapCheckTime(firstPageReader.getStatistics());
    }

    /*
     * put all currently directly overlapped unseq page reader to merge reader
     */
    unpackAllOverlappedUnseqPageReadersToMergeReader(currentPageEndpointTime);
  }
}
