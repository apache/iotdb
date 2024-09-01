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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemAlignedPageReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemPageReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.DescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.MergeReaderPriority;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.PriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.TsBlockUtil;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.page.AlignedPageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.ToLongFunction;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED;

public class SeriesScanUtil implements Accountable {

  protected final FragmentInstanceContext context;

  // The path of the target series which will be scanned.
  protected final IFullPath seriesPath;

  private final IDeviceID deviceID;
  protected boolean isAligned = false;
  private final TSDataType dataType;

  // inner class of SeriesReader for order purpose
  private final TimeOrderUtils orderUtils;

  private QueryDataSource dataSource;

  // file index
  private int curSeqFileIndex;
  private int curUnseqFileIndex;

  // TimeSeriesMetadata cache
  private ITimeSeriesMetadata firstTimeSeriesMetadata;
  private final List<ITimeSeriesMetadata> seqTimeSeriesMetadata;
  private final PriorityQueue<ITimeSeriesMetadata> unSeqTimeSeriesMetadata;

  // chunk cache
  private IChunkMetadata firstChunkMetadata;
  private final PriorityQueue<IChunkMetadata> cachedChunkMetadata;

  // page cache
  private VersionPageReader firstPageReader;
  private final List<VersionPageReader> seqPageReaders;
  private final PriorityQueue<VersionPageReader> unSeqPageReaders;

  // point cache
  private final PriorityMergeReader mergeReader;

  // result cache
  private boolean hasCachedNextOverlappedPage;
  private TsBlock cachedTsBlock;

  protected SeriesScanOptions scanOptions;
  private final PaginationController paginationController;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SeriesScanUtil.class)
          + RamUsageEstimator.shallowSizeOfInstance(IDeviceID.class)
          + RamUsageEstimator.shallowSizeOfInstance(TimeOrderUtils.class)
          + RamUsageEstimator.shallowSizeOfInstance(PaginationController.class)
          + RamUsageEstimator.shallowSizeOfInstance(SeriesScanOptions.class);

  public SeriesScanUtil(
      IFullPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context) {
    this.seriesPath = seriesPath;
    this.deviceID = seriesPath.getDeviceId();
    this.dataType = seriesPath.getSeriesType();

    this.scanOptions = scanOptions;
    this.paginationController = scanOptions.getPaginationController();

    this.context = context;

    if (scanOrder.isAscending()) {
      this.orderUtils = new AscTimeOrderUtils();
      this.mergeReader = getPriorityMergeReader();
    } else {
      this.orderUtils = new DescTimeOrderUtils();
      this.mergeReader = getDescPriorityMergeReader();
    }
    this.mergeReader.setMemoryReservationManager(context.getMemoryReservationContext());

    // init TimeSeriesMetadata materializer
    this.seqTimeSeriesMetadata = new LinkedList<>();
    this.unSeqTimeSeriesMetadata =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                timeSeriesMetadata -> orderUtils.getOrderTime(timeSeriesMetadata.getStatistics())));

    // init ChunkMetadata materializer
    this.cachedChunkMetadata =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                chunkMetadata -> orderUtils.getOrderTime(chunkMetadata.getStatistics())));

    // init PageReader materializer
    this.seqPageReaders = new LinkedList<>();
    this.unSeqPageReaders =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                versionPageReader -> orderUtils.getOrderTime(versionPageReader.getStatistics())));
  }

  /**
   * Initialize the query data source. This method should be called <b>before any other methods</b>.
   *
   * @param dataSource the query data source
   */
  public void initQueryDataSource(QueryDataSource dataSource) {
    dataSource.fillOrderIndexes(deviceID, orderUtils.getAscending());
    this.dataSource = dataSource;

    // updated filter concerning TTL
    scanOptions.setTTL(DataNodeTTLCache.getInstance().getTTL(seriesPath.getDeviceId()));

    // init file index
    orderUtils.setCurSeqFileIndex(dataSource);
    curUnseqFileIndex = 0;
  }

  protected PriorityMergeReader getPriorityMergeReader() {
    return new PriorityMergeReader();
  }

  protected DescPriorityMergeReader getDescPriorityMergeReader() {
    return new DescPriorityMergeReader();
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // file level methods
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public boolean hasNextFile() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    if (!unSeqPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IllegalStateException(
          "all cached pages should be consumed first unSeqPageReaders.isEmpty() is "
              + unSeqPageReaders.isEmpty()
              + " firstPageReader != null is "
              + (firstPageReader != null)
              + " mergeReader.hasNextTimeValuePair() = "
              + mergeReader.hasNextTimeValuePair());
    }

    if (firstChunkMetadata != null || !cachedChunkMetadata.isEmpty()) {
      throw new IllegalStateException("all cached chunks should be consumed first");
    }

    if (firstTimeSeriesMetadata != null) {
      return true;
    }

    while (firstTimeSeriesMetadata == null
        && (orderUtils.hasNextSeqResource()
            || orderUtils.hasNextUnseqResource()
            || !seqTimeSeriesMetadata.isEmpty()
            || !unSeqTimeSeriesMetadata.isEmpty())) {
      // init first time series metadata whose startTime is minimum
      tryToUnpackAllOverlappedFilesToTimeSeriesMetadata();
      // filter file based on push-down conditions
      filterFirstTimeSeriesMetadata();
    }

    return firstTimeSeriesMetadata != null;
  }

  private boolean currentFileOverlapped() {
    Statistics<? extends Serializable> fileStatistics = firstTimeSeriesMetadata.getStatistics();
    return !seqTimeSeriesMetadata.isEmpty()
            && orderUtils.isOverlapped(fileStatistics, seqTimeSeriesMetadata.get(0).getStatistics())
        || !unSeqTimeSeriesMetadata.isEmpty()
            && orderUtils.isOverlapped(
                fileStatistics, unSeqTimeSeriesMetadata.peek().getStatistics());
  }

  public boolean canUseCurrentFileStatistics() {
    checkState(firstTimeSeriesMetadata != null, "no first file");

    if (currentFileOverlapped() || firstTimeSeriesMetadata.isModified()) {
      return false;
    }
    return filterAllSatisfy(scanOptions.getGlobalTimeFilter(), firstTimeSeriesMetadata)
        && filterAllSatisfy(scanOptions.getPushDownFilter(), firstTimeSeriesMetadata);
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentFileTimeStatistics() {
    return firstTimeSeriesMetadata.getTimeStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentFileStatistics(int index) {
    return firstTimeSeriesMetadata.getMeasurementStatistics(index).orElse(null);
  }

  public void skipCurrentFile() {
    firstTimeSeriesMetadata = null;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // chunk level methods
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * This method should be called after hasNextFile() until no next chunk, make sure that all
   * overlapped chunks are consumed.
   *
   * @throws IllegalStateException illegal state
   */
  public boolean hasNextChunk() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    if (!unSeqPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IllegalStateException(
          "all cached pages should be consumed first unSeqPageReaders.isEmpty() is "
              + unSeqPageReaders.isEmpty()
              + " firstPageReader != null is "
              + (firstPageReader != null)
              + " mergeReader.hasNextTimeValuePair() = "
              + mergeReader.hasNextTimeValuePair());
    }

    if (firstChunkMetadata != null) {
      return true;
      // hasNextFile() has not been invoked
    } else if (firstTimeSeriesMetadata == null && cachedChunkMetadata.isEmpty()) {
      return false;
    }

    while (firstChunkMetadata == null && (!cachedChunkMetadata.isEmpty() || hasNextFile())) {
      initFirstChunkMetadata();
      // filter chunk based on push-down conditions
      filterFirstChunkMetadata();
    }
    return firstChunkMetadata != null;
  }

  private void filterFirstChunkMetadata() {
    if (firstChunkMetadata == null) {
      return;
    }

    if (currentChunkOverlapped() || firstChunkMetadata.isModified()) {
      return;
    }

    // globalTimeFilter.canSkip() must be FALSE
    Filter pushDownFilter = scanOptions.getPushDownFilter();
    if (pushDownFilter != null && pushDownFilter.canSkip(firstChunkMetadata)) {
      skipCurrentChunk();
      return;
    }

    Filter globalTimeFilter = scanOptions.getGlobalTimeFilter();
    if (filterAllSatisfy(globalTimeFilter, firstChunkMetadata)
        && filterAllSatisfy(pushDownFilter, firstChunkMetadata)
        && timeAllSelected(firstChunkMetadata)) {
      long rowCount = firstChunkMetadata.getStatistics().getCount();
      if (paginationController.hasCurOffset(rowCount)) {
        skipCurrentChunk();
        paginationController.consumeOffset(rowCount);
      }
    }
  }

  /** construct first chunk metadata */
  private void initFirstChunkMetadata() throws IOException {
    if (firstTimeSeriesMetadata != null) {
      /*
       * try to unpack all overlapped TimeSeriesMetadata to cachedChunkMetadata
       */
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(
          orderUtils.getOverlapCheckTime(firstTimeSeriesMetadata.getStatistics()));
      unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
          orderUtils.getOverlapCheckTime(firstTimeSeriesMetadata.getStatistics()), true);
    } else {
      /*
       * first time series metadata is already unpacked, consume cached ChunkMetadata
       */
      while (!cachedChunkMetadata.isEmpty()) {
        firstChunkMetadata = cachedChunkMetadata.peek();
        unpackAllOverlappedTsFilesToTimeSeriesMetadata(
            orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()));
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
            orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()), false);
        if (firstChunkMetadata.equals(cachedChunkMetadata.peek())) {
          firstChunkMetadata = cachedChunkMetadata.poll();
          break;
        }
      }
    }
  }

  private void unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
      long endpointTime, boolean init) {
    while (!seqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(endpointTime, seqTimeSeriesMetadata.get(0).getStatistics())) {
      unpackOneTimeSeriesMetadata(seqTimeSeriesMetadata.remove(0));
    }
    while (!unSeqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(endpointTime, unSeqTimeSeriesMetadata.peek().getStatistics())) {
      unpackOneTimeSeriesMetadata(unSeqTimeSeriesMetadata.poll());
    }

    if (firstTimeSeriesMetadata != null
        && orderUtils.isOverlapped(endpointTime, firstTimeSeriesMetadata.getStatistics())) {
      unpackOneTimeSeriesMetadata(firstTimeSeriesMetadata);
      firstTimeSeriesMetadata = null;
    }

    if (init && firstChunkMetadata == null && !cachedChunkMetadata.isEmpty()) {
      firstChunkMetadata = cachedChunkMetadata.poll();
    }
  }

  protected void unpackOneTimeSeriesMetadata(ITimeSeriesMetadata timeSeriesMetadata) {
    List<IChunkMetadata> chunkMetadataList =
        FileLoaderUtils.loadChunkMetadataList(timeSeriesMetadata);
    chunkMetadataList.forEach(chunkMetadata -> chunkMetadata.setSeq(timeSeriesMetadata.isSeq()));

    cachedChunkMetadata.addAll(chunkMetadataList);
  }

  private boolean currentChunkOverlapped() {
    Statistics<? extends Serializable> chunkStatistics = firstChunkMetadata.getStatistics();
    return !cachedChunkMetadata.isEmpty()
        && orderUtils.isOverlapped(chunkStatistics, cachedChunkMetadata.peek().getStatistics());
  }

  public boolean canUseCurrentChunkStatistics() {
    checkState(firstChunkMetadata != null, "no first chunk");

    if (currentChunkOverlapped() || firstChunkMetadata.isModified()) {
      return false;
    }
    return filterAllSatisfy(scanOptions.getGlobalTimeFilter(), firstChunkMetadata)
        && filterAllSatisfy(scanOptions.getPushDownFilter(), firstChunkMetadata);
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentChunkTimeStatistics() {
    return firstChunkMetadata.getTimeStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentChunkStatistics(int index) {
    return firstChunkMetadata.getMeasurementStatistics(index).orElse(null);
  }

  public void skipCurrentChunk() {
    firstChunkMetadata = null;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // page level methods
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * This method should be called after hasNextChunk() until no next page, make sure that all
   * overlapped pages are consumed
   */
  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  public boolean hasNextPage() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    /*
     * has overlapped data before
     */
    if (hasCachedNextOverlappedPage) {
      return true;
    } else if (mergeReader.hasNextTimeValuePair() || firstPageOverlapped()) {
      if (hasNextOverlappedPage()) {
        cachedTsBlock = nextOverlappedPage();
        if (cachedTsBlock != null && !cachedTsBlock.isEmpty()) {
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
      unpackAllOverlappedChunkMetadataToPageReaders(
          orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()), true);
    } else {
      /*
       * first chunk metadata is already unpacked, consume cached pages
       */
      initFirstPageReader();
    }

    if (isExistOverlappedPage()) {
      return true;
    }

    // make sure firstPageReader won't be null while the unSeqPageReaders has more cached page
    // readers
    while (firstPageReader == null && (!seqPageReaders.isEmpty() || !unSeqPageReaders.isEmpty())) {

      initFirstPageReader();

      if (isExistOverlappedPage()) {
        return true;
      }
    }
    return firstPageReader != null;
  }

  private boolean isExistOverlappedPage() throws IOException {
    if (firstPageOverlapped()) {
      /*
       * next page is overlapped, read overlapped data and cache it
       */
      if (hasNextOverlappedPage()) {
        cachedTsBlock = nextOverlappedPage();
        if (cachedTsBlock != null && !cachedTsBlock.isEmpty()) {
          hasCachedNextOverlappedPage = true;
          return true;
        }
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
    unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(endpointTime, false);
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

  private void unpackAllOverlappedChunkMetadataToPageReaders(long endpointTime, boolean init)
      throws IOException {
    if (firstChunkMetadata != null
        && orderUtils.isOverlapped(endpointTime, firstChunkMetadata.getStatistics())) {
      unpackOneChunkMetaData(firstChunkMetadata);
      firstChunkMetadata = null;
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
        FileLoaderUtils.loadPageReaderList(chunkMetaData, scanOptions.getGlobalTimeFilter());
    long timestampInFileName = FileLoaderUtils.getTimestampInFileName(chunkMetaData);

    // init TsBlockBuilder for each page reader
    pageReaderList.forEach(p -> p.initTsBlockBuilder(getTsDataTypeList()));

    if (chunkMetaData.isSeq()) {
      if (orderUtils.getAscending()) {
        for (IPageReader iPageReader : pageReaderList) {
          seqPageReaders.add(
              new VersionPageReader(
                  context,
                  timestampInFileName,
                  chunkMetaData.getVersion(),
                  chunkMetaData.getOffsetOfChunkHeader(),
                  iPageReader,
                  true));
        }
      } else {
        for (int i = pageReaderList.size() - 1; i >= 0; i--) {
          seqPageReaders.add(
              new VersionPageReader(
                  context,
                  timestampInFileName,
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
                  new VersionPageReader(
                      context,
                      timestampInFileName,
                      chunkMetaData.getVersion(),
                      chunkMetaData.getOffsetOfChunkHeader(),
                      pageReader,
                      false)));
    }
  }

  @SuppressWarnings("unchecked")
  private boolean currentPageOverlapped() throws IOException {
    // This method should be called after calling hasNextPage.
    // hasNextPage may cache firstPageReader if it is not overlapped or cached a tsBlock if the
    // first page is overlapped

    // has cached overlapped page
    if (hasCachedNextOverlappedPage) {
      return true;
    }

    // has a non-overlapped page in firstPageReader
    if (mergeReader.hasNextTimeValuePair()) {
      long mergeReaderTime = mergeReader.currentTimeValuePair().getTimestamp();
      if ((orderUtils.getAscending()
              && mergeReaderTime <= firstPageReader.getStatistics().getEndTime())
          || (!orderUtils.getAscending()
              && mergeReaderTime >= firstPageReader.getStatistics().getStartTime())) {
        throw new IllegalStateException("overlapped data should be consumed first");
      }
    }

    Statistics<? extends Serializable> firstPageStatistics = firstPageReader.getStatistics();
    return !unSeqPageReaders.isEmpty()
        && orderUtils.isOverlapped(firstPageStatistics, unSeqPageReaders.peek().getStatistics());
  }

  @SuppressWarnings("unchecked")
  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics<? extends Serializable> currentPageStatistics = currentPageTimeStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    if (currentPageOverlapped() || firstPageReader.isModified()) {
      return false;
    }
    return filterAllSatisfy(scanOptions.getGlobalTimeFilter(), firstPageReader.data)
        && filterAllSatisfy(scanOptions.getPushDownFilter(), firstPageReader.data);
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentPageTimeStatistics() {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getTimeStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentPageStatistics(int index) {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getMeasurementStatistics(index);
  }

  public void skipCurrentPage() {
    firstPageReader = null;
  }

  public TsBlock nextPage() throws IOException {

    if (hasCachedNextOverlappedPage) {
      hasCachedNextOverlappedPage = false;
      TsBlock res =
          applyPushDownFilterAndLimitOffset(
              cachedTsBlock, scanOptions.getPushDownFilter(), paginationController);
      cachedTsBlock = null;

      // cached tsblock has handled by pagination controller & push down filter, return directly
      return res;
    } else {
      // next page is not overlapped, push down filter & limit offset
      filterFirstPageReader();
      if (firstPageReader == null) {
        return null;
      }

      firstPageReader.addPushDownFilter(scanOptions.getPushDownFilter());
      TsBlock tsBlock;
      if (orderUtils.getAscending()) {
        firstPageReader.setLimitOffset(paginationController);
        tsBlock = firstPageReader.getAllSatisfiedPageData(orderUtils.getAscending());
      } else {
        tsBlock =
            paginationController.applyTsBlock(
                firstPageReader.getAllSatisfiedPageData(orderUtils.getAscending()));
      }

      firstPageReader = null;

      return tsBlock;
    }
  }

  private TsBlock applyPushDownFilterAndLimitOffset(
      TsBlock tsBlock, Filter pushDownFilter, PaginationController paginationController) {
    if (pushDownFilter == null) {
      return paginationController.applyTsBlock(tsBlock);
    }
    return TsBlockUtil.applyFilterAndLimitOffsetToTsBlock(
        tsBlock, new TsBlockBuilder(getTsDataTypeList()), pushDownFilter, paginationController);
  }

  private void filterFirstPageReader() {
    if (firstPageReader == null) {
      return;
    }

    IPageReader pageReader = firstPageReader.data;
    if (pageReader.isModified()) {
      return;
    }

    // globalTimeFilter.canSkip() must be FALSE
    Filter pushDownFilter = scanOptions.getPushDownFilter();
    if (pushDownFilter != null && pushDownFilter.canSkip(pageReader)) {
      skipCurrentPage();
      return;
    }

    Filter globalTimeFilter = scanOptions.getGlobalTimeFilter();
    if (filterAllSatisfy(globalTimeFilter, pageReader)
        && filterAllSatisfy(pushDownFilter, pageReader)
        && timeAllSelected(pageReader)) {
      long rowCount = pageReader.getStatistics().getCount();
      if (paginationController.hasCurOffset(rowCount)) {
        skipCurrentPage();
        paginationController.consumeOffset(rowCount);
      }
    }
  }

  /**
   * read overlapped data till currentLargestEndTime in mergeReader, if current batch does not
   * contain data, read till next currentLargestEndTime again
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean hasNextOverlappedPage() throws IOException {
    long startTime = System.nanoTime();
    try {
      if (hasCachedNextOverlappedPage) {
        return true;
      }

      tryToPutAllDirectlyOverlappedUnseqPageReadersIntoMergeReader();

      while (true) {

        // may has overlapped data
        if (mergeReader.hasNextTimeValuePair()) {

          TsBlockBuilder builder = new TsBlockBuilder(getTsDataTypeList());
          long currentPageEndPointTime = mergeReader.getCurrentReadStopTime();
          while (mergeReader.hasNextTimeValuePair()) {

            /*
             * get current first point in mergeReader, this maybe overlapped later
             */
            TimeValuePair timeValuePair = mergeReader.currentTimeValuePair();

            if (orderUtils.isExcessEndpoint(
                timeValuePair.getTimestamp(), currentPageEndPointTime)) {
              /*
               * when the merged point excesses the currentPageEndPointTime, we have read all overlapped data before currentPageEndPointTime
               * 1. has cached batch data, we don't need to read more data, just use the cached data later
               * 2. has first page reader, which means first page reader last endTime < currentTimeValuePair.getTimestamp(),
               * we could just use the first page reader later
               * 3. sequence page reader is not empty, which means first page reader last endTime < currentTimeValuePair.getTimestamp(),
               * we could use the first sequence page reader later
               */
              if (!builder.isEmpty() || firstPageReader != null || !seqPageReaders.isEmpty()) {
                break;
              }
              // so, we don't have other data except mergeReader
              currentPageEndPointTime = mergeReader.getCurrentReadStopTime();
            }

            // unpack all overlapped data for the first timeValuePair
            unpackAllOverlappedTsFilesToTimeSeriesMetadata(timeValuePair.getTimestamp());
            unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
                timeValuePair.getTimestamp(), false);
            unpackAllOverlappedChunkMetadataToPageReaders(timeValuePair.getTimestamp(), false);
            unpackAllOverlappedUnseqPageReadersToMergeReader(timeValuePair.getTimestamp());

            // update if there are unpacked unSeqPageReaders
            timeValuePair = mergeReader.currentTimeValuePair();

            // from now, the unsequence reader is all unpacked, so we don't need to consider it
            // we has first page reader now
            if (firstPageReader != null) {
              // if current timeValuePair excesses the first page reader's end time, we just use the
              // cached data
              if ((orderUtils.getAscending()
                      && timeValuePair.getTimestamp()
                          > firstPageReader.getStatistics().getEndTime())
                  || (!orderUtils.getAscending()
                      && timeValuePair.getTimestamp()
                          < firstPageReader.getStatistics().getStartTime())) {
                hasCachedNextOverlappedPage = !builder.isEmpty();
                cachedTsBlock = builder.build();
                return hasCachedNextOverlappedPage;
              } else if (orderUtils.isOverlapped(
                  timeValuePair.getTimestamp(), firstPageReader.getStatistics())) {
                // current timeValuePair is overlapped with firstPageReader, add it to merged reader
                // and update endTime to the max end time
                mergeReader.addReader(
                    getPointReader(
                        firstPageReader.getAllSatisfiedPageData(orderUtils.getAscending())),
                    firstPageReader.version,
                    orderUtils.getOverlapCheckTime(firstPageReader.getStatistics()));
                context
                    .getQueryStatistics()
                    .getPageReaderMaxUsedMemorySize()
                    .updateAndGet(v -> Math.max(v, mergeReader.getUsedMemorySize()));
                currentPageEndPointTime =
                    updateEndPointTime(currentPageEndPointTime, firstPageReader);
                firstPageReader = null;
              }
            }

            // the seq page readers is not empty, just like first page reader
            if (!seqPageReaders.isEmpty()) {
              if ((orderUtils.getAscending()
                      && timeValuePair.getTimestamp()
                          > seqPageReaders.get(0).getStatistics().getEndTime())
                  || (!orderUtils.getAscending()
                      && timeValuePair.getTimestamp()
                          < seqPageReaders.get(0).getStatistics().getStartTime())) {
                hasCachedNextOverlappedPage = !builder.isEmpty();
                cachedTsBlock = builder.build();
                return hasCachedNextOverlappedPage;
              } else if (orderUtils.isOverlapped(
                  timeValuePair.getTimestamp(), seqPageReaders.get(0).getStatistics())) {
                VersionPageReader pageReader = seqPageReaders.remove(0);
                mergeReader.addReader(
                    getPointReader(pageReader.getAllSatisfiedPageData(orderUtils.getAscending())),
                    pageReader.version,
                    orderUtils.getOverlapCheckTime(pageReader.getStatistics()));
                context
                    .getQueryStatistics()
                    .getPageReaderMaxUsedMemorySize()
                    .updateAndGet(v -> Math.max(v, mergeReader.getUsedMemorySize()));
                currentPageEndPointTime = updateEndPointTime(currentPageEndPointTime, pageReader);
              }
            }

            // get the latest first point in mergeReader
            timeValuePair = mergeReader.nextTimeValuePair();
            addTimeValuePairToResult(timeValuePair, builder);
          }
          hasCachedNextOverlappedPage = !builder.isEmpty();
          cachedTsBlock = builder.build();
          /*
           * if current overlapped page has valid data, return, otherwise read next overlapped page
           */
          if (hasCachedNextOverlappedPage) {
            return true;
          } else if (mergeReader.hasNextTimeValuePair()) {
            // condition: seqPage.endTime < mergeReader.currentTime
            return false;
          }
        } else {
          return false;
        }
      }
    } finally {
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          isAligned
              ? BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED
              : BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED,
          System.nanoTime() - startTime);
    }
  }

  private long updateEndPointTime(long currentPageEndPointTime, VersionPageReader pageReader) {
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

  private void addTimeValuePairToResult(TimeValuePair timeValuePair, TsBlockBuilder builder) {
    builder.getTimeColumnBuilder().writeLong(timeValuePair.getTimestamp());
    switch (dataType) {
      case BOOLEAN:
        builder.getColumnBuilder(0).writeBoolean(timeValuePair.getValue().getBoolean());
        break;
      case INT32:
      case DATE:
        builder.getColumnBuilder(0).writeInt(timeValuePair.getValue().getInt());
        break;
      case INT64:
      case TIMESTAMP:
        builder.getColumnBuilder(0).writeLong(timeValuePair.getValue().getLong());
        break;
      case FLOAT:
        builder.getColumnBuilder(0).writeFloat(timeValuePair.getValue().getFloat());
        break;
      case DOUBLE:
        builder.getColumnBuilder(0).writeDouble(timeValuePair.getValue().getDouble());
        break;
      case TEXT:
      case BLOB:
      case STRING:
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

  private void initFirstPageReader() throws IOException {
    while (this.firstPageReader == null) {
      VersionPageReader firstPageReader = getFirstPageReaderFromCachedReaders();

      // unpack overlapped page using current page reader
      if (firstPageReader != null) {
        long overlapCheckTime = orderUtils.getOverlapCheckTime(firstPageReader.getStatistics());
        unpackAllOverlappedTsFilesToTimeSeriesMetadata(overlapCheckTime);
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(overlapCheckTime, false);
        unpackAllOverlappedChunkMetadataToPageReaders(overlapCheckTime, false);

        // this page after unpacking must be the first page
        if (firstPageReader.equals(getFirstPageReaderFromCachedReaders())) {
          this.firstPageReader = firstPageReader;
          if (!seqPageReaders.isEmpty() && firstPageReader.equals(seqPageReaders.get(0))) {
            seqPageReaders.remove(0);
            break;
          } else if (!unSeqPageReaders.isEmpty()
              && firstPageReader.equals(unSeqPageReaders.peek())) {
            unSeqPageReaders.poll();
            break;
          }
        }
      } else {
        return;
      }
    }
  }

  // We use get() and peek() here in case it's not the first page reader before unpacking
  private VersionPageReader getFirstPageReaderFromCachedReaders() {
    VersionPageReader firstPageReader = null;
    if (!seqPageReaders.isEmpty() && !unSeqPageReaders.isEmpty()) {
      if (orderUtils.isTakeSeqAsFirst(
          seqPageReaders.get(0).getStatistics(), unSeqPageReaders.peek().getStatistics())) {
        firstPageReader = seqPageReaders.get(0);
      } else {
        firstPageReader = unSeqPageReaders.peek();
      }
    } else if (!seqPageReaders.isEmpty()) {
      firstPageReader = seqPageReaders.get(0);
    } else if (!unSeqPageReaders.isEmpty()) {
      firstPageReader = unSeqPageReaders.peek();
    }
    return firstPageReader;
  }

  private void unpackAllOverlappedUnseqPageReadersToMergeReader(long endpointTime)
      throws IOException {
    while (!unSeqPageReaders.isEmpty()
        && orderUtils.isOverlapped(endpointTime, unSeqPageReaders.peek().data.getStatistics())) {
      putPageReaderToMergeReader(unSeqPageReaders.poll());
    }
    if (firstPageReader != null
        && !firstPageReader.isSeq()
        && orderUtils.isOverlapped(endpointTime, firstPageReader.getStatistics())) {
      putPageReaderToMergeReader(firstPageReader);
      firstPageReader = null;
    }
  }

  private void putPageReaderToMergeReader(VersionPageReader pageReader) throws IOException {
    mergeReader.addReader(
        getPointReader(pageReader.getAllSatisfiedPageData(orderUtils.getAscending())),
        pageReader.version,
        orderUtils.getOverlapCheckTime(pageReader.getStatistics()));
    context
        .getQueryStatistics()
        .getPageReaderMaxUsedMemorySize()
        .updateAndGet(v -> Math.max(v, mergeReader.getUsedMemorySize()));
  }

  private TsBlock nextOverlappedPage() throws IOException {
    if (hasCachedNextOverlappedPage || hasNextOverlappedPage()) {
      hasCachedNextOverlappedPage = false;
      return cachedTsBlock;
    }
    throw new IOException("No more batch data");
  }

  /**
   * unpack all overlapped seq/unseq files and find the first TimeSeriesMetadata
   *
   * <p>Because there may be too many files in the scenario used by the user, we cannot open all the
   * chunks at once, which may cause OOM, so we can only unpack one file at a time when needed. This
   * approach is likely to be ubiquitous, but it keeps the system running smoothly
   *
   * @throws IOException exception in unpacking
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void tryToUnpackAllOverlappedFilesToTimeSeriesMetadata() throws IOException {
    /*
     * Fill sequence TimeSeriesMetadata List until it is not empty
     */
    while (seqTimeSeriesMetadata.isEmpty() && orderUtils.hasNextSeqResource()) {
      unpackSeqTsFileResource();
    }

    /*
     * Fill unSequence TimeSeriesMetadata Priority Queue until it is not empty
     */
    while (unSeqTimeSeriesMetadata.isEmpty() && orderUtils.hasNextUnseqResource()) {
      unpackUnseqTsFileResource();
    }

    /*
     * find end time of the first TimeSeriesMetadata
     */
    long endTime = -1L;
    if (!seqTimeSeriesMetadata.isEmpty() && unSeqTimeSeriesMetadata.isEmpty()) {
      // only has seq
      endTime = orderUtils.getOverlapCheckTime(seqTimeSeriesMetadata.get(0).getStatistics());
    } else if (seqTimeSeriesMetadata.isEmpty() && !unSeqTimeSeriesMetadata.isEmpty()) {
      // only has unseq
      endTime = orderUtils.getOverlapCheckTime(unSeqTimeSeriesMetadata.peek().getStatistics());
    } else if (!seqTimeSeriesMetadata.isEmpty()) {
      // has seq and unseq
      endTime =
          orderUtils.getCurrentEndPoint(
              seqTimeSeriesMetadata.get(0).getStatistics(),
              unSeqTimeSeriesMetadata.peek().getStatistics());
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
      if (orderUtils.isTakeSeqAsFirst(
          seqTimeSeriesMetadata.get(0).getStatistics(),
          unSeqTimeSeriesMetadata.peek().getStatistics())) {
        firstTimeSeriesMetadata = seqTimeSeriesMetadata.remove(0);
      } else {
        firstTimeSeriesMetadata = unSeqTimeSeriesMetadata.poll();
      }
    }
  }

  private void filterFirstTimeSeriesMetadata() {
    if (firstTimeSeriesMetadata == null) {
      return;
    }

    if (currentFileOverlapped() || firstTimeSeriesMetadata.isModified()) {
      return;
    }

    // globalTimeFilter.canSkip() must be FALSE
    Filter pushDownFilter = scanOptions.getPushDownFilter();
    if (pushDownFilter != null && pushDownFilter.canSkip(firstTimeSeriesMetadata)) {
      skipCurrentFile();
      return;
    }

    Filter globalTimeFilter = scanOptions.getGlobalTimeFilter();
    if (filterAllSatisfy(globalTimeFilter, firstTimeSeriesMetadata)
        && filterAllSatisfy(pushDownFilter, firstTimeSeriesMetadata)
        && timeAllSelected(firstTimeSeriesMetadata)) {
      long rowCount = firstTimeSeriesMetadata.getStatistics().getCount();
      if (paginationController.hasCurOffset(rowCount)) {
        skipCurrentFile();
        paginationController.consumeOffset(rowCount);
      }
    }
  }

  private void unpackAllOverlappedTsFilesToTimeSeriesMetadata(long endpointTime)
      throws IOException {
    while (orderUtils.hasNextUnseqResource() && orderUtils.isCurUnSeqOverlappedWith(endpointTime)) {
      unpackUnseqTsFileResource();
    }
    while (orderUtils.hasNextSeqResource() && orderUtils.isCurSeqOverlappedWith(endpointTime)) {
      unpackSeqTsFileResource();
    }
  }

  private void unpackSeqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(orderUtils.getNextSeqFileResource(true), true);
    // skip if data type is mismatched which may be caused by delete
    if (timeseriesMetadata != null && timeseriesMetadata.typeMatch(getTsDataTypeList())) {
      timeseriesMetadata.setSeq(true);
      seqTimeSeriesMetadata.add(timeseriesMetadata);
    }
  }

  private void unpackUnseqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(orderUtils.getNextUnseqFileResource(true), false);
    // skip if data type is mismatched which may be caused by delete
    if (timeseriesMetadata != null && timeseriesMetadata.typeMatch(getTsDataTypeList())) {
      timeseriesMetadata.setSeq(false);
      unSeqTimeSeriesMetadata.add(timeseriesMetadata);
    }
  }

  protected ITimeSeriesMetadata loadTimeSeriesMetadata(TsFileResource resource, boolean isSeq)
      throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource,
        (NonAlignedFullPath) seriesPath,
        context,
        scanOptions.getGlobalTimeFilter(),
        scanOptions.getAllSensors(),
        isSeq);
  }

  public List<TSDataType> getTsDataTypeList() {
    return Collections.singletonList(dataType);
  }

  protected IPointReader getPointReader(TsBlock tsBlock) {
    return tsBlock.getTsBlockSingleColumnIterator();
  }

  protected boolean timeAllSelected(IMetadata metadata) {
    return true;
  }

  private boolean filterAllSatisfy(Filter filter, IMetadata metadata) {
    return filter == null || filter.allSatisfy(metadata);
  }

  protected static class VersionPageReader {
    private final QueryContext context;
    private final MergeReaderPriority version;
    private final IPageReader data;

    private final boolean isSeq;
    private final boolean isAligned;
    private final boolean isMem;

    VersionPageReader(
        QueryContext context,
        long fileTimestamp,
        long version,
        long offset,
        IPageReader data,
        boolean isSeq) {
      this.context = context;
      this.version = new MergeReaderPriority(fileTimestamp, version, offset, isSeq);
      this.data = data;
      this.isSeq = isSeq;
      this.isAligned = data instanceof AlignedPageReader || data instanceof MemAlignedPageReader;
      this.isMem = data instanceof MemPageReader || data instanceof MemAlignedPageReader;
    }

    @SuppressWarnings("squid:S3740")
    Statistics getStatistics() {
      return data.getStatistics();
    }

    @SuppressWarnings("squid:S3740")
    Statistics getMeasurementStatistics(int index) {
      return data.getMeasurementStatistics(index).orElse(null);
    }

    @SuppressWarnings("squid:S3740")
    Statistics getTimeStatistics() {
      return data.getTimeStatistics();
    }

    TsBlock getAllSatisfiedPageData(boolean ascending) throws IOException {
      long startTime = System.nanoTime();
      try {
        TsBlock tsBlock = data.getAllSatisfiedData();
        if (!ascending) {
          tsBlock.reverse();
        }
        return tsBlock;
      } finally {
        long time = System.nanoTime() - startTime;
        if (isAligned) {
          if (isMem) {
            context.getQueryStatistics().getPageReadersDecodeAlignedMemCount().getAndAdd(1);
            context.getQueryStatistics().getPageReadersDecodeAlignedMemTime().getAndAdd(time);
          } else {
            context.getQueryStatistics().getPageReadersDecodeAlignedDiskCount().getAndAdd(1);
            context.getQueryStatistics().getPageReadersDecodeAlignedDiskTime().getAndAdd(time);
          }
        } else {
          if (isMem) {
            context.getQueryStatistics().getPageReadersDecodeNonAlignedMemCount().getAndAdd(1);
            context.getQueryStatistics().getPageReadersDecodeNonAlignedMemTime().getAndAdd(time);
          } else {
            context.getQueryStatistics().getPageReadersDecodeNonAlignedDiskCount().getAndAdd(1);
            context.getQueryStatistics().getPageReadersDecodeNonAlignedDiskTime().getAndAdd(time);
          }
        }
      }
    }

    void addPushDownFilter(Filter pushDownFilter) {
      data.addRecordFilter(pushDownFilter);
    }

    boolean isModified() {
      return data.isModified();
    }

    public boolean isSeq() {
      return isSeq;
    }

    public void setLimitOffset(PaginationController paginationController) {
      data.setLimitOffset(paginationController);
    }
  }

  public interface TimeOrderUtils {

    long getOrderTime(Statistics<? extends Object> statistics);

    long getOverlapCheckTime(Statistics<? extends Object> range);

    boolean isOverlapped(Statistics<? extends Object> left, Statistics<? extends Object> right);

    boolean isOverlapped(long time, Statistics<? extends Object> right);

    boolean isCurSeqOverlappedWith(long time);

    boolean isCurUnSeqOverlappedWith(long time);

    <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor);

    long getCurrentEndPoint(long time, Statistics<? extends Object> statistics);

    long getCurrentEndPoint(
        Statistics<? extends Object> seqStatistics, Statistics<? extends Object> unseqStatistics);

    boolean isExcessEndpoint(long time, long endpointTime);

    /** Return true if taking first page reader from seq readers */
    boolean isTakeSeqAsFirst(
        Statistics<? extends Object> seqStatistics, Statistics<? extends Object> unseqStatistics);

    boolean getAscending();

    boolean hasNextSeqResource();

    boolean hasNextUnseqResource();

    TsFileResource getNextSeqFileResource(boolean isDelete);

    TsFileResource getNextUnseqFileResource(boolean isDelete);

    void setCurSeqFileIndex(QueryDataSource dataSource);
  }

  class DescTimeOrderUtils implements TimeOrderUtils {

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getEndTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOverlapCheckTime(Statistics range) {
      return range.getStartTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public boolean isOverlapped(Statistics left, Statistics right) {
      return left.getStartTime() <= right.getEndTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public boolean isOverlapped(long time, Statistics right) {
      return time <= right.getEndTime();
    }

    @Override
    public boolean isCurSeqOverlappedWith(long time) {
      return time <= dataSource.getCurrentSeqOrderTime(curSeqFileIndex);
    }

    @Override
    public boolean isCurUnSeqOverlappedWith(long time) {
      return time <= dataSource.getCurrentUnSeqOrderTime(curUnseqFileIndex);
    }

    @Override
    public <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor) {
      Objects.requireNonNull(keyExtractor);
      return (Comparator<T> & Serializable)
          (c1, c2) -> Long.compare(keyExtractor.applyAsLong(c2), keyExtractor.applyAsLong(c1));
    }

    @Override
    public long getCurrentEndPoint(long time, Statistics<? extends Object> statistics) {
      return Math.max(time, statistics.getStartTime());
    }

    @Override
    public long getCurrentEndPoint(
        Statistics<? extends Object> seqStatistics, Statistics<? extends Object> unseqStatistics) {
      return Math.max(seqStatistics.getStartTime(), unseqStatistics.getStartTime());
    }

    @Override
    public boolean isExcessEndpoint(long time, long endpointTime) {
      return time < endpointTime;
    }

    @Override
    public boolean isTakeSeqAsFirst(
        Statistics<? extends Object> seqStatistics, Statistics<? extends Object> unseqStatistics) {
      return seqStatistics.getEndTime() > unseqStatistics.getEndTime();
    }

    @Override
    public boolean getAscending() {
      return false;
    }

    @Override
    public boolean hasNextSeqResource() {
      while (dataSource.hasNextSeqResource(curSeqFileIndex, false, deviceID)) {
        if (dataSource.isSeqSatisfied(
            deviceID, curSeqFileIndex, scanOptions.getGlobalTimeFilter(), false)) {
          break;
        }
        curSeqFileIndex--;
      }
      return dataSource.hasNextSeqResource(curSeqFileIndex, false, deviceID);
    }

    @Override
    public boolean hasNextUnseqResource() {
      while (dataSource.hasNextUnseqResource(curUnseqFileIndex, false, deviceID)) {
        if (dataSource.isUnSeqSatisfied(
            deviceID, curUnseqFileIndex, scanOptions.getGlobalTimeFilter(), false)) {
          break;
        }
        curUnseqFileIndex++;
      }
      return dataSource.hasNextUnseqResource(curUnseqFileIndex, false, deviceID);
    }

    @Override
    public TsFileResource getNextSeqFileResource(boolean isDelete) {
      TsFileResource tsFileResource = dataSource.getSeqResourceByIndex(curSeqFileIndex);
      if (isDelete) {
        curSeqFileIndex--;
      }
      return tsFileResource;
    }

    @Override
    public TsFileResource getNextUnseqFileResource(boolean isDelete) {
      TsFileResource tsFileResource = dataSource.getUnseqResourceByIndex(curUnseqFileIndex);
      if (isDelete) {
        curUnseqFileIndex++;
      }
      return tsFileResource;
    }

    @Override
    public void setCurSeqFileIndex(QueryDataSource dataSource) {
      curSeqFileIndex = dataSource.getSeqResourcesSize() - 1;
    }
  }

  class AscTimeOrderUtils implements TimeOrderUtils {

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getStartTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOverlapCheckTime(Statistics range) {
      return range.getEndTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public boolean isOverlapped(Statistics left, Statistics right) {
      return left.getEndTime() >= right.getStartTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public boolean isOverlapped(long time, Statistics right) {
      return time >= right.getStartTime();
    }

    @Override
    public boolean isCurSeqOverlappedWith(long time) {
      return time >= dataSource.getCurrentSeqOrderTime(curSeqFileIndex);
    }

    @Override
    public boolean isCurUnSeqOverlappedWith(long time) {
      return time >= dataSource.getCurrentUnSeqOrderTime(curUnseqFileIndex);
    }

    @Override
    public <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor) {
      Objects.requireNonNull(keyExtractor);
      return (Comparator<T> & Serializable)
          (c1, c2) -> Long.compare(keyExtractor.applyAsLong(c1), keyExtractor.applyAsLong(c2));
    }

    @Override
    public long getCurrentEndPoint(long time, Statistics<? extends Object> statistics) {
      return Math.min(time, statistics.getEndTime());
    }

    @Override
    public long getCurrentEndPoint(
        Statistics<? extends Object> seqStatistics, Statistics<? extends Object> unseqStatistics) {
      return Math.min(seqStatistics.getEndTime(), unseqStatistics.getEndTime());
    }

    @Override
    public boolean isExcessEndpoint(long time, long endpointTime) {
      return time > endpointTime;
    }

    @Override
    public boolean isTakeSeqAsFirst(
        Statistics<? extends Object> seqStatistics, Statistics<? extends Object> unseqStatistics) {
      return seqStatistics.getStartTime() < unseqStatistics.getStartTime();
    }

    @Override
    public boolean getAscending() {
      return true;
    }

    @Override
    public boolean hasNextSeqResource() {
      while (dataSource.hasNextSeqResource(curSeqFileIndex, true, deviceID)) {
        if (dataSource.isSeqSatisfied(
            deviceID, curSeqFileIndex, scanOptions.getGlobalTimeFilter(), false)) {
          break;
        }
        curSeqFileIndex++;
      }
      return dataSource.hasNextSeqResource(curSeqFileIndex, true, deviceID);
    }

    @Override
    public boolean hasNextUnseqResource() {
      while (dataSource.hasNextUnseqResource(curUnseqFileIndex, true, deviceID)) {
        if (dataSource.isUnSeqSatisfied(
            deviceID, curUnseqFileIndex, scanOptions.getGlobalTimeFilter(), false)) {
          break;
        }
        curUnseqFileIndex++;
      }
      return dataSource.hasNextUnseqResource(curUnseqFileIndex, true, deviceID);
    }

    @Override
    public TsFileResource getNextSeqFileResource(boolean isDelete) {
      TsFileResource tsFileResource = dataSource.getSeqResourceByIndex(curSeqFileIndex);
      if (isDelete) {
        curSeqFileIndex++;
      }
      return tsFileResource;
    }

    @Override
    public TsFileResource getNextUnseqFileResource(boolean isDelete) {
      TsFileResource tsFileResource = dataSource.getUnseqResourceByIndex(curUnseqFileIndex);
      if (isDelete) {
        curUnseqFileIndex++;
      }
      return tsFileResource;
    }

    @Override
    public void setCurSeqFileIndex(QueryDataSource dataSource) {
      curSeqFileIndex = 0;
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + deviceID.ramBytesUsed() + seriesPath.ramBytesUsed();
  }
}
