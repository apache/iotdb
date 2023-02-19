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
package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.MemAlignedPageReader;
import org.apache.iotdb.db.query.reader.chunk.MemPageReader;
import org.apache.iotdb.db.query.reader.universal.DescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IAlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.ToLongFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK;
import static org.apache.iotdb.db.mpp.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM;

public class SeriesScanUtil {

  private final QueryContext context;

  // The path of the target series which will be scanned.
  private final PartialPath seriesPath;
  protected boolean isAligned = false;
  protected final TSDataType dataType;

  // inner class of SeriesReader for order purpose
  private final TimeOrderUtils orderUtils;

  private QueryDataSource dataSource;

  // file index
  protected int curSeqFileIndex;
  protected int curUnseqFileIndex;

  // TimeSeriesMetadata cache
  protected ITimeSeriesMetadata firstTimeSeriesMetadata;
  protected final List<ITimeSeriesMetadata> seqTimeSeriesMetadata;
  protected final PriorityQueue<ITimeSeriesMetadata> unSeqTimeSeriesMetadata;

  // chunk cache
  protected IChunkMetadata firstChunkMetadata;
  protected final PriorityQueue<IChunkMetadata> cachedChunkMetadata;

  // page cache
  protected VersionPageReader firstPageReader;
  protected final List<VersionPageReader> seqPageReaders;
  protected final PriorityQueue<VersionPageReader> unSeqPageReaders;

  // point cache
  protected final PriorityMergeReader mergeReader;

  // result cache
  protected boolean hasCachedNextOverlappedPage;
  protected TsBlock cachedTsBlock;

  protected SeriesScanOptions scanOptions;
  protected PaginationController paginationController;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public SeriesScanUtil(
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context) {
    this.seriesPath = IDTable.translateQueryPath(seriesPath);
    dataType = seriesPath.getSeriesType();

    this.scanOptions = scanOptions;
    paginationController = scanOptions.getPaginationController();

    this.context = context;

    if (scanOrder.isAscending()) {
      orderUtils = new AscTimeOrderUtils();
      mergeReader = getPriorityMergeReader();
    } else {
      orderUtils = new DescTimeOrderUtils();
      mergeReader = getDescPriorityMergeReader();
    }

    // init TimeSeriesMetadata materializer
    seqTimeSeriesMetadata = new LinkedList<>();
    unSeqTimeSeriesMetadata =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                timeSeriesMetadata -> orderUtils.getOrderTime(timeSeriesMetadata.getStatistics())));

    // init ChunkMetadata materializer
    cachedChunkMetadata =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                chunkMetadata -> orderUtils.getOrderTime(chunkMetadata.getStatistics())));

    // init PageReader materializer
    seqPageReaders = new LinkedList<>();
    unSeqPageReaders =
        new PriorityQueue<>(
            orderUtils.comparingLong(
                versionPageReader -> orderUtils.getOrderTime(versionPageReader.getStatistics())));
  }

  public void initQueryDataSource(QueryDataSource dataSource) {
    dataSource.fillOrderIndexes(seriesPath.getDevice(), orderUtils.getAscending());
    this.dataSource = dataSource;

    // updated filter concerning TTL
    scanOptions.setTTL(dataSource.getDataTTL());

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

  public boolean hasNextFile() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    if (!unSeqPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IOException(
          "all cached pages should be consumed first unSeqPageReaders.isEmpty() is "
              + unSeqPageReaders.isEmpty()
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

  boolean isFileOverlapped() throws IOException {
    if (firstTimeSeriesMetadata == null) {
      throw new IOException("no first file");
    }

    Statistics fileStatistics = firstTimeSeriesMetadata.getStatistics();
    return !seqTimeSeriesMetadata.isEmpty()
            && orderUtils.isOverlapped(fileStatistics, seqTimeSeriesMetadata.get(0).getStatistics())
        || !unSeqTimeSeriesMetadata.isEmpty()
            && orderUtils.isOverlapped(
                fileStatistics, unSeqTimeSeriesMetadata.peek().getStatistics());
  }

  Statistics currentFileStatistics() {
    return firstTimeSeriesMetadata.getStatistics();
  }

  protected Statistics currentFileStatistics(int index) throws IOException {
    checkArgument(index == 0, "Only one sensor in non-aligned SeriesScanUtil.");
    return currentFileStatistics();
  }

  protected Statistics currentFileTimeStatistics() throws IOException {
    return currentFileStatistics();
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
  public boolean hasNextChunk() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    if (!unSeqPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IOException(
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

  protected void filterFirstChunkMetadata() throws IOException {
    if (firstChunkMetadata != null && !isChunkOverlapped() && !firstChunkMetadata.isModified()) {
      Filter queryFilter = scanOptions.getQueryFilter();
      if (queryFilter != null) {
        if (!queryFilter.satisfy(firstChunkMetadata.getStatistics())) {
          skipCurrentChunk();
        }
        // TODO implement allSatisfied interface for filter, then we can still skip offset.
      } else {
        long rowCount = firstChunkMetadata.getStatistics().getCount();
        if (paginationController.hasCurOffset(rowCount)) {
          skipCurrentChunk();
          paginationController.consumeOffset(rowCount);
        }
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
      long endpointTime, boolean init) throws IOException {
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

  protected void unpackOneTimeSeriesMetadata(ITimeSeriesMetadata timeSeriesMetadata)
      throws IOException {
    List<IChunkMetadata> chunkMetadataList =
        FileLoaderUtils.loadChunkMetadataList(timeSeriesMetadata);
    chunkMetadataList.forEach(chunkMetadata -> chunkMetadata.setSeq(timeSeriesMetadata.isSeq()));

    cachedChunkMetadata.addAll(chunkMetadataList);
  }

  boolean isChunkOverlapped() throws IOException {
    if (firstChunkMetadata == null) {
      throw new IOException("no first chunk");
    }

    Statistics chunkStatistics = firstChunkMetadata.getStatistics();
    return !cachedChunkMetadata.isEmpty()
        && orderUtils.isOverlapped(chunkStatistics, cachedChunkMetadata.peek().getStatistics());
  }

  Statistics currentChunkStatistics() {
    return firstChunkMetadata.getStatistics();
  }

  protected Statistics currentChunkStatistics(int index) throws IOException {
    checkArgument(index == 0, "Only one sensor in non-aligned SeriesScanUtil.");
    return currentChunkStatistics();
  }

  protected Statistics currentChunkTimeStatistics() throws IOException {
    return currentChunkStatistics();
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
        FileLoaderUtils.loadPageReaderList(chunkMetaData, getGlobalTimeFilter());

    // init TsBlockBuilder for each page reader
    pageReaderList.forEach(p -> p.initTsBlockBuilder(getTsDataTypeList()));

    if (chunkMetaData.isSeq()) {
      if (orderUtils.getAscending()) {
        for (IPageReader iPageReader : pageReaderList) {
          seqPageReaders.add(
              new VersionPageReader(
                  chunkMetaData.getVersion(),
                  chunkMetaData.getOffsetOfChunkHeader(),
                  iPageReader,
                  true));
        }
      } else {
        for (int i = pageReaderList.size() - 1; i >= 0; i--) {
          seqPageReaders.add(
              new VersionPageReader(
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
                      chunkMetaData.getVersion(),
                      chunkMetaData.getOffsetOfChunkHeader(),
                      pageReader,
                      false)));
    }
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
    if (mergeReader.hasNextTimeValuePair()
        && ((orderUtils.getAscending()
                && mergeReader.currentTimeValuePair().getTimestamp()
                    <= firstPageReader.getStatistics().getEndTime())
            || (!orderUtils.getAscending()
                && mergeReader.currentTimeValuePair().getTimestamp()
                    >= firstPageReader.getStatistics().getStartTime()))) {
      throw new IOException("overlapped data should be consumed first");
    }

    Statistics firstPageStatistics = firstPageReader.getStatistics();

    return !unSeqPageReaders.isEmpty()
        && orderUtils.isOverlapped(firstPageStatistics, unSeqPageReaders.peek().getStatistics());
  }

  Statistics currentPageStatistics() {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getStatistics();
  }

  protected Statistics currentPageStatistics(int index) throws IOException {
    checkArgument(index == 0, "Only one sensor in non-aligned SeriesScanUtil.");
    return currentPageStatistics();
  }

  protected Statistics currentPageTimeStatistics() throws IOException {
    return currentPageStatistics();
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

  /** This method should only be used when the method isPageOverlapped() return true. */
  public TsBlock nextPage() throws IOException {

    if (hasCachedNextOverlappedPage) {
      hasCachedNextOverlappedPage = false;
      TsBlock res = cachedTsBlock;
      cachedTsBlock = null;
      return res;
    } else {
      // next page is not overlapped, push down value filter & limit offset
      Filter queryFilter = scanOptions.getQueryFilter();
      if (queryFilter != null) {
        firstPageReader.setFilter(queryFilter);
      }
      firstPageReader.setLimitOffset(paginationController);
      TsBlock tsBlock = firstPageReader.getAllSatisfiedPageData(orderUtils.getAscending());
      firstPageReader = null;

      return tsBlock;
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

          // TODO we still need to consider data type, ascending and descending here
          TsBlockBuilder builder = new TsBlockBuilder(getTsDataTypeList());
          TimeColumnBuilder timeBuilder = builder.getTimeColumnBuilder();
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
                    orderUtils.getOverlapCheckTime(firstPageReader.getStatistics()),
                    context);
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
                    orderUtils.getOverlapCheckTime(pageReader.getStatistics()),
                    context);
                currentPageEndPointTime = updateEndPointTime(currentPageEndPointTime, pageReader);
              }
            }

            /*
             * get the latest first point in mergeReader
             */
            timeValuePair = mergeReader.nextTimeValuePair();

            Object valueForFilter = timeValuePair.getValue().getValue();

            // TODO fix value filter firstNotNullObject, currently, if it's a value filter, it will
            // only accept AlignedPath with only one sub sensor
            if (timeValuePair.getValue().getDataType() == TSDataType.VECTOR) {
              for (TsPrimitiveType tsPrimitiveType : timeValuePair.getValue().getVector()) {
                if (tsPrimitiveType != null) {
                  valueForFilter = tsPrimitiveType.getValue();
                  break;
                }
              }
            }

            Filter queryFilter = scanOptions.getQueryFilter();
            if (queryFilter != null
                && !queryFilter.satisfy(timeValuePair.getTimestamp(), valueForFilter)) {
              continue;
            }
            if (paginationController.hasCurOffset()) {
              paginationController.consumeOffset();
              continue;
            }
            if (paginationController.hasCurLimit()) {
              timeBuilder.writeLong(timeValuePair.getTimestamp());
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
              paginationController.consumeLimit();
            } else {
              break;
            }
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
      QUERY_METRICS.recordSeriesScanCost(
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
        orderUtils.getOverlapCheckTime(pageReader.getStatistics()),
        context);
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
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  protected void tryToUnpackAllOverlappedFilesToTimeSeriesMetadata() throws IOException {
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

  protected void filterFirstTimeSeriesMetadata() throws IOException {
    if (firstTimeSeriesMetadata != null
        && !isFileOverlapped()
        && !firstTimeSeriesMetadata.isModified()) {
      Filter queryFilter = scanOptions.getQueryFilter();
      if (queryFilter != null) {
        if (!queryFilter.satisfy(firstTimeSeriesMetadata.getStatistics())) {
          skipCurrentFile();
        }
        // TODO implement allSatisfied interface for filter, then we can still skip offset.
      } else {
        long rowCount = firstTimeSeriesMetadata.getStatistics().getCount();
        if (paginationController.hasCurOffset(rowCount)) {
          skipCurrentFile();
          paginationController.consumeOffset(rowCount);
        }
      }
    }
  }

  protected void unpackAllOverlappedTsFilesToTimeSeriesMetadata(long endpointTime)
      throws IOException {
    while (orderUtils.hasNextUnseqResource()
        && orderUtils.isOverlapped(endpointTime, orderUtils.getNextUnseqFileResource(false))) {
      unpackUnseqTsFileResource();
    }
    while (orderUtils.hasNextSeqResource()
        && orderUtils.isOverlapped(endpointTime, orderUtils.getNextSeqFileResource(false))) {
      unpackSeqTsFileResource();
    }
  }

  private void unpackSeqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(
            orderUtils.getNextSeqFileResource(true),
            seriesPath,
            context,
            getGlobalTimeFilter(),
            scanOptions.getAllSensors());
    if (timeseriesMetadata != null) {
      timeseriesMetadata.setSeq(true);
      seqTimeSeriesMetadata.add(timeseriesMetadata);
    }
  }

  private void unpackUnseqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(
            orderUtils.getNextUnseqFileResource(true),
            seriesPath,
            context,
            getGlobalTimeFilter(),
            scanOptions.getAllSensors());
    if (timeseriesMetadata != null) {
      timeseriesMetadata.setModified(true);
      timeseriesMetadata.setSeq(false);
      unSeqTimeSeriesMetadata.add(timeseriesMetadata);
    }
  }

  protected ITimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource, seriesPath, context, filter, allSensors);
  }

  protected List<TSDataType> getTsDataTypeList() {
    return Collections.singletonList(dataType);
  }

  protected IPointReader getPointReader(TsBlock tsBlock) {
    return tsBlock.getTsBlockSingleColumnIterator();
  }

  Filter getGlobalTimeFilter() {
    return scanOptions.getGlobalTimeFilter();
  }

  protected static class VersionPageReader {

    private final PriorityMergeReader.MergeReaderPriority version;
    private final IPageReader data;

    private final boolean isSeq;
    private final boolean isAligned;
    private final boolean isMem;

    VersionPageReader(long version, long offset, IPageReader data, boolean isSeq) {
      this.version = new PriorityMergeReader.MergeReaderPriority(version, offset);
      this.data = data;
      this.isSeq = isSeq;
      this.isAligned = data instanceof IAlignedPageReader;
      this.isMem = data instanceof MemPageReader || data instanceof MemAlignedPageReader;
    }

    Statistics getStatistics() {
      return data.getStatistics();
    }

    Statistics getStatistics(int index) throws IOException {
      if (!(data instanceof IAlignedPageReader)) {
        throw new IOException("Can only get statistics by index from AlignedPageReader");
      }
      return ((IAlignedPageReader) data).getStatistics(index);
    }

    Statistics getTimeStatistics() throws IOException {
      if (!(data instanceof IAlignedPageReader)) {
        throw new IOException("Can only get statistics of time column from AlignedPageReader");
      }
      return ((IAlignedPageReader) data).getTimeStatistics();
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
        QUERY_METRICS.recordSeriesScanCost(
            isAligned
                ? (isMem
                    ? BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM
                    : BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK)
                : (isMem
                    ? BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM
                    : BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK),
            System.nanoTime() - startTime);
      }
    }

    void setFilter(Filter filter) {
      data.setFilter(filter);
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

    long getOrderTime(TsFileResource fileResource);

    long getOverlapCheckTime(Statistics<? extends Object> range);

    boolean isOverlapped(Statistics<? extends Object> left, Statistics<? extends Object> right);

    boolean isOverlapped(long time, Statistics<? extends Object> right);

    boolean isOverlapped(long time, TsFileResource right);

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

    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getEndTime();
    }

    @Override
    public long getOrderTime(TsFileResource fileResource) {
      return fileResource.getEndTime(seriesPath.getDevice());
    }

    @Override
    public long getOverlapCheckTime(Statistics range) {
      return range.getStartTime();
    }

    @Override
    public boolean isOverlapped(Statistics left, Statistics right) {
      return left.getStartTime() <= right.getEndTime();
    }

    @Override
    public boolean isOverlapped(long time, Statistics right) {
      return time <= right.getEndTime();
    }

    @Override
    public boolean isOverlapped(long time, TsFileResource right) {
      return time <= right.getEndTime(seriesPath.getDevice());
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
      while (dataSource.hasNextSeqResource(curSeqFileIndex, getAscending())) {
        TsFileResource tsFileResource = dataSource.getSeqResourceByIndex(curSeqFileIndex);
        if (tsFileResource != null
            && tsFileResource.isSatisfied(
                seriesPath.getDevice(), getGlobalTimeFilter(), true, false)) {
          break;
        }
        curSeqFileIndex--;
      }
      return dataSource.hasNextSeqResource(curSeqFileIndex, getAscending());
    }

    @Override
    public boolean hasNextUnseqResource() {
      while (dataSource.hasNextUnseqResource(curUnseqFileIndex)) {
        TsFileResource tsFileResource = dataSource.getUnseqResourceByIndex(curUnseqFileIndex);
        if (tsFileResource != null
            && tsFileResource.isSatisfied(
                seriesPath.getDevice(), getGlobalTimeFilter(), false, false)) {
          break;
        }
        curUnseqFileIndex++;
      }
      return dataSource.hasNextUnseqResource(curUnseqFileIndex);
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

    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getStartTime();
    }

    @Override
    public long getOrderTime(TsFileResource fileResource) {
      return fileResource.getStartTime(seriesPath.getDevice());
    }

    @Override
    public long getOverlapCheckTime(Statistics range) {
      return range.getEndTime();
    }

    @Override
    public boolean isOverlapped(Statistics left, Statistics right) {
      return left.getEndTime() >= right.getStartTime();
    }

    @Override
    public boolean isOverlapped(long time, Statistics right) {
      return time >= right.getStartTime();
    }

    @Override
    public boolean isOverlapped(long time, TsFileResource right) {
      return time >= right.getStartTime(seriesPath.getDevice());
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
      while (dataSource.hasNextSeqResource(curSeqFileIndex, getAscending())) {
        TsFileResource tsFileResource = dataSource.getSeqResourceByIndex(curSeqFileIndex);
        if (tsFileResource != null
            && tsFileResource.isSatisfied(
                seriesPath.getDevice(), getGlobalTimeFilter(), true, false)) {
          break;
        }
        curSeqFileIndex++;
      }
      return dataSource.hasNextSeqResource(curSeqFileIndex, getAscending());
    }

    @Override
    public boolean hasNextUnseqResource() {
      while (dataSource.hasNextUnseqResource(curUnseqFileIndex)) {
        TsFileResource tsFileResource = dataSource.getUnseqResourceByIndex(curUnseqFileIndex);
        if (tsFileResource != null
            && tsFileResource.isSatisfied(
                seriesPath.getDevice(), getGlobalTimeFilter(), false, false)) {
          break;
        }
        curUnseqFileIndex++;
      }
      return dataSource.hasNextUnseqResource(curUnseqFileIndex);
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
}
