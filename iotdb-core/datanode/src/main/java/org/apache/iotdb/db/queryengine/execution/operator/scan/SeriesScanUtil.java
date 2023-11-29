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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemAlignedPageReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemPageReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.PriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IAlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.series.PaginationController;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.ToLongFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM;

public class SeriesScanUtil {

  private static final String ONE_SENSOR_ERROR_MSG =
      "Only one sensor in non-aligned SeriesScanUtil.";

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
  protected TimeSeriesMetadataProvider timeSeriesMetadataProvider;

  // chunk cache
  protected ChunkMetadataProvider chunkMetadataProvider;

  // page cache
  protected PageReaderProvider pageReaderProvider;

  protected SeriesScanOptions scanOptions;
  protected PaginationController paginationController;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();

  public SeriesScanUtil(PartialPath seriesPath, Ordering scanOrder, SeriesScanOptions scanOptions) {
    this.seriesPath = seriesPath;
    dataType = seriesPath.getSeriesType();

    this.scanOptions = scanOptions;
    paginationController = scanOptions.getPaginationController();

    if (scanOrder.isAscending()) {
      orderUtils = new AscTimeOrderUtils();
    } else {
      orderUtils = new DescTimeOrderUtils();
    }

    // init TimeSeriesMetadata materializer
    timeSeriesMetadataProvider = new TimeSeriesMetadataProvider(orderUtils);

    // init ChunkMetadata materializer
    chunkMetadataProvider = new ChunkMetadataProvider(timeSeriesMetadataProvider, orderUtils);

    // init PageReader materializer
    pageReaderProvider = new PageReaderProvider(chunkMetadataProvider, orderUtils);
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

  public boolean hasNextFile() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    checkState(
        pageReaderProvider.isCachedPageReaderConsumed(),
        "all cached pages should be consumed first");
    checkState(
        chunkMetadataProvider.isCachedChunkMetadataConsumed(),
        "all cached chunks should be consumed first");

    if (timeSeriesMetadataProvider.hasCurrentFile()) {
      return true;
    }

    while (!timeSeriesMetadataProvider.hasCurrentFile() && timeSeriesMetadataProvider.hasNext()) {
      // init first time series metadata whose startTime is minimum
      timeSeriesMetadataProvider.tryToUnpackAllOverlappedFilesToTimeSeriesMetadata();
      // filter file based on push-down conditions
      timeSeriesMetadataProvider.filterFirstTimeSeriesMetadata(
          scanOptions.getPushDownFilter(), paginationController);
    }

    return timeSeriesMetadataProvider.hasCurrentFile();
  }

  @SuppressWarnings("squid:S3740")
  public boolean isFileOverlapped() {
    return timeSeriesMetadataProvider.currentFileOverlapped();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentFileStatistics() {
    return timeSeriesMetadataProvider.getCurrentFile().getStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentFileStatistics(int index) {
    checkArgument(index == 0, ONE_SENSOR_ERROR_MSG);
    return currentFileStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentFileTimeStatistics() {
    return currentFileStatistics();
  }

  public boolean currentFileModified() {
    return timeSeriesMetadataProvider.currentFileModified();
  }

  public void skipCurrentFile() {
    timeSeriesMetadataProvider.skipCurrentFile();
  }

  /**
   * This method should be called after hasNextFile() until no next chunk, make sure that all
   * overlapped chunks are consumed
   */
  public boolean hasNextChunk() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    checkState(
        pageReaderProvider.isCachedPageReaderConsumed(),
        "all cached pages should be consumed first");

    if (chunkMetadataProvider.hasCurrentChunk()) {
      return true;
    }

    if (!timeSeriesMetadataProvider.hasCurrentFile()
        && chunkMetadataProvider.isCachedChunkMetadataConsumed()) {
      return false;
    }

    while (!chunkMetadataProvider.hasCurrentChunk() && chunkMetadataProvider.hasNext()) {
      chunkMetadataProvider.initFirstChunkMetadata();
      // filter chunk based on push-down conditions
      chunkMetadataProvider.filterFirstChunkMetadata();
    }
    return chunkMetadataProvider.hasCurrentChunk();
  }

  @SuppressWarnings("squid:S3740")
  public boolean isChunkOverlapped() throws IOException {
    if (!chunkMetadataProvider.hasCurrentChunk()) {
      throw new IOException("no first chunk");
    }
    return chunkMetadataProvider.currentChunkOverlapped();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentChunkStatistics() {
    return chunkMetadataProvider.getCurrentChunk().getStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentChunkStatistics(int index) {
    checkArgument(index == 0, ONE_SENSOR_ERROR_MSG);
    return currentChunkStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentChunkTimeStatistics() {
    return currentChunkStatistics();
  }

  public boolean currentChunkModified() throws IOException {
    if (!chunkMetadataProvider.hasCurrentChunk()) {
      throw new IOException("no first chunk");
    }
    return chunkMetadataProvider.currentChunkModified();
  }

  public void skipCurrentChunk() {
    chunkMetadataProvider.skipCurrentChunk();
  }

  /**
   * This method should be called after hasNextChunk() until no next page, make sure that all
   * overlapped pages are consumed
   */
  public boolean hasNextPage() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return false;
    }

    // has overlapped data before
    if (pageReaderProvider.hasCachedNextOverlappedPage()) {
      return true;
    }

    if ((mergeReader.hasNextTimeValuePair() || firstPageOverlapped()) && hasNextOverlappedPage()) {
      cachedTsBlock = nextOverlappedPage();
      if (cachedTsBlock != null && !cachedTsBlock.isEmpty()) {
        hasCachedNextOverlappedPage = true;
        return true;
      }
    }

    if (pageReaderProvider.hasCurrentPage()) {
      return true;
    }

    while (!pageReaderProvider.hasCurrentPage() && pageReaderProvider.hasNext()) {
      pageReaderProvider.peekCurrentPage();
      if (pageReaderProvider.isExistOverlappedPage()) {
        return true;
      }
    }
    return pageReaderProvider.hasCurrentPage();
  }

  /**
   * This method should be called after calling hasNextPage.
   *
   * <p>hasNextPage may cache firstPageReader if it is not overlapped or cached a BatchData if the
   * first page is overlapped
   */
  public boolean currentPageOverlapped() throws IOException {
    return pageReaderProvider.currentPageOverlapped();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentPageStatistics() {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentPageStatistics(int index) {
    checkArgument(index == 0, ONE_SENSOR_ERROR_MSG);
    return currentPageStatistics();
  }

  @SuppressWarnings("squid:S3740")
  public Statistics currentPageTimeStatistics() {
    return currentPageStatistics();
  }

  public boolean currentPageModified() throws IOException {
    if (firstPageReader == null) {
      throw new IOException("no first page");
    }
    return firstPageReader.isModified();
  }

  public void skipCurrentPage() {
    firstPageReader = null;
  }

  /** This method should only be used when the method currentPageOverlapped() return true. */
  public TsBlock nextPage() throws IOException {

    if (hasCachedNextOverlappedPage) {
      hasCachedNextOverlappedPage = false;
      TsBlock res = cachedTsBlock;
      cachedTsBlock = null;
      return res;
    } else {
      // next page is not overlapped, push down value filter & limit offset
      Filter queryFilter = scanOptions.getPushDownFilter();
      if (queryFilter != null) {
        firstPageReader.setFilter(queryFilter);
      }
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
  }

  private TsBlock nextOverlappedPage() throws IOException {
    if (hasCachedNextOverlappedPage || hasNextOverlappedPage()) {
      hasCachedNextOverlappedPage = false;
      return cachedTsBlock;
    }
    throw new IOException("No more batch data");
  }

  protected void unpackAllOverlappedTsFilesToTimeSeriesMetadata(long endpointTime)
      throws IOException {
    timeSeriesMetadataProvider.unpackAllOverlappedTsFilesToTimeSeriesMetadata(endpointTime);
  }

  protected void unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(long endpointTime) {
    chunkMetadataProvider.unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
        endpointTime, false);
  }

  public List<TSDataType> getTsDataTypeList() {
    return Collections.singletonList(dataType);
  }

  protected IPointReader getPointReader(TsBlock tsBlock) {
    return tsBlock.getTsBlockSingleColumnIterator();
  }

  public Filter getGlobalTimeFilter() {
    return scanOptions.getGlobalTimeFilter();
  }

  @SuppressWarnings({"squid:S3740"})
  public boolean canUseCurrentFileStatistics() {
    Statistics fileStatistics = currentFileTimeStatistics();
    return !isFileOverlapped()
        && fileStatistics.containedByTimeFilter(getGlobalTimeFilter())
        && !currentFileModified();
  }

  @SuppressWarnings({"squid:S3740"})
  public boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = currentChunkTimeStatistics();
    return !isChunkOverlapped()
        && chunkStatistics.containedByTimeFilter(getGlobalTimeFilter())
        && !currentChunkModified();
  }

  @SuppressWarnings({"squid:S3740"})
  public boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = currentPageTimeStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !currentPageOverlapped()
        && currentPageStatistics.containedByTimeFilter(getGlobalTimeFilter())
        && !currentPageModified();
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

    @SuppressWarnings("squid:S3740")
    Statistics getStatistics() {
      return data.getStatistics();
    }

    @SuppressWarnings("squid:S3740")
    Statistics getStatistics(int index) {
      if (!(data instanceof IAlignedPageReader)) {
        throw new IllegalArgumentException(
            "Can only get statistics by index from AlignedPageReader");
      }
      return ((IAlignedPageReader) data).getStatistics(index);
    }

    @SuppressWarnings("squid:S3740")
    Statistics getTimeStatistics() {
      if (!(data instanceof IAlignedPageReader)) {
        throw new IllegalArgumentException(
            "Can only get statistics of time column from AlignedPageReader");
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
        SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
            getMetricType(), System.nanoTime() - startTime);
      }
    }

    private String metricType = null;

    private String getMetricType() {
      if (metricType != null) {
        return metricType;
      }
      if (isAligned) {
        if (isMem) {
          metricType = BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_MEM;
        } else {
          metricType = BUILD_TSBLOCK_FROM_PAGE_READER_ALIGNED_DISK;
        }
      } else {
        if (isMem) {
          metricType = BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_MEM;
        } else {
          metricType = BUILD_TSBLOCK_FROM_PAGE_READER_NONALIGNED_DISK;
        }
      }
      return metricType;
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

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getEndTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOrderTime(TsFileResource fileResource) {
      return fileResource.getEndTime(seriesPath.getDevice());
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

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getStartTime();
    }

    @SuppressWarnings("squid:S3740")
    @Override
    public long getOrderTime(TsFileResource fileResource) {
      return fileResource.getStartTime(seriesPath.getDevice());
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
