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
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemAlignedPageReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemPageReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.DescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.MergeReaderPriority;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.NoDataPointReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.PriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.TsBlockUtil;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.page.AlignedPageReader;
import org.apache.tsfile.read.reader.page.TablePageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.ToLongFunction;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED;
import static org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet.BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED;

public class SeriesScanUtil implements Accountable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SeriesScanUtil.class);
  public static final StringArrayDeviceID EMPTY_DEVICE_ID = new StringArrayDeviceID("");
  protected final FragmentInstanceContext context;

  // The path of the target series which will be scanned.
  protected final IFullPath seriesPath;

  private final IDeviceID deviceID;
  protected boolean isAligned = false;
  private final TSDataType dataType;

  // inner class of SeriesReader for order purpose
  protected final TimeOrderUtils orderUtils;

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
  private IVersionPageReader firstPageReader;
  private final List<IVersionPageReader> seqPageReaders;
  private final PriorityQueue<IVersionPageReader> unSeqPageReaders;

  // point cache
  private final PriorityMergeReader mergeReader;

  // result cache
  private boolean hasCachedNextOverlappedPage;
  private TsBlock cachedTsBlock;

  protected SeriesScanOptions scanOptions;
  private final PaginationController paginationController;

  private static final SeriesScanCostMetricSet SERIES_SCAN_COST_METRIC_SET =
      SeriesScanCostMetricSet.getInstance();
  protected final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SeriesScanUtil.class)
          + RamUsageEstimator.shallowSizeOfInstance(IDeviceID.class)
          + RamUsageEstimator.shallowSizeOfInstance(TimeOrderUtils.class)
          + RamUsageEstimator.shallowSizeOfInstance(PaginationController.class)
          + RamUsageEstimator.shallowSizeOfInstance(SeriesScanOptions.class)
          + RamUsageEstimator.shallowSizeOfInstance(TimeRange.class);

  protected TimeRange satisfiedTimeRange;

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
    // IgnoreAllNullRows is false indicating that the current query is a table model query.
    // In most cases, We can use this condition to determine from which model to obtain the ttl
    // of the current device. However, it should be noted that for tree model data queried using
    // table view, ttl also needs to be obtained from the tree model.
    if (context.isIgnoreAllNullRows() || scanOptions.isTableViewForTreeModel()) {
      if (deviceID != EMPTY_DEVICE_ID) {
        long ttl = DataNodeTTLCache.getInstance().getTTLForTree(deviceID);
        scanOptions.setTTLForTreeDevice(ttl);
      }
    } else {
      if (scanOptions.timeFilterNeedUpdatedByTtl()) {
        String databaseName = dataSource.getDatabaseName();
        long ttl =
            databaseName == null
                ? Long.MAX_VALUE
                : DataNodeTTLCache.getInstance()
                    .getTTLForTable(databaseName, deviceID.getTableName());
        scanOptions.setTTLForTableDevice(ttl);
      }
    }

    // init file index
    orderUtils.setCurSeqFileIndex(dataSource);
    curUnseqFileIndex = 0;

    if (dataSource.isEmpty()) {
      // no satisfied resources
      return;
    }

    if (satisfiedTimeRange == null) {
      long startTime = Long.MAX_VALUE;
      long endTime = Long.MIN_VALUE;
      if (scanOptions.getGlobalTimeFilter() == null) {
        satisfiedTimeRange = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE);
        return;
      }
      for (TimeRange timeRange : context.getGlobalTimeFilterTimeRanges()) {
        startTime = Math.min(startTime, timeRange.getMin());
        endTime = Math.max(endTime, timeRange.getMax());
      }
      satisfiedTimeRange = new TimeRange(startTime, endTime);
    }
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

  // When Optional.empty() is returned, it means that the current hasNextFile has not been fully
  // executed. In order to avoid the execution time of this method exceeding the allocated time
  // slice, it is return early in this way. For the upper-level method, when encountering
  // Optional.empty(), it needs to return directly to the checkpoint method that checks the operator
  // execution time slice.
  public Optional<Boolean> hasNextFile() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return Optional.of(false);
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
      return Optional.of(true);
    }

    boolean checked = false;
    if (orderUtils.hasNextSeqResource()
        || orderUtils.hasNextUnseqResource()
        || !seqTimeSeriesMetadata.isEmpty()
        || !unSeqTimeSeriesMetadata.isEmpty()) {
      // init first time series metadata whose startTime is minimum
      tryToUnpackAllOverlappedFilesToTimeSeriesMetadata();
      // filter file based on push-down conditions
      filterFirstTimeSeriesMetadata();
      checked = true;
    }

    if (checked && firstTimeSeriesMetadata == null) {
      return Optional.empty();
    }
    return Optional.of(firstTimeSeriesMetadata != null);
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
   * @return Optional<Boolean> When Optional.empty() is returned, it means that the current
   *     hasNextFile has not been fully executed. In order to avoid the execution time of this
   *     method exceeding the allocated time slice, it is return early in this way. For the
   *     upper-level method, when encountering Optional.empty(), it needs to return directly to the
   *     checkpoint method who checks the operator execution time slice.
   * @throws IllegalStateException illegal state
   */
  public Optional<Boolean> hasNextChunk() throws IOException {
    if (!paginationController.hasCurLimit()) {
      return Optional.of(false);
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
      return Optional.of(true);
      // hasNextFile() has not been invoked
    } else if (firstTimeSeriesMetadata == null && cachedChunkMetadata.isEmpty()) {
      return Optional.of(false);
    }

    Optional<Boolean> hasNextFileReturnValue = null;
    while (firstChunkMetadata == null) {
      if (cachedChunkMetadata.isEmpty()) {
        if (hasNextFileReturnValue != null) {
          return Optional.empty();
        }
        hasNextFileReturnValue = hasNextFile();
        if (!hasNextFileReturnValue.isPresent() || !hasNextFileReturnValue.get()) {
          return hasNextFileReturnValue;
        }
      }
      initFirstChunkMetadata();
      // filter chunk based on push-down conditions
      filterFirstChunkMetadata();
    }
    return Optional.of(firstChunkMetadata != null);
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
        if (isAligned) {
          SchemaUtils.changeAlignedMetadataModified(
              (AbstractAlignedChunkMetadata) firstChunkMetadata,
              firstChunkMetadata.getDataType(),
              getTsDataTypeList());
        } else {
          SchemaUtils.changeMetadataModified(
              firstChunkMetadata, firstChunkMetadata.getDataType(), dataType);
        }
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
      if (isAligned) {
        SchemaUtils.changeAlignedMetadataModified(
            (AbstractAlignedChunkMetadata) firstChunkMetadata,
            firstChunkMetadata.getDataType(),
            getTsDataTypeList());
      } else {
        SchemaUtils.changeMetadataModified(
            firstChunkMetadata, firstChunkMetadata.getDataType(), dataType);
      }
    }
  }

  protected void unpackOneTimeSeriesMetadata(ITimeSeriesMetadata timeSeriesMetadata) {
    List<IChunkMetadata> chunkMetadataList =
        FileLoaderUtils.loadChunkMetadataList(timeSeriesMetadata);
    chunkMetadataList.forEach(
        chunkMetadata -> {
          if (chunkMetadata instanceof AbstractAlignedChunkMetadata) {
            AbstractAlignedChunkMetadata alignedChunkMetadata =
                (AbstractAlignedChunkMetadata) chunkMetadata;
            for (int i = 0; i < alignedChunkMetadata.getValueChunkMetadataList().size(); i++) {
              if ((alignedChunkMetadata.getValueChunkMetadataList().get(i) != null)
                  && !SchemaUtils.isUsingSameColumn(
                      alignedChunkMetadata.getValueChunkMetadataList().get(i).getDataType(),
                      getTsDataTypeList().get(i))
                  && getTsDataTypeList().get(i).equals(TSDataType.STRING)) {
                alignedChunkMetadata.getValueChunkMetadataList().get(i).setModified(true);
              }
            }
            chunkMetadata = alignedChunkMetadata;
          } else if (chunkMetadata instanceof ChunkMetadata) {
            if (!SchemaUtils.isUsingSameColumn(
                    chunkMetadata.getDataType(), getTsDataTypeList().get(0))
                && getTsDataTypeList().get(0).equals(TSDataType.STRING)) {
              chunkMetadata.setModified(true);
            }
          }
          chunkMetadata.setSeq(timeSeriesMetadata.isSeq());
        });

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
    long timestampInFileName = FileLoaderUtils.getTimestampInFileName(chunkMetaData);

    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    if ((chunkLoader instanceof MemChunkLoader)
        && ((MemChunkLoader) chunkLoader).isStreamingQueryMemChunk()) {
      unpackOneFakeMemChunkMetaData(
          chunkMetaData, (MemChunkLoader) chunkLoader, timestampInFileName);
      return;
    }
    List<IPageReader> pageReaderList =
        FileLoaderUtils.loadPageReaderList(
            chunkMetaData, scanOptions.getGlobalTimeFilter(), isAligned, getTsDataTypeList());

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

    if (LOGGER.isDebugEnabled()) {
      for (IPageReader pageReader : pageReaderList) {
        LOGGER.debug("[SeriesScanUtil] pageReader.isModified() is {}", pageReader.isModified());
      }
    }
  }

  private void unpackOneFakeMemChunkMetaData(
      IChunkMetadata chunkMetaData, MemChunkLoader chunkLoader, long timestampInFileName) {
    ReadOnlyMemChunk readOnlyMemChunk = chunkLoader.getReadOnlyMemChunk();
    boolean isAligned = readOnlyMemChunk instanceof AlignedReadOnlyMemChunk;
    List<Statistics<? extends Serializable>> statisticsList =
        isAligned
            ? ((AlignedReadOnlyMemChunk) readOnlyMemChunk).getTimeStatisticsList()
            : readOnlyMemChunk.getPageStatisticsList();

    // we need to create a new MemPointIterator for SeriesScanUtil because streaming scan method
    // don't support sharing MemPointIterator
    MemPointIterator memPointIterator =
        readOnlyMemChunk.createMemPointIterator(
            orderUtils.getScanOrder(), scanOptions.getGlobalTimeFilter());
    for (Statistics<? extends Serializable> statistics : statisticsList) {
      long orderTime = orderUtils.getOrderTime(statistics);
      boolean canSkip =
          (orderUtils.getAscending() && orderTime > satisfiedTimeRange.getMax())
              || (!orderUtils.getAscending() && orderTime < satisfiedTimeRange.getMin());
      if (canSkip) {
        break;
      }
      IVersionPageReader versionPageReader =
          new LazyMemVersionPageReader(
              context,
              timestampInFileName,
              chunkMetaData.getVersion(),
              chunkMetaData.getOffsetOfChunkHeader(),
              isAligned,
              statistics,
              memPointIterator,
              chunkMetaData.isSeq());
      if (chunkMetaData.isSeq()) {
        seqPageReaders.add(versionPageReader);
      } else {
        unSeqPageReaders.add(versionPageReader);
      }
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
    return filterAllSatisfy(scanOptions.getGlobalTimeFilter(), firstPageReader.getPageReader())
        && filterAllSatisfy(scanOptions.getPushDownFilter(), firstPageReader.getPageReader());
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

      if (firstPageReader instanceof LazyMemVersionPageReader) {
        firstPageReader.setLimitOffset(paginationController);
        LazyMemVersionPageReader lazyMemVersionPageReader =
            (LazyMemVersionPageReader) firstPageReader;
        // We must set the time range before actually reading the required data because
        // MemPointIterator is shared by multiple Pages
        lazyMemVersionPageReader.setCurrentPageTimeRangeToMemPointIterator();
        lazyMemVersionPageReader.setInited();
        // There is no need to consider scan order here, because the tsBlock returned here has been
        // processed in MemPointIterator
        TsBlock tsBlock =
            lazyMemVersionPageReader.hasNextBatch() ? lazyMemVersionPageReader.nextBatch() : null;
        if (!lazyMemVersionPageReader.hasNextBatch()) {
          firstPageReader = null;
        }
        return tsBlock == null ? null : getTransferedDataTypeTsBlock(tsBlock);
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

      return getTransferedDataTypeTsBlock(tsBlock);
    }
  }

  private TsBlock getTransferedDataTypeTsBlock(TsBlock tsBlock) {
    Column[] valueColumns = tsBlock.getValueColumns();
    int length = tsBlock.getValueColumnCount();
    boolean isTypeInconsistent = false;
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        TSDataType finalDataType = getTsDataTypeList().get(i);
        if ((valueColumns[i].getDataType() != finalDataType)
            && (!SchemaUtils.isUsingSameColumn(valueColumns[i].getDataType(), finalDataType)
                || (valueColumns[i].getDataType().equals(TSDataType.DATE)
                    && (finalDataType == TSDataType.STRING || finalDataType == TSDataType.TEXT)))) {
          isTypeInconsistent = true;
          break;
        }
      }
    }

    if (!isTypeInconsistent) {
      return tsBlock;
    }

    int positionCount = tsBlock.getPositionCount();
    Column[] newValueColumns = new Column[length];
    for (int i = 0; i < length; i++) {
      TSDataType sourceType = valueColumns[i].getDataType();
      TSDataType finalDataType = getTsDataTypeList().get(i);
      switch (finalDataType) {
        case BOOLEAN:
          if (sourceType == TSDataType.BOOLEAN) {
            newValueColumns[i] = valueColumns[i];
          } else {
            newValueColumns[i] =
                new BooleanColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new boolean[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case INT32:
          if (sourceType == TSDataType.INT32) {
            newValueColumns[i] = valueColumns[i];
          } else {
            newValueColumns[i] =
                new IntColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new int[positionCount],
                    TSDataType.INT32);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case INT64:
          if (sourceType == TSDataType.INT64) {
            newValueColumns[i] = valueColumns[i];
          } else if (sourceType == TSDataType.INT32) {
            newValueColumns[i] =
                new LongColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new long[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getLongs()[j] =
                    ((Number) valueColumns[i].getInts()[j]).longValue();
              }
            }
          } else if (sourceType == TSDataType.TIMESTAMP) {
            newValueColumns[i] = valueColumns[i];
          } else {
            newValueColumns[i] =
                new LongColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new long[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case FLOAT:
          if (sourceType == TSDataType.FLOAT) {
            newValueColumns[i] = valueColumns[i];
          } else if (sourceType == TSDataType.INT32) {
            newValueColumns[i] =
                new FloatColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new float[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getFloats()[j] =
                    ((Number) valueColumns[i].getInts()[j]).floatValue();
              }
            }
          } else {
            newValueColumns[i] =
                new FloatColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new float[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case DOUBLE:
          if (sourceType == TSDataType.DOUBLE) {
            newValueColumns[i] = valueColumns[i];
          } else if (sourceType == TSDataType.INT32) {
            newValueColumns[i] =
                new DoubleColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new double[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getDoubles()[j] =
                    ((Number) valueColumns[i].getInts()[j]).doubleValue();
              }
            }
          } else if (sourceType == TSDataType.INT64) {
            newValueColumns[i] =
                new DoubleColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new double[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getDoubles()[j] =
                    ((Number) valueColumns[i].getLongs()[j]).doubleValue();
              }
            }
          } else if (sourceType == TSDataType.FLOAT) {
            newValueColumns[i] =
                new DoubleColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new double[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getDoubles()[j] =
                    ((Number) valueColumns[i].getFloats()[j]).doubleValue();
              }
            }
          } else if (sourceType == TSDataType.TIMESTAMP) {
            newValueColumns[i] =
                new DoubleColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new double[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getDoubles()[j] =
                    ((Number) valueColumns[i].getLongs()[j]).doubleValue();
              }
            }
          } else {
            newValueColumns[i] =
                new DoubleColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new double[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case TEXT:
          if (SchemaUtils.isUsingSameColumn(sourceType, TSDataType.TEXT)) {
            newValueColumns[i] = valueColumns[i];
          } else if (sourceType == TSDataType.INT32) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getInts()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.DATE) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        TSDataType.getDateStringValue(valueColumns[i].getInts()[j]),
                        StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.INT64 || sourceType == TSDataType.TIMESTAMP) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getLongs()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.FLOAT) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getFloats()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.DOUBLE) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getDoubles()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.BOOLEAN) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getBooleans()[j]), StandardCharsets.UTF_8);
              }
            }
          } else {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case TIMESTAMP:
          if (SchemaUtils.isUsingSameColumn(sourceType, TSDataType.TIMESTAMP)) {
            newValueColumns[i] = valueColumns[i];
          } else if (sourceType == TSDataType.INT32) {
            newValueColumns[i] =
                new LongColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new long[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getLongs()[j] =
                    ((Number) valueColumns[i].getInts()[j]).longValue();
              }
            }
          } else if (sourceType == TSDataType.INT64) {
            newValueColumns[i] = valueColumns[i];
          } else {
            newValueColumns[i] =
                new LongColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new long[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case DATE:
          if (SchemaUtils.isUsingSameColumn(sourceType, TSDataType.DATE)) {
            newValueColumns[i] = valueColumns[i];
          } else {
            newValueColumns[i] =
                new IntColumn(
                    positionCount, Optional.of(new boolean[positionCount]), new int[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case BLOB:
          if (SchemaUtils.isUsingSameColumn(sourceType, TSDataType.BLOB)) {
            newValueColumns[i] = valueColumns[i];
          } else {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case STRING:
          if (SchemaUtils.isUsingSameColumn(sourceType, TSDataType.STRING)) {
            newValueColumns[i] = valueColumns[i];
          } else if (sourceType == TSDataType.INT32) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getInts()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.DATE) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        TSDataType.getDateStringValue(valueColumns[i].getInts()[j]),
                        StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.INT64 || sourceType == TSDataType.TIMESTAMP) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getLongs()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.FLOAT) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getFloats()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.DOUBLE) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getDoubles()[j]), StandardCharsets.UTF_8);
              }
            }
          } else if (sourceType == TSDataType.BOOLEAN) {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);

            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = valueColumns[i].isNull()[j];
              if (!valueColumns[i].isNull()[j]) {
                newValueColumns[i].getBinaries()[j] =
                    new Binary(
                        String.valueOf(valueColumns[i].getBooleans()[j]), StandardCharsets.UTF_8);
              }
            }
          } else {
            newValueColumns[i] =
                new BinaryColumn(
                    positionCount,
                    Optional.of(new boolean[positionCount]),
                    new Binary[positionCount]);
            for (int j = 0; j < valueColumns[i].getPositionCount(); j++) {
              newValueColumns[i].isNull()[j] = true;
            }
          }
          break;
        case OBJECT:
          newValueColumns[i] = valueColumns[i];
        case VECTOR:
        case UNKNOWN:
        default:
          break;
      }
    }

    tsBlock = new TsBlock(tsBlock.getTimeColumn(), newValueColumns);
    return tsBlock;
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
    if (firstPageReader == null || firstPageReader.isModified()) {
      return;
    }

    IPageReader pageReader = firstPageReader.getPageReader();

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

      // init the merge reader for current call
      // The original process is changed to lazy loading because different mem page readers
      // belonging to the same mem chunk need to be read in a streaming manner. Therefore, it is
      // necessary to ensure that these mem page readers cannot coexist in the mergeReader at the
      // same time.
      // The initial endPointTime is calculated as follows:
      // 1. If mergeReader is empty, use the endpoint of firstPageReader to find all overlapped
      // unseq pages and take the end point.
      // 2. If mergeReader is not empty, use the readStopTime of mergeReader to find all overlapping
      // unseq pages and take the end point.
      long initialEndPointTime = tryToPutAllDirectlyOverlappedUnseqPageReadersIntoMergeReader();

      while (true) {

        // may has overlapped data
        if (mergeReader.hasNextTimeValuePair()) {

          TsBlockBuilder builder = new TsBlockBuilder(getTsDataTypeList());
          long currentPageEndPointTime =
              orderUtils.getAscending()
                  ? Math.max(mergeReader.getCurrentReadStopTime(), initialEndPointTime)
                  : Math.min(mergeReader.getCurrentReadStopTime(), initialEndPointTime);
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
            unpackAllOverlappedUnseqPageReadersToMergeReader();

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
                putPageReaderToMergeReader(firstPageReader);
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
                IVersionPageReader pageReader = seqPageReaders.remove(0);
                putPageReaderToMergeReader(pageReader);
                currentPageEndPointTime = updateEndPointTime(currentPageEndPointTime, pageReader);
              }
            }

            // get the latest first point in mergeReader
            timeValuePair = mergeReader.nextTimeValuePair();
            addTimeValuePairToResult(timeValuePair, builder);
            // A PageReader from MemChunk may have a lot of data, so it needs to be checked here
            if (builder.getPositionCount() >= MAX_NUMBER_OF_POINTS_IN_PAGE) {
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
      SERIES_SCAN_COST_METRIC_SET.recordSeriesScanCost(
          isAligned
              ? BUILD_TSBLOCK_FROM_MERGE_READER_ALIGNED
              : BUILD_TSBLOCK_FROM_MERGE_READER_NONALIGNED,
          System.nanoTime() - startTime);
    }
  }

  private long updateEndPointTime(long currentPageEndPointTime, IVersionPageReader pageReader) {
    if (orderUtils.getAscending()) {
      return Math.min(currentPageEndPointTime, pageReader.getStatistics().getEndTime());
    } else {
      return Math.max(currentPageEndPointTime, pageReader.getStatistics().getStartTime());
    }
  }

  private long tryToPutAllDirectlyOverlappedUnseqPageReadersIntoMergeReader() throws IOException {
    do {
      /*
       * no cached page readers
       */
      if (firstPageReader == null && unSeqPageReaders.isEmpty() && seqPageReaders.isEmpty()) {
        return mergeReader.getCurrentReadStopTime();
      }

      /*
       * init firstPageReader
       */
      if (firstPageReader == null) {
        initFirstPageReader();
      }
      if (!mergeReader.hasNextTimeValuePair()) {
        putPageReaderToMergeReader(firstPageReader);
        firstPageReader = null;
      }
    } while (!mergeReader.hasNextTimeValuePair());

    /*
     * put all currently directly overlapped unseq page reader to merge reader
     */
    long mergeReaderStopTime = mergeReader.getCurrentReadStopTime();
    unpackAllOverlappedUnseqPageReadersToMergeReader();

    return calculateInitialEndPointTime(mergeReaderStopTime);
  }

  private long calculateInitialEndPointTime(final long currentReadStopTime) {
    long initialReadStopTime = currentReadStopTime;
    if (firstPageReader != null
        && !firstPageReader.isSeq()
        && orderUtils.isOverlapped(currentReadStopTime, firstPageReader.getStatistics())) {
      if (orderUtils.getAscending()) {
        initialReadStopTime =
            Math.max(
                initialReadStopTime,
                orderUtils.getOverlapCheckTime(firstPageReader.getStatistics()));
      } else {
        initialReadStopTime =
            Math.min(
                initialReadStopTime,
                orderUtils.getOverlapCheckTime(firstPageReader.getStatistics()));
      }
    }
    for (IVersionPageReader unSeqPageReader : unSeqPageReaders) {
      if (orderUtils.isOverlapped(currentReadStopTime, unSeqPageReader.getStatistics())) {
        if (orderUtils.getAscending()) {
          initialReadStopTime =
              Math.max(
                  initialReadStopTime,
                  orderUtils.getOverlapCheckTime(unSeqPageReader.getStatistics()));
        } else {
          initialReadStopTime =
              Math.min(
                  initialReadStopTime,
                  orderUtils.getOverlapCheckTime(unSeqPageReader.getStatistics()));
        }
      } else {
        break;
      }
    }
    return initialReadStopTime;
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
      case OBJECT:
      case STRING:
        if (timeValuePair.getValue().getDataType() == TSDataType.DATE) {
          builder
              .getColumnBuilder(0)
              .writeBinary(
                  new Binary(
                      TSDataType.getDateStringValue(timeValuePair.getValue().getInt()),
                      StandardCharsets.UTF_8));
        } else {
          builder.getColumnBuilder(0).writeBinary(timeValuePair.getValue().getBinary());
        }
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
      IVersionPageReader firstPageReader = getFirstPageReaderFromCachedReaders();

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
  private IVersionPageReader getFirstPageReaderFromCachedReaders() {
    IVersionPageReader firstPageReader = null;
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

  // This process loads overlapped unseq pages based on the current time value pair of the
  // mergeReader. The current time value pair of the mergeReader is recalculated each time an unseq
  // page is added.
  // The current time obtained from mergeReader each time is not necessarily the minimum among all
  // the actual unseq data, so it is necessary to repeatedly calculate and include potentially
  // overlapping unseq pages.
  private void unpackAllOverlappedUnseqPageReadersToMergeReader() throws IOException {
    long actualFirstTimeOfMergeReader = mergeReader.currentTimeValuePair().getTimestamp();
    if (firstPageReader != null
        && !firstPageReader.isSeq()
        && orderUtils.isOverlapped(actualFirstTimeOfMergeReader, firstPageReader.getStatistics())) {
      putPageReaderToMergeReader(firstPageReader);
      firstPageReader = null;
      actualFirstTimeOfMergeReader = mergeReader.currentTimeValuePair().getTimestamp();
    }
    while (!unSeqPageReaders.isEmpty()
        && orderUtils.isOverlapped(
            actualFirstTimeOfMergeReader, unSeqPageReaders.peek().getStatistics())) {
      putPageReaderToMergeReader(unSeqPageReaders.poll());
      actualFirstTimeOfMergeReader = mergeReader.currentTimeValuePair().getTimestamp();
    }
  }

  private void putPageReaderToMergeReader(IVersionPageReader pageReader) throws IOException {
    IPointReader pointReader;
    if (pageReader instanceof LazyMemVersionPageReader) {
      // There may be many VersionPageReaders belonging to the same MemChunk sharing a
      // MemPointIterator, but the time ranges of these pages sharing it must not overlap, so there
      // will be no problem of reading data from the MemPointIterator at the same time
      LazyMemVersionPageReader lazyMemPageReader = (LazyMemVersionPageReader) pageReader;
      // We must set the time range before actually reading the required data because
      // MemPointIterator is shared by multiple Pages
      lazyMemPageReader.setCurrentPageTimeRangeToMemPointIterator();
      lazyMemPageReader.setInited();
      // There is no need to consider scanOrder here, because MemPointIterator has already returned
      // according to the current scan order
      pointReader = lazyMemPageReader.getPointReader();
    } else {
      pointReader = getPointReader(pageReader.getAllSatisfiedPageData(orderUtils.getAscending()));
    }
    mergeReader.addReader(
        pointReader,
        pageReader.getVersion(),
        orderUtils.getOverlapCheckTime(pageReader.getStatistics()));
    context
        .getQueryStatistics()
        .getPageReaderMaxUsedMemorySize()
        .updateAndGet(v -> Math.max(v, mergeReader.getUsedMemorySize()));
  }

  private TsBlock nextOverlappedPage() throws IOException {
    if (hasCachedNextOverlappedPage || hasNextOverlappedPage()) {
      hasCachedNextOverlappedPage = false;
      return getTransferedDataTypeTsBlock(cachedTsBlock);
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
    if (seqTimeSeriesMetadata.isEmpty() && orderUtils.hasNextSeqResource()) {
      // Avoid exceeding the time slice when a series cannot be found
      if (!unpackSeqTsFileResource().isPresent()) {
        return;
      }
    }

    /*
     * Fill unSequence TimeSeriesMetadata Priority Queue until it is not empty
     */
    if (unSeqTimeSeriesMetadata.isEmpty() && orderUtils.hasNextUnseqResource()) {
      // Avoid exceeding the time slice when a series cannot be found
      if (!unpackUnseqTsFileResource().isPresent()) {
        return;
      }
    }

    /*
     * find end time of the first TimeSeriesMetadata
     */
    Long endTime = null;
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
    if (endTime != null) {
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
      Optional<ITimeSeriesMetadata> timeSeriesMetadata = unpackSeqTsFileResource();
      // asc: if current seq tsfile's endTime >= endpointTime, we don't need to continue
      // desc: if current seq tsfile's startTime <= endpointTime, we don't need to continue
      if (timeSeriesMetadata.isPresent()
          && orderUtils.overlappedSeqResourceSearchingNeedStop(
              endpointTime, timeSeriesMetadata.get().getStatistics())) {
        break;
      }
    }
  }

  private Optional<ITimeSeriesMetadata> unpackSeqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(orderUtils.getNextSeqFileResource(true), true);
    // skip if data type is mismatched which may be caused by delete
    if (timeseriesMetadata != null && timeseriesMetadata.typeMatch(getTsDataTypeList())) {
      timeseriesMetadata.setSeq(true);
      seqTimeSeriesMetadata.add(timeseriesMetadata);
      return Optional.of(timeseriesMetadata);
    } else {
      return Optional.empty();
    }
  }

  private Optional<ITimeSeriesMetadata> unpackUnseqTsFileResource() throws IOException {
    ITimeSeriesMetadata timeseriesMetadata =
        loadTimeSeriesMetadata(orderUtils.getNextUnseqFileResource(true), false);
    // skip if data type is mismatched which may be caused by delete
    if (timeseriesMetadata != null && timeseriesMetadata.typeMatch(getTsDataTypeList())) {
      timeseriesMetadata.setSeq(false);
      unSeqTimeSeriesMetadata.add(timeseriesMetadata);
      return Optional.of(timeseriesMetadata);
    } else {
      return Optional.empty();
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

  protected interface IVersionPageReader {
    Statistics getStatistics();

    Statistics getMeasurementStatistics(int index);

    Statistics getTimeStatistics();

    TsBlock getAllSatisfiedPageData(boolean ascending) throws IOException;

    MergeReaderPriority getVersion();

    IPageReader getPageReader();

    void addPushDownFilter(Filter pushDownFilter);

    boolean isModified();

    boolean isSeq();

    void setLimitOffset(PaginationController paginationController);
  }

  protected static class VersionPageReader implements IVersionPageReader {
    protected final QueryContext context;
    protected final MergeReaderPriority version;
    protected final IPageReader data;

    protected final boolean isSeq;
    protected final boolean isAligned;
    protected final boolean isMem;

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
      this.isAligned =
          data instanceof AlignedPageReader
              || data instanceof MemAlignedPageReader
              || data instanceof TablePageReader;
      this.isMem = data instanceof MemPageReader || data instanceof MemAlignedPageReader;
    }

    @SuppressWarnings("squid:S3740")
    public Statistics getStatistics() {
      return data.getStatistics();
    }

    @SuppressWarnings("squid:S3740")
    public Statistics getMeasurementStatistics(int index) {
      return data.getMeasurementStatistics(index).orElse(null);
    }

    @SuppressWarnings("squid:S3740")
    public Statistics getTimeStatistics() {
      return data.getTimeStatistics();
    }

    @Override
    public MergeReaderPriority getVersion() {
      return version;
    }

    public IPageReader getPageReader() {
      return data;
    }

    public TsBlock getAllSatisfiedPageData(boolean ascending) throws IOException {
      long startTime = System.nanoTime();
      try {
        TsBlock tsBlock = data.getAllSatisfiedData();
        if (!ascending) {
          tsBlock.reverse();
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("[getAllSatisfiedPageData] TsBlock:{}", CommonUtils.toString(tsBlock));
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

    public void addPushDownFilter(Filter pushDownFilter) {
      data.addRecordFilter(pushDownFilter);
    }

    public boolean isModified() {
      return data.isModified();
    }

    public boolean isSeq() {
      return isSeq;
    }

    public void setLimitOffset(PaginationController paginationController) {
      data.setLimitOffset(paginationController);
    }
  }

  protected static class LazyMemVersionPageReader implements IVersionPageReader {

    private final Statistics<? extends Serializable> statistics;
    private final MemPointIterator memPointIterator;
    protected final QueryContext context;
    protected final MergeReaderPriority version;

    protected final boolean isSeq;
    protected final boolean isAligned;
    private boolean inited = false;
    private boolean hasData = true;

    LazyMemVersionPageReader(
        QueryContext context,
        long fileTimestamp,
        long version,
        long offset,
        boolean isAligned,
        Statistics<? extends Serializable> statistics,
        MemPointIterator memPointIterator,
        boolean isSeq) {
      this.statistics = statistics;
      this.memPointIterator = memPointIterator;
      this.context = context;
      this.version = new MergeReaderPriority(fileTimestamp, version, offset, isSeq);
      this.isSeq = isSeq;
      this.isAligned = isAligned;
    }

    public IPointReader getPointReader() {
      if (!hasData) {
        return NoDataPointReader.getInstance();
      }
      return memPointIterator;
    }

    public boolean hasNextBatch() {
      return hasData && memPointIterator.hasNextBatch();
    }

    public void setCurrentPageTimeRangeToMemPointIterator() {
      if (inited) {
        return;
      }
      if (statistics.getStartTime() > statistics.getEndTime()) {
        // empty
        hasData = false;
        return;
      }
      Filter globalTimeFilter = ((FragmentInstanceContext) context).getGlobalTimeFilter();
      if (globalTimeFilter == null) {
        this.memPointIterator.setCurrentPageTimeRange(
            new TimeRange(statistics.getStartTime(), statistics.getEndTime()));
        return;
      }

      long startTime = statistics.getStartTime();
      long endTime = statistics.getEndTime();
      long minStart = Long.MAX_VALUE;
      long maxEnd = Long.MIN_VALUE;
      for (TimeRange timeRange :
          ((FragmentInstanceContext) context).getGlobalTimeFilterTimeRanges()) {
        if (timeRange.overlaps(new TimeRange(startTime, endTime))) {
          minStart = Math.min(minStart, Math.max(timeRange.getMin(), startTime));
          maxEnd = Math.max(maxEnd, Math.min(timeRange.getMax(), endTime));
        }
      }

      if (minStart > maxEnd) {
        hasData = false;
        return;
      }

      this.memPointIterator.setCurrentPageTimeRange(new TimeRange(minStart, maxEnd));
    }

    public TsBlock nextBatch() {
      long startTime = System.nanoTime();
      try {
        return memPointIterator.nextBatch();
      } finally {
        long time = System.nanoTime() - startTime;
        if (isAligned) {
          context.getQueryStatistics().getPageReadersDecodeAlignedMemCount().getAndAdd(1);
          context.getQueryStatistics().getPageReadersDecodeAlignedMemTime().getAndAdd(time);
        } else {
          context.getQueryStatistics().getPageReadersDecodeAlignedMemCount().getAndAdd(1);
          context.getQueryStatistics().getPageReadersDecodeNonAlignedMemTime().getAndAdd(time);
        }
      }
    }

    @Override
    public Statistics getStatistics() {
      return statistics;
    }

    @Override
    public Statistics getMeasurementStatistics(int index) {
      return statistics;
    }

    @Override
    public Statistics getTimeStatistics() {
      return statistics;
    }

    @Override
    public TsBlock getAllSatisfiedPageData(boolean ascending) {
      throw new UnsupportedOperationException("getAllSatisfiedPageData() shouldn't be called here");
    }

    @Override
    public MergeReaderPriority getVersion() {
      return version;
    }

    @Override
    public IPageReader getPageReader() {
      throw new UnsupportedOperationException("getPageReader() shouldn't be called here");
    }

    @Override
    public void addPushDownFilter(Filter pushDownFilter) {
      if (inited) {
        return;
      }
      memPointIterator.setPushDownFilter(pushDownFilter);
    }

    @Override
    public void setLimitOffset(PaginationController paginationController) {
      if (inited) {
        return;
      }
      memPointIterator.setLimitAndOffset(paginationController);
    }

    @Override
    public boolean isModified() {
      return true;
    }

    @Override
    public boolean isSeq() {
      return isSeq;
    }

    public void setInited() {
      inited = true;
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

    Ordering getScanOrder();

    boolean hasNextSeqResource();

    boolean hasNextUnseqResource();

    TsFileResource getNextSeqFileResource(boolean isDelete);

    TsFileResource getNextUnseqFileResource(boolean isDelete);

    void setCurSeqFileIndex(QueryDataSource dataSource);

    boolean overlappedSeqResourceSearchingNeedStop(
        long endPointTime, Statistics<? extends Object> currentStatistics);
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
      return Math.max(satisfiedTimeRange.getMin(), range.getStartTime());
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
    public Ordering getScanOrder() {
      return Ordering.DESC;
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

    @Override
    public boolean overlappedSeqResourceSearchingNeedStop(
        long endPointTime, Statistics<?> currentStatistics) {
      return currentStatistics.getStartTime() <= endPointTime;
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
      return Math.min(satisfiedTimeRange.getMax(), range.getEndTime());
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
    public Ordering getScanOrder() {
      return Ordering.ASC;
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

    @Override
    public boolean overlappedSeqResourceSearchingNeedStop(
        long endPointTime, Statistics<?> currentStatistics) {
      return currentStatistics.getEndTime() >= endPointTime;
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + deviceID.ramBytesUsed() + seriesPath.ramBytesUsed();
  }
}
