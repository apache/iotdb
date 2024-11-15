/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.path.IFullPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.metric.QueryRelatedResourceMetricSet;
import org.apache.iotdb.db.queryengine.metric.SeriesScanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.planner.memory.ThreadSafeMemoryReservationManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.TimePredicate;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceForRegionScan;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSourceType;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class FragmentInstanceContext extends QueryContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(FragmentInstanceContext.class);
  private static final long END_TIME_INITIAL_VALUE = -1L;
  private final FragmentInstanceId id;

  private final FragmentInstanceStateMachine stateMachine;

  private final MemoryReservationManager memoryReservationManager;

  private IDataRegionForQuery dataRegion;
  private Filter globalTimeFilter;

  // it will only be used once, after sharedQueryDataSource being inited, it will be set to null
  private List<IFullPath> sourcePaths;

  // Used for region scan, relating methods are to be added.
  private Map<IDeviceID, DeviceContext> devicePathsToContext;

  // Shared by all scan operators in this fragment instance to avoid memory problem
  private IQueryDataSource sharedQueryDataSource;

  /** closed tsfile used in this fragment instance. */
  private Set<TsFileResource> closedFilePaths;

  /** unClosed tsfile used in this fragment instance. */
  private Set<TsFileResource> unClosedFilePaths;

  /** check if there is tmp file to be deleted. */
  private boolean mayHaveTmpFile = false;

  // null for all time partitions
  // empty for zero time partitions
  private List<Long> timePartitions;

  private QueryDataSourceType queryDataSourceType = QueryDataSourceType.SERIES_SCAN;

  private final AtomicLong startNanos = new AtomicLong();
  private final AtomicLong endNanos = new AtomicLong();

  private final AtomicReference<Long> executionStartTime = new AtomicReference<>();
  private final AtomicReference<Long> lastExecutionStartTime = new AtomicReference<>();
  private final AtomicReference<Long> executionEndTime = new AtomicReference<>();

  private CountDownLatch allDriversClosed;

  // session info
  private SessionInfo sessionInfo;

  private final Map<QueryId, DataNodeQueryContext> dataNodeQueryContextMap;
  private DataNodeQueryContext dataNodeQueryContext;

  // Used for EXPLAIN ANALYZE to cache statistics result when the FI is finished,
  // it will not be released until it's fetched.
  private TFetchFragmentInstanceStatisticsResp fragmentInstanceStatistics = null;

  private long initQueryDataSourceCost = 0;
  private final AtomicLong readyQueueTime = new AtomicLong(0);
  private final AtomicLong blockQueueTime = new AtomicLong(0);
  private long unclosedSeqFileNum = 0;
  private long unclosedUnseqFileNum = 0;
  private long closedSeqFileNum = 0;
  private long closedUnseqFileNum = 0;

  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id, FragmentInstanceStateMachine stateMachine, SessionInfo sessionInfo) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(id, stateMachine, sessionInfo);
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  // This method is only used in groupby
  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      IDataRegionForQuery dataRegion,
      Filter timeFilter) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(id, stateMachine, sessionInfo, dataRegion, timeFilter);
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      IDataRegionForQuery dataRegion,
      TimePredicate globalTimePredicate,
      Map<QueryId, DataNodeQueryContext> dataNodeQueryContextMap) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(
            id,
            stateMachine,
            sessionInfo,
            dataRegion,
            globalTimePredicate,
            dataNodeQueryContextMap);
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  public static FragmentInstanceContext createFragmentInstanceContextForCompaction(long queryId) {
    return new FragmentInstanceContext(queryId);
  }

  public void setQueryDataSourceType(QueryDataSourceType queryDataSourceType) {
    this.queryDataSourceType = queryDataSourceType;
  }

  @TestOnly
  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id, FragmentInstanceStateMachine stateMachine) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(
            id, stateMachine, new SessionInfo(1, "test", ZoneId.systemDefault()));
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  private FragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      IDataRegionForQuery dataRegion,
      TimePredicate globalTimePredicate,
      Map<QueryId, DataNodeQueryContext> dataNodeQueryContextMap) {
    this.id = id;
    this.stateMachine = stateMachine;
    this.executionEndTime.set(END_TIME_INITIAL_VALUE);
    this.sessionInfo = sessionInfo;
    this.dataRegion = dataRegion;
    this.globalTimeFilter =
        globalTimePredicate == null ? null : globalTimePredicate.convertPredicateToTimeFilter();
    this.dataNodeQueryContextMap = dataNodeQueryContextMap;
    this.dataNodeQueryContext = dataNodeQueryContextMap.get(id.getQueryId());
    this.memoryReservationManager =
        new ThreadSafeMemoryReservationManager(id.getQueryId(), this.getClass().getName());
  }

  private FragmentInstanceContext(
      FragmentInstanceId id, FragmentInstanceStateMachine stateMachine, SessionInfo sessionInfo) {
    this.id = id;
    this.stateMachine = stateMachine;
    this.executionEndTime.set(END_TIME_INITIAL_VALUE);
    this.sessionInfo = sessionInfo;
    this.dataNodeQueryContextMap = null;
    this.dataNodeQueryContext = null;
    this.memoryReservationManager =
        new ThreadSafeMemoryReservationManager(id.getQueryId(), this.getClass().getName());
  }

  private FragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      IDataRegionForQuery dataRegion,
      Filter globalTimeFilter) {
    this.id = id;
    this.stateMachine = stateMachine;
    this.executionEndTime.set(END_TIME_INITIAL_VALUE);
    this.sessionInfo = sessionInfo;
    this.dataRegion = dataRegion;
    this.globalTimeFilter = globalTimeFilter;
    this.dataNodeQueryContextMap = null;
    this.memoryReservationManager =
        new ThreadSafeMemoryReservationManager(id.getQueryId(), this.getClass().getName());
  }

  @TestOnly
  public void setDataRegion(IDataRegionForQuery dataRegion) {
    this.dataRegion = dataRegion;
  }

  // used for compaction
  private FragmentInstanceContext(long queryId) {
    this.queryId = queryId;
    this.id = null;
    this.stateMachine = null;
    this.dataNodeQueryContextMap = null;
    this.dataNodeQueryContext = null;
    this.memoryReservationManager = null;
  }

  public void start() {
    long now = System.currentTimeMillis();
    executionStartTime.compareAndSet(null, now);
    startNanos.compareAndSet(0, System.nanoTime());

    // always update last execution start time
    lastExecutionStartTime.set(now);
  }

  // the state change listener is added here in a separate initialize() method
  // instead of the constructor to prevent leaking the "this" reference to
  // another thread, which will cause unsafe publication of this instance.
  private void initialize() {
    stateMachine.addStateChangeListener(this::updateStatsIfDone);
  }

  private void updateStatsIfDone(FragmentInstanceState newState) {
    if (newState.isDone()) {
      long now = System.currentTimeMillis();

      // before setting the end times, make sure a start has been recorded
      executionStartTime.compareAndSet(null, now);
      startNanos.compareAndSet(0, System.nanoTime());

      // Only update last start time, if the nothing was started
      lastExecutionStartTime.compareAndSet(null, now);

      // use compare and set from initial value to avoid overwriting if there
      // were a duplicate notification, which shouldn't happen
      executionEndTime.compareAndSet(END_TIME_INITIAL_VALUE, now);
      endNanos.compareAndSet(0, System.nanoTime());

      // release some query resource in FragmentInstanceContext
      // why not release them in releaseResourceWhenAllDriversAreClosed() together?
      // because we may have no chane to run the releaseResourceWhenAllDriversAreClosed which is
      // called in callback of FragmentInstanceExecution
      // FragmentInstanceExecution won't be created if we meet some errors like MemoryNotEnough
      releaseDataNodeQueryContext();
      sourcePaths = null;
    }
  }

  public FragmentInstanceId getId() {
    return id;
  }

  public void failed(Throwable cause) {
    stateMachine.failed(cause);
  }

  /** return Message string of all failures */
  public String getFailedCause() {
    return stateMachine.getFailureCauses().stream()
        .findFirst()
        .map(Throwable::getMessage)
        .orElse("");
  }

  /** return List of specific throwable and stack trace */
  public List<FragmentInstanceFailureInfo> getFailureInfoList() {
    return stateMachine.getFailureCauses().stream()
        .map(FragmentInstanceFailureInfo::toFragmentInstanceFailureInfo)
        .collect(Collectors.toList());
  }

  public Optional<TSStatus> getErrorCode() {
    return stateMachine.getFailureCauses().stream()
        .filter(e -> e instanceof IoTDBException || e instanceof IoTDBRuntimeException)
        .findFirst()
        .flatMap(
            t -> {
              TSStatus status;
              if (t instanceof IoTDBException) {
                status = new TSStatus(((IoTDBException) t).getErrorCode());
              } else {
                status = new TSStatus(((IoTDBRuntimeException) t).getErrorCode());
              }
              status.setMessage(t.getMessage());
              return Optional.of(status);
            });
  }

  public void finished() {
    stateMachine.finished();
  }

  public void transitionToFlushing() {
    stateMachine.transitionToFlushing();
  }

  public void cancel() {
    stateMachine.cancel();
  }

  public void abort() {
    stateMachine.abort();
  }

  public long getEndTime() {
    return executionEndTime.get();
  }

  public boolean isEndTimeUpdate() {
    return executionEndTime.get() != END_TIME_INITIAL_VALUE;
  }

  @Override
  public long getStartTime() {
    return executionStartTime.get();
  }

  public DataNodeQueryContext getDataNodeQueryContext() {
    return dataNodeQueryContext;
  }

  public void setDataNodeQueryContext(DataNodeQueryContext dataNodeQueryContext) {
    this.dataNodeQueryContext = dataNodeQueryContext;
  }

  public FragmentInstanceInfo getInstanceInfo() {
    return getErrorCode()
        .map(
            s ->
                new FragmentInstanceInfo(
                    stateMachine.getState(),
                    getEndTime(),
                    getFailedCause(),
                    getFailureInfoList(),
                    s))
        .orElseGet(
            () ->
                new FragmentInstanceInfo(
                    stateMachine.getState(), getEndTime(), getFailedCause(), getFailureInfoList()));
  }

  public FragmentInstanceStateMachine getStateMachine() {
    return stateMachine;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }

  public Optional<Throwable> getFailureCause() {
    return Optional.ofNullable(
        stateMachine.getFailureCauses().stream()
            .filter(e -> e instanceof IoTDBException || e instanceof IoTDBRuntimeException)
            .findFirst()
            .orElse(stateMachine.getFailureCauses().peek()));
  }

  public Filter getGlobalTimeFilter() {
    return globalTimeFilter;
  }

  public void setTimeFilterForTableModel(Filter timeFilter) {
    if (globalTimeFilter == null) {
      globalTimeFilter = timeFilter;
    } else {
      // In join case, there may exist more than one table and time filter
      globalTimeFilter = FilterFactory.or(globalTimeFilter, timeFilter);
      // throw new IllegalStateException(
      //    "globalTimeFilter in FragmentInstanceContext should only be set once in Table Model!");
    }
  }

  public IDataRegionForQuery getDataRegion() {
    return dataRegion;
  }

  public void setSourcePaths(List<IFullPath> sourcePaths) {
    this.sourcePaths = sourcePaths;
  }

  public void setDevicePathsToContext(Map<IDeviceID, DeviceContext> devicePathsToContext) {
    this.devicePathsToContext = devicePathsToContext;
  }

  public MemoryReservationManager getMemoryReservationContext() {
    return memoryReservationManager;
  }

  public void releaseMemoryReservationManager() {
    memoryReservationManager.releaseAllReservedMemory();
  }

  public void initQueryDataSource(List<IFullPath> sourcePaths) throws QueryProcessException {
    long startTime = System.nanoTime();
    if (sourcePaths == null) {
      return;
    }
    dataRegion.readLock();
    try {
      List<IFullPath> pathList = new ArrayList<>();
      Set<IDeviceID> selectedDeviceIdSet = new HashSet<>();
      for (IFullPath path : sourcePaths) {
        pathList.add(path);
        selectedDeviceIdSet.add(path.getDeviceId());
      }

      this.sharedQueryDataSource =
          dataRegion.query(
              pathList,
              // when all the selected series are under the same device, the QueryDataSource will be
              // filtered according to timeIndex
              selectedDeviceIdSet.size() == 1 ? selectedDeviceIdSet.iterator().next() : null,
              this,
              // time filter may be stateful, so we need to copy it
              globalTimeFilter != null ? globalTimeFilter.copy() : null,
              timePartitions);

      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      if (sharedQueryDataSource != null) {
        closedFilePaths = new HashSet<>();
        unClosedFilePaths = new HashSet<>();
        addUsedFilesForQuery((QueryDataSource) sharedQueryDataSource);
        ((QueryDataSource) sharedQueryDataSource).setSingleDevice(selectedDeviceIdSet.size() == 1);
      }
    } finally {
      setInitQueryDataSourceCost(System.nanoTime() - startTime);
      dataRegion.readUnlock();
    }
  }

  public void initRegionScanQueryDataSource(Map<IDeviceID, DeviceContext> devicePathsToContext)
      throws QueryProcessException {
    long startTime = System.nanoTime();
    if (devicePathsToContext == null) {
      return;
    }
    dataRegion.readLock();
    try {
      this.sharedQueryDataSource =
          dataRegion.queryForDeviceRegionScan(
              devicePathsToContext,
              this,
              globalTimeFilter != null ? globalTimeFilter.copy() : null,
              timePartitions);

      if (sharedQueryDataSource != null) {
        closedFilePaths = new HashSet<>();
        unClosedFilePaths = new HashSet<>();
        addUsedFilesForRegionQuery((QueryDataSourceForRegionScan) sharedQueryDataSource);
      }
    } finally {
      setInitQueryDataSourceCost(System.nanoTime() - startTime);
      dataRegion.readUnlock();
    }
  }

  public void initRegionScanQueryDataSource(List<IFullPath> pathList) throws QueryProcessException {
    long startTime = System.nanoTime();
    if (pathList == null) {
      return;
    }
    dataRegion.readLock();
    try {
      this.sharedQueryDataSource =
          dataRegion.queryForSeriesRegionScan(
              pathList,
              this,
              globalTimeFilter != null ? globalTimeFilter.copy() : null,
              timePartitions);

      if (sharedQueryDataSource != null) {
        closedFilePaths = new HashSet<>();
        unClosedFilePaths = new HashSet<>();
        addUsedFilesForRegionQuery((QueryDataSourceForRegionScan) sharedQueryDataSource);
      }
    } finally {
      setInitQueryDataSourceCost(System.nanoTime() - startTime);
      dataRegion.readUnlock();
    }
  }

  public synchronized IQueryDataSource getSharedQueryDataSource() throws QueryProcessException {
    if (sharedQueryDataSource == null) {
      switch (queryDataSourceType) {
        case SERIES_SCAN:
          initQueryDataSource(sourcePaths);
          // Friendly for gc
          sourcePaths = null;
          break;
        case DEVICE_REGION_SCAN:
          initRegionScanQueryDataSource(devicePathsToContext);
          devicePathsToContext = null;
          break;
        case TIME_SERIES_REGION_SCAN:
          initRegionScanQueryDataSource(sourcePaths);
          sourcePaths = null;
          break;
        default:
          throw new QueryProcessException(
              "Unsupported query data source type: " + queryDataSourceType);
      }
    }
    return sharedQueryDataSource;
  }

  /** Lock and check if tsFileResource is deleted */
  private boolean processTsFileResource(TsFileResource tsFileResource, boolean isClosed) {
    addFilePathToMap(tsFileResource, isClosed);
    // this file may be deleted just before we lock it
    if (tsFileResource.isDeleted()) {
      Set<TsFileResource> pathSet = isClosed ? closedFilePaths : unClosedFilePaths;
      // This resource may be removed by other threads of this query.
      if (pathSet.remove(tsFileResource)) {
        FileReaderManager.getInstance().decreaseFileReaderReference(tsFileResource, isClosed);
      }
      return true;
    } else {
      return false;
    }
  }

  /** Add the unique file paths to closeddFilePathsMap and unClosedFilePathsMap. */
  private void addUsedFilesForQuery(QueryDataSource dataSource) {

    // sequence data
    dataSource
        .getSeqResources()
        .removeIf(
            tsFileResource -> processTsFileResource(tsFileResource, tsFileResource.isClosed()));

    // Record statistics of seqFiles
    unclosedSeqFileNum = unClosedFilePaths.size();
    closedSeqFileNum = closedFilePaths.size();

    // unsequence data
    dataSource
        .getUnseqResources()
        .removeIf(
            tsFileResource -> processTsFileResource(tsFileResource, tsFileResource.isClosed()));

    // Record statistics of files of unseqFiles
    unclosedUnseqFileNum = unClosedFilePaths.size() - unclosedSeqFileNum;
    closedUnseqFileNum = closedFilePaths.size() - closedSeqFileNum;
  }

  private void addUsedFilesForRegionQuery(QueryDataSourceForRegionScan dataSource) {
    dataSource
        .getSeqFileScanHandles()
        .removeIf(
            fileScanHandle ->
                processTsFileResource(fileScanHandle.getTsResource(), fileScanHandle.isClosed()));

    unclosedSeqFileNum = unClosedFilePaths.size();
    closedSeqFileNum = closedFilePaths.size();

    dataSource
        .getUnseqFileScanHandles()
        .removeIf(
            fileScanHandle ->
                processTsFileResource(fileScanHandle.getTsResource(), fileScanHandle.isClosed()));

    unclosedUnseqFileNum = unClosedFilePaths.size() - unclosedSeqFileNum;
    closedUnseqFileNum = closedFilePaths.size() - closedSeqFileNum;
  }

  /**
   * Increase the usage reference of filePath of job id. Before the invoking of this method, <code>
   * this.setqueryIdForCurrentRequestThread</code> has been invoked, so <code>
   * sealedFilePathsMap.get(queryId)</code> or <code>unsealedFilePathsMap.get(queryId)</code> must
   * not return null.
   */
  private void addFilePathToMap(TsFileResource tsFile, boolean isClosed) {
    Set<TsFileResource> pathSet = isClosed ? closedFilePaths : unClosedFilePaths;
    if (!pathSet.contains(tsFile)) {
      pathSet.add(tsFile);
      FileReaderManager.getInstance().increaseFileReaderReference(tsFile, isClosed);
    }
  }

  public void initializeNumOfDrivers(int numOfDrivers) {
    // initialize with the num of Drivers
    allDriversClosed = new CountDownLatch(numOfDrivers);
  }

  public void decrementNumOfUnClosedDriver() {
    allDriversClosed.countDown();
  }

  @SuppressWarnings("squid:S2142")
  public void releaseResourceWhenAllDriversAreClosed() {
    while (true) {
      try {
        allDriversClosed.await();
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(
            "Interrupted when await on allDriversClosed, FragmentInstance Id is {}", this.getId());
      }
    }
    releaseResource();
  }

  /**
   * All file paths used by this fragment instance must be cleared and thus the usage reference must
   * be decreased.
   */
  public synchronized void releaseResource() {
    // For schema related query FI, closedFilePaths and unClosedFilePaths will be null
    if (closedFilePaths != null) {
      for (TsFileResource tsFile : closedFilePaths) {
        FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, true);
      }
      closedFilePaths = null;
    }

    if (unClosedFilePaths != null) {
      for (TsFileResource tsFile : unClosedFilePaths) {
        FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, false);
      }
      unClosedFilePaths = null;
    }

    dataRegion = null;
    globalTimeFilter = null;
    sharedQueryDataSource = null;

    // record fragment instance execution time and metadata get time to metrics
    long durationTime = System.currentTimeMillis() - executionStartTime.get();
    QueryRelatedResourceMetricSet.getInstance().updateFragmentInstanceTime(durationTime);

    SeriesScanCostMetricSet.getInstance()
        .recordBloomFilterMetrics(
            getQueryStatistics().getLoadBloomFilterFromCacheCount().get(),
            getQueryStatistics().getLoadBloomFilterFromDiskCount().get(),
            getQueryStatistics().getLoadBloomFilterActualIOSize().get(),
            getQueryStatistics().getLoadBloomFilterTime().get());

    SeriesScanCostMetricSet.getInstance()
        .recordNonAlignedTimeSeriesMetadataCount(
            getQueryStatistics().getLoadTimeSeriesMetadataDiskSeqCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataDiskUnSeqCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataMemSeqCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataMemUnSeqCount().get());
    SeriesScanCostMetricSet.getInstance()
        .recordNonAlignedTimeSeriesMetadataTime(
            getQueryStatistics().getLoadTimeSeriesMetadataDiskSeqTime().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataDiskUnSeqTime().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataMemSeqTime().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataMemUnSeqTime().get());
    SeriesScanCostMetricSet.getInstance()
        .recordAlignedTimeSeriesMetadataCount(
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedDiskSeqCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedDiskUnSeqCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedMemSeqCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedMemUnSeqCount().get());
    SeriesScanCostMetricSet.getInstance()
        .recordAlignedTimeSeriesMetadataTime(
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedDiskSeqTime().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedDiskUnSeqTime().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedMemSeqTime().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataAlignedMemUnSeqTime().get());

    SeriesScanCostMetricSet.getInstance()
        .recordTimeSeriesMetadataMetrics(
            getQueryStatistics().getLoadTimeSeriesMetadataFromCacheCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataFromDiskCount().get(),
            getQueryStatistics().getLoadTimeSeriesMetadataActualIOSize().get());

    SeriesScanCostMetricSet.getInstance()
        .recordConstructChunkReadersCount(
            getQueryStatistics().getConstructAlignedChunkReadersMemCount().get(),
            getQueryStatistics().getConstructAlignedChunkReadersDiskCount().get(),
            getQueryStatistics().getConstructNonAlignedChunkReadersMemCount().get(),
            getQueryStatistics().getConstructNonAlignedChunkReadersDiskCount().get());
    SeriesScanCostMetricSet.getInstance()
        .recordConstructChunkReadersTime(
            getQueryStatistics().getConstructAlignedChunkReadersMemTime().get(),
            getQueryStatistics().getConstructAlignedChunkReadersDiskTime().get(),
            getQueryStatistics().getConstructNonAlignedChunkReadersMemTime().get(),
            getQueryStatistics().getConstructNonAlignedChunkReadersDiskTime().get());

    SeriesScanCostMetricSet.getInstance()
        .recordChunkMetrics(
            getQueryStatistics().getLoadChunkFromCacheCount().get(),
            getQueryStatistics().getLoadChunkFromDiskCount().get(),
            getQueryStatistics().getLoadChunkActualIOSize().get());

    SeriesScanCostMetricSet.getInstance()
        .recordPageReadersDecompressCount(
            getQueryStatistics().getPageReadersDecodeAlignedMemCount().get(),
            getQueryStatistics().getPageReadersDecodeAlignedDiskCount().get(),
            getQueryStatistics().getPageReadersDecodeNonAlignedMemCount().get(),
            getQueryStatistics().getPageReadersDecodeNonAlignedDiskCount().get());
    SeriesScanCostMetricSet.getInstance()
        .recordPageReadersDecompressTime(
            getQueryStatistics().getPageReadersDecodeAlignedMemTime().get(),
            getQueryStatistics().getPageReadersDecodeAlignedDiskTime().get(),
            getQueryStatistics().getPageReadersDecodeNonAlignedMemTime().get(),
            getQueryStatistics().getPageReadersDecodeNonAlignedDiskTime().get());

    SeriesScanCostMetricSet.getInstance()
        .recordTimeSeriesMetadataModification(
            getQueryStatistics().getAlignedTimeSeriesMetadataModificationCount().get(),
            getQueryStatistics().getNonAlignedTimeSeriesMetadataModificationCount().get(),
            getQueryStatistics().getAlignedTimeSeriesMetadataModificationTime().get(),
            getQueryStatistics().getNonAlignedTimeSeriesMetadataModificationTime().get());

    SeriesScanCostMetricSet.getInstance()
        .updatePageReaderMemoryUsage(getQueryStatistics().getPageReaderMaxUsedMemorySize().get());
  }

  private synchronized void releaseDataNodeQueryContext() {
    if (dataNodeQueryContextMap == null) {
      // this process is in fetch schema, nothing need to release
      return;
    }

    if (dataNodeQueryContext.decreaseDataNodeFINum() == 0) {
      dataNodeQueryContext = null;
      dataNodeQueryContextMap.remove(id.getQueryId());
    }
  }

  public void setMayHaveTmpFile(boolean mayHaveTmpFile) {
    this.mayHaveTmpFile = mayHaveTmpFile;
  }

  public boolean mayHaveTmpFile() {
    return mayHaveTmpFile;
  }

  public Optional<List<Long>> getTimePartitions() {
    return Optional.ofNullable(timePartitions);
  }

  public void setTimePartitions(List<Long> timePartitions) {
    this.timePartitions = timePartitions;
  }

  // Only used in EXPLAIN ANALYZE
  public void setFragmentInstanceStatistics(TFetchFragmentInstanceStatisticsResp statistics) {
    this.fragmentInstanceStatistics = statistics;
  }

  public TFetchFragmentInstanceStatisticsResp getFragmentInstanceStatistics() {
    return fragmentInstanceStatistics;
  }

  public void setInitQueryDataSourceCost(long initQueryDataSourceCost) {
    this.initQueryDataSourceCost = initQueryDataSourceCost;
  }

  public long getInitQueryDataSourceCost() {
    return initQueryDataSourceCost;
  }

  public void addReadyQueuedTime(long time) {
    readyQueueTime.addAndGet(time);
  }

  public void addBlockQueuedTime(long time) {
    blockQueueTime.addAndGet(time);
  }

  public long getReadyQueueTime() {
    return readyQueueTime.get();
  }

  public long getBlockQueueTime() {
    return blockQueueTime.get();
  }

  public long getClosedSeqFileNum() {
    return closedSeqFileNum;
  }

  public long getUnclosedUnseqFileNum() {
    return unclosedUnseqFileNum;
  }

  public long getClosedUnseqFileNum() {
    return closedUnseqFileNum;
  }

  public long getUnclosedSeqFileNum() {
    return unclosedSeqFileNum;
  }
}
