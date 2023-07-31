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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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

  private IDataRegionForQuery dataRegion;
  private Filter timeFilter;
  private List<PartialPath> sourcePaths;
  // Shared by all scan operators in this fragment instance to avoid memory problem
  private QueryDataSource sharedQueryDataSource;
  /** closed tsfile used in this fragment instance. */
  private Set<TsFileResource> closedFilePaths;
  /** unClosed tsfile used in this fragment instance. */
  private Set<TsFileResource> unClosedFilePaths;
  /** check if there is tmp file to be deleted. */
  private boolean mayHaveTmpFile = false;

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

  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id, FragmentInstanceStateMachine stateMachine, SessionInfo sessionInfo) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(id, stateMachine, sessionInfo);
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

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
      Filter timeFilter,
      Map<QueryId, DataNodeQueryContext> dataNodeQueryContextMap) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(
            id, stateMachine, sessionInfo, dataRegion, timeFilter, dataNodeQueryContextMap);
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  public static FragmentInstanceContext createFragmentInstanceContextForCompaction(long queryId) {
    return new FragmentInstanceContext(queryId);
  }

  @TestOnly
  public static FragmentInstanceContext createFragmentInstanceContext(
      FragmentInstanceId id, FragmentInstanceStateMachine stateMachine) {
    FragmentInstanceContext instanceContext =
        new FragmentInstanceContext(
            id, stateMachine, new SessionInfo(1, "test", ZoneId.systemDefault().getId()));
    instanceContext.initialize();
    instanceContext.start();
    return instanceContext;
  }

  private FragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      IDataRegionForQuery dataRegion,
      Filter timeFilter,
      Map<QueryId, DataNodeQueryContext> dataNodeQueryContextMap) {
    this.id = id;
    this.stateMachine = stateMachine;
    this.executionEndTime.set(END_TIME_INITIAL_VALUE);
    this.sessionInfo = sessionInfo;
    this.dataRegion = dataRegion;
    this.timeFilter = timeFilter;
    this.dataNodeQueryContextMap = dataNodeQueryContextMap;
    this.dataNodeQueryContext = dataNodeQueryContextMap.get(id.getQueryId());
  }

  private FragmentInstanceContext(
      FragmentInstanceId id, FragmentInstanceStateMachine stateMachine, SessionInfo sessionInfo) {
    this.id = id;
    this.stateMachine = stateMachine;
    this.executionEndTime.set(END_TIME_INITIAL_VALUE);
    this.sessionInfo = sessionInfo;
    this.dataNodeQueryContextMap = null;
    this.dataNodeQueryContext = null;
  }

  private FragmentInstanceContext(
      FragmentInstanceId id,
      FragmentInstanceStateMachine stateMachine,
      SessionInfo sessionInfo,
      IDataRegionForQuery dataRegion,
      Filter timeFilter) {
    this.id = id;
    this.stateMachine = stateMachine;
    this.executionEndTime.set(END_TIME_INITIAL_VALUE);
    this.sessionInfo = sessionInfo;
    this.dataRegion = dataRegion;
    this.timeFilter = timeFilter;
    this.dataNodeQueryContextMap = null;
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
    return new FragmentInstanceInfo(
        stateMachine.getState(), getEndTime(), getFailedCause(), getFailureInfoList());
  }

  public FragmentInstanceStateMachine getStateMachine() {
    return stateMachine;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }

  public Optional<Throwable> getFailureCause() {
    return Optional.ofNullable(stateMachine.getFailureCauses().peek());
  }

  public Filter getTimeFilter() {
    return timeFilter;
  }

  public IDataRegionForQuery getDataRegion() {
    return dataRegion;
  }

  public void setSourcePaths(List<PartialPath> sourcePaths) {
    this.sourcePaths = sourcePaths;
  }

  public void initQueryDataSource(List<PartialPath> sourcePaths) throws QueryProcessException {
    if (sourcePaths == null) {
      return;
    }
    dataRegion.readLock();
    try {
      List<PartialPath> pathList = new ArrayList<>();
      Set<String> selectedDeviceIdSet = new HashSet<>();
      for (PartialPath path : sourcePaths) {
        pathList.add(path);
        selectedDeviceIdSet.add(path.getDevice());
      }

      this.sharedQueryDataSource =
          dataRegion.query(
              pathList,
              // when all the selected series are under the same device, the QueryDataSource will be
              // filtered according to timeIndex
              selectedDeviceIdSet.size() == 1 ? selectedDeviceIdSet.iterator().next() : null,
              this,
              timeFilter != null ? timeFilter.copy() : null);

      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      if (sharedQueryDataSource != null) {
        closedFilePaths = new HashSet<>();
        unClosedFilePaths = new HashSet<>();
        addUsedFilesForQuery(sharedQueryDataSource);
      }
    } finally {
      dataRegion.readUnlock();
    }
  }

  public synchronized QueryDataSource getSharedQueryDataSource() throws QueryProcessException {
    if (sharedQueryDataSource == null) {
      initQueryDataSource(sourcePaths);
    }
    return sharedQueryDataSource;
  }

  /** Add the unique file paths to closeddFilePathsMap and unClosedFilePathsMap. */
  private void addUsedFilesForQuery(QueryDataSource dataSource) {

    // sequence data
    addUsedFilesForQuery(dataSource.getSeqResources());

    // unsequence data
    addUsedFilesForQuery(dataSource.getUnseqResources());
  }

  private void addUsedFilesForQuery(List<TsFileResource> resources) {
    Iterator<TsFileResource> iterator = resources.iterator();
    while (iterator.hasNext()) {
      TsFileResource tsFileResource = iterator.next();
      boolean isClosed = tsFileResource.isClosed();
      addFilePathToMap(tsFileResource, isClosed);

      // this file may be deleted just before we lock it
      if (tsFileResource.isDeleted()) {
        Set<TsFileResource> pathSet = isClosed ? closedFilePaths : unClosedFilePaths;
        // This resource may be removed by other threads of this query.
        if (pathSet.remove(tsFileResource)) {
          FileReaderManager.getInstance().decreaseFileReaderReference(tsFileResource, isClosed);
        }
        iterator.remove();
      }
    }
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
    timeFilter = null;
    sourcePaths = null;
    sharedQueryDataSource = null;
    releaseDataNodeQueryContext();
  }

  private void releaseDataNodeQueryContext() {
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
}
