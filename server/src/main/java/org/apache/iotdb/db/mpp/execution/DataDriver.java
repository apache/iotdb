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
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.buffer.ISinkHandle;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.source.SourceOperator;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.apache.iotdb.db.mpp.operator.Operator.NOT_BLOCKED;

@NotThreadSafe
public class DataDriver implements Driver {

  private static final Logger logger = LoggerFactory.getLogger(DataDriver.class);

  private final Operator root;
  private final ISinkHandle sinkHandle;
  private final DataDriverContext driverContext;

  private boolean init;
  private boolean closed;

  /** closed tsfile used in this fragment instance */
  private Set<TsFileResource> closedFilePaths;
  /** unClosed tsfile used in this fragment instance */
  private Set<TsFileResource> unClosedFilePaths;

  private final AtomicReference<SettableFuture<Void>> driverBlockedFuture = new AtomicReference<>();

  public DataDriver(Operator root, ISinkHandle sinkHandle, DataDriverContext driverContext) {
    this.root = root;
    this.sinkHandle = sinkHandle;
    this.driverContext = driverContext;
    this.closedFilePaths = new HashSet<>();
    this.unClosedFilePaths = new HashSet<>();
    // initially the driverBlockedFuture is not blocked (it is completed)
    SettableFuture<Void> future = SettableFuture.create();
    future.set(null);
    driverBlockedFuture.set(future);
  }

  @Override
  public boolean isFinished() {
    try {
      boolean isFinished =
          closed || (driverBlockedFuture.get().isDone() && root != null && root.isFinished());
      if (isFinished) {
        driverContext.finish();
      }
      return isFinished;
    } catch (Throwable t) {
      logger.error(
          "Failed to query whether the data driver {} is finished", driverContext.getId(), t);
      driverContext.failed(t);
      close();
      return true;
    }
  }

  @Override
  public ListenableFuture<Void> processFor(Duration duration) {

    SettableFuture<Void> blockedFuture = driverBlockedFuture.get();
    // initialization may be time-consuming, so we keep it in the processFor method
    // in normal case, it won't cause deadlock and should finish soon, otherwise it will be a
    // critical bug
    if (!init) {
      try {
        initialize();
      } catch (Throwable t) {
        logger.error(
            "Failed to do the initialization for fragment instance {} ", driverContext.getId(), t);
        driverContext.failed(t);
        close();
        blockedFuture.setException(t);
        return blockedFuture;
      }
    }

    // if the driver is blocked we don't need to continue
    if (!blockedFuture.isDone()) {
      return blockedFuture;
    }

    long maxRuntime = duration.roundTo(TimeUnit.NANOSECONDS);

    long start = System.nanoTime();
    try {
      do {
        ListenableFuture<Void> future = processInternal();
        if (!future.isDone()) {
          return updateDriverBlockedFuture(future);
        }
      } while (System.nanoTime() - start < maxRuntime && !root.isFinished());
    } catch (Throwable t) {
      logger.error("Failed to execute fragment instance {}", driverContext.getId(), t);
      driverContext.failed(t);
      close();
      blockedFuture.setException(t);
      return blockedFuture;
    }
    return NOT_BLOCKED;
  }

  @Override
  public FragmentInstanceId getInfo() {
    return driverContext.getId();
  }

  @Override
  public void close() {
    closed = true;
    try {
      if (root != null) {
        root.close();
      }
      if (sinkHandle != null) {
        sinkHandle.close();
      }
    } catch (Throwable t) {
      logger.error("Failed to closed driver {}", driverContext.getId(), t);
      driverContext.failed(t);
    } finally {
      removeUsedFilesForQuery();
    }
  }

  /**
   * init seq file list and unseq file list in QueryDataSource and set it into each SourceNode TODO
   * we should change all the blocked lock operation into tryLock
   */
  private void initialize() throws QueryProcessException {
    List<SourceOperator> sourceOperators = driverContext.getSourceOperators();
    if (sourceOperators != null && !sourceOperators.isEmpty()) {
      QueryDataSource dataSource = initQueryDataSourceCache();
      sourceOperators.forEach(
          sourceOperator -> {
            // construct QueryDataSource for source operator
            QueryDataSource queryDataSource =
                new QueryDataSource(dataSource.getSeqResources(), dataSource.getUnseqResources());

            queryDataSource.setDataTTL(dataSource.getDataTTL());

            sourceOperator.initQueryDataSource(queryDataSource);
          });
    }

    this.init = true;
  }

  /**
   * The method is called in mergeLock() when executing query. This method will get all the
   * QueryDataSource needed for this query
   */
  public QueryDataSource initQueryDataSourceCache() throws QueryProcessException {
    DataRegion dataRegion = driverContext.getDataRegion();
    dataRegion.readLock();
    try {
      List<PartialPath> pathList =
          driverContext.getPaths().stream()
              .map(IDTable::translateQueryPath)
              .collect(Collectors.toList());
      // when all the selected series are under the same device, the QueryDataSource will be
      // filtered according to timeIndex
      Set<String> selectedDeviceIdSet =
          pathList.stream().map(PartialPath::getDevice).collect(Collectors.toSet());

      QueryDataSource dataSource =
          dataRegion.query(
              pathList,
              selectedDeviceIdSet.size() == 1 ? selectedDeviceIdSet.iterator().next() : null,
              driverContext.getFragmentInstanceContext(),
              driverContext.getTimeFilter());

      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      addUsedFilesForQuery(dataSource);

      return dataSource;
    } finally {
      dataRegion.readUnlock();
    }
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
   * All file paths used by this fragment instance must be cleared and thus the usage reference must
   * be decreased.
   */
  private void removeUsedFilesForQuery() {
    for (TsFileResource tsFile : closedFilePaths) {
      FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, true);
    }
    closedFilePaths = null;
    for (TsFileResource tsFile : unClosedFilePaths) {
      FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, true);
    }
    unClosedFilePaths = null;
  }

  /**
   * Increase the usage reference of filePath of job id. Before the invoking of this method, <code>
   * this.setqueryIdForCurrentRequestThread</code> has been invoked, so <code>
   * sealedFilePathsMap.get(queryId)</code> or <code>unsealedFilePathsMap.get(queryId)</code> must
   * not return null.
   */
  void addFilePathToMap(TsFileResource tsFile, boolean isClosed) {
    Set<TsFileResource> pathSet = isClosed ? closedFilePaths : unClosedFilePaths;
    if (!pathSet.contains(tsFile)) {
      pathSet.add(tsFile);
      FileReaderManager.getInstance().increaseFileReaderReference(tsFile, isClosed);
    }
  }

  private ListenableFuture<Void> processInternal() throws IOException {
    ListenableFuture<Void> blocked = root.isBlocked();
    if (!blocked.isDone()) {
      return blocked;
    }
    blocked = sinkHandle.isFull();
    if (!blocked.isDone()) {
      return blocked;
    }
    if (root.hasNext()) {
      TsBlock tsBlock = root.next();
      if (tsBlock != null && !tsBlock.isEmpty()) {
        sinkHandle.send(Collections.singletonList(tsBlock));
      }
    }
    return NOT_BLOCKED;
  }

  private ListenableFuture<Void> updateDriverBlockedFuture(
      ListenableFuture<Void> sourceBlockedFuture) {
    // driverBlockedFuture will be completed as soon as the sourceBlockedFuture is completed
    // or any of the operators gets a memory revocation request
    SettableFuture<Void> newDriverBlockedFuture = SettableFuture.create();
    driverBlockedFuture.set(newDriverBlockedFuture);
    sourceBlockedFuture.addListener(() -> newDriverBlockedFuture.set(null), directExecutor());

    // TODO Although we don't have memory management for operator now, we should consider it for
    // future
    // it's possible that memory revoking is requested for some operator
    // before we update driverBlockedFuture above and we don't want to miss that
    // notification, so we check to see whether that's the case before returning.

    return newDriverBlockedFuture;
  }
}
