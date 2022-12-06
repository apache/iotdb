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
package org.apache.iotdb.db.mpp.execution.driver;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.IDataRegionForQuery;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.mpp.execution.exchange.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.mpp.statistics.QueryStatistics.QUERY_RESOURCE_INIT;

/**
 * One dataDriver is responsible for one FragmentInstance which is for data query, which may
 * contains several series.
 */
@NotThreadSafe
public class DataDriver extends Driver {

  private boolean init;

  /** closed tsfile used in this fragment instance */
  private Set<TsFileResource> closedFilePaths;
  /** unClosed tsfile used in this fragment instance */
  private Set<TsFileResource> unClosedFilePaths;

  public DataDriver(Operator root, ISinkHandle sinkHandle, DataDriverContext driverContext) {
    super(root, sinkHandle, driverContext);
    this.closedFilePaths = new HashSet<>();
    this.unClosedFilePaths = new HashSet<>();
  }

  @Override
  protected boolean init(SettableFuture<?> blockedFuture) {
    if (!init) {
      try {
        initialize();
      } catch (Throwable t) {
        LOGGER.error(
            "Failed to do the initialization for fragment instance {} ", driverContext.getId(), t);
        driverContext.failed(t);
        blockedFuture.setException(t);
        return false;
      }
    }
    return true;
  }

  /**
   * All file paths used by this fragment instance must be cleared and thus the usage reference must
   * be decreased.
   */
  @Override
  protected void releaseResource() {
    for (TsFileResource tsFile : closedFilePaths) {
      FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, true);
    }
    closedFilePaths = null;
    for (TsFileResource tsFile : unClosedFilePaths) {
      FileReaderManager.getInstance().decreaseFileReaderReference(tsFile, false);
    }
    unClosedFilePaths = null;
  }

  /**
   * init seq file list and unseq file list in QueryDataSource and set it into each SourceNode TODO
   * we should change all the blocked lock operation into tryLock
   */
  private void initialize() throws QueryProcessException {
    long startTime = System.nanoTime();
    try {
      List<DataSourceOperator> sourceOperators =
          ((DataDriverContext) driverContext).getSourceOperators();
      if (sourceOperators != null && !sourceOperators.isEmpty()) {
        QueryDataSource dataSource = initQueryDataSource();
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
    } finally {
      driverContext
          .getFragmentInstanceContext()
          .addOperationTime(QUERY_RESOURCE_INIT, System.nanoTime() - startTime);
    }
  }

  /**
   * The method is called in mergeLock() when executing query. This method will get all the
   * QueryDataSource needed for this query
   */
  private QueryDataSource initQueryDataSource() throws QueryProcessException {
    DataDriverContext context = (DataDriverContext) driverContext;
    IDataRegionForQuery dataRegion = context.getDataRegion();
    dataRegion.readLock();
    try {
      List<PartialPath> pathList =
          context.getPaths().stream().map(IDTable::translateQueryPath).collect(Collectors.toList());
      // when all the selected series are under the same device, the QueryDataSource will be
      // filtered according to timeIndex
      Set<String> selectedDeviceIdSet =
          pathList.stream().map(PartialPath::getDevice).collect(Collectors.toSet());

      Filter timeFilter = context.getTimeFilter();
      QueryDataSource dataSource =
          dataRegion.query(
              pathList,
              selectedDeviceIdSet.size() == 1 ? selectedDeviceIdSet.iterator().next() : null,
              driverContext.getFragmentInstanceContext(),
              timeFilter != null ? timeFilter.copy() : null);

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
}
