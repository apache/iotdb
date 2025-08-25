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

package org.apache.iotdb.db.queryengine.execution.driver;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;

import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.QUERY_RESOURCE_INIT;
import static org.apache.iotdb.db.storageengine.dataregion.VirtualDataRegion.UNFINISHED_QUERY_DATA_SOURCE;

/**
 * One {@link DataDriver} is responsible for one {@link FragmentInstance} which is for data query,
 * which may contains several series.
 */
@NotThreadSafe
public class DataDriver extends Driver {

  private boolean init = false;

  // Unit : Byte
  private final long estimatedMemorySize;

  public DataDriver(Operator root, DriverContext driverContext, long estimatedMemorySize) {
    super(root, driverContext);
    this.estimatedMemorySize = estimatedMemorySize;
  }

  @SuppressWarnings("squid:S1181")
  @Override
  protected boolean init(SettableFuture<?> blockedFuture) {
    if (!init) {
      try {
        if (!initialize()) { // failed to init this time, but now exception thrown, possibly failed
          // to acquire lock within the specific time
          blockedFuture.set(null);
          return false;
        } else {
          return true;
        }
      } catch (Throwable t) {
        LOGGER.error(
            "Failed to do the initialization for driver {} ", driverContext.getDriverTaskID(), t);
        driverContext.failed(t);
        blockedFuture.setException(t);
        return false;
      }
    }
    return true;
  }

  /**
   * Init seq file list and unseq file list in {@link
   * org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource} and set it into each
   * SourceNode.
   *
   * @throws QueryProcessException while failed to init query resource, QueryProcessException will
   *     be thrown
   * @throws IllegalStateException if {@link
   *     org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource} is null after
   *     initialization, IllegalStateException will be thrown
   */
  private boolean initialize() throws QueryProcessException {
    try {
      List<DataSourceOperator> sourceOperators =
          ((DataDriverContext) driverContext).getSourceOperators();
      if (sourceOperators != null && !sourceOperators.isEmpty()) {
        IQueryDataSource dataSource = initQueryDataSource();
        if (dataSource == null) {
          // If this driver is being initialized, meanwhile the whole FI was aborted or cancelled
          // for some reasons, we may get null QueryDataSource here.
          // And it's safe for us to throw this exception here in such case.
          throw new IllegalStateException("QueryDataSource should never be null!");
        } else if (dataSource == UNFINISHED_QUERY_DATA_SOURCE) {
          // init query data source timeout. Maybe failed to acquire the read lock within the
          // specified time
          // do nothing, wait for next try
        } else {
          sourceOperators.forEach(
              sourceOperator -> {
                // Construct QueryDataSource for source operator
                sourceOperator.initQueryDataSource(dataSource.clone());
              });
          this.init = true;
        }
      } else {
        this.init = true;
      }
    } finally {
      if (this.init) {
        ((DataDriverContext) driverContext).clearSourceOperators();
        QUERY_EXECUTION_METRICS.recordExecutionCost(
            QUERY_RESOURCE_INIT,
            driverContext.getFragmentInstanceContext().getInitQueryDataSourceCost());
      }
    }
    return this.init;
  }

  @Override
  protected void releaseResource() {
    driverContext.getFragmentInstanceContext().decrementNumOfUnClosedDriver();
  }

  /**
   * The method is called in mergeLock() when executing query. This method will get all the {@link
   * org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource} needed for this query.
   *
   * @throws QueryProcessException while failed to init query resource, QueryProcessException will
   *     be thrown
   */
  private IQueryDataSource initQueryDataSource() throws QueryProcessException {
    return ((DataDriverContext) driverContext).getSharedQueryDataSource();
  }

  @Override
  public long getEstimatedMemorySize() {
    return estimatedMemorySize;
  }
}
