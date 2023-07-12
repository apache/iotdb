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
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.QUERY_RESOURCE_INIT;

/**
 * One {@link DataDriver} is responsible for one {@link FragmentInstance} which is for data query,
 * which may contains several series.
 */
@NotThreadSafe
public class DataDriver extends Driver {

  private boolean init;

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
        initialize();
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
   * Init seq file list and unseq file list in {@link QueryDataSource} and set it into each
   * SourceNode.
   *
   * @throws QueryProcessException while failed to init query resource, QueryProcessException will
   *     be thrown
   * @throws IllegalStateException if {@link QueryDataSource} is null after initialization,
   *     IllegalStateException will be thrown
   */
  private void initialize() throws QueryProcessException {
    long startTime = System.nanoTime();
    try {
      List<DataSourceOperator> sourceOperators =
          ((DataDriverContext) driverContext).getSourceOperators();
      if (sourceOperators != null && !sourceOperators.isEmpty()) {
        QueryDataSource dataSource = initQueryDataSource();
        if (dataSource == null) {
          // If this driver is being initialized, meanwhile the whole FI was aborted or cancelled
          // for some reasons, we may get null QueryDataSource here.
          // And it's safe for us to throw this exception here in such case.
          throw new IllegalStateException("QueryDataSource should never be null!");
        }
        sourceOperators.forEach(
            sourceOperator -> {
              // Construct QueryDataSource for source operator
              QueryDataSource queryDataSource =
                  new QueryDataSource(dataSource.getSeqResources(), dataSource.getUnseqResources());

              queryDataSource.setDataTTL(dataSource.getDataTTL());

              sourceOperator.initQueryDataSource(queryDataSource);
            });
      }

      this.init = true;
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(
          QUERY_RESOURCE_INIT, System.nanoTime() - startTime);
    }
  }

  @Override
  protected void releaseResource() {
    driverContext.getFragmentInstanceContext().decrementNumOfUnClosedDriver();
  }

  /**
   * The method is called in mergeLock() when executing query. This method will get all the {@link
   * QueryDataSource} needed for this query.
   *
   * @throws QueryProcessException while failed to init query resource, QueryProcessException will
   *     be thrown
   */
  private QueryDataSource initQueryDataSource() throws QueryProcessException {
    return ((DataDriverContext) driverContext).getSharedQueryDataSource();
  }

  @Override
  public long getEstimatedMemorySize() {
    return estimatedMemorySize;
  }
}
