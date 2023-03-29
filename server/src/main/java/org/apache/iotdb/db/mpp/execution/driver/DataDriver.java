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

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.source.DataSourceOperator;

import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

import static org.apache.iotdb.db.mpp.metric.QueryExecutionMetricSet.QUERY_RESOURCE_INIT;

/**
 * One dataDriver is responsible for one FragmentInstance which is for data query, which may
 * contains several series.
 */
@NotThreadSafe
public class DataDriver extends Driver {

  private boolean init;

  public DataDriver(Operator root, DriverContext driverContext) {
    super(root, driverContext);
  }

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
        if (dataSource == null) {
          // if this driver is being initialized, meanwhile the whole FI was aborted or cancelled
          // for some reasons, we may get null QueryDataSource here.
          // And it's safe for us to throw this exception here in such case.
          throw new IllegalStateException("QueryDataSource should never be null!");
        }
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
      QUERY_METRICS.recordExecutionCost(QUERY_RESOURCE_INIT, System.nanoTime() - startTime);
    }
  }

  @Override
  protected void releaseResource() {
    // do nothing
  }

  /**
   * The method is called in mergeLock() when executing query. This method will get all the
   * QueryDataSource needed for this query
   */
  private QueryDataSource initQueryDataSource() throws QueryProcessException {
    return ((DataDriverContext) driverContext).getSharedQueryDataSource();
  }
}
