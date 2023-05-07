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

package org.apache.iotdb.db.mpp.plan.execution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.db.mpp.metric.QueryPlanCostMetricSet.DISTRIBUTION_PLANNER;

public class FastAggregationQueryExecution extends QueryExecution {
  private static final Logger logger = LoggerFactory.getLogger(FastAggregationQueryExecution.class);

  public FastAggregationQueryExecution(
      Statement statement,
      MPPQueryContext context,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      ScheduledExecutorService scheduledExecutor,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    super(
        statement,
        context,
        executor,
        writeOperationExecutor,
        scheduledExecutor,
        partitionFetcher,
        schemaFetcher,
        syncInternalServiceClientManager,
        asyncInternalServiceClientManager);
  }

  @Override
  public void doLogicalPlan() {
    // do nothing
  }

  @Override
  public void doDistributedPlan() {
    long startTime = System.nanoTime();

    // TODO this.distributedPlan = planner.planFragments();

    if (rawStatement.isQuery()) {
      QUERY_METRICS.recordPlanCost(DISTRIBUTION_PLANNER, System.nanoTime() - startTime);
    }
    if (isQuery() && logger.isDebugEnabled()) {
      logger.debug(
          "distribution plan done. Fragment instance count is {}, details is: \n {}",
          distributedPlan.getInstances().size(),
          printFragmentInstances(distributedPlan.getInstances()));
    }
    // check timeout after building distribution plan because it could be time-consuming in some
    // cases.
    checkTimeOutForQuery();
  }
}
