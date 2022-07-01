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
package org.apache.iotdb.db.mpp.plan;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.execution.QueryIdGenerator;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.constant.DataNodeEndPoints;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.execution.QueryExecution;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigExecution;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

import io.airlift.concurrent.SetThreadName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The coordinator for MPP. It manages all the queries which are executed in current Node. And it
 * will be responsible for the lifecycle of a query. A query request will be represented as a
 * QueryExecution.
 */
public class Coordinator {
  private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);

  private static final String COORDINATOR_EXECUTOR_NAME = "MPPCoordinator";
  private static final String COORDINATOR_WRITE_EXECUTOR_NAME = "MPPCoordinatorWrite";
  private static final String COORDINATOR_SCHEDULED_EXECUTOR_NAME = "MPPCoordinatorScheduled";
  private static final int COORDINATOR_SCHEDULED_EXECUTOR_SIZE = 10;

  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      INTERNAL_SERVICE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new DataNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  private final ExecutorService executor;
  private final ExecutorService writeOperationExecutor;
  private final ScheduledExecutorService scheduledExecutor;

  private final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

  private static final Coordinator INSTANCE = new Coordinator();

  private final ConcurrentHashMap<Long, IQueryExecution> queryExecutionMap;

  private Coordinator() {
    this.queryExecutionMap = new ConcurrentHashMap<>();
    this.executor = getQueryExecutor();
    this.writeOperationExecutor = getWriteExecutor();
    this.scheduledExecutor = getScheduledExecutor();
  }

  private IQueryExecution createQueryExecution(
      Statement statement,
      MPPQueryContext queryContext,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    if (statement instanceof IConfigStatement) {
      queryContext.setQueryType(((IConfigStatement) statement).getQueryType());
      return new ConfigExecution(queryContext, statement, executor);
    }
    return new QueryExecution(
        statement,
        queryContext,
        executor,
        writeOperationExecutor,
        scheduledExecutor,
        partitionFetcher,
        schemaFetcher,
        INTERNAL_SERVICE_CLIENT_MANAGER);
  }

  public ExecutionResult execute(
      Statement statement,
      long queryId,
      SessionInfo session,
      String sql,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    QueryId globalQueryId = queryIdGenerator.createNextQueryId();
    try (SetThreadName queryName = new SetThreadName(globalQueryId.getId())) {
      if (sql != null && sql.length() > 0) {
        LOGGER.info("start executing sql: {}", sql);
      }
      IQueryExecution execution =
          createQueryExecution(
              statement,
              new MPPQueryContext(
                  sql,
                  globalQueryId,
                  session,
                  DataNodeEndPoints.LOCAL_HOST_DATA_BLOCK_ENDPOINT,
                  DataNodeEndPoints.LOCAL_HOST_INTERNAL_ENDPOINT),
              partitionFetcher,
              schemaFetcher);
      if (execution.isQuery()) {
        queryExecutionMap.put(queryId, execution);
      }
      execution.start();

      return execution.getStatus();
    }
  }

  public IQueryExecution getQueryExecution(Long queryId) {
    return queryExecutionMap.get(queryId);
  }

  public void removeQueryExecution(Long queryId) {
    queryExecutionMap.remove(queryId);
  }

  // TODO: (xingtanzjr) need to redo once we have a concrete policy for the threadPool management
  private ExecutorService getQueryExecutor() {
    int coordinatorReadExecutorSize =
        IoTDBDescriptor.getInstance().getConfig().getCoordinatorReadExecutorSize();
    return IoTDBThreadPoolFactory.newFixedThreadPool(
        coordinatorReadExecutorSize, COORDINATOR_EXECUTOR_NAME);
  }

  private ExecutorService getWriteExecutor() {
    int coordinatorWriteExecutorSize =
        IoTDBDescriptor.getInstance().getConfig().getCoordinatorWriteExecutorSize();
    return IoTDBThreadPoolFactory.newFixedThreadPool(
        coordinatorWriteExecutorSize, COORDINATOR_WRITE_EXECUTOR_NAME);
  }

  // TODO: (xingtanzjr) need to redo once we have a concrete policy for the threadPool management
  private ScheduledExecutorService getScheduledExecutor() {
    return IoTDBThreadPoolFactory.newScheduledThreadPool(
        COORDINATOR_SCHEDULED_EXECUTOR_SIZE, COORDINATOR_SCHEDULED_EXECUTOR_NAME);
  }

  public QueryId createQueryId() {
    return queryIdGenerator.createNextQueryId();
  }

  public static Coordinator getInstance() {
    return INSTANCE;
  }
}
