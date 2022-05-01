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
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.execution.IQueryExecution;
import org.apache.iotdb.db.mpp.plan.execution.QueryExecution;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigExecution;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

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
  private static final int COORDINATOR_EXECUTOR_SIZE = 10;
  private static final String COORDINATOR_SCHEDULED_EXECUTOR_NAME = "MPPCoordinatorScheduled";
  private static final int COORDINATOR_SCHEDULED_EXECUTOR_SIZE = 1;

  private static final TEndPoint LOCAL_HOST_DATA_BLOCK_ENDPOINT =
      new TEndPoint(
          IoTDBDescriptor.getInstance().getConfig().getInternalIp(),
          IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort());

  private static final TEndPoint LOCAL_HOST_INTERNAL_ENDPOINT =
      new TEndPoint(
          IoTDBDescriptor.getInstance().getConfig().getInternalIp(),
          IoTDBDescriptor.getInstance().getConfig().getInternalPort());

  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      INTERNAL_SERVICE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new DataNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  private final ExecutorService executor;
  private final ScheduledExecutorService scheduledExecutor;

  private static final Coordinator INSTANCE = new Coordinator();

  private final ConcurrentHashMap<QueryId, IQueryExecution> queryExecutionMap;

  private Coordinator() {
    this.queryExecutionMap = new ConcurrentHashMap<>();
    this.executor = getQueryExecutor();
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
        scheduledExecutor,
        partitionFetcher,
        schemaFetcher,
        INTERNAL_SERVICE_CLIENT_MANAGER);
  }

  public ExecutionResult execute(
      Statement statement,
      QueryId queryId,
      SessionInfo session,
      String sql,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {

    IQueryExecution execution =
        createQueryExecution(
            statement,
            new MPPQueryContext(
                sql,
                queryId,
                session,
                LOCAL_HOST_DATA_BLOCK_ENDPOINT,
                LOCAL_HOST_INTERNAL_ENDPOINT),
            partitionFetcher,
            schemaFetcher);
    queryExecutionMap.put(queryId, execution);
    execution.start();

    return execution.getStatus();
  }

  public IQueryExecution getQueryExecution(QueryId queryId) {
    return queryExecutionMap.get(queryId);
  }

  // TODO: (xingtanzjr) need to redo once we have a concrete policy for the threadPool management
  private ExecutorService getQueryExecutor() {
    return IoTDBThreadPoolFactory.newFixedThreadPool(
        COORDINATOR_EXECUTOR_SIZE, COORDINATOR_EXECUTOR_NAME);
  }
  // TODO: (xingtanzjr) need to redo once we have a concrete policy for the threadPool management
  private ScheduledExecutorService getScheduledExecutor() {
    return IoTDBThreadPoolFactory.newScheduledThreadPool(
        COORDINATOR_SCHEDULED_EXECUTOR_SIZE, COORDINATOR_SCHEDULED_EXECUTOR_NAME);
  }

  public static Coordinator getInstance() {
    return INSTANCE;
  }
}
