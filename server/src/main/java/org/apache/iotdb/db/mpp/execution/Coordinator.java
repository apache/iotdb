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

import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.common.SessionInfo;
import org.apache.iotdb.db.mpp.execution.config.ConfigExecution;
import org.apache.iotdb.db.mpp.sql.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.sql.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.metadata.SetStorageGroupStatement;

import org.apache.commons.lang3.Validate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The coordinator for MPP. It manages all the queries which are executed in current Node. And it
 * will be responsible for the lifecycle of a query. A query request will be represented as a
 * QueryExecution.
 */
public class Coordinator {
  private static final String COORDINATOR_EXECUTOR_NAME = "MPPCoordinator";
  private static final int COORDINATOR_EXECUTOR_SIZE = 2;
  private static final String COORDINATOR_SCHEDULED_EXECUTOR_NAME = "MPPCoordinatorScheduled";
  private static final int COORDINATOR_SCHEDULED_EXECUTOR_SIZE = 2;

  private static final Endpoint LOCAL_HOST =
      new Endpoint(
          IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
          IoTDBDescriptor.getInstance().getConfig().getInternalPort());

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
    if (statement instanceof SetStorageGroupStatement) {
      queryContext.setQueryType(QueryType.WRITE);
      return new ConfigExecution(queryContext, statement, executor);
    }
    return new QueryExecution(
        statement, queryContext, executor, scheduledExecutor, partitionFetcher, schemaFetcher);
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
            new MPPQueryContext(sql, queryId, session, getHostEndpoint()),
            partitionFetcher,
            schemaFetcher);
    queryExecutionMap.put(queryId, execution);

    execution.start();

    return execution.getStatus();
  }

  public IQueryExecution getQueryExecution(QueryId queryId) {
    IQueryExecution execution = queryExecutionMap.get(queryId);
    Validate.notNull(execution, "invalid queryId %s", queryId.getId());
    return execution;
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

  // Get the hostname of current coordinator
  private Endpoint getHostEndpoint() {
    return LOCAL_HOST;
  }

  public static Coordinator getInstance() {
    return INSTANCE;
  }
  //    private TQueryResponse executeQuery(TQueryRequest request) {
  //
  //    }
}
