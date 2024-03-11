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

package org.apache.iotdb.db.queryengine.plan;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.QueryIdGenerator;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeSchemaCache;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigExecution;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.service.rpc.thrift.TSAggregationQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSFastLastDataQueryForOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.commons.utils.StatusUtils.needRetry;

/**
 * The coordinator for MPP. It manages all the queries which are executed in current Node. And it
 * will be responsible for the lifecycle of a query. A query request will be represented as a
 * QueryExecution.
 */
public class Coordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);
  private static final int COORDINATOR_SCHEDULED_EXECUTOR_SIZE = 10;
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final Logger SLOW_SQL_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.SLOW_SQL_LOGGER_NAME);

  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      SYNC_INTERNAL_SERVICE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  private static final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());

  private final ExecutorService executor;
  private final ExecutorService writeOperationExecutor;
  private final ScheduledExecutorService scheduledExecutor;

  private final QueryIdGenerator queryIdGenerator =
      new QueryIdGenerator(IoTDBDescriptor.getInstance().getConfig().getDataNodeId());

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
      ISchemaFetcher schemaFetcher,
      long timeOut,
      long startTime) {
    queryContext.setTimeOut(timeOut);
    queryContext.setStartTime(startTime);
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
        SYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
        ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER);
  }

  public ExecutionResult execute(
      Statement statement,
      long queryId,
      SessionInfo session,
      String sql,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher,
      long timeOut) {
    long startTime = System.currentTimeMillis();
    QueryId globalQueryId = queryIdGenerator.createNextQueryId();
    MPPQueryContext queryContext = null;
    try (SetThreadName queryName = new SetThreadName(globalQueryId.getId())) {
      if (sql != null && !sql.isEmpty()) {
        LOGGER.debug("[QueryStart] sql: {}", sql);
      }
      queryContext =
          new MPPQueryContext(
              sql,
              globalQueryId,
              queryId,
              session,
              DataNodeEndPoints.LOCAL_HOST_DATA_BLOCK_ENDPOINT,
              DataNodeEndPoints.LOCAL_HOST_INTERNAL_ENDPOINT);
      IQueryExecution execution =
          createQueryExecution(
              statement,
              queryContext,
              partitionFetcher,
              schemaFetcher,
              timeOut > 0 ? timeOut : CONFIG.getQueryTimeoutThreshold(),
              startTime);
      if (execution.isQuery()) {
        queryExecutionMap.put(queryId, execution);
      } else {
        // we won't limit write operation's execution time
        queryContext.setTimeOut(Long.MAX_VALUE);
      }
      execution.start();
      ExecutionResult result = execution.getStatus();
      if (!execution.isQuery() && result.status != null && needRetry(result.status)) {
        // if it's write request and the result status needs to retry
        result.status.setNeedRetry(true);
      }
      return result;
    } finally {
      int lockNums = queryContext.getAcquiredLockNum();
      if (queryContext != null && lockNums > 0) {
        for (int i = 0; i < lockNums; i++) DataNodeSchemaCache.getInstance().releaseInsertLock();
      }
    }
  }

  /** This method is called by the write method. So it does not set the timeout parameter. */
  public ExecutionResult execute(
      Statement statement,
      long queryId,
      SessionInfo session,
      String sql,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    return execute(
        statement, queryId, session, sql, partitionFetcher, schemaFetcher, Long.MAX_VALUE);
  }

  public IQueryExecution getQueryExecution(Long queryId) {
    return queryExecutionMap.get(queryId);
  }

  public List<IQueryExecution> getAllQueryExecutions() {
    return new ArrayList<>(queryExecutionMap.values());
  }

  public int getQueryExecutionMapSize() {
    return queryExecutionMap.size();
  }

  // TODO: (xingtanzjr) need to redo once we have a concrete policy for the threadPool management
  private ExecutorService getQueryExecutor() {
    int coordinatorReadExecutorSize =
        CONFIG.isClusterMode() ? CONFIG.getCoordinatorReadExecutorSize() : 1;
    return IoTDBThreadPoolFactory.newFixedThreadPool(
        coordinatorReadExecutorSize, ThreadName.MPP_COORDINATOR_EXECUTOR_POOL.getName());
  }

  private ExecutorService getWriteExecutor() {
    int coordinatorWriteExecutorSize = CONFIG.getCoordinatorWriteExecutorSize();
    return IoTDBThreadPoolFactory.newFixedThreadPool(
        coordinatorWriteExecutorSize, ThreadName.MPP_COORDINATOR_WRITE_EXECUTOR.getName());
  }

  // TODO: (xingtanzjr) need to redo once we have a concrete policy for the threadPool management
  private ScheduledExecutorService getScheduledExecutor() {
    return IoTDBThreadPoolFactory.newScheduledThreadPool(
        COORDINATOR_SCHEDULED_EXECUTOR_SIZE,
        ThreadName.MPP_COORDINATOR_SCHEDULED_EXECUTOR.getName());
  }

  public QueryId createQueryId() {
    return queryIdGenerator.createNextQueryId();
  }

  public void cleanupQueryExecution(
      Long queryId, org.apache.thrift.TBase nativeApiRequest, Throwable t) {
    IQueryExecution queryExecution = getQueryExecution(queryId);
    if (queryExecution != null) {
      try (SetThreadName threadName = new SetThreadName(queryExecution.getQueryId())) {
        LOGGER.debug("[CleanUpQuery]]");
        queryExecution.stopAndCleanup(t);
        queryExecutionMap.remove(queryId);
        if (queryExecution.isQuery()) {
          long costTime = queryExecution.getTotalExecutionTime();
          outputSlowSql(queryExecution, costTime, nativeApiRequest);
        }
      }
    }
  }

  private void outputSlowSql(
      IQueryExecution queryExecution, long costTime, org.apache.thrift.TBase request) {
    if (costTime / 1_000_000 < CONFIG.getSlowQueryThreshold()) {
      return;
    }

    String slowContent = "";
    if (request == null || !queryExecution.getExecuteSQL().orElse("").isEmpty()) {
      slowContent = queryExecution.getExecuteSQL().orElse("UNKNOWN");
    } else if (request instanceof TSRawDataQueryReq) {
      TSRawDataQueryReq req = (TSRawDataQueryReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(req.getPathsSize(), 10); i++) {
        sb.append(i == 0 ? "" : ",").append(req.getPaths().get(i));
      }
      slowContent =
          String.format(
              "Request name: TSRawDataQueryReq, paths size: %s, starTime: %s, "
                  + "endTime: %s, some paths: %s",
              req.getPathsSize(), req.getStartTime(), req.getEndTime(), sb);
    } else if (request instanceof TSLastDataQueryReq) {
      TSLastDataQueryReq req = (TSLastDataQueryReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(req.getPathsSize(), 10); i++) {
        sb.append(i == 0 ? "" : ",").append(req.getPaths().get(i));
      }
      slowContent =
          String.format(
              "Request name: TSLastDataQueryReq, paths size: %s, some paths: %s",
              req.getPathsSize(), sb);
    } else if (request instanceof TSAggregationQueryReq) {
      TSAggregationQueryReq req = (TSAggregationQueryReq) request;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < Math.min(req.getPathsSize(), 10); i++) {
        sb.append(i == 0 ? "" : ",").append(req.getPaths().get(i));
      }
      slowContent =
          String.format(
              "Request name: TSAggregationQueryReq, paths size: %s, some paths: %s",
              req.getPathsSize(), sb);
    } else if (request instanceof TSFastLastDataQueryForOneDeviceReq) {
      TSFastLastDataQueryForOneDeviceReq req = (TSFastLastDataQueryForOneDeviceReq) request;
      slowContent =
          String.format(
              "Request name: TSFastLastDataQueryForOneDeviceReq, db: %s, deviceId: %s, sensorSize: %s, sensors: %s",
              req.getDb(), req.getDeviceId(), req.getSensorsSize(), req.getSensors());
    } else if (request instanceof TSFetchResultsReq) {
      TSFetchResultsReq req = (TSFetchResultsReq) request;
      slowContent =
          String.format(
              "Request name: TSFetchResultsReq, statement: %s, fetchSize: %s",
              req.getStatement(), req.getFetchSize());
    }

    SLOW_SQL_LOGGER.info("Cost: {} ms, {}", costTime / 1_000_000, slowContent);
  }

  public void cleanupQueryExecution(Long queryId) {
    cleanupQueryExecution(queryId, null, null);
  }

  public IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      getInternalServiceClientManager() {
    return SYNC_INTERNAL_SERVICE_CLIENT_MANAGER;
  }

  public static Coordinator getInstance() {
    return INSTANCE;
  }

  public void recordExecutionTime(long queryId, long executionTime) {
    IQueryExecution queryExecution = getQueryExecution(queryId);
    if (queryExecution != null) {
      queryExecution.recordExecutionTime(executionTime);
    }
  }

  public long getTotalExecutionTime(long queryId) {
    IQueryExecution queryExecution = getQueryExecution(queryId);
    if (queryExecution != null) {
      return queryExecution.getTotalExecutionTime();
    }
    return -1L;
  }
}
