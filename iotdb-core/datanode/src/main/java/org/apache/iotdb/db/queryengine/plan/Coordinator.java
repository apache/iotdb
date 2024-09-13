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
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.QueryIdGenerator;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.execution.config.TreeConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.TreeModelPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Flush;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDataNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.utils.SetThreadName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import static org.apache.iotdb.commons.utils.StatusUtils.needRetry;
import static org.apache.iotdb.db.utils.CommonUtils.getContentOfRequest;

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

  private ExecutionResult execution(
      long queryId,
      SessionInfo session,
      String sql,
      BiFunction<MPPQueryContext, Long, IQueryExecution> iQueryExecutionFactory) {
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
      IQueryExecution execution = iQueryExecutionFactory.apply(queryContext, startTime);
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
      if (queryContext != null) {
        queryContext.releaseAllMemoryReservedForFrontEnd();
      }
      DataNodeSchemaLockManager.getInstance().releaseReadLock(queryContext);
    }
  }

  /** This method is called by the write method. So it does not set the timeout parameter. */
  public ExecutionResult executeForTreeModel(
      Statement statement,
      long queryId,
      SessionInfo session,
      String sql,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    return executeForTreeModel(
        statement, queryId, session, sql, partitionFetcher, schemaFetcher, Long.MAX_VALUE);
  }

  public ExecutionResult executeForTreeModel(
      Statement statement,
      long queryId,
      SessionInfo session,
      String sql,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher,
      long timeOut) {
    return execution(
        queryId,
        session,
        sql,
        ((queryContext, startTime) ->
            createQueryExecutionForTreeModel(
                statement,
                queryContext,
                partitionFetcher,
                schemaFetcher,
                timeOut > 0 ? timeOut : CONFIG.getQueryTimeoutThreshold(),
                startTime)));
  }

  private IQueryExecution createQueryExecutionForTreeModel(
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
      return new ConfigExecution(
          queryContext,
          statement.getType(),
          executor,
          statement.accept(new TreeConfigTaskVisitor(), queryContext));
    }
    TreeModelPlanner treeModelPlanner =
        new TreeModelPlanner(
            statement,
            executor,
            writeOperationExecutor,
            scheduledExecutor,
            partitionFetcher,
            schemaFetcher,
            SYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
            ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER);
    return new QueryExecution(treeModelPlanner, queryContext, executor);
  }

  public ExecutionResult executeForTableModel(
      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement statement,
      SqlParser sqlParser,
      IClientSession clientSession,
      long queryId,
      SessionInfo session,
      String sql,
      Metadata metadata,
      long timeOut) {
    return execution(
        queryId,
        session,
        sql,
        ((queryContext, startTime) ->
            createQueryExecutionForTableModel(
                statement,
                sqlParser,
                clientSession,
                queryContext,
                metadata,
                timeOut > 0 ? timeOut : CONFIG.getQueryTimeoutThreshold(),
                startTime)));
  }

  public ExecutionResult executeForTableModel(
      Statement statement,
      SqlParser sqlParser,
      IClientSession clientSession,
      long queryId,
      SessionInfo session,
      String sql,
      Metadata metadata,
      long timeOut) {
    return execution(
        queryId,
        session,
        sql,
        ((queryContext, startTime) ->
            createQueryExecutionForTableModel(
                statement,
                sqlParser,
                clientSession,
                queryContext,
                metadata,
                timeOut > 0 ? timeOut : CONFIG.getQueryTimeoutThreshold(),
                startTime)));
  }

  private IQueryExecution createQueryExecutionForTableModel(
      Statement statement,
      SqlParser sqlParser,
      IClientSession clientSession,
      MPPQueryContext queryContext,
      Metadata metadata,
      long timeOut,
      long startTime) {
    queryContext.setTableQuery(true);
    queryContext.setTimeOut(timeOut);
    queryContext.setStartTime(startTime);
    TableModelPlanner tableModelPlanner =
        new TableModelPlanner(
            statement.toRelationalStatement(queryContext),
            sqlParser,
            metadata,
            executor,
            writeOperationExecutor,
            scheduledExecutor,
            SYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
            ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER);
    return new QueryExecution(tableModelPlanner, queryContext, executor);
  }

  private IQueryExecution createQueryExecutionForTableModel(
      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement statement,
      SqlParser sqlParser,
      IClientSession clientSession,
      MPPQueryContext queryContext,
      Metadata metadata,
      long timeOut,
      long startTime) {
    queryContext.setTableQuery(true);
    queryContext.setTimeOut(timeOut);
    queryContext.setStartTime(startTime);
    if (statement instanceof DropDB
        || statement instanceof ShowDB
        || statement instanceof CreateDB
        || statement instanceof Use
        || statement instanceof CreateTable
        || statement instanceof DescribeTable
        || statement instanceof ShowTables
        || statement instanceof AddColumn
        || statement instanceof SetProperties
        || statement instanceof DropTable
        || statement instanceof ShowCluster
        || statement instanceof ShowRegions
        || statement instanceof ShowDataNodes
        || statement instanceof ShowConfigNodes
        || statement instanceof Flush
        || statement instanceof SetConfiguration) {
      return new ConfigExecution(
          queryContext,
          null,
          executor,
          statement.accept(new TableConfigTaskVisitor(clientSession, metadata), queryContext));
    }
    if (statement instanceof WrappedInsertStatement) {
      ((WrappedInsertStatement) statement).setContext(queryContext);
    }
    TableModelPlanner tableModelPlanner =
        new TableModelPlanner(
            statement,
            sqlParser,
            metadata,
            executor,
            writeOperationExecutor,
            scheduledExecutor,
            SYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
            ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER);
    return new QueryExecution(tableModelPlanner, queryContext, executor);
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
    int coordinatorReadExecutorSize = CONFIG.getCoordinatorReadExecutorSize();
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
      Long queryId, org.apache.thrift.TBase<?, ?> nativeApiRequest, Throwable t) {
    IQueryExecution queryExecution = getQueryExecution(queryId);
    if (queryExecution != null) {
      try (SetThreadName threadName = new SetThreadName(queryExecution.getQueryId())) {
        LOGGER.debug("[CleanUpQuery]]");
        queryExecution.stopAndCleanup(t);
        queryExecutionMap.remove(queryId);
        if (queryExecution.isQuery()) {
          long costTime = queryExecution.getTotalExecutionTime();
          if (costTime / 1_000_000 >= CONFIG.getSlowQueryThreshold()) {
            SLOW_SQL_LOGGER.info(
                "Cost: {} ms, {}",
                costTime / 1_000_000,
                getContentOfRequest(nativeApiRequest, queryExecution));
          }
        }
      }
    }
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
