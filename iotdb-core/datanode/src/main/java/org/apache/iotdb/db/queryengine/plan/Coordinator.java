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
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.PreparedStatementInfo;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext.ExplainType;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.execution.QueryIdGenerator;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.lock.DataNodeSchemaLockManager;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigExecution;
import org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.execution.config.TreeConfigTaskVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.TreeModelPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.DataNodeLocationSupplierFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.DistributedOptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.LogicalOptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ParameterExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterColumnDataType;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AlterDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ClearCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateExternalService;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateModel;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateTraining;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Deallocate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropExternalService;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropFunction;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropModel;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Execute;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExecuteImmediate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExtendRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Flush;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.KillQuery;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadModel;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.MigrateRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Prepare;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ReconstructRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RelationalAuthorStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveAINode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveConfigNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveRegion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetColumnComment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetSystemStatus;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetTableComment;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAIDevices;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAINodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowAvailableUrls;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowClusterId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowConfiguration;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentSqlDialect;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentTimestamp;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDataNodes;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowFunctions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowLoadedModels;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowModels;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVariables;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowVersion;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartExternalService;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StartRepairData;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopExternalService;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StopRepairData;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SubscriptionStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.UnloadModel;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedInsertStatement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewrite;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewriteFactory;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.utils.SetThreadName;

import org.apache.thrift.TBase;
import org.apache.tsfile.utils.Accountable;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.utils.StatusUtils.needRetry;
import static org.apache.iotdb.db.queryengine.plan.Coordinator.QueryInfo.DEFAULT_END_TIME;
import static org.apache.iotdb.db.utils.CommonUtils.getContentOfRequest;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfCharArray;

/**
 * The coordinator for MPP. It manages all the queries which are executed in current Node. And it
 * will be responsible for the lifecycle of a query. A query request will be represented as a
 * QueryExecution.
 */
public class Coordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);
  private static final int COORDINATOR_SCHEDULED_EXECUTOR_SIZE = 10;
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private static final Logger SLOW_SQL_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.SLOW_SQL_LOGGER_NAME);

  private static final Logger SAMPLED_QUERIES_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.SAMPLED_QUERIES_LOGGER_NAME);

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
  private final ExecutorService dispatchExecutor;

  private final QueryIdGenerator queryIdGenerator =
      new QueryIdGenerator(IoTDBDescriptor.getInstance().getConfig().getDataNodeId());

  private static final Coordinator INSTANCE = new Coordinator();

  private static final IMemoryBlock coordinatorMemoryBlock;

  private final ConcurrentHashMap<Long, IQueryExecution> queryExecutionMap;

  private final BlockingDeque<QueryInfo> currentQueriesInfo = new LinkedBlockingDeque<>();
  private final AtomicInteger[] currentQueriesCostHistogram = new AtomicInteger[61];
  private final ScheduledExecutorService retryFailTasksExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.EXPIRED_QUERIES_INFO_CLEAR.getName());

  private final StatementRewrite statementRewrite;
  private final List<PlanOptimizer> logicalPlanOptimizers;
  private final List<PlanOptimizer> distributionPlanOptimizers;
  private final DataNodeLocationSupplierFactory.DataNodeLocationSupplier dataNodeLocationSupplier;
  private final TypeManager typeManager;

  {
    for (int i = 0; i < 61; i++) {
      currentQueriesCostHistogram[i] = new AtomicInteger();
    }

    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        retryFailTasksExecutor,
        this::clearExpiredQueriesInfoTask,
        1_000L,
        1_000L,
        TimeUnit.MILLISECONDS);
    LOGGER.info("Expired-Queries-Info-Clear thread is successfully started.");
  }

  static {
    coordinatorMemoryBlock =
        IoTDBDescriptor.getInstance()
            .getMemoryConfig()
            .getCoordinatorMemoryManager()
            .exactAllocate("Coordinator", MemoryBlockType.DYNAMIC);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Initialized shared MemoryBlock 'Coordinator' with all available memory: {} bytes",
          coordinatorMemoryBlock.getTotalMemorySizeInBytes());
    }
  }

  private Coordinator() {
    this.queryExecutionMap = new ConcurrentHashMap<>();
    this.typeManager = new InternalTypeManager();
    this.executor = getQueryExecutor();
    this.writeOperationExecutor = getWriteExecutor();
    this.scheduledExecutor = getScheduledExecutor();
    int dispatchThreadNum = Math.max(20, Runtime.getRuntime().availableProcessors() * 2);
    this.dispatchExecutor =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            ThreadName.FRAGMENT_INSTANCE_DISPATCH.getName(),
            dispatchThreadNum,
            dispatchThreadNum,
            new ThreadPoolExecutor.CallerRunsPolicy());
    this.statementRewrite = new StatementRewriteFactory().getStatementRewrite();
    this.logicalPlanOptimizers =
        new LogicalOptimizeFactory(
                new PlannerContext(LocalExecutionPlanner.getInstance().metadata, typeManager))
            .getPlanOptimizers();
    this.distributionPlanOptimizers =
        new DistributedOptimizeFactory(
                new PlannerContext(LocalExecutionPlanner.getInstance().metadata, typeManager))
            .getPlanOptimizers();
    this.dataNodeLocationSupplier = DataNodeLocationSupplierFactory.getSupplier();
  }

  private ExecutionResult execution(
      long queryId,
      SessionInfo session,
      String sql,
      boolean userQuery,
      BiFunction<MPPQueryContext, Long, IQueryExecution> iQueryExecutionFactory) {
    long startTime = System.currentTimeMillis();
    QueryId globalQueryId = queryIdGenerator.createNextQueryId();
    MPPQueryContext queryContext = null;
    try (SetThreadName queryName = new SetThreadName(globalQueryId.getId())) {
      if (LOGGER.isDebugEnabled() && sql != null && !sql.isEmpty()) {
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
      queryContext.setUserQuery(userQuery);
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
        statement, queryId, session, sql, partitionFetcher, schemaFetcher, Long.MAX_VALUE, false);
  }

  public ExecutionResult executeForTreeModel(
      Statement statement,
      long queryId,
      SessionInfo session,
      String sql,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher,
      long timeOut,
      boolean userQuery) {
    return execution(
        queryId,
        session,
        sql,
        userQuery,
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

  /**
   * This method is specifically used following subquery:
   *
   * <p>1. When uncorrelated scalar subquery is handled
   * (fetchUncorrelatedSubqueryResultForPredicate), we try to fold it and get constant value. Since
   * CTE might be referenced, we need to add CTE materialization result into subquery's
   * MPPQueryContext.
   *
   * <p>2. When CTE subquery is handled (fetchCteQueryResult), the main query, however, might be
   * 'Explain' or 'Explain Analyze' statement. So we need to keep explain/explain analyze results
   * along with CTE query dataset.
   */
  public ExecutionResult executeForTableModel(
      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement statement,
      SqlParser sqlParser,
      IClientSession clientSession,
      long queryId,
      SessionInfo session,
      String sql,
      Metadata metadata,
      Map<NodeRef<Table>, Query> cteQueries,
      ExplainType explainType,
      long timeOut,
      boolean userQuery) {
    return execution(
        queryId,
        session,
        sql,
        userQuery,
        ((queryContext, startTime) -> {
          queryContext.setInnerTriggeredQuery(true);
          queryContext.setCteQueries(cteQueries);
          queryContext.setExplainType(explainType);
          return createQueryExecutionForTableModel(
              statement,
              sqlParser,
              clientSession,
              queryContext,
              metadata,
              timeOut > 0 ? timeOut : CONFIG.getQueryTimeoutThreshold(),
              startTime);
        }));
  }

  public ExecutionResult executeForTableModel(
      org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement statement,
      SqlParser sqlParser,
      IClientSession clientSession,
      long queryId,
      SessionInfo session,
      String sql,
      Metadata metadata,
      long timeOut,
      boolean userQuery) {
    return execution(
        queryId,
        session,
        sql,
        userQuery,
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
        false,
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
    queryContext.setTimeOut(timeOut);
    queryContext.setStartTime(startTime);
    TableModelPlanner tableModelPlanner =
        new TableModelPlanner(
            statement.toRelationalStatement(queryContext),
            sqlParser,
            metadata,
            scheduledExecutor,
            SYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
            ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
            statementRewrite,
            logicalPlanOptimizers,
            distributionPlanOptimizers,
            AuthorityChecker.getAccessControl(),
            dataNodeLocationSupplier,
            Collections.emptyList(),
            Collections.emptyMap(),
            typeManager);
    return new QueryExecution(tableModelPlanner, queryContext, executor);
  }

  private IQueryExecution createQueryExecutionForTableModel(
      final org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement statement,
      final SqlParser sqlParser,
      final IClientSession clientSession,
      final MPPQueryContext queryContext,
      final Metadata metadata,
      final long timeOut,
      final long startTime) {
    queryContext.setTimeOut(timeOut);
    queryContext.setStartTime(startTime);
    if (statement instanceof DropDB
        || statement instanceof ShowDB
        || statement instanceof CreateDB
        || statement instanceof AlterDB
        || statement instanceof Use
        || statement instanceof CreateTable
        || statement instanceof DescribeTable
        || statement instanceof ShowTables
        || statement instanceof AddColumn
        || statement instanceof AlterColumnDataType
        || statement instanceof SetProperties
        || statement instanceof DropColumn
        || statement instanceof DropTable
        || statement instanceof SetTableComment
        || statement instanceof SetColumnComment
        || statement instanceof DeleteDevice
        || statement instanceof RenameColumn
        || statement instanceof RenameTable
        || statement instanceof ShowCluster
        || statement instanceof ShowRegions
        || statement instanceof ShowDataNodes
        || statement instanceof ShowAvailableUrls
        || statement instanceof ShowConfigNodes
        || statement instanceof ShowAINodes
        || statement instanceof Flush
        || statement instanceof ClearCache
        || statement instanceof SetConfiguration
        || statement instanceof ShowConfiguration
        || statement instanceof LoadConfiguration
        || statement instanceof SetSystemStatus
        || statement instanceof StartRepairData
        || statement instanceof StopRepairData
        || statement instanceof PipeStatement
        || statement instanceof RemoveDataNode
        || statement instanceof RemoveConfigNode
        || statement instanceof RemoveAINode
        || statement instanceof SubscriptionStatement
        || statement instanceof ShowCurrentSqlDialect
        || statement instanceof SetSqlDialect
        || statement instanceof ShowCurrentUser
        || statement instanceof ShowCurrentDatabase
        || statement instanceof ShowVersion
        || statement instanceof ShowVariables
        || statement instanceof ShowClusterId
        || statement instanceof ShowCurrentTimestamp
        || statement instanceof KillQuery
        || statement instanceof CreateFunction
        || statement instanceof DropFunction
        || statement instanceof ShowFunctions
        || statement instanceof CreateExternalService
        || statement instanceof StartExternalService
        || statement instanceof StopExternalService
        || statement instanceof DropExternalService
        || statement instanceof RelationalAuthorStatement
        || statement instanceof MigrateRegion
        || statement instanceof ReconstructRegion
        || statement instanceof ExtendRegion
        || statement instanceof CreateModel
        || statement instanceof CreateTraining
        || statement instanceof ShowModels
        || statement instanceof ShowAIDevices
        || statement instanceof DropModel
        || statement instanceof LoadModel
        || statement instanceof UnloadModel
        || statement instanceof ShowLoadedModels
        || statement instanceof RemoveRegion
        || statement instanceof Prepare
        || statement instanceof Deallocate) {
      return new ConfigExecution(
          queryContext,
          null,
          executor,
          statement.accept(
              new TableConfigTaskVisitor(
                  clientSession, metadata, AuthorityChecker.getAccessControl(), typeManager),
              queryContext));
    }
    // Initialize variables for TableModelPlanner
    org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement statementToUse = statement;
    List<Expression> parameters = Collections.emptyList();
    Map<NodeRef<Parameter>, Expression> parameterLookup = Collections.emptyMap();

    if (statement instanceof Execute) {
      Execute executeStatement = (Execute) statement;
      String statementName = executeStatement.getStatementName().getValue();

      // Get prepared statement from session (contains cached AST)
      PreparedStatementInfo preparedInfo = clientSession.getPreparedStatement(statementName);
      if (preparedInfo == null) {
        throw new SemanticException(
            String.format("Prepared statement '%s' does not exist", statementName));
      }

      // Use cached AST
      statementToUse = preparedInfo.getSql();

      // Bind parameters: create parameterLookup map
      // Note: bindParameters() internally validates parameter count
      parameterLookup =
          ParameterExtractor.bindParameters(statementToUse, executeStatement.getParameters());
      parameters = new ArrayList<>(executeStatement.getParameters());

    } else if (statement instanceof ExecuteImmediate) {
      ExecuteImmediate executeImmediateStatement = (ExecuteImmediate) statement;

      // EXECUTE IMMEDIATE needs to parse SQL first
      String sql = executeImmediateStatement.getSqlString();
      List<Literal> literalParameters = executeImmediateStatement.getParameters();

      statementToUse = sqlParser.createStatement(sql, clientSession.getZoneId(), clientSession);

      if (!literalParameters.isEmpty()) {
        parameterLookup = ParameterExtractor.bindParameters(statementToUse, literalParameters);
        parameters = new ArrayList<>(literalParameters);
      }
    }

    if (statement instanceof WrappedInsertStatement) {
      ((WrappedInsertStatement) statement).setContext(queryContext);
    }

    // Create QueryExecution with TableModelPlanner
    TableModelPlanner tableModelPlanner =
        new TableModelPlanner(
            statementToUse,
            sqlParser,
            metadata,
            scheduledExecutor,
            SYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
            ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER,
            statementRewrite,
            logicalPlanOptimizers,
            distributionPlanOptimizers,
            AuthorityChecker.getAccessControl(),
            dataNodeLocationSupplier,
            parameters,
            parameterLookup,
            typeManager);
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
        boolean isUserQuery = queryExecution.isQuery() && queryExecution.isUserQuery();
        Supplier<String> contentOfQuerySupplier =
            new ContentOfQuerySupplier(nativeApiRequest, queryExecution);
        if (isUserQuery) {
          recordCurrentQueries(
              queryExecution.getQueryId(),
              queryExecution.getStartExecutionTime(),
              System.currentTimeMillis(),
              queryExecution.getTotalExecutionTime(),
              contentOfQuerySupplier,
              queryExecution.getUser(),
              queryExecution.getClientHostname());
        }
        queryExecutionMap.remove(queryId);
        if (isUserQuery) {
          recordQueries(queryExecution::getTotalExecutionTime, contentOfQuerySupplier, t);
        }
      }
    }
  }

  private static class ContentOfQuerySupplier implements Supplier<String> {

    private final org.apache.thrift.TBase<?, ?> nativeApiRequest;
    private final IQueryExecution queryExecution;

    private ContentOfQuerySupplier(TBase<?, ?> nativeApiRequest, IQueryExecution queryExecution) {
      this.nativeApiRequest = nativeApiRequest;
      this.queryExecution = queryExecution;
    }

    @Override
    public String get() {
      return getContentOfRequest(nativeApiRequest, queryExecution);
    }
  }

  public static void recordQueries(
      LongSupplier executionTime, Supplier<String> contentOfQuerySupplier, Throwable t) {

    long costTime = executionTime.getAsLong();
    // print slow query
    if (costTime / 1_000_000 >= CONFIG.getSlowQueryThreshold()) {
      SLOW_SQL_LOGGER.info("Cost: {} ms, {}", costTime / 1_000_000, contentOfQuerySupplier.get());
    }

    // only sample successful query
    if (t == null && COMMON_CONFIG.isEnableQuerySampling()) { // sampling is enabled
      String queryRequest = contentOfQuerySupplier.get();
      if (COMMON_CONFIG.isQuerySamplingHasRateLimit()) {
        if (COMMON_CONFIG.getQuerySamplingRateLimiter().tryAcquire(queryRequest.length())) {
          SAMPLED_QUERIES_LOGGER.info(queryRequest);
        }
      } else {
        // no limit, always sampled
        SAMPLED_QUERIES_LOGGER.info(queryRequest);
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

  public static IMemoryBlock getCoordinatorMemoryBlock() {
    return coordinatorMemoryBlock;
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

  public List<PlanOptimizer> getDistributionPlanOptimizers() {
    return distributionPlanOptimizers;
  }

  public List<PlanOptimizer> getLogicalPlanOptimizers() {
    return logicalPlanOptimizers;
  }

  public DataNodeLocationSupplierFactory.DataNodeLocationSupplier getDataNodeLocationSupplier() {
    return dataNodeLocationSupplier;
  }

  public ExecutorService getDispatchExecutor() {
    return dispatchExecutor;
  }

  /** record query info in memory data structure */
  public void recordCurrentQueries(
      String queryId,
      long startTime,
      long endTime,
      long costTimeInNs,
      Supplier<String> contentOfQuerySupplier,
      String user,
      String clientHost) {
    if (CONFIG.getQueryCostStatWindow() <= 0) {
      return;
    }

    if (queryId == null) {
      // fast Last query API executeFastLastDataQueryForOnePrefixPath will enter this
      queryId = queryIdGenerator.createNextQueryId().getId();
    }

    // ns -> s
    float costTimeInSeconds = costTimeInNs * 1e-9f;

    QueryInfo queryInfo =
        new QueryInfo(
            queryId,
            startTime,
            endTime,
            costTimeInSeconds,
            contentOfQuerySupplier.get(),
            user,
            clientHost);

    while (!coordinatorMemoryBlock.allocate(RamUsageEstimator.sizeOfObject(queryInfo))) {
      // try to release memory from the head of queue
      QueryInfo queryInfoToRelease = currentQueriesInfo.poll();
      if (queryInfoToRelease == null) {
        // no element in the queue and the memory is still not enough, skip this record
        return;
      } else {
        // release memory and unrecord in histogram
        coordinatorMemoryBlock.release(RamUsageEstimator.sizeOfObject(queryInfoToRelease));
        unrecordInHistogram(queryInfoToRelease.costTime);
      }
    }

    currentQueriesInfo.addLast(queryInfo);
    recordInHistogram(costTimeInSeconds);
  }

  private void recordInHistogram(float costTimeInSeconds) {
    int bucket = (int) costTimeInSeconds;
    if (bucket < 60) {
      currentQueriesCostHistogram[bucket].getAndIncrement();
    } else {
      currentQueriesCostHistogram[60].getAndIncrement();
    }
  }

  private void unrecordInHistogram(float costTimeInSeconds) {
    int bucket = (int) costTimeInSeconds;
    if (bucket < 60) {
      currentQueriesCostHistogram[bucket].getAndDecrement();
    } else {
      currentQueriesCostHistogram[60].getAndDecrement();
    }
  }

  private void clearExpiredQueriesInfoTask() {
    int queryCostStatWindow = CONFIG.getQueryCostStatWindow();
    if (queryCostStatWindow <= 0 && currentQueriesInfo.isEmpty()) {
      return;
    }

    // the QueryInfo smaller than expired time will be cleared
    long expiredTime = System.currentTimeMillis() - 1_000L * 60 * queryCostStatWindow;
    // peek head, the head QueryInfo is in the time window, return directly
    QueryInfo queryInfo = currentQueriesInfo.peekFirst();
    if (queryInfo == null || queryInfo.endTime >= expiredTime) {
      return;
    }

    queryInfo = currentQueriesInfo.poll();
    while (queryInfo != null) {
      if (queryInfo.endTime < expiredTime) {
        // out of time window, clear queryInfo
        coordinatorMemoryBlock.release(RamUsageEstimator.sizeOfObject(queryInfo));
        unrecordInHistogram(queryInfo.costTime);
        queryInfo = currentQueriesInfo.poll();
      } else {
        // the head of the queue is not expired, add back
        currentQueriesInfo.addFirst(queryInfo);
        //  there is no more candidate to clear
        return;
      }
    }
  }

  public List<StatedQueriesInfo> getRunningQueriesInfos() {
    long currentTime = System.currentTimeMillis();
    return getAllQueryExecutions().stream()
        .map(
            queryExecution ->
                new StatedQueriesInfo(
                    QueryState.RUNNING,
                    queryExecution.getQueryId(),
                    queryExecution.getStartExecutionTime(),
                    DEFAULT_END_TIME,
                    (currentTime - queryExecution.getStartExecutionTime()) / 1000,
                    queryExecution.getExecuteSQL().orElse("UNKNOWN"),
                    queryExecution.getUser(),
                    queryExecution.getClientHostname()))
        .collect(Collectors.toList());
  }

  public List<StatedQueriesInfo> getFinishedQueriesInfos() {
    long currentTime = System.currentTimeMillis();
    List<StatedQueriesInfo> result = new ArrayList<>();
    Iterator<QueryInfo> historyQueriesIterator = currentQueriesInfo.iterator();
    long needRecordTime = currentTime - 1_000L * 60 * CONFIG.getQueryCostStatWindow();
    while (historyQueriesIterator.hasNext()) {
      QueryInfo queryInfo = historyQueriesIterator.next();
      if (queryInfo.endTime < needRecordTime) {
        // out of time window, ignore it
      } else {
        result.add(new StatedQueriesInfo(QueryState.FINISHED, queryInfo));
      }
    }
    return result;
  }

  public List<StatedQueriesInfo> getCurrentQueriesInfo() {
    List<IQueryExecution> runningQueries = getAllQueryExecutions();
    Set<String> runningQueryIdSet =
        runningQueries.stream().map(IQueryExecution::getQueryId).collect(Collectors.toSet());
    List<StatedQueriesInfo> result = new ArrayList<>();

    // add History queries (satisfy the time window) info
    Iterator<QueryInfo> historyQueriesIterator = currentQueriesInfo.iterator();
    Set<String> repetitionQueryIdSet = new HashSet<>();
    long currentTime = System.currentTimeMillis();
    long needRecordTime = currentTime - 1_000L * 60 * CONFIG.getQueryCostStatWindow();
    while (historyQueriesIterator.hasNext()) {
      QueryInfo queryInfo = historyQueriesIterator.next();
      if (queryInfo.endTime < needRecordTime) {
        // out of time window, ignore it
      } else {
        if (runningQueryIdSet.contains(queryInfo.queryId)) {
          repetitionQueryIdSet.add(queryInfo.queryId);
        }
        result.add(new StatedQueriesInfo(QueryState.FINISHED, queryInfo));
      }
    }

    // add Running queries info after remove the repetitions which has recorded in History queries
    result.addAll(
        runningQueries.stream()
            .filter(queryExecution -> !repetitionQueryIdSet.contains(queryExecution.getQueryId()))
            .map(
                queryExecution ->
                    new StatedQueriesInfo(
                        QueryState.RUNNING,
                        queryExecution.getQueryId(),
                        queryExecution.getStartExecutionTime(),
                        DEFAULT_END_TIME,
                        (currentTime - queryExecution.getStartExecutionTime()) / 1000,
                        queryExecution.getExecuteSQL().orElse("UNKNOWN"),
                        queryExecution.getUser(),
                        queryExecution.getClientHostname()))
            .collect(Collectors.toList()));
    return result;
  }

  public int[] getCurrentQueriesCostHistogram() {
    return Arrays.stream(currentQueriesCostHistogram).mapToInt(AtomicInteger::get).toArray();
  }

  public static class QueryInfo implements Accountable {
    public static final long DEFAULT_END_TIME = -1L;
    private static final long INSTANCE_SIZE = shallowSizeOfInstance(QueryInfo.class);

    private final String queryId;

    // unit: millisecond
    private final long startTime;
    private final long endTime;
    // unit: second
    private final float costTime;

    private final String statement;
    private final String user;
    private final String clientHost;

    public QueryInfo(
        String queryId,
        long startTime,
        long endTime,
        float costTime,
        String statement,
        String user,
        String clientHost) {
      this.queryId = queryId;
      this.startTime = startTime;
      this.endTime = endTime;
      this.costTime = costTime;
      this.statement = statement;
      this.user = user;
      this.clientHost = clientHost;
    }

    public String getClientHost() {
      return clientHost;
    }

    public String getUser() {
      return user;
    }

    public long getStartTime() {
      return startTime;
    }

    public long getEndTime() {
      return endTime;
    }

    public float getCostTime() {
      return costTime;
    }

    public String getQueryId() {
      return queryId;
    }

    public String getStatement() {
      return statement;
    }

    @Override
    public long ramBytesUsed() {
      return INSTANCE_SIZE
          + sizeOfCharArray(statement.length())
          + sizeOfCharArray(user.length())
          + sizeOfCharArray(clientHost.length());
    }
  }

  public static class StatedQueriesInfo extends QueryInfo {
    private final QueryState queryState;

    private StatedQueriesInfo(QueryState queryState, QueryInfo queryInfo) {
      super(
          queryInfo.queryId,
          queryInfo.startTime,
          queryInfo.endTime,
          queryInfo.costTime,
          queryInfo.statement,
          queryInfo.user,
          queryInfo.clientHost);
      this.queryState = queryState;
    }

    private StatedQueriesInfo(
        QueryState queryState,
        String queryId,
        long startTime,
        long endTime,
        long costTime,
        String statement,
        String user,
        String clientHost) {
      super(queryId, startTime, endTime, costTime, statement, user, clientHost);
      this.queryState = queryState;
    }

    public String getQueryState() {
      return queryState.name();
    }
  }
}
