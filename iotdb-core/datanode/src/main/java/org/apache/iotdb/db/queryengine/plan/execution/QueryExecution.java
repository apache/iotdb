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
package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.service.metric.PerformanceOverviewMetrics;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.KilledByOthersException;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.Analysis;
import org.apache.iotdb.db.queryengine.plan.analyze.Analyzer;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.memory.MemorySourceHandle;
import org.apache.iotdb.db.queryengine.plan.execution.memory.StatementMemorySource;
import org.apache.iotdb.db.queryengine.plan.execution.memory.StatementMemorySourceContext;
import org.apache.iotdb.db.queryengine.plan.execution.memory.StatementMemorySourceVisitor;
import org.apache.iotdb.db.queryengine.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.scheduler.ClusterScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.queryengine.plan.scheduler.load.LoadTsFileScheduler;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.PipeEnrichedLoadTsFileStatement;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode;
import static org.apache.iotdb.db.queryengine.metric.QueryExecutionMetricSet.WAIT_FOR_RESULT;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.DISTRIBUTION_PLANNER;

/**
 * QueryExecution stores all the status of a query which is being prepared or running inside the MPP
 * frame. It takes three main responsibilities: 1. Prepare a query. Transform a query from statement
 * to DistributedQueryPlan with fragment instances. 2. Dispatch all the fragment instances to
 * corresponding physical nodes. 3. Collect and monitor the progress/states of this query.
 */
public class QueryExecution implements IQueryExecution {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecution.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final int MAX_RETRY_COUNT = 3;
  private static final long RETRY_INTERVAL_IN_MS = 2000;
  private int retryCount = 0;
  private final MPPQueryContext context;
  private IScheduler scheduler;
  private final QueryStateMachine stateMachine;

  private final List<PlanOptimizer> planOptimizers;

  private final Statement rawStatement;
  private Analysis analysis;
  private LogicalQueryPlan logicalPlan;
  private DistributedQueryPlan distributedPlan;

  private final ExecutorService executor;
  private final ExecutorService writeOperationExecutor;
  private final ScheduledExecutorService scheduledExecutor;
  // TODO need to use factory to decide standalone or cluster
  private final IPartitionFetcher partitionFetcher;
  // TODO need to use factory to decide standalone or cluster,
  private final ISchemaFetcher schemaFetcher;

  // The result of QueryExecution will be written to the MPPDataExchangeManager in current Node.
  // We use this SourceHandle to fetch the TsBlock from it.
  private ISourceHandle resultHandle;

  // used for cleaning resultHandle up exactly once
  private final AtomicBoolean resultHandleCleanUp;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      syncInternalServiceClientManager;

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;

  private final AtomicBoolean stopped;

  // cost time in ns
  private long totalExecutionTime = 0;

  private static final QueryExecutionMetricSet QUERY_EXECUTION_METRIC_SET =
      QueryExecutionMetricSet.getInstance();
  private static final QueryPlanCostMetricSet QUERY_PLAN_COST_METRIC_SET =
      QueryPlanCostMetricSet.getInstance();
  private static final PerformanceOverviewMetrics PERFORMANCE_OVERVIEW_METRICS =
      PerformanceOverviewMetrics.getInstance();

  @SuppressWarnings("squid:S107")
  public QueryExecution(
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
    this.rawStatement = statement;
    this.executor = executor;
    this.writeOperationExecutor = writeOperationExecutor;
    this.scheduledExecutor = scheduledExecutor;
    this.context = context;
    this.planOptimizers = new ArrayList<>();
    this.analysis = analyze(statement, context, partitionFetcher, schemaFetcher);
    this.stateMachine = new QueryStateMachine(context.getQueryId(), executor);
    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;
    this.syncInternalServiceClientManager = syncInternalServiceClientManager;
    this.asyncInternalServiceClientManager = asyncInternalServiceClientManager;

    // We add the abort logic inside the QueryExecution.
    // So that the other components can only focus on the state change.
    stateMachine.addStateChangeListener(
        state -> {
          try (SetThreadName queryName = new SetThreadName(context.getQueryId().getId())) {
            if (!state.isDone()) {
              return;
            }
            // TODO: (xingtanzjr) If the query is in abnormal state, the releaseResource() should be
            // invoked
            if (state == QueryState.FAILED
                || state == QueryState.ABORTED
                || state == QueryState.CANCELED) {
              logger.debug("[ReleaseQueryResource] state is: {}", state);
              Throwable cause = stateMachine.getFailureException();
              releaseResource(cause);
            }
            this.stop(null);
          }
        });
    this.stopped = new AtomicBoolean(false);
    this.resultHandleCleanUp = new AtomicBoolean(false);
  }

  @FunctionalInterface
  interface ISourceHandleSupplier<T> {
    T get() throws IoTDBException;
  }

  public void start() {
    final long startTime = System.nanoTime();
    if (skipExecute()) {
      logger.debug("[SkipExecute]");
      if (context.getQueryType() == QueryType.WRITE && analysis.isFailed()) {
        stateMachine.transitionToFailed(analysis.getFailStatus());
      } else {
        constructResultForMemorySource();
        stateMachine.transitionToRunning();
      }
      return;
    }

    // check timeout for query first
    checkTimeOutForQuery();
    doLogicalPlan();
    doDistributedPlan();
    // update timeout after finishing plan stage
    context.setTimeOut(
        context.getTimeOut() - (System.currentTimeMillis() - context.getStartTime()));

    stateMachine.transitionToPlanned();
    if (context.getQueryType() == QueryType.READ) {
      initResultHandle();
    }
    PERFORMANCE_OVERVIEW_METRICS.recordPlanCost(System.nanoTime() - startTime);
    schedule();

    // set partial insert error message
    // When some columns in one insert failed, other column will continue executing insertion.
    // The error message should be return to client, therefore we need to set it after the insertion
    // of other column finished.
    if (context.getQueryType() == QueryType.WRITE && analysis.isFailed()) {
      stateMachine.transitionToFailed(analysis.getFailStatus());
    }
  }

  private void checkTimeOutForQuery() {
    // only check query operation's timeout because we will never limit write operation's execution
    // time
    if (isQuery()) {
      long currentTime = System.currentTimeMillis();
      if (currentTime - context.getStartTime() >= context.getTimeOut()) {
        throw new QueryTimeoutRuntimeException(
            context.getStartTime(), currentTime, context.getTimeOut());
      }
    }
  }

  private ExecutionResult retry() {
    if (retryCount >= MAX_RETRY_COUNT) {
      logger.warn("[ReachMaxRetryCount]");
      stateMachine.transitionToFailed();
      return getStatus();
    }
    logger.warn("error when executing query. {}", stateMachine.getFailureMessage());
    // stop and clean up resources the QueryExecution used
    this.stopAndCleanup(stateMachine.getFailureException());
    logger.info("[WaitBeforeRetry] wait {}ms.", RETRY_INTERVAL_IN_MS);
    try {
      Thread.sleep(RETRY_INTERVAL_IN_MS);
    } catch (InterruptedException e) {
      logger.warn("interrupted when waiting retry");
      Thread.currentThread().interrupt();
    }
    retryCount++;
    logger.info("[Retry] retry count is: {}", retryCount);
    stateMachine.transitionToQueued();
    // force invalid PartitionCache
    partitionFetcher.invalidAllCache();
    // clear runtime variables in MPPQueryContext
    context.prepareForRetry();
    // re-stop
    this.stopped.compareAndSet(true, false);
    this.resultHandleCleanUp.compareAndSet(true, false);
    // re-analyze the query
    this.analysis = analyze(rawStatement, context, partitionFetcher, schemaFetcher);
    // re-start the QueryExecution
    this.start();
    return getStatus();
  }

  private boolean skipExecute() {
    return analysis.isFinishQueryAfterAnalyze()
        || (context.getQueryType() == QueryType.READ && !analysis.hasDataSource());
  }

  private void constructResultForMemorySource() {
    StatementMemorySource memorySource =
        new StatementMemorySourceVisitor()
            .process(analysis.getStatement(), new StatementMemorySourceContext(context, analysis));
    this.resultHandle = new MemorySourceHandle(memorySource.getTsBlock());
    this.analysis.setRespDatasetHeader(memorySource.getDatasetHeader());
  }

  // Analyze the statement in QueryContext. Generate the analysis this query need
  private Analysis analyze(
      Statement statement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    final long startTime = System.nanoTime();
    Analysis result;
    try {
      result = new Analyzer(context, partitionFetcher, schemaFetcher).analyze(statement);
    } finally {
      PERFORMANCE_OVERVIEW_METRICS.recordAnalyzeCost(System.nanoTime() - startTime);
    }
    return result;
  }

  private void schedule() {
    final long startTime = System.nanoTime();
    if (rawStatement instanceof LoadTsFileStatement) {
      this.scheduler =
          new LoadTsFileScheduler(
              distributedPlan,
              context,
              stateMachine,
              syncInternalServiceClientManager,
              partitionFetcher,
              rawStatement instanceof PipeEnrichedLoadTsFileStatement);
      this.scheduler.start();
      return;
    }

    // TODO: (xingtanzjr) initialize the query scheduler according to configuration
    this.scheduler =
        new ClusterScheduler(
            context,
            stateMachine,
            distributedPlan.getInstances(),
            context.getQueryType(),
            executor,
            writeOperationExecutor,
            scheduledExecutor,
            syncInternalServiceClientManager,
            asyncInternalServiceClientManager);
    this.scheduler.start();
    PERFORMANCE_OVERVIEW_METRICS.recordScheduleCost(System.nanoTime() - startTime);
  }

  // Use LogicalPlanner to do the logical query plan and logical optimization
  public void doLogicalPlan() {
    LogicalPlanner planner = new LogicalPlanner(this.context, this.planOptimizers);
    this.logicalPlan = planner.plan(this.analysis);
    if (isQuery() && logger.isDebugEnabled()) {
      logger.debug(
          "logical plan is: \n {}", PlanNodeUtil.nodeToString(this.logicalPlan.getRootNode()));
    }
    // check timeout after building logical plan because it could be time-consuming in some cases.
    checkTimeOutForQuery();
  }

  // Generate the distributed plan and split it into fragments
  public void doDistributedPlan() {
    long startTime = System.nanoTime();
    DistributionPlanner planner = new DistributionPlanner(this.analysis, this.logicalPlan);
    this.distributedPlan = planner.planFragments();

    if (rawStatement.isQuery()) {
      QUERY_PLAN_COST_METRIC_SET.recordPlanCost(
          DISTRIBUTION_PLANNER, System.nanoTime() - startTime);
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

  private String printFragmentInstances(List<FragmentInstance> instances) {
    StringBuilder ret = new StringBuilder();
    for (FragmentInstance instance : instances) {
      ret.append(System.lineSeparator()).append(instance);
    }
    return ret.toString();
  }

  // Stop the workers for this query
  public void stop(Throwable t) {
    // only stop once
    if (stopped.compareAndSet(false, true) && this.scheduler != null) {
      this.scheduler.stop(t);
    }
  }

  // Stop the query and clean up all the resources this query occupied
  public void stopAndCleanup() {
    stop(null);
    releaseResource();
  }

  @Override
  public void cancel() {
    stateMachine.transitionToCanceled(
        new KilledByOthersException(),
        new TSStatus(TSStatusCode.QUERY_WAS_KILLED.getStatusCode())
            .setMessage(KilledByOthersException.MESSAGE));
  }

  /** Release the resources that current QueryExecution hold. */
  private void releaseResource() {
    // close ResultHandle to unblock client's getResult request
    // Actually, we should not close the ResultHandle when the QueryExecution is Finished.
    // There are only two scenarios where the ResultHandle should be closed:
    //   1. The client fetch all the result and the ResultHandle is finished.
    //   2. The client's connection is closed that all owned QueryExecution should be cleaned up
    // If the QueryExecution's state is abnormal, we should also abort the resultHandle without
    // waiting it to be finished.
    if (resultHandle != null) {
      resultHandle.close();
      cleanUpResultHandle();
    }
  }

  private void cleanUpResultHandle() {
    // Result handle belongs to special fragment instance, so we need to deregister it alone
    // We don't need to deal with MemorySourceHandle because it doesn't register to memory pool
    // We don't need to deal with LocalSourceHandle because the SharedTsBlockQueue uses the upstream
    // FragmentInstanceId to register
    if (resultHandleCleanUp.compareAndSet(false, true) && resultHandle instanceof SourceHandle) {
      TFragmentInstanceId fragmentInstanceId = resultHandle.getLocalFragmentInstanceId();
      MPPDataExchangeService.getInstance()
          .getMPPDataExchangeManager()
          .deRegisterFragmentInstanceFromMemoryPool(
              fragmentInstanceId.queryId,
              FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                  fragmentInstanceId));
    }
  }

  // Stop the query and clean up all the resources this query occupied
  public void stopAndCleanup(Throwable t) {
    stop(t);
    releaseResource(t);
  }

  /** Release the resources that current QueryExecution hold with a specified exception */
  private void releaseResource(Throwable t) {
    // close ResultHandle to unblock client's getResult request
    // Actually, we should not close the ResultHandle when the QueryExecution is Finished.
    // There are only two scenarios where the ResultHandle should be closed:
    //   1. The client fetch all the result and the ResultHandle is finished.
    //   2. The client's connection is closed that all owned QueryExecution should be cleaned up
    // If the QueryExecution's state is abnormal, we should also abort the resultHandle without
    // waiting it to be finished.
    if (resultHandle != null) {
      if (t != null) {
        resultHandle.abort(t);
      } else {
        resultHandle.close();
      }
      cleanUpResultHandle();
    }
  }

  /**
   * This method will be called by the request thread from client connection. This method will block
   * until one of these conditions occurs: 1. There is a batch of result 2. There is no more result
   * 3. The query has been cancelled 4. The query is timeout This method will fetch the result from
   * DataStreamManager use the virtual ResultOperator's ID (This part will be designed and
   * implemented with DataStreamManager)
   */
  private <T> Optional<T> getResult(ISourceHandleSupplier<T> dataSupplier) throws IoTDBException {
    checkArgument(resultHandle != null, "ResultHandle in Coordinator should be init firstly.");
    // iterate until we get a non-nullable TsBlock or result is finished
    while (true) {
      try {
        if (resultHandle.isAborted()) {
          logger.warn("[ResultHandleAborted]");
          stateMachine.transitionToAborted();
          if (stateMachine.getFailureStatus() != null) {
            throw new IoTDBException(
                stateMachine.getFailureStatus().getMessage(), stateMachine.getFailureStatus().code);
          } else {
            throw new IoTDBException(
                stateMachine.getFailureMessage(),
                TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
          }
        } else if (resultHandle.isFinished()) {
          logger.debug("[ResultHandleFinished]");
          stateMachine.transitionToFinished();
          return Optional.empty();
        }

        long startTime = System.nanoTime();
        try {
          ListenableFuture<?> blocked = resultHandle.isBlocked();
          blocked.get();
        } finally {
          QUERY_EXECUTION_METRIC_SET.recordExecutionCost(
              WAIT_FOR_RESULT, System.nanoTime() - startTime);
        }

        if (!resultHandle.isFinished()) {
          // use the getSerializedTsBlock instead of receive to get ByteBuffer result
          T res = dataSupplier.get();
          if (res == null) {
            continue;
          }
          return Optional.of(res);
        } else {
          return Optional.empty();
        }
      } catch (ExecutionException | CancellationException e) {
        dealWithException(e.getCause() != null ? e.getCause() : e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        dealWithException(e);
      } catch (Throwable t) {
        dealWithException(t);
      }
    }
  }

  private void dealWithException(Throwable t) throws IoTDBException {
    stateMachine.transitionToFailed(t);
    if (stateMachine.getFailureStatus() != null) {
      throw new IoTDBException(
          stateMachine.getFailureStatus().getMessage(), stateMachine.getFailureStatus().code);
    } else if (stateMachine.getFailureException() != null) {
      Throwable rootCause = stateMachine.getFailureException();
      throw new IoTDBException(rootCause, TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    } else {
      throwIfUnchecked(t);
      throw new IoTDBException(t, TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
    }
  }

  @Override
  public Optional<TsBlock> getBatchResult() throws IoTDBException {
    return getResult(this::getDeserializedTsBlock);
  }

  private TsBlock getDeserializedTsBlock() {
    return resultHandle.receive();
  }

  @Override
  public Optional<ByteBuffer> getByteBufferBatchResult() throws IoTDBException {
    return getResult(this::getSerializedTsBlock);
  }

  private ByteBuffer getSerializedTsBlock() throws IoTDBException {
    return resultHandle.getSerializedTsBlock();
  }

  /** @return true if there is more tsblocks, otherwise false */
  @Override
  public boolean hasNextResult() {
    return resultHandle != null && !resultHandle.isFinished();
  }

  /** return the result column count without the time column */
  @Override
  public int getOutputValueColumnCount() {
    return analysis.getRespDatasetHeader().getOutputValueColumnCount();
  }

  @Override
  public DatasetHeader getDatasetHeader() {
    return analysis.getRespDatasetHeader();
  }

  /**
   * This method is a synchronized method. For READ, it will block until all the FragmentInstances
   * have been submitted. For WRITE, it will block until all the FragmentInstances have finished.
   *
   * @return ExecutionStatus. Contains the QueryId and the TSStatus.
   */
  public ExecutionResult getStatus() {
    // Although we monitor the state to transition to RUNNING, the future will return if any
    // Terminated state is triggered
    try {
      if (stateMachine.getState() == QueryState.FINISHED) {
        return getExecutionResult(QueryState.FINISHED);
      }
      SettableFuture<QueryState> future = SettableFuture.create();
      stateMachine.addStateChangeListener(
          state -> {
            if (state == QueryState.RUNNING
                || state.isDone()
                || state == QueryState.PENDING_RETRY) {
              future.set(state);
            }
          });
      QueryState state = future.get();
      if (state == QueryState.PENDING_RETRY) {
        // That we put retry() here is aimed to leverage the ClientRPC thread rather than
        // create another new thread to do the retry() logic.
        // This way will lead to recursive call because retry() calls getStatus() inside.
        // The max depths of recursive call is equal to the max retry count.
        return retry();
      }
      // TODO: (xingtanzjr) use more TSStatusCode if the QueryState isn't FINISHED
      return getExecutionResult(state);
    } catch (InterruptedException e) {
      // TODO: (xingtanzjr) use more accurate error handling
      Thread.currentThread().interrupt();
      return new ExecutionResult(
          context.getQueryId(),
          stateMachine.getFailureStatus() == null
              ? RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR, stateMachine.getFailureMessage())
              : stateMachine.getFailureStatus());
    } catch (ExecutionException e) {
      return new ExecutionResult(
          context.getQueryId(),
          stateMachine.getFailureStatus() == null
              ? RpcUtils.getStatus(
                  TSStatusCode.INTERNAL_SERVER_ERROR, stateMachine.getFailureMessage())
              : stateMachine.getFailureStatus());
    }
  }

  private void initResultHandle() {
    TEndPoint upstreamEndPoint = context.getResultNodeContext().getUpStreamEndpoint();

    this.resultHandle =
        isSameNode(upstreamEndPoint)
            ? MPPDataExchangeService.getInstance()
                .getMPPDataExchangeManager()
                .createLocalSourceHandleForFragment(
                    context.getResultNodeContext().getVirtualFragmentInstanceId().toThrift(),
                    context.getResultNodeContext().getVirtualResultNodeId().getId(),
                    context.getResultNodeContext().getUpStreamPlanNodeId().getId(),
                    context.getResultNodeContext().getUpStreamFragmentInstanceId().toThrift(),
                    0, // Upstream of result ExchangeNode will only have one child.
                    stateMachine::transitionToFailed)
            : MPPDataExchangeService.getInstance()
                .getMPPDataExchangeManager()
                .createSourceHandle(
                    context.getResultNodeContext().getVirtualFragmentInstanceId().toThrift(),
                    context.getResultNodeContext().getVirtualResultNodeId().getId(),
                    0,
                    upstreamEndPoint,
                    context.getResultNodeContext().getUpStreamFragmentInstanceId().toThrift(),
                    stateMachine::transitionToFailed);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private ExecutionResult getExecutionResult(QueryState state) {
    TSStatusCode statusCode;
    if (context.getQueryType() == QueryType.WRITE && analysis.isFailed()) {
      // For WRITE, the state should be FINISHED
      statusCode =
          state == QueryState.FINISHED
              ? TSStatusCode.SUCCESS_STATUS
              : TSStatusCode.WRITE_PROCESS_ERROR;
    } else {
      // For READ, the state could be FINISHED and RUNNING
      statusCode =
          state == QueryState.FINISHED || state == QueryState.RUNNING
              ? TSStatusCode.SUCCESS_STATUS
              : TSStatusCode.EXECUTE_STATEMENT_ERROR;
    }

    TSStatus tsstatus =
        RpcUtils.getStatus(
            statusCode,
            statusCode == TSStatusCode.SUCCESS_STATUS ? "" : stateMachine.getFailureMessage());

    // If RETRYING is triggered by this QueryExecution, the stateMachine.getFailureStatus() is also
    // not null. We should only return the failure status when QueryExecution is in Done state.
    if (state.isDone() && stateMachine.getFailureStatus() != null) {
      tsstatus = stateMachine.getFailureStatus();
    }

    // collect redirect info to client for writing
    // if 0.13_data_insert_adapt is true and ClientVersion is NOT V_1_0, stop returning redirect
    // info to client
    if (analysis.getStatement() instanceof InsertBaseStatement
        && !analysis.isFinishQueryAfterAnalyze()
        && (!config.isEnable13DataInsertAdapt()
            || IoTDBConstant.ClientVersion.V_1_0.equals(context.getSession().getVersion()))) {
      InsertBaseStatement insertStatement = (InsertBaseStatement) analysis.getStatement();
      List<TEndPoint> redirectNodeList = analysis.getRedirectNodeList();
      if (insertStatement instanceof InsertRowsStatement
          || insertStatement instanceof InsertMultiTabletsStatement) {
        // multiple devices
        if (statusCode == TSStatusCode.SUCCESS_STATUS) {
          boolean needRedirect = false;
          List<TSStatus> subStatus = new ArrayList<>();
          for (TEndPoint endPoint : redirectNodeList) {
            // redirect writing only if the redirectEndPoint is not the current node
            if (!config.getAddressAndPort().equals(endPoint)) {
              subStatus.add(
                  RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS).setRedirectNode(endPoint));
              needRedirect = true;
            } else {
              subStatus.add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            }
          }
          if (needRedirect) {
            tsstatus.setCode(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
            tsstatus.setSubStatus(subStatus);
          }
        }
      } else {
        // single device
        TEndPoint redirectEndPoint = redirectNodeList.get(0);
        // redirect writing only if the redirectEndPoint is not the current node
        if (!config.getAddressAndPort().equals(redirectEndPoint)) {
          tsstatus.setRedirectNode(redirectEndPoint);
        }
      }
    }

    return new ExecutionResult(context.getQueryId(), tsstatus);
  }

  public DistributedQueryPlan getDistributedPlan() {
    return distributedPlan;
  }

  public LogicalQueryPlan getLogicalPlan() {
    return logicalPlan;
  }

  @Override
  public boolean isQuery() {
    return context.getQueryType() == QueryType.READ;
  }

  @Override
  public String getQueryId() {
    return context.getQueryId().getId();
  }

  @Override
  public long getStartExecutionTime() {
    return context.getStartTime();
  }

  @Override
  public void recordExecutionTime(long executionTime) {
    totalExecutionTime += executionTime;
  }

  @Override
  public long getTotalExecutionTime() {
    return totalExecutionTime;
  }

  @Override
  public Optional<String> getExecuteSQL() {
    return Optional.ofNullable(context.getSql());
  }

  @Override
  public Statement getStatement() {
    return analysis.getStatement();
  }

  public String toString() {
    return String.format("QueryExecution[%s]", context.getQueryId());
  }
}
