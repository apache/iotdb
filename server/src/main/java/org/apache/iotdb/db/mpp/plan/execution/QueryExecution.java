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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.execution.QueryState;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.exchange.ISourceHandle;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.analyze.Analyzer;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.execution.memory.MemorySourceHandle;
import org.apache.iotdb.db.mpp.plan.execution.memory.StatementMemorySource;
import org.apache.iotdb.db.mpp.plan.execution.memory.StatementMemorySourceContext;
import org.apache.iotdb.db.mpp.plan.execution.memory.StatementMemorySourceVisitor;
import org.apache.iotdb.db.mpp.plan.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.plan.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.plan.planner.distribution.DistributionPlanner;
import org.apache.iotdb.db.mpp.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.mpp.plan.scheduler.ClusterScheduler;
import org.apache.iotdb.db.mpp.plan.scheduler.IScheduler;
import org.apache.iotdb.db.mpp.plan.scheduler.StandaloneScheduler;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertBaseStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static org.apache.iotdb.db.mpp.plan.constant.DataNodeEndPoints.isSameNode;

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

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientManager;

  public QueryExecution(
      Statement statement,
      MPPQueryContext context,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      ScheduledExecutorService scheduledExecutor,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
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
    this.internalServiceClientManager = internalServiceClientManager;

    // We add the abort logic inside the QueryExecution.
    // So that the other components can only focus on the state change.
    stateMachine.addStateChangeListener(
        state -> {
          try (SetThreadName queryName = new SetThreadName(context.getQueryId().getId())) {
            if (!state.isDone()) {
              return;
            }
            this.stop();
            // TODO: (xingtanzjr) If the query is in abnormal state, the releaseResource() should be
            // invoked
            if (state == QueryState.FAILED
                || state == QueryState.ABORTED
                || state == QueryState.CANCELED) {
              logger.info("release resource because Query State is: {}", state);
              releaseResource();
            }
          }
        });
  }

  public void start() {
    if (skipExecute()) {
      logger.info("execution of query will be skipped. Transit to RUNNING immediately.");
      constructResultForMemorySource();
      stateMachine.transitionToRunning();
      return;
    }
    long remainTime = context.getTimeOut() - (System.currentTimeMillis() - context.getStartTime());
    if (remainTime <= 0) {
      throw new QueryTimeoutRuntimeException();
    }
    context.setTimeOut(remainTime);

    doLogicalPlan();
    doDistributedPlan();
    stateMachine.transitionToPlanned();
    if (context.getQueryType() == QueryType.READ) {
      initResultHandle();
    }
    schedule();
  }

  private ExecutionResult retry() {
    if (retryCount >= MAX_RETRY_COUNT) {
      logger.error("reach max retry count. transit query to failed");
      stateMachine.transitionToFailed();
      return getStatus();
    }
    logger.warn("error when executing query. {}", stateMachine.getFailureMessage());
    // stop and clean up resources the QueryExecution used
    this.stopAndCleanup();
    logger.info("wait {}ms before retry...", RETRY_INTERVAL_IN_MS);
    try {
      Thread.sleep(RETRY_INTERVAL_IN_MS);
    } catch (InterruptedException e) {
      logger.error("interrupted when waiting retry");
      Thread.currentThread().interrupt();
    }
    retryCount++;
    logger.info("start to retry. Retry count is: {}", retryCount);
    stateMachine.transitionToQueued();
    // force invalid PartitionCache
    partitionFetcher.invalidAllCache();
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
    return new Analyzer(context, partitionFetcher, schemaFetcher).analyze(statement);
  }

  private void schedule() {
    // TODO: (xingtanzjr) initialize the query scheduler according to configuration
    this.scheduler =
        config.isClusterMode()
            ? new ClusterScheduler(
                context,
                stateMachine,
                distributedPlan.getInstances(),
                context.getQueryType(),
                executor,
                writeOperationExecutor,
                scheduledExecutor,
                internalServiceClientManager)
            : new StandaloneScheduler(
                context,
                stateMachine,
                distributedPlan.getInstances(),
                context.getQueryType(),
                scheduledExecutor,
                internalServiceClientManager);
    this.scheduler.start();
  }

  // Use LogicalPlanner to do the logical query plan and logical optimization
  public void doLogicalPlan() {
    LogicalPlanner planner = new LogicalPlanner(this.context, this.planOptimizers);
    this.logicalPlan = planner.plan(this.analysis);
    if (isQuery()) {
      logger.info(
          "logical plan is: \n {}", PlanNodeUtil.nodeToString(this.logicalPlan.getRootNode()));
    }
  }

  // Generate the distributed plan and split it into fragments
  public void doDistributedPlan() {
    DistributionPlanner planner = new DistributionPlanner(this.analysis, this.logicalPlan);
    this.distributedPlan = planner.planFragments();
    if (isQuery()) {
      logger.info(
          "distribution plan done. Fragment instance count is {}, details is: \n {}",
          distributedPlan.getInstances().size(),
          printFragmentInstances(distributedPlan.getInstances()));
    }
  }

  private String printFragmentInstances(List<FragmentInstance> instances) {
    StringBuilder ret = new StringBuilder();
    for (FragmentInstance instance : instances) {
      ret.append(System.lineSeparator()).append(instance);
    }
    return ret.toString();
  }

  // Stop the workers for this query
  public void stop() {
    if (this.scheduler != null) {
      this.scheduler.stop();
    }
  }

  // Stop the query and clean up all the resources this query occupied
  public void stopAndCleanup() {
    stop();
    releaseResource();
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
      resultHandle.abort();
    }
  }

  /**
   * This method will be called by the request thread from client connection. This method will block
   * until one of these conditions occurs: 1. There is a batch of result 2. There is no more result
   * 3. The query has been cancelled 4. The query is timeout This method will fetch the result from
   * DataStreamManager use the virtual ResultOperator's ID (This part will be designed and
   * implemented with DataStreamManager)
   */
  @Override
  public Optional<TsBlock> getBatchResult() throws IoTDBException {
    checkArgument(resultHandle != null, "ResultHandle in Coordinator should be init firstly.");
    // iterate until we get a non-nullable TsBlock or result is finished
    while (true) {
      try {
        if (resultHandle.isAborted()) {
          logger.info("resultHandle for client is aborted");
          stateMachine.transitionToAborted();
          if (stateMachine.getFailureStatus() != null) {
            throw new IoTDBException(
                stateMachine.getFailureStatus().getMessage(), stateMachine.getFailureStatus().code);
          } else {
            throw new IoTDBException(
                stateMachine.getFailureMessage(), TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
          }
        } else if (resultHandle.isFinished()) {
          // Once the resultHandle is finished, we should transit the state of this query to
          // FINISHED.
          // So that the corresponding cleanup work could be triggered.
          logger.info("resultHandle for client is finished");
          stateMachine.transitionToFinished();
          return Optional.empty();
        }

        ListenableFuture<?> blocked = resultHandle.isBlocked();
        blocked.get();
        if (!resultHandle.isFinished()) {
          TsBlock res = resultHandle.receive();
          if (res == null) {
            continue;
          }
          return Optional.of(res);
        } else {
          return Optional.empty();
        }
      } catch (ExecutionException | CancellationException e) {
        stateMachine.transitionToFailed(e);
        if (stateMachine.getFailureStatus() != null) {
          throw new IoTDBException(
              stateMachine.getFailureStatus().getMessage(), stateMachine.getFailureStatus().code);
        }
        Throwable t = e.getCause() == null ? e : e.getCause();
        throwIfUnchecked(t);
        throw new IoTDBException(t, TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
      } catch (InterruptedException e) {
        stateMachine.transitionToFailed(e);
        Thread.currentThread().interrupt();
        throw new IoTDBException(e, TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
      } catch (Throwable t) {
        stateMachine.transitionToFailed(t);
        throw t;
      }
    }
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
    } catch (InterruptedException | ExecutionException e) {
      // TODO: (xingtanzjr) use more accurate error handling
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
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
                .createLocalSourceHandle(
                    context.getResultNodeContext().getVirtualFragmentInstanceId().toThrift(),
                    context.getResultNodeContext().getVirtualResultNodeId().getId(),
                    context.getResultNodeContext().getUpStreamFragmentInstanceId().toThrift(),
                    stateMachine::transitionToFailed)
            : MPPDataExchangeService.getInstance()
                .getMPPDataExchangeManager()
                .createSourceHandle(
                    context.getResultNodeContext().getVirtualFragmentInstanceId().toThrift(),
                    context.getResultNodeContext().getVirtualResultNodeId().getId(),
                    upstreamEndPoint,
                    context.getResultNodeContext().getUpStreamFragmentInstanceId().toThrift(),
                    stateMachine::transitionToFailed);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private ExecutionResult getExecutionResult(QueryState state) {
    TSStatusCode statusCode =
        // For WRITE, the state should be FINISHED; For READ, the state could be RUNNING
        state == QueryState.FINISHED || state == QueryState.RUNNING
            ? TSStatusCode.SUCCESS_STATUS
            : TSStatusCode.QUERY_PROCESS_ERROR;

    TSStatus tsstatus = RpcUtils.getStatus(statusCode, stateMachine.getFailureMessage());

    // If RETRYING is triggered by this QueryExecution, the stateMachine.getFailureStatus() is also
    // not null. We should only return the failure status when QueryExecution is in Done state.
    if (state.isDone() && stateMachine.getFailureStatus() != null) {
      tsstatus = stateMachine.getFailureStatus();
    }

    // collect redirect info to client for writing
    if (analysis.getStatement() instanceof InsertBaseStatement) {
      InsertBaseStatement insertStatement = (InsertBaseStatement) analysis.getStatement();
      List<TEndPoint> redirectNodeList;
      if (config.isClusterMode()) {
        redirectNodeList = insertStatement.collectRedirectInfo(analysis.getDataPartitionInfo());
      } else {
        redirectNodeList = Collections.emptyList();
      }
      if (insertStatement instanceof InsertRowsStatement
          || insertStatement instanceof InsertMultiTabletsStatement) {
        // multiple devices
        if (statusCode == TSStatusCode.SUCCESS_STATUS) {
          List<TSStatus> subStatus = new ArrayList<>();
          tsstatus.setCode(TSStatusCode.NEED_REDIRECTION.getStatusCode());
          for (TEndPoint endPoint : redirectNodeList) {
            subStatus.add(
                StatusUtils.getStatus(TSStatusCode.NEED_REDIRECTION).setRedirectNode(endPoint));
          }
          tsstatus.setSubStatus(subStatus);
        }
      } else {
        // single device
        if (config.isClusterMode()) {
          tsstatus.setRedirectNode(redirectNodeList.get(0));
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

  public String toString() {
    return String.format("QueryExecution[%s]", context.getQueryId());
  }
}
