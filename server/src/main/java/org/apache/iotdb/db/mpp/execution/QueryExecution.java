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

import org.apache.iotdb.db.mpp.buffer.ISourceHandle;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.execution.scheduler.ClusterScheduler;
import org.apache.iotdb.db.mpp.execution.scheduler.IScheduler;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.analyze.Analyzer;
import org.apache.iotdb.db.mpp.sql.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.sql.planner.DistributionPlanner;
import org.apache.iotdb.db.mpp.sql.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.mpp.sql.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Throwables.throwIfUnchecked;

/**
 * QueryExecution stores all the status of a query which is being prepared or running inside the MPP
 * frame. It takes three main responsibilities: 1. Prepare a query. Transform a query from statement
 * to DistributedQueryPlan with fragment instances. 2. Dispatch all the fragment instances to
 * corresponding physical nodes. 3. Collect and monitor the progress/states of this query.
 */
public class QueryExecution implements IQueryExecution {
  private MPPQueryContext context;
  private IScheduler scheduler;
  private QueryStateMachine stateMachine;

  private List<PlanOptimizer> planOptimizers;

  private final Analysis analysis;
  private LogicalQueryPlan logicalPlan;
  private DistributedQueryPlan distributedPlan;

  private ExecutorService executor;
  private ScheduledExecutorService scheduledExecutor;

  // The result of QueryExecution will be written to the DataBlockManager in current Node.
  // We use this SourceHandle to fetch the TsBlock from it.
  private ISourceHandle resultHandle;

  public QueryExecution(
      Statement statement,
      MPPQueryContext context,
      ExecutorService executor,
      ScheduledExecutorService scheduledExecutor) {
    this.executor = executor;
    this.scheduledExecutor = scheduledExecutor;
    this.context = context;
    this.planOptimizers = new ArrayList<>();
    this.analysis = analyze(statement, context);
    this.stateMachine = new QueryStateMachine(context.getQueryId(), executor);

    // TODO: (xingtanzjr) Initialize the result handle after the DataBlockManager is merged.
    //    resultHandle = xxxx

    // We add the abort logic inside the QueryExecution.
    // So that the other components can only focus on the state change.
    stateMachine.addStateChangeListener(
        state -> {
          if (!state.isDone()) {
            return;
          }
          this.stop();
        });
  }

  public void start() {
    doLogicalPlan();
    doDistributedPlan();
    schedule();
  }

  // Analyze the statement in QueryContext. Generate the analysis this query need
  private static Analysis analyze(Statement statement, MPPQueryContext context) {
    // initialize the variable `analysis`
    return new Analyzer(context).analyze(statement);
  }

  private void schedule() {
    // TODO: (xingtanzjr) initialize the query scheduler according to configuration
    this.scheduler =
        new ClusterScheduler(
            context,
            stateMachine,
            distributedPlan.getInstances(),
            context.getQueryType(),
            executor,
            scheduledExecutor);
    // TODO: (xingtanzjr) how to make the schedule running asynchronously
    this.scheduler.start();
  }

  // Use LogicalPlanner to do the logical query plan and logical optimization
  public void doLogicalPlan() {
    LogicalPlanner planner = new LogicalPlanner(this.context, this.planOptimizers);
    this.logicalPlan = planner.plan(this.analysis);
  }

  // Generate the distributed plan and split it into fragments
  public void doDistributedPlan() {
    DistributionPlanner planner = new DistributionPlanner(this.analysis, this.logicalPlan);
    this.distributedPlan = planner.planFragments();
  }

  /** Abort the query and do cleanup work including QuerySchedule aborting and resource releasing */
  public void stop() {
    if (this.scheduler != null) {
      this.scheduler.stop();
    }
    releaseResource();
  }

  /** Release the resources that current QueryExecution hold. */
  private void releaseResource() {}

  /**
   * This method will be called by the request thread from client connection. This method will block
   * until one of these conditions occurs: 1. There is a batch of result 2. There is no more result
   * 3. The query has been cancelled 4. The query is timeout This method will fetch the result from
   * DataStreamManager use the virtual ResultOperator's ID (This part will be designed and
   * implemented with DataStreamManager)
   */
  public TsBlock getBatchResult() {
    ListenableFuture<Void> blocked = resultHandle.isBlocked();
    try {
      blocked.get();
      return resultHandle.receive();

    } catch (ExecutionException | IOException e) {
      throwIfUnchecked(e.getCause());
      throw new RuntimeException(e.getCause());
    } catch (InterruptedException e) {
      stateMachine.transitionToFailed();
      Thread.currentThread().interrupt();
      throw new RuntimeException(new SQLException("ResultSet thread was interrupted", e));
    }
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
    SettableFuture<QueryState> future = SettableFuture.create();
    stateMachine.addStateChangeListener(
        state -> {
          if (state == QueryState.RUNNING || state.isDone()) {
            future.set(state);
          }
        });

    try {
      QueryState state = future.get();
      // TODO: (xingtanzjr) use more TSStatusCode if the QueryState isn't FINISHED
      TSStatusCode statusCode =
          // For WRITE, the state should be FINISHED; For READ, the state could be RUNNING
          state == QueryState.FINISHED || state == QueryState.RUNNING
              ? TSStatusCode.SUCCESS_STATUS
              : TSStatusCode.QUERY_PROCESS_ERROR;
      return new ExecutionResult(context.getQueryId(), RpcUtils.getStatus(statusCode));
    } catch (InterruptedException | ExecutionException e) {
      // TODO: (xingtanzjr) use more accurate error handling
      return new ExecutionResult(
          context.getQueryId(), RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR));
    }
  }

  public DistributedQueryPlan getDistributedPlan() {
    return distributedPlan;
  }

  public LogicalQueryPlan getLogicalPlan() {
    return logicalPlan;
  }
}
