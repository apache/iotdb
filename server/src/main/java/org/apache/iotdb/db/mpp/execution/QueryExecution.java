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

import com.google.common.util.concurrent.ListenableFuture;
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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * QueryExecution stores all the status of a query which is being prepared or running inside the MPP
 * frame. It takes three main responsibilities: 1. Prepare a query. Transform a query from statement
 * to DistributedQueryPlan with fragment instances. 2. Dispatch all the fragment instances to
 * corresponding physical nodes. 3. Collect and monitor the progress/states of this query.
 */
public class QueryExecution {
  private MPPQueryContext context;
  private IScheduler scheduler;
  private QueryStateMachine stateMachine;

  private List<PlanOptimizer> planOptimizers;

  private final Analysis analysis;
  private LogicalQueryPlan logicalPlan;
  private DistributedQueryPlan distributedPlan;

  public QueryExecution(Statement statement, MPPQueryContext context) {
    this.context = context;
    this.analysis = analyze(statement, context);
    this.stateMachine = new QueryStateMachine(context.getQueryId());

    // We add the abort logic inside the QueryExecution.
    // So that the other components can only focus on the state change.
    stateMachine.addStateChangeListener(state -> {
      if (!state.isDone()) {
        return;
      }
      this.cleanup();
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
    this.scheduler = new ClusterScheduler(stateMachine, distributedPlan.getInstances(), context.getQueryType());
    // TODO: (xingtanzjr) how to make the schedule running asynchronously
    this.scheduler.start();
  }

  // Use LogicalPlanner to do the logical query plan and logical optimization
  private void doLogicalPlan() {
    LogicalPlanner planner = new LogicalPlanner(this.context, this.planOptimizers);
    this.logicalPlan = planner.plan(this.analysis);
  }

  // Generate the distributed plan and split it into fragments
  private void doDistributedPlan() {
    DistributionPlanner planner = new DistributionPlanner(this.analysis, this.logicalPlan);
    this.distributedPlan = planner.planFragments();
  }

  /**
   * Do cleanup work for current QueryExecution including QuerySchedule aborting and resource releasing
   */
  private void cleanup() {
    if (this.scheduler != null) {
      this.scheduler.abort();
    }
    releaseResource();
  }

  /**
   * Release the resources that current QueryExecution hold.
   */
  private void releaseResource() {

  }

  /**
   * This method will be called by the request thread from client connection. This method will block
   * until one of these conditions occurs: 1. There is a batch of result 2. There is no more result
   * 3. The query has been cancelled 4. The query is timeout This method wil
   * l fetch the result from
   * DataStreamManager use the virtual ResultOperator's ID (This part will be designed and
   * implemented with DataStreamManager)
   */
  public ByteBuffer getBatchResult() {
    return null;
  }

  /**
   * This method is a synchronized method.
   * For READ, it will block until all the FragmentInstances have been submitted.
   * For WRITE, it will block until all the FragmentInstances have finished.
   * @return ExecutionStatus. Contains the QueryId and the TSStatus.
   */
  public ExecutionStatus getStatus() {
    // Although we monitor the state to transition to RUNNING, the future will return if any Terminated state is triggered
    ListenableFuture<QueryState> future =  stateMachine.getStateChange(QueryState.RUNNING);
    try {
      QueryState state = future.get();
      // TODO: (xingtanzjr) use more TSStatusCode if the QueryState isn't FINISHED
      TSStatusCode statusCode = state == QueryState.FINISHED ? TSStatusCode.SUCCESS_STATUS : TSStatusCode.QUERY_PROCESS_ERROR;
      return new ExecutionStatus(context.getQueryId(), RpcUtils.getStatus(statusCode));
    } catch (InterruptedException | ExecutionException e) {
      // TODO: (xingtanzjr) use more accurate error handling
      return new ExecutionStatus(context.getQueryId(), RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR));
    }
  }
}
