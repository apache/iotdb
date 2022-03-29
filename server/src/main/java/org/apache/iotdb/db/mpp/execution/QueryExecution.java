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

import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.execution.scheduler.ClusterScheduler;
import org.apache.iotdb.db.mpp.execution.scheduler.IScheduler;
import org.apache.iotdb.db.mpp.sql.analyze.Analysis;
import org.apache.iotdb.db.mpp.sql.analyze.Analyzer;
import org.apache.iotdb.db.mpp.sql.optimization.PlanOptimizer;
import org.apache.iotdb.db.mpp.sql.planner.DistributionPlanner;
import org.apache.iotdb.db.mpp.sql.planner.LogicalPlanner;
import org.apache.iotdb.db.mpp.sql.planner.plan.*;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.rpc.RpcUtils.getStatus;

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
  private List<FragmentInstance> fragmentInstances;

  public QueryExecution(Statement statement, MPPQueryContext context) {
    this.context = context;
    this.planOptimizers = new ArrayList<>();
    this.analysis = analyze(statement, context);
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
    this.scheduler = new ClusterScheduler(this.stateMachine, this.fragmentInstances);
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

  /**
   * This method will be called by the request thread from client connection. This method will block
   * until one of these conditions occurs: 1. There is a batch of result 2. There is no more result
   * 3. The query has been cancelled 4. The query is timeout This method will fetch the result from
   * DataStreamManager use the virtual ResultOperator's ID (This part will be designed and
   * implemented with DataStreamManager)
   */
  public ByteBuffer getBatchResult() {
    return null;
  }

  public ExecutionResult getResult() {

    return new ExecutionResult(context.getQueryId(), getStatus(TSStatusCode.SUCCESS_STATUS));
  }

  public DistributedQueryPlan getDistributedPlan() {
    return distributedPlan;
  }

  public LogicalQueryPlan getLogicalPlan() {
    return logicalPlan;
  }

  public List<FragmentInstance> getFragmentInstances() {
    return fragmentInstances;
  }
}
