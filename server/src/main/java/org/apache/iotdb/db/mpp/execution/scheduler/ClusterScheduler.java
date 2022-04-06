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
package org.apache.iotdb.db.mpp.execution.scheduler;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.FragmentInfo;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceState;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.sql.analyze.QueryType;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/**
 * QueryScheduler is used to dispatch the fragment instances of a query to target nodes. And it will
 * continue to collect and monitor the query execution before the query is finished.
 *
 * <p>Later, we can add more control logic for a QueryExecution such as retry, kill and so on by
 * this scheduler.
 */
public class ClusterScheduler implements IScheduler {
  private MPPQueryContext queryContext;
  // The stateMachine of the QueryExecution owned by this QueryScheduler
  private QueryStateMachine stateMachine;
  private QueryType queryType;
  // The fragment instances which should be sent to corresponding Nodes.
  private List<FragmentInstance> instances;

  private ExecutorService executor;
  private ScheduledExecutorService scheduledExecutor;

  private IFragInstanceDispatcher dispatcher;
  private IFragInstanceStateTracker stateTracker;
  private IQueryTerminator queryTerminator;

  public ClusterScheduler(
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      List<FragmentInstance> instances,
      QueryType queryType,
      ExecutorService executor,
      ScheduledExecutorService scheduledExecutor) {
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.instances = instances;
    this.queryType = queryType;
    this.executor = executor;
    this.scheduledExecutor = scheduledExecutor;
    this.dispatcher = new SimpleFragInstanceDispatcher(executor);
    this.stateTracker =
        new FixedRateFragInsStateTracker(stateMachine, executor, scheduledExecutor, instances);
    this.queryTerminator = new SimpleQueryTerminator(executor, queryContext.getQueryId(), instances);
  }

  @Override
  public void start() {
    // TODO: consider where the state transition should be put
    stateMachine.transitionToDispatching();
    Future<FragInstanceDispatchResult> dispatchResultFuture = dispatcher.dispatch(instances);

    // NOTICE: the FragmentInstance may be dispatched to another Host due to consensus redirect.
    // So we need to start the state fetcher after the dispatching stage.
    boolean success = waitDispatchingFinished(dispatchResultFuture);
    // If the dispatch failed, we make the QueryState as failed, and return.
    if (!success) {
      stateMachine.transitionToFailed();
      return;
    }

    // For the FragmentInstance of WRITE, it will be executed directly when dispatching.
    if (queryType == QueryType.WRITE) {
      stateMachine.transitionToFinished();
      return;
    }

    // The FragmentInstances has been dispatched successfully to corresponding host, we mark the
    // QueryState to Running
    stateMachine.transitionToRunning();
    instances.forEach(
        instance -> {
          stateMachine.initialFragInstanceState(instance.getId(), FragmentInstanceState.RUNNING);
        });

    // TODO: (xingtanzjr) start the stateFetcher/heartbeat for each fragment instance
    this.stateTracker.start();
  }

  private boolean waitDispatchingFinished(Future<FragInstanceDispatchResult> dispatchResultFuture) {
    try {
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      if (result.isSuccessful()) {
        return true;
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO: (xingtanzjr) record the dispatch failure reason.
    }
    return false;
  }

  @Override
  public void abort() {
    // TODO: It seems that it is unnecessary to check whether they are null or not. Is it a best
    // practice ?
    dispatcher.abort();
    stateTracker.abort();
    // TODO: (xingtanzjr) handle the exception when the termination cannot succeed
    queryTerminator.terminate();
  }

  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }

  @Override
  public void abortFragmentInstance(FragmentInstanceId instanceId, Throwable failureCause) {}

  @Override
  public void cancelFragment(PlanFragmentId planFragmentId) {}

  // Send the instances to other nodes
  private void sendFragmentInstances() {}

  // After sending, start to collect the states of these fragment instances
  private void startMonitorInstances() {}
}
