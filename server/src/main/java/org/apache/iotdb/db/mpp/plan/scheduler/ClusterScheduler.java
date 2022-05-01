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
package org.apache.iotdb.db.mpp.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterScheduler.class);

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
      ScheduledExecutorService scheduledExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager) {
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.instances = instances;
    this.queryType = queryType;
    this.executor = executor;
    this.scheduledExecutor = scheduledExecutor;
    this.dispatcher = new SimpleFragInstanceDispatcher(executor, internalServiceClientManager);
    this.stateTracker =
        new FixedRateFragInsStateTracker(
            stateMachine, executor, scheduledExecutor, instances, internalServiceClientManager);
    this.queryTerminator =
        new SimpleQueryTerminator(
            executor, queryContext.getQueryId(), instances, internalServiceClientManager);
  }

  @Override
  public void start() {
    stateMachine.transitionToDispatching();
    Future<FragInstanceDispatchResult> dispatchResultFuture = dispatcher.dispatch(instances);

    // NOTICE: the FragmentInstance may be dispatched to another Host due to consensus redirect.
    // So we need to start the state fetcher after the dispatching stage.
    try {
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      if (!result.isSuccessful()) {
        stateMachine.transitionToFailed(new IllegalStateException("Fragment cannot be dispatched"));
        return;
      }
    } catch (InterruptedException | ExecutionException e) {
      // If the dispatch failed, we make the QueryState as failed, and return.
      Thread.currentThread().interrupt();
      stateMachine.transitionToFailed(e);
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

  @Override
  public void stop() {
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
