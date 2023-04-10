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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.MPPQueryContext;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.execution.QueryStateMachine;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.mpp.metric.QueryMetricsManager;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.db.mpp.metric.QueryExecutionMetricSet.WAIT_FOR_DISPATCH;

/**
 * QueryScheduler is used to dispatch the fragment instances of a query to target nodes. And it will
 * continue to collect and monitor the query execution before the query is finished.
 *
 * <p>Later, we can add more control logic for a QueryExecution such as retry, kill and so on by
 * this scheduler.
 */
public class ClusterScheduler implements IScheduler {
  private static final Logger logger = LoggerFactory.getLogger(ClusterScheduler.class);

  // The stateMachine of the QueryExecution owned by this QueryScheduler
  private final QueryStateMachine stateMachine;
  private final QueryType queryType;
  // The fragment instances which should be sent to corresponding Nodes.
  private final List<FragmentInstance> instances;

  private final IFragInstanceDispatcher dispatcher;
  private IFragInstanceStateTracker stateTracker;
  private IQueryTerminator queryTerminator;

  private static final QueryMetricsManager QUERY_METRICS = QueryMetricsManager.getInstance();

  public ClusterScheduler(
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      List<FragmentInstance> instances,
      QueryType queryType,
      ExecutorService executor,
      ExecutorService writeOperationExecutor,
      ScheduledExecutorService scheduledExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    this.stateMachine = stateMachine;
    this.instances = instances;
    this.queryType = queryType;
    this.dispatcher =
        new FragmentInstanceDispatcherImpl(
            queryType,
            queryContext,
            executor,
            writeOperationExecutor,
            syncInternalServiceClientManager,
            asyncInternalServiceClientManager);
    if (queryType == QueryType.READ) {
      this.stateTracker =
          new FixedRateFragInsStateTracker(
              stateMachine, scheduledExecutor, instances, syncInternalServiceClientManager);
      this.queryTerminator =
          new SimpleQueryTerminator(
              scheduledExecutor,
              queryContext,
              instances,
              syncInternalServiceClientManager,
              stateTracker);
    }
  }

  private boolean needRetry(TSStatus failureStatus) {
    return failureStatus != null
        && queryType == QueryType.READ
        && failureStatus.getCode() == TSStatusCode.DISPATCH_ERROR.getStatusCode();
  }

  @Override
  public void start() {
    stateMachine.transitionToDispatching();
    long startTime = System.nanoTime();
    Future<FragInstanceDispatchResult> dispatchResultFuture = dispatcher.dispatch(instances);

    // NOTICE: the FragmentInstance may be dispatched to another Host due to consensus redirect.
    // So we need to start the state fetcher after the dispatching stage.
    try {
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      if (!result.isSuccessful()) {
        if (needRetry(result.getFailureStatus())) {
          stateMachine.transitionToPendingRetry(result.getFailureStatus());
        } else {
          stateMachine.transitionToFailed(result.getFailureStatus());
        }
        return;
      }
    } catch (InterruptedException | ExecutionException e) {
      // If the dispatch request cannot be sent or TException is caught, we will retry this query.
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      stateMachine.transitionToFailed(e);
      return;
    } finally {
      QUERY_METRICS.recordExecutionCost(WAIT_FOR_DISPATCH, System.nanoTime() - startTime);
    }

    // For the FragmentInstance of WRITE, it will be executed directly when dispatching.
    if (queryType == QueryType.WRITE) {
      stateMachine.transitionToFinished();
      return;
    }

    // The FragmentInstances has been dispatched successfully to corresponding host, we mark the
    // QueryState to Running
    stateMachine.transitionToRunning();

    // TODO: (xingtanzjr) start the stateFetcher/heartbeat for each fragment instance
    this.stateTracker.start();
    logger.debug("state tracker starts");
  }

  @Override
  public void stop() {
    // TODO: It seems that it is unnecessary to check whether they are null or not. Is it a best
    // practice ?
    dispatcher.abort();
    if (stateTracker != null) {
      stateTracker.abort();
    }
    // TODO: (xingtanzjr) handle the exception when the termination cannot succeed
    if (queryTerminator != null) {
      queryTerminator.terminate();
    }
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
