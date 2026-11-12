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
package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.calc.metric.QueryExecutionMetricSet;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.iotdb.calc.metric.QueryExecutionMetricSet.WAIT_FOR_DISPATCH;

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
  private final SubPlan rootSubPlan;
  // The fragment instances which should be sent to corresponding Nodes.
  private final List<FragmentInstance> instances;

  private final IFragInstanceDispatcher dispatcher;
  private IFragInstanceStateTracker stateTracker;
  private IQueryTerminator queryTerminator;
  // Set when the dispatch of the fragment instances failed (see start()).
  private volatile boolean dispatchFailed = false;
  private boolean started;
  private boolean stopped;
  private boolean dispatchComplete;

  private static final QueryExecutionMetricSet QUERY_EXECUTION_METRICS =
      QueryExecutionMetricSet.getInstance();

  public ClusterScheduler(
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      DistributedQueryPlan distributedQueryPlan,
      QueryType queryType,
      ScheduledExecutorService scheduledExecutor,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncInternalServiceClientManager,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
          asyncInternalServiceClientManager) {
    this.stateMachine = stateMachine;
    this.rootSubPlan = distributedQueryPlan.getRootSubPlan();
    this.instances = distributedQueryPlan.getInstances();
    this.queryType = queryType;
    FragmentInstanceDispatcherImpl fragmentDispatcher =
        new FragmentInstanceDispatcherImpl(
            queryType,
            queryContext,
            syncInternalServiceClientManager,
            asyncInternalServiceClientManager);
    this.dispatcher = fragmentDispatcher;
    if (isQuery()) {
      this.stateTracker =
          new FixedRateFragInsStateTracker(
              stateMachine, scheduledExecutor, instances, syncInternalServiceClientManager);
      this.queryTerminator =
          new SimpleQueryTerminator(
              scheduledExecutor,
              queryContext,
              instances,
              syncInternalServiceClientManager,
              stateTracker,
              fragmentDispatcher::getIssuedAdmissionTokens);
    }
  }

  ClusterScheduler(
      QueryStateMachine stateMachine,
      IFragInstanceDispatcher dispatcher,
      IFragInstanceStateTracker stateTracker,
      IQueryTerminator queryTerminator) {
    this.stateMachine = stateMachine;
    this.queryType = QueryType.READ;
    this.rootSubPlan = null;
    this.instances = java.util.Collections.emptyList();
    this.dispatcher = dispatcher;
    this.stateTracker = stateTracker;
    this.queryTerminator = queryTerminator;
  }

  private boolean needRetry(TSStatus failureStatus) {
    return failureStatus != null
        && isQuery()
        && (failureStatus.getCode() == TSStatusCode.DISPATCH_ERROR.getStatusCode()
            || failureStatus.getCode()
                == TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode());
  }

  @Override
  public void start() {
    synchronized (this) {
      if (started || stopped) {
        return;
      }
      started = true;
    }
    stateMachine.transitionToDispatching();
    long startTime = System.nanoTime();

    // NOTICE: the FragmentInstance may be dispatched to another Host due to consensus redirect.
    // So we need to start the state fetcher after the dispatching stage.
    try {
      Future<FragInstanceDispatchResult> dispatchResultFuture =
          dispatcher.dispatch(rootSubPlan, instances);
      FragInstanceDispatchResult result = dispatchResultFuture.get();
      synchronized (this) {
        if (stopped) {
          return;
        }
        dispatchComplete = true;
        dispatchFailed = !result.isSuccessful();
      }
      if (!result.isSuccessful()) {
        if (needRetry(result.getFailureStatus())) {
          stateMachine.transitionToPendingRetry(result.getFailureStatus());
        } else {
          stateMachine.transitionToFailed(
              result.getFailureStatus(), result.isFailureCausedByInterruption());
        }
        return;
      }
    } catch (InterruptedException | ExecutionException | RuntimeException e) {
      // If the dispatch request cannot be sent or TException is caught, we will retry this query.
      dispatchFailed = true;
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      stateMachine.transitionToFailed(e);
      return;
    } finally {
      QUERY_EXECUTION_METRICS.recordExecutionCost(WAIT_FOR_DISPATCH, System.nanoTime() - startTime);
    }

    // For the FragmentInstance of non-query, it will be executed directly when dispatching.
    if (!isQuery()) {
      stateMachine.transitionToFinished();
      return;
    }

    // The FragmentInstances has been dispatched successfully to corresponding host, we mark the
    // QueryState to Running
    stateMachine.transitionToRunning();

    this.stateTracker.start();
    logger.debug(DataNodeQueryMessages.STATE_TRACKER_STARTS);
  }

  @Override
  public void stop(Throwable t) {
    boolean terminateImmediately;
    synchronized (this) {
      if (stopped) {
        return;
      }
      stopped = true;
      terminateImmediately = dispatchFailed || !dispatchComplete;
    }
    dispatcher.abort();
    if (stateTracker != null) {
      stateTracker.abort();
    }
    if (queryTerminator != null) {
      if (terminateImmediately) {
        // Dispatch failed, or stop arrived before dispatch completed. The FIs already sent may
        // depend on unsent FIs and hold operator memory, so do not wait for the grace period.
        queryTerminator.terminateImmediately(t);
      } else {
        queryTerminator.terminate(t);
      }
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

  private boolean isQuery() {
    return queryType == QueryType.READ || queryType == QueryType.READ_WRITE;
  }
}
