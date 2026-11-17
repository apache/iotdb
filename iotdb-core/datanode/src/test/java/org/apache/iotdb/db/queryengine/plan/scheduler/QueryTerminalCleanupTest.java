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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryTerminalCleanupTest {
  @Test
  public void staleQueryCleanupDuringDispatchMustUnblockGetStatus() throws Exception {
    assertTerminalCleanup(
        new QueryTimeoutRuntimeException(0, 120_001, 60_000),
        QueryState.FAILED,
        TSStatusCode.QUERY_TIMEOUT.getStatusCode());
  }

  @Test
  public void nullCauseCleanupDuringDispatchAbortsAndUnblocksGetStatus() throws Exception {
    assertTerminalCleanup(
        null, QueryState.ABORTED, TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
  }

  private void assertTerminalCleanup(Throwable cause, QueryState expectedState, int expectedStatus)
      throws Exception {
    ExecutorService notifications = Executors.newSingleThreadExecutor();
    ExecutorService rpc = Executors.newSingleThreadExecutor();
    ExecutorService statusWaiter = Executors.newSingleThreadExecutor();
    CountDownLatch dispatchStarted = new CountDownLatch(1);
    CountDownLatch finishDispatch = new CountDownLatch(1);
    AtomicReference<QueryStateMachine> state = new AtomicReference<>();
    IFragInstanceDispatcher dispatcher = mock(IFragInstanceDispatcher.class);
    when(dispatcher.dispatch(any(), any()))
        .thenAnswer(
            ignored -> {
              dispatchStarted.countDown();
              assertTrue(finishDispatch.await(10, TimeUnit.SECONDS));
              return immediateFuture(new FragInstanceDispatchResult(true));
            });
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(any())).thenReturn(mock(IAnalysis.class));
    when(planner.doLogicalPlan(any(), any())).thenReturn(mock(LogicalQueryPlan.class));
    DistributedQueryPlan plan = mock(DistributedQueryPlan.class);
    when(plan.getInstances()).thenReturn(Collections.emptyList());
    when(planner.doDistributionPlan(any(), any(), any())).thenReturn(plan);
    when(planner.createScheduler(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              QueryStateMachine machine = invocation.getArgument(3);
              state.set(machine);
              return new ClusterScheduler(
                  machine,
                  dispatcher,
                  mock(IFragInstanceStateTracker.class),
                  mock(IQueryTerminator.class));
            });
    QueryId queryId = new QueryId("stale_cleanup_during_dispatch");
    TEndPoint localExchange = new TEndPoint("127.0.0.1", 10740);
    MPPQueryContext context =
        new MPPQueryContext(
            "", queryId, mock(SessionInfo.class), localExchange, new TEndPoint("127.0.0.1", 10730));
    context.setQueryType(QueryType.READ);
    context.setStartTime(System.currentTimeMillis());
    context.setTimeOut(60_000L);
    context
        .getResultNodeContext()
        .setUpStream(
            localExchange,
            queryId.genPlanFragmentId().genFragmentInstanceId(),
            new PlanNodeId("upstream"));
    QueryExecution execution = new QueryExecution(planner, context, notifications);
    Future<ExecutionResult> result = null;
    try {
      result =
          rpc.submit(
              () -> {
                execution.start();
                return execution.getStatus();
              });
      assertTrue(dispatchStarted.await(10, TimeUnit.SECONDS));
      assertEquals(QueryState.DISPATCHING, state.get().getState());
      // Coordinator.cleanUpStaleQueries calls stopAndCleanup directly, without cancel().
      execution.stopAndCleanup(cause);
      // Status waiters must wake even before the in-flight dispatch returns.
      assertEquals(
          expectedStatus,
          statusWaiter.submit(execution::getStatus).get(10, TimeUnit.SECONDS).status.getCode());
      finishDispatch.countDown();
      assertEquals(expectedStatus, result.get(10, TimeUnit.SECONDS).status.getCode());
      assertEquals(expectedState, state.get().getState());
    } finally {
      finishDispatch.countDown();
      execution.stopAndCleanup(null);
      if (result != null) {
        result.cancel(true);
      }
      statusWaiter.shutdownNow();
      assertTrue(statusWaiter.awaitTermination(10, TimeUnit.SECONDS));
      rpc.shutdownNow();
      notifications.shutdownNow();
      assertTrue(rpc.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(notifications.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
