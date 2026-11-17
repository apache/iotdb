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

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryExecutionLifecycleTest {
  @Test
  public void stopBeforeFactoryReturnsIsDeliveredToCreatedScheduler() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch creating = new CountDownLatch(1);
    CountDownLatch allowCreation = new CountDownLatch(1);
    IScheduler scheduler = mock(IScheduler.class);
    IPlanner planner = planner();
    when(planner.createScheduler(any(), any(), any(), any()))
        .thenAnswer(
            ignored -> {
              creating.countDown();
              assertTrue(allowCreation.await(10, TimeUnit.SECONDS));
              return scheduler;
            });
    QueryExecution execution = new QueryExecution(planner, context(), executor);
    Throwable cause = new RuntimeException("stop before scheduler publication");
    try {
      Future<?> start = executor.submit(execution::start);
      assertTrue(creating.await(10, TimeUnit.SECONDS));
      execution.stop(cause);
      allowCreation.countDown();
      start.get(10, TimeUnit.SECONDS);
      execution.stop(cause);
      verify(scheduler).stop(cause);
      verify(scheduler, never()).start();
    } finally {
      allowCreation.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void asynchronousFailureStopsSchedulerBeforeStartReturns() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch stopped = new CountDownLatch(1);
    Throwable cause = new RuntimeException("non-retryable dispatch failure");
    IScheduler scheduler = mock(IScheduler.class);
    IPlanner planner = planner();
    doAnswer(
            ignored -> {
              stopped.countDown();
              return null;
            })
        .when(scheduler)
        .stop(cause);
    when(planner.createScheduler(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              QueryStateMachine state = invocation.getArgument(3);
              doAnswer(
                      ignored -> {
                        state.transitionToFailed(cause);
                        assertTrue(stopped.await(10, TimeUnit.SECONDS));
                        return null;
                      })
                  .when(scheduler)
                  .start();
              return scheduler;
            });
    QueryExecution execution = new QueryExecution(planner, context(), executor);
    try {
      execution.start();
      execution.stopAndCleanup(cause);
      verify(scheduler).stop(cause);
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void terminalCleanupPreservesAnExistingStatusOnlyFailure() throws Exception {
    ExecutorService notifications = Executors.newSingleThreadExecutor();
    IPlanner planner = planner();
    org.apache.iotdb.common.rpc.thrift.TSStatus original =
        new org.apache.iotdb.common.rpc.thrift.TSStatus(
            org.apache.iotdb.rpc.TSStatusCode.QUERY_EXECUTION_MEMORY_NOT_ENOUGH.getStatusCode());
    original.setMessage("original failure");
    when(planner.createScheduler(any(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              QueryStateMachine state = invocation.getArgument(3);
              IScheduler scheduler = mock(IScheduler.class);
              doAnswer(
                      ignored -> {
                        state.transitionToFailed(original);
                        return null;
                      })
                  .when(scheduler)
                  .start();
              return scheduler;
            });
    QueryExecution execution = new QueryExecution(planner, context(), notifications);
    try {
      execution.start();
      execution.stopAndCleanup(
          new org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException(0, 100, 1));
      org.junit.Assert.assertEquals(original, execution.getStatus().status);
    } finally {
      execution.stopAndCleanup(null);
      notifications.shutdownNow();
      assertTrue(notifications.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static IPlanner planner() {
    IPlanner planner = mock(IPlanner.class);
    when(planner.analyze(any())).thenReturn(mock(IAnalysis.class));
    when(planner.doLogicalPlan(any(), any())).thenReturn(mock(LogicalQueryPlan.class));
    DistributedQueryPlan plan = mock(DistributedQueryPlan.class);
    when(plan.getInstances()).thenReturn(Collections.emptyList());
    when(planner.doDistributionPlan(any(), any(), any())).thenReturn(plan);
    return planner;
  }

  private static MPPQueryContext context() {
    MPPQueryContext context = new MPPQueryContext(new QueryId("scheduler_publication"));
    context.setQueryType(QueryType.OTHER);
    return context;
  }
}
