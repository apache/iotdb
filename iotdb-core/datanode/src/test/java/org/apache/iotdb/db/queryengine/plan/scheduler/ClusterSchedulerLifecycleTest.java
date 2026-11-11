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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterSchedulerLifecycleTest {
  private final QueryStateMachine state =
      new QueryStateMachine(
          new QueryId("scheduler_test"),
          com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService());
  private final IFragInstanceDispatcher dispatcher = mock(IFragInstanceDispatcher.class);
  private final IFragInstanceStateTracker tracker = mock(IFragInstanceStateTracker.class);
  private final IQueryTerminator terminator = mock(IQueryTerminator.class);
  private final ClusterScheduler scheduler =
      new ClusterScheduler(state, dispatcher, tracker, terminator);

  @Test
  public void stopBeforeStartPreventsDispatchAndTerminatesOnce() {
    Throwable cause = new RuntimeException("stopped before dispatch");
    scheduler.stop(cause);
    scheduler.start();
    scheduler.stop(cause);
    verify(dispatcher).abort();
    verify(dispatcher, never()).dispatch(any(), any());
    verify(terminator).terminateImmediately(cause);
    verify(tracker, never()).start();
  }

  @Test
  public void stopDuringDispatchDoesNotWaitForDispatchOrStartTracker() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch dispatching = new CountDownLatch(1);
    CountDownLatch finishDispatch = new CountDownLatch(1);
    when(dispatcher.dispatch(any(), any()))
        .thenAnswer(
            ignored -> {
              dispatching.countDown();
              assertTrue(finishDispatch.await(10, TimeUnit.SECONDS));
              return immediateFuture(new FragInstanceDispatchResult(true));
            });
    try {
      Future<?> start = executor.submit(scheduler::start);
      assertTrue(dispatching.await(10, TimeUnit.SECONDS));
      Throwable cause = new RuntimeException("stopped during dispatch");
      scheduler.stop(cause);
      verify(dispatcher).abort();
      verify(terminator).terminateImmediately(cause);
      finishDispatch.countDown();
      start.get(10, TimeUnit.SECONDS);
      verify(tracker, never()).start();
    } finally {
      finishDispatch.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void nonRetryableDispatchFailureTerminatesImmediately() {
    when(dispatcher.dispatch(any(), any()))
        .thenReturn(
            immediateFuture(
                new FragInstanceDispatchResult(
                    new TSStatus(TSStatusCode.QUERY_EXECUTION_MEMORY_NOT_ENOUGH.getStatusCode()))));
    scheduler.start();
    Throwable cause = new RuntimeException("dispatch failed");
    scheduler.stop(cause);
    verify(terminator).terminateImmediately(cause);
  }

  @Test
  public void normalCompletionKeepsGracePeriod() {
    when(dispatcher.dispatch(any(), any()))
        .thenReturn(immediateFuture(new FragInstanceDispatchResult(true)));
    scheduler.start();
    scheduler.stop(null);
    verify(terminator).terminate(null);
    verify(terminator, never()).terminateImmediately(any());
  }
}
