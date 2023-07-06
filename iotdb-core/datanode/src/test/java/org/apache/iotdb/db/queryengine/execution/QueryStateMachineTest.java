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

package org.apache.iotdb.db.queryengine.execution;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryStateMachineTest {

  @Test
  public void TestBasicTransition() {
    QueryStateMachine stateMachine = genQueryStateMachine();
    Assert.assertEquals(QueryState.QUEUED, stateMachine.getState());
    stateMachine.transitionToPlanned();
    Assert.assertEquals(QueryState.PLANNED, stateMachine.getState());
    stateMachine.transitionToQueued();
    Assert.assertEquals(QueryState.QUEUED, stateMachine.getState());
    stateMachine.transitionToPlanned();
    Assert.assertEquals(QueryState.PLANNED, stateMachine.getState());
    stateMachine.transitionToDispatching();
    Assert.assertEquals(QueryState.DISPATCHING, stateMachine.getState());
    stateMachine.transitionToRunning();
    Assert.assertEquals(QueryState.RUNNING, stateMachine.getState());
    stateMachine.transitionToAborted();
    Assert.assertEquals(QueryState.ABORTED, stateMachine.getState());

    // StateMachine with Terminal State is not allowed to transition state
    stateMachine = genQueryStateMachine();
    stateMachine.transitionToCanceled();
    Assert.assertEquals(QueryState.CANCELED, stateMachine.getState());

    stateMachine = genQueryStateMachine();
    stateMachine.transitionToFinished();
    Assert.assertEquals(QueryState.FINISHED, stateMachine.getState());

    stateMachine = genQueryStateMachine();
    stateMachine.transitionToFailed();
    Assert.assertEquals(QueryState.FAILED, stateMachine.getState());

    stateMachine = genQueryStateMachine();
    TSStatus status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
    status.setMessage("Unknown");
    stateMachine.transitionToFailed(status);
    Assert.assertEquals(QueryState.FAILED, stateMachine.getState());
    assertEquals(
        "no detailed failure reason in QueryStateMachine", stateMachine.getFailureMessage());
    Throwable t = stateMachine.getFailureException();
    assertTrue(t instanceof IoTDBException);
    assertEquals(
        TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode(), ((IoTDBException) t).getErrorCode());
    assertEquals("Unknown", t.getMessage());
  }

  @Test
  public void testFailure() {
    QueryStateMachine stateMachine = genQueryStateMachine();
    stateMachine.transitionToDispatching();
    assertNotEquals(QueryState.DISPATCHING, stateMachine.getState());
    assertEquals(QueryState.QUEUED, stateMachine.getState());
    stateMachine.transitionToPlanned();
    assertEquals(QueryState.PLANNED, stateMachine.getState());
    RuntimeException expected = new RuntimeException("a");
    stateMachine.transitionToCanceled(
        expected, new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode()));
    assertEquals(QueryState.CANCELED, stateMachine.getState());
    assertEquals(
        TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode(), stateMachine.getFailureStatus().code);
    assertEquals(expected, stateMachine.getFailureException());
    assertEquals("a", stateMachine.getFailureMessage());

    try {
      stateMachine.transitionToQueued();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("test_query cannot transition from CANCELED to QUEUED", e.getMessage());
      assertNotEquals(QueryState.QUEUED, stateMachine.getState());
    }
  }

  @Test
  public void TestListener() throws ExecutionException, InterruptedException {
    AtomicInteger stateChangeCounter = new AtomicInteger(0);
    QueryStateMachine stateMachine = genQueryStateMachine();
    stateMachine.addStateChangeListener(state -> stateChangeCounter.getAndIncrement());
    stateMachine.transitionToFinished();
    SettableFuture<QueryState> future = SettableFuture.create();
    stateMachine.addStateChangeListener(
        state -> {
          if (state == QueryState.FINISHED) {
            future.set(QueryState.FINISHED);
          }
        });
    future.get();
    Assert.assertEquals(2, stateChangeCounter.get());
  }

  @Test
  public void TestGetStateChange() throws ExecutionException, InterruptedException {
    AtomicInteger stateChangeCounter = new AtomicInteger(0);
    QueryStateMachine stateMachine = genQueryStateMachine();
    SettableFuture<Void> callbackFuture = SettableFuture.create();
    ListenableFuture<QueryState> future = stateMachine.getStateChange(QueryState.QUEUED);
    future.addListener(
        () -> {
          stateChangeCounter.getAndIncrement();
          callbackFuture.set(null);
        },
        directExecutor());
    Assert.assertEquals(0, stateChangeCounter.get());
    stateMachine.transitionToRunning();
    future.get();
    callbackFuture.get();
    Assert.assertEquals(1, stateChangeCounter.get());
  }

  @Test
  public void testTransitionToPendingRetry() {
    QueryStateMachine stateMachine = genQueryStateMachine();
    stateMachine.transitionToPlanned();
    stateMachine.transitionToPendingRetry(
        new TSStatus(TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode()));
    assertEquals(
        TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode(),
        stateMachine.getFailureStatus().code);
  }

  private QueryStateMachine genQueryStateMachine() {
    return new QueryStateMachine(
        genQueryId(), IoTDBThreadPoolFactory.newSingleThreadExecutor("TestQueryStateMachine"));
  }

  private QueryId genQueryId() {
    return new QueryId("test_query");
  }
}
