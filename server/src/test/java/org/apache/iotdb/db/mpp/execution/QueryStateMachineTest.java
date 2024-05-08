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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class QueryStateMachineTest {

  @Test
  public void TestBasicTransition() {
    QueryStateMachine stateMachine = genQueryStateMachine();
    Assert.assertEquals(stateMachine.getState(), QueryState.QUEUED);
    stateMachine.transitionToDispatching();
    Assert.assertEquals(stateMachine.getState(), QueryState.DISPATCHING);
    stateMachine.transitionToRunning();
    Assert.assertEquals(stateMachine.getState(), QueryState.RUNNING);
    stateMachine.transitionToAborted();
    Assert.assertEquals(stateMachine.getState(), QueryState.ABORTED);

    // StateMachine with Terminal State is not allowed to transition state
    stateMachine = genQueryStateMachine();
    stateMachine.transitionToCanceled();
    Assert.assertEquals(stateMachine.getState(), QueryState.CANCELED);

    stateMachine = genQueryStateMachine();
    stateMachine.transitionToFinished();
    Assert.assertEquals(stateMachine.getState(), QueryState.FINISHED);
  }

  @Test
  public void TestListener() throws ExecutionException, InterruptedException {
    AtomicInteger stateChangeCounter = new AtomicInteger(0);
    QueryStateMachine stateMachine = genQueryStateMachine();
    stateMachine.addStateChangeListener(
        state -> {
          stateChangeCounter.getAndIncrement();
        });
    stateMachine.transitionToFinished();
    SettableFuture<QueryState> future = SettableFuture.create();
    stateMachine.addStateChangeListener(
        state -> {
          if (state == QueryState.FINISHED) {
            future.set(QueryState.FINISHED);
          }
        });
    future.get();
    Assert.assertEquals(stateChangeCounter.get(), 2);
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
    Assert.assertEquals(stateChangeCounter.get(), 0);
    stateMachine.transitionToRunning();
    future.get();
    callbackFuture.get();
    Assert.assertEquals(stateChangeCounter.get(), 1);
  }

  private QueryStateMachine genQueryStateMachine() {
    return new QueryStateMachine(
        genQueryId(), IoTDBThreadPoolFactory.newSingleThreadExecutor("TestQueryStateMachine"));
  }

  private List<FragmentInstanceId> genFragmentInstanceIdList() {
    return Arrays.asList(
        new FragmentInstanceId(new PlanFragmentId(genQueryId(), 1), "1"),
        new FragmentInstanceId(new PlanFragmentId(genQueryId(), 2), "1"),
        new FragmentInstanceId(new PlanFragmentId(genQueryId(), 3), "1"),
        new FragmentInstanceId(new PlanFragmentId(genQueryId(), 4), "1"),
        new FragmentInstanceId(new PlanFragmentId(genQueryId(), 4), "2"));
  }

  private QueryId genQueryId() {
    return new QueryId("test_query");
  }
}
