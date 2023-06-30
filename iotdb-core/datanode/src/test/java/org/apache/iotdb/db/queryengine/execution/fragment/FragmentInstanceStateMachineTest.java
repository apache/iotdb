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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.common.QueryId.MOCK_QUERY_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FragmentInstanceStateMachineTest {

  @Test
  public void testToFinished() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
      long time1 = System.currentTimeMillis();
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      long time2 = System.currentTimeMillis();

      assertEquals(FragmentInstanceState.RUNNING, stateMachine.getState());
      assertTrue(stateMachine.getCreatedTime() >= time1 && stateMachine.getCreatedTime() <= time2);

      assertEquals(instanceId, stateMachine.getFragmentInstanceId());

      ListenableFuture<FragmentInstanceState> future =
          stateMachine.getStateChange(FragmentInstanceState.RUNNING);

      stateMachine.transitionToFlushing();
      assertEquals(FragmentInstanceState.FLUSHING, stateMachine.getState());
      assertEquals(FragmentInstanceState.FLUSHING, future.get());

      stateMachine.addSourceTaskFailureListener(
          Mockito.mock(FragmentInstanceFailureListener.class));
      stateMachine.sourceTaskFailed(instanceId, new RuntimeException("Unknown"));

      stateMachine.finished();
      assertEquals(FragmentInstanceState.FINISHED, stateMachine.getState());

    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testToCancel() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

      assertEquals(FragmentInstanceState.RUNNING, stateMachine.getState());

      stateMachine.cancel();

      assertEquals(FragmentInstanceState.CANCELLED, stateMachine.getState());

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }

  @Test
  public void testToAbort() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    try {
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

      assertEquals(FragmentInstanceState.RUNNING, stateMachine.getState());

      stateMachine.abort();

      assertEquals(FragmentInstanceState.ABORTED, stateMachine.getState());

    } finally {
      instanceNotificationExecutor.shutdown();
    }
  }
}
