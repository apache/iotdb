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
package org.apache.iotdb.db.mpp.schedule;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.Driver;
import org.apache.iotdb.db.mpp.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTask;
import org.apache.iotdb.db.mpp.schedule.task.FragmentInstanceTaskStatus;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public class FragmentInstanceTimeoutSentinelTest {

  @Test
  public void testHandleInvalidStateTask() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              FragmentInstanceTask task = ans.getArgument(0);
              if (task.getStatus() != FragmentInstanceTaskStatus.READY) {
                return false;
              }
              task.setStatus(FragmentInstanceTaskStatus.RUNNING);
              return true;
            });
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    IndexedBlockingQueue<FragmentInstanceTask> taskQueue =
        new L1PriorityQueue<>(
            100, new FragmentInstanceTask.TimeoutComparator(), new FragmentInstanceTask());
    Driver mockDriver = Mockito.mock(Driver.class);
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);

    AbstractExecutor executor =
        new FragmentInstanceTaskExecutor(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler);

    // FINISHED status test
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(
            mockDriver, 100L, FragmentInstanceTaskStatus.FINISHED);
    executor.execute(testTask);
    Assert.assertEquals(FragmentInstanceTaskStatus.FINISHED, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.times(0)).processFor(Mockito.any());

    // ABORTED status test
    testTask =
        new FragmentInstanceTask(
            mockDriver, 100L, FragmentInstanceTaskStatus.ABORTED);
    executor.execute(testTask);
    Assert.assertEquals(FragmentInstanceTaskStatus.ABORTED, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.times(0)).processFor(Mockito.any());

    // RUNNING status test
    testTask =
        new FragmentInstanceTask(
            mockDriver, 100L, FragmentInstanceTaskStatus.RUNNING);
    executor.execute(testTask);
    Assert.assertEquals(FragmentInstanceTaskStatus.RUNNING, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.times(0)).processFor(Mockito.any());

    // BLOCKED status test
    testTask =
        new FragmentInstanceTask(
            mockDriver, 100L, FragmentInstanceTaskStatus.BLOCKED);
    executor.execute(testTask);
    Assert.assertEquals(FragmentInstanceTaskStatus.BLOCKED, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.times(0)).processFor(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByCancelledInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              FragmentInstanceTask task = ans.getArgument(0);
              if (task.getStatus() != FragmentInstanceTaskStatus.READY) {
                return false;
              }
              task.setStatus(FragmentInstanceTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<FragmentInstanceTask> taskQueue =
        new L1PriorityQueue<>(
            100, new FragmentInstanceTask.TimeoutComparator(), new FragmentInstanceTask());

    // Mock the instance with a cancelled future
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    Mockito.when(mockDriver.processFor(Mockito.any()))
        .thenReturn(Futures.immediateCancelledFuture());

    AbstractExecutor executor =
        new FragmentInstanceTaskExecutor(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler);
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.READY);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByFinishedInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              FragmentInstanceTask task = ans.getArgument(0);
              if (task.getStatus() != FragmentInstanceTaskStatus.READY) {
                return false;
              }
              task.setStatus(FragmentInstanceTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<FragmentInstanceTask> taskQueue =
        new L1PriorityQueue<>(
            100, new FragmentInstanceTask.TimeoutComparator(), new FragmentInstanceTask());

    // Mock the instance with a cancelled future
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    Mockito.when(mockDriver.processFor(Mockito.any()))
        .thenReturn(Futures.immediateVoidFuture());
    Mockito.when(mockDriver.isFinished()).thenReturn(true);
    AbstractExecutor executor =
        new FragmentInstanceTaskExecutor(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler);
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.READY);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByBlockedInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              FragmentInstanceTask task = ans.getArgument(0);
              if (task.getStatus() != FragmentInstanceTaskStatus.READY) {
                return false;
              }
              task.setStatus(FragmentInstanceTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<FragmentInstanceTask> taskQueue =
        new L1PriorityQueue<>(
            100, new FragmentInstanceTask.TimeoutComparator(), new FragmentInstanceTask());

    // Mock the instance with a blocked future
    ListenableFuture<Void> mockFuture = Mockito.mock(ListenableFuture.class);
    Mockito.when(mockFuture.isDone()).thenReturn(false);
    Mockito.doAnswer(
            ans -> {
              Runnable listener = ans.getArgument(0);
              Executor executor = ans.getArgument(1);
              executor.execute(listener);
              return null;
            })
        .when(mockFuture)
        .addListener(Mockito.any(), Mockito.any());
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    Mockito.when(mockDriver.processFor(Mockito.any())).thenReturn(mockFuture);
    Mockito.when(mockDriver.isFinished()).thenReturn(false);
    AbstractExecutor executor =
        new FragmentInstanceTaskExecutor(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler);
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.READY);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByReadyInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              FragmentInstanceTask task = ans.getArgument(0);
              if (task.getStatus() != FragmentInstanceTaskStatus.READY) {
                return false;
              }
              task.setStatus(FragmentInstanceTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<FragmentInstanceTask> taskQueue =
        new L1PriorityQueue<>(
            100, new FragmentInstanceTask.TimeoutComparator(), new FragmentInstanceTask());

    // Mock the instance with a ready future
    ListenableFuture<Void> mockFuture = Mockito.mock(ListenableFuture.class);
    Mockito.when(mockFuture.isDone()).thenReturn(true);
    Mockito.doAnswer(
            ans -> {
              Runnable listener = ans.getArgument(0);
              Executor executor = ans.getArgument(1);
              executor.execute(listener);
              return null;
            })
        .when(mockFuture)
        .addListener(Mockito.any(), Mockito.any());
    Driver mockDriver = Mockito.mock(Driver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    Mockito.when(mockDriver.getInfo()).thenReturn(instanceId);
    Mockito.when(mockDriver.processFor(Mockito.any())).thenReturn(mockFuture);
    Mockito.when(mockDriver.isFinished()).thenReturn(false);
    AbstractExecutor executor =
        new FragmentInstanceTaskExecutor(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler);
    FragmentInstanceTask testTask =
        new FragmentInstanceTask(mockDriver, 100L, FragmentInstanceTaskStatus.READY);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(0)).blockedToReady(Mockito.any());
  }
}
