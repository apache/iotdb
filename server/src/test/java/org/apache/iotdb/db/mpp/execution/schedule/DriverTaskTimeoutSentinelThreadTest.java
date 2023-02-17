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
package org.apache.iotdb.db.mpp.execution.schedule;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.driver.IDriver;
import org.apache.iotdb.db.mpp.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.db.mpp.execution.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.mpp.execution.schedule.queue.L2PriorityQueue;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.mpp.execution.schedule.task.DriverTaskStatus;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class DriverTaskTimeoutSentinelThreadTest {

  private static final ThreadProducer producer =
      (threadName, workerGroups, queue, producer) -> {
        // do nothing;
      };

  @Test
  public void testHandleInvalidStateTask() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              DriverTask task = ans.getArgument(0);
              if (task.getStatus() != DriverTaskStatus.READY) {
                return false;
              }
              task.setStatus(DriverTaskStatus.RUNNING);
              return true;
            });
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    IndexedBlockingQueue<DriverTask> taskQueue =
        new L1PriorityQueue<>(100, new DriverTask.TimeoutComparator(), new DriverTask());
    IDriver mockDriver = Mockito.mock(IDriver.class);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);

    AbstractDriverThread executor =
        new DriverTaskThread(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler, producer);

    // FINISHED status test
    DriverTask testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.FINISHED, null);
    executor.execute(testTask);
    Assert.assertEquals(DriverTaskStatus.FINISHED, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.never()).processFor(Mockito.any());
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());

    // ABORTED status test
    testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.ABORTED, null);
    executor.execute(testTask);
    Assert.assertEquals(DriverTaskStatus.ABORTED, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.never()).processFor(Mockito.any());
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());

    // RUNNING status test
    testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.RUNNING, null);
    executor.execute(testTask);
    Assert.assertEquals(DriverTaskStatus.RUNNING, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.never()).processFor(Mockito.any());
    Mockito.verify(mockDriver, Mockito.never()).failed(Mockito.any());

    // BLOCKED status test
    testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.BLOCKED, null);
    executor.execute(testTask);
    Assert.assertEquals(DriverTaskStatus.BLOCKED, testTask.getStatus());
    Mockito.verify(mockDriver, Mockito.never()).processFor(Mockito.any());
    Assert.assertNull(testTask.getAbortCause());
    Mockito.verify(mockScheduler, Mockito.never()).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByCancelledInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              DriverTask task = ans.getArgument(0);
              if (task.getStatus() != DriverTaskStatus.READY) {
                return false;
              }
              task.setStatus(DriverTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<DriverTask> taskQueue =
        new L1PriorityQueue<>(100, new DriverTask.TimeoutComparator(), new DriverTask());

    // Mock the instance with a cancelled future
    IDriver mockDriver = Mockito.mock(IDriver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    Mockito.when(mockDriver.processFor(Mockito.any()))
        .thenReturn(Futures.immediateCancelledFuture());

    AbstractDriverThread executor =
        new DriverTaskThread(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler, producer);
    DriverTask testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.READY, null);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Assert.assertEquals(
        DriverTaskAbortedException.BY_ALREADY_BEING_CANCELLED, testTask.getAbortCause());
    Mockito.verify(mockScheduler, Mockito.times(1)).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByFinishedInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              DriverTask task = ans.getArgument(0);
              if (task.getStatus() != DriverTaskStatus.READY) {
                return false;
              }
              task.setStatus(DriverTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<DriverTask> taskQueue =
        new L1PriorityQueue<>(100, new DriverTask.TimeoutComparator(), new DriverTask());

    // Mock the instance with a cancelled future
    IDriver mockDriver = Mockito.mock(IDriver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    Mockito.when(mockDriver.processFor(Mockito.any()))
        .thenAnswer(ans -> Futures.immediateVoidFuture());
    Mockito.when(mockDriver.isFinished()).thenReturn(true);
    AbstractDriverThread executor =
        new DriverTaskThread(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler, producer);
    DriverTask testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.READY, null);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Assert.assertNull(testTask.getAbortCause());
    Mockito.verify(mockScheduler, Mockito.never()).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByBlockedInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              DriverTask task = ans.getArgument(0);
              if (task.getStatus() != DriverTaskStatus.READY) {
                return false;
              }
              task.setStatus(DriverTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<DriverTask> taskQueue =
        new L1PriorityQueue<>(100, new DriverTask.TimeoutComparator(), new DriverTask());

    // Mock the instance with a blocked future
    ListenableFuture<?> mockFuture = Mockito.mock(ListenableFuture.class);
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
    IDriver mockDriver = Mockito.mock(IDriver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    Mockito.when(mockDriver.processFor(Mockito.any())).thenAnswer(ans -> mockFuture);
    Mockito.when(mockDriver.isFinished()).thenReturn(false);
    AbstractDriverThread executor =
        new DriverTaskThread(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler, producer);
    DriverTask testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.READY, null);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Assert.assertNull(testTask.getAbortCause());
    Mockito.verify(mockScheduler, Mockito.never()).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToFinished(Mockito.any(), Mockito.any());
    TimeUnit.MILLISECONDS.sleep(500);
    Mockito.verify(mockScheduler, Mockito.times(1)).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskByReadyInstance() throws ExecutionException, InterruptedException {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              DriverTask task = ans.getArgument(0);
              if (task.getStatus() != DriverTaskStatus.READY) {
                return false;
              }
              task.setStatus(DriverTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<DriverTask> taskQueue =
        new L1PriorityQueue<>(100, new DriverTask.TimeoutComparator(), new DriverTask());

    // Mock the instance with a ready future
    ListenableFuture<?> mockFuture = Mockito.mock(ListenableFuture.class);
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
    IDriver mockDriver = Mockito.mock(IDriver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    Mockito.when(mockDriver.processFor(Mockito.any())).thenAnswer(ans -> mockFuture);
    Mockito.when(mockDriver.isFinished()).thenReturn(false);
    AbstractDriverThread executor =
        new DriverTaskThread(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler, producer);
    DriverTask testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.READY, null);
    executor.execute(testTask);
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Assert.assertNull(testTask.getAbortCause());
    Mockito.verify(mockScheduler, Mockito.never()).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.times(1)).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).blockedToReady(Mockito.any());
  }

  @Test
  public void testHandleTaskWithInternalError() {
    ITaskScheduler mockScheduler = Mockito.mock(ITaskScheduler.class);
    Mockito.when(mockScheduler.readyToRunning(Mockito.any()))
        .thenAnswer(
            ans -> {
              DriverTask task = ans.getArgument(0);
              if (task.getStatus() != DriverTaskStatus.READY) {
                return false;
              }
              task.setStatus(DriverTaskStatus.RUNNING);
              return true;
            });
    IndexedBlockingQueue<DriverTask> taskQueue =
        new L2PriorityQueue<>(100, new DriverTask.SchedulePriorityComparator(), new DriverTask());
    IDriver mockDriver = Mockito.mock(IDriver.class);
    QueryId queryId = new QueryId("test");
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstanceId instanceId = new FragmentInstanceId(fragmentId, "inst-0");
    DriverTaskId driverTaskID = new DriverTaskId(instanceId, 0);
    Mockito.when(mockDriver.getDriverTaskId()).thenReturn(driverTaskID);
    AbstractDriverThread executor =
        new DriverTaskThread(
            "0", new ThreadGroup("timeout-test"), taskQueue, mockScheduler, producer);
    Mockito.when(mockDriver.processFor(Mockito.any()))
        .thenAnswer(
            ans -> {
              executor.close();
              throw new RuntimeException("mock exception");
            });
    DriverTask testTask = new DriverTask(mockDriver, 100L, DriverTaskStatus.READY, null);
    taskQueue.push(testTask);
    executor.run(); // Here we use run() instead of start() to execute the task in the same thread
    Mockito.verify(mockDriver, Mockito.times(1)).processFor(Mockito.any());
    Assert.assertEquals(
        DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED, testTask.getAbortCause());
    Assert.assertEquals(0, taskQueue.size());
    Mockito.verify(mockScheduler, Mockito.times(1)).toAborted(Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToReady(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToBlocked(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).runningToFinished(Mockito.any(), Mockito.any());
    Mockito.verify(mockScheduler, Mockito.never()).blockedToReady(Mockito.any());
  }
}
