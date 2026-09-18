/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.queryengine.execution.schedule;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskStatus;

import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DriverTaskThreadTest {

  @Test
  public void testBlockedDriverNotificationsQueueWithoutRunningOnCompletingThread()
      throws Exception {
    int taskCount = 12;
    int threadCount = 2;
    ExecutorService executor = DriverTaskThread.createNotificationExecutor(threadCount);
    CountDownLatch workersStarted = new CountDownLatch(threadCount);
    CountDownLatch releaseWorkers = new CountDownLatch(1);
    CountDownLatch notified = new CountDownLatch(taskCount);
    Set<Thread> callbackThreads = ConcurrentHashMap.newKeySet();
    ITaskScheduler scheduler = mock(ITaskScheduler.class);
    when(scheduler.readyToRunning(any())).thenReturn(true);
    doAnswer(
            invocation -> {
              callbackThreads.add(Thread.currentThread());
              workersStarted.countDown();
              assertTrue(releaseWorkers.await(10, TimeUnit.SECONDS));
              notified.countDown();
              return null;
            })
        .when(scheduler)
        .blockedToReady(any());

    DriverTaskThread worker =
        new DriverTaskThread("test-worker", null, null, scheduler, null, executor);
    List<SettableFuture<Void>> futures = new ArrayList<>();
    List<DriverTask> tasks = new ArrayList<>();
    try {
      for (int i = 0; i < taskCount; i++) {
        IDriver driver = mock(IDriver.class);
        DriverTaskId id =
            new DriverTaskId(
                new FragmentInstanceId(
                    new PlanFragmentId(new QueryId("notifications"), 0), "i" + i),
                0);
        when(driver.getDriverTaskId()).thenReturn(id);
        SettableFuture<Void> future = SettableFuture.create();
        doReturn(future).when(driver).processFor(any());
        DriverTask task = new DriverTask(driver, 30000, DriverTaskStatus.READY, null, 0, false);
        tasks.add(task);
        futures.add(future);
        worker.execute(task);
      }
      // Completing futures must remain nonblocking even when all notification workers are busy.
      for (SettableFuture<Void> future : futures) {
        future.set(null);
      }
      assertTrue(workersStarted.await(10, TimeUnit.SECONDS));
      assertEquals(threadCount, callbackThreads.size());
      assertEquals(taskCount - threadCount, ((ThreadPoolExecutor) executor).getQueue().size());
      assertFalse(callbackThreads.contains(Thread.currentThread()));

      releaseWorkers.countDown();
      assertTrue(notified.await(10, TimeUnit.SECONDS));
      assertEquals(threadCount, ((ThreadPoolExecutor) executor).getLargestPoolSize());
      for (DriverTask task : tasks) {
        verify(scheduler, times(1)).runningToBlocked(eq(task), any());
        verify(scheduler, times(1)).blockedToReady(task);
      }
    } finally {
      releaseWorkers.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }
}
