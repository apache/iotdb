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

package org.apache.iotdb.db.queryengine.execution.schedule;

import org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskStatus;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractDriverThreadShutdownTest {

  private final Logger logger = (Logger) LoggerFactory.getLogger(AbstractDriverThread.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private final ITaskScheduler scheduler = mock(ITaskScheduler.class);
  private final ThreadProducer producer = mock(ThreadProducer.class);
  private final ThreadGroup threadGroup = new ThreadGroup("shutdown-test");
  private Level originalLevel;

  @Before
  public void setUp() {
    originalLevel = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    appender.start();
    logger.addAppender(appender);
  }

  @After
  public void tearDown() {
    logger.detachAppender(appender);
    appender.stop();
    logger.setLevel(originalLevel);
  }

  @Test
  public void testCloseWhilePolling() throws Exception {
    CountDownLatch polling = new CountDownLatch(1);
    IndexedBlockingQueue<DriverTask> queue = pollingQueue(polling);
    AbstractDriverThread thread =
        new DriverTaskTimeoutSentinelThread("sentinel", threadGroup, queue, scheduler, producer);

    interruptAfterEntered(thread, polling, true);

    assertFalse(hasWarning());
    verify(scheduler, never()).toAborted(any());
    verify(producer, never()).produce(anyString(), any(), any(), any());
  }

  @Test
  public void testUnexpectedPollingInterruptStillWarnsAndReplacesThread() throws Exception {
    CountDownLatch polling = new CountDownLatch(1);
    IndexedBlockingQueue<DriverTask> queue = pollingQueue(polling);
    AbstractDriverThread thread =
        new DriverTaskTimeoutSentinelThread("sentinel", threadGroup, queue, scheduler, producer);

    interruptAfterEntered(thread, polling, false);

    assertTrue(hasWarning());
    verify(producer).produce("sentinel", threadGroup, queue, producer);
  }

  @Test
  public void testCloseWhileSentinelWaitsStillAbortsTask() throws Exception {
    IndexedBlockingQueue<DriverTask> queue = pollingQueue(new CountDownLatch(0));
    DriverTask task = newTask(60_000);
    queue.push(task);
    CountDownLatch executing = new CountDownLatch(1);
    AbstractDriverThread thread = sentinel(queue, executing);
    AtomicBoolean interruptedDuringAbort = new AtomicBoolean();
    doAnswer(
            invocation -> {
              interruptedDuringAbort.set(Thread.currentThread().isInterrupted());
              return null;
            })
        .when(scheduler)
        .toAborted(task);

    interruptAfterEntered(thread, executing, true);

    assertFalse(hasWarning());
    assertTrue(interruptedDuringAbort.get());
    assertAbortCause(task, DriverTaskAbortedException.BY_SCHEDULER_SHUTDOWN);
    verify(scheduler).toAborted(task);
    verify(scheduler, never()).enforceTimeLimit(any());
    verify(producer, never()).produce(anyString(), any(), any(), any());
  }

  @Test
  public void testUnexpectedSentinelInterruptStillAbortsAndWarns() throws Exception {
    IndexedBlockingQueue<DriverTask> queue = pollingQueue(new CountDownLatch(0));
    DriverTask task = newTask(60_000);
    queue.push(task);
    CountDownLatch executing = new CountDownLatch(1);
    AbstractDriverThread thread = sentinel(queue, executing);
    doAnswer(
            invocation -> {
              // End the test only after the running thread has handled the unexpected interrupt.
              thread.close();
              return null;
            })
        .when(scheduler)
        .toAborted(task);

    interruptAfterEntered(thread, executing, false);

    assertTrue(hasWarning());
    assertAbortCause(task, DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED);
    verify(scheduler).toAborted(task);
  }

  @Test
  public void testWrappedShutdownInterruptStillAbortsTask() throws Exception {
    verifyFailureDuringShutdown(new RuntimeException(new InterruptedException()), false);
  }

  @Test
  public void testNonInterruptFailureDuringShutdownStillWarns() throws Exception {
    verifyFailureDuringShutdown(new IllegalStateException("mock failure"), true);
  }

  @Test
  public void testSentinelStillEnforcesTimeout() throws Exception {
    DriverTask task = newTask(0);
    DriverTaskTimeoutSentinelThread thread =
        new DriverTaskTimeoutSentinelThread(
            "sentinel", threadGroup, pollingQueue(new CountDownLatch(0)), scheduler, producer);

    thread.execute(task);

    assertTrue(task.getAbortCause().get() instanceof QueryTimeoutException);
    verify(scheduler).toAborted(task);
    verify(scheduler, never()).enforceTimeLimit(any());
  }

  private void verifyFailureDuringShutdown(RuntimeException failure, boolean shouldWarn)
      throws Exception {
    IndexedBlockingQueue<DriverTask> queue = pollingQueue(new CountDownLatch(0));
    DriverTask task = newTask(60_000);
    queue.push(task);
    CountDownLatch executing = new CountDownLatch(1);
    AbstractDriverThread thread =
        new AbstractDriverThread("worker", threadGroup, queue, scheduler, producer) {
          @Override
          protected void execute(DriverTask task) {
            executing.countDown();
            try {
              new CountDownLatch(1).await();
            } catch (InterruptedException e) {
              throw failure;
            }
          }
        };

    interruptAfterEntered(thread, executing, true);

    assertEquals(shouldWarn, hasWarning());
    assertAbortCause(
        task,
        shouldWarn
            ? DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED
            : DriverTaskAbortedException.BY_SCHEDULER_SHUTDOWN);
    verify(scheduler).toAborted(task);
  }

  private AbstractDriverThread sentinel(
      IndexedBlockingQueue<DriverTask> queue, CountDownLatch executing) {
    return new DriverTaskTimeoutSentinelThread(
        "sentinel", threadGroup, queue, scheduler, producer) {
      @Override
      public void execute(DriverTask task) throws InterruptedException {
        executing.countDown();
        super.execute(task);
      }
    };
  }

  private IndexedBlockingQueue<DriverTask> pollingQueue(CountDownLatch polling) {
    return new L1PriorityQueue<DriverTask>(
        10, new DriverTask.TimeoutComparator(), new DriverTask()) {
      @Override
      public DriverTask poll() throws InterruptedException {
        polling.countDown();
        return super.poll();
      }
    };
  }

  private DriverTask newTask(long timeoutMs) {
    IDriver driver = mock(IDriver.class);
    when(driver.getDriverTaskId())
        .thenReturn(
            new DriverTaskId(
                new FragmentInstanceId(new PlanFragmentId(new QueryId("test"), 0), "instance"), 0));
    return new DriverTask(driver, timeoutMs, DriverTaskStatus.READY, null, 0, false);
  }

  private void interruptAfterEntered(
      AbstractDriverThread thread, CountDownLatch entered, boolean close) throws Exception {
    thread.setDaemon(true);
    thread.start();
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      if (close) {
        thread.close();
      }
      thread.interrupt();
      thread.join(10_000);
      assertFalse("Driver thread did not exit", thread.isAlive());
    } finally {
      thread.close();
      thread.interrupt();
      thread.join(10_000);
    }
  }

  private void assertAbortCause(DriverTask task, String reason) {
    assertEquals(
        new DriverTaskAbortedException(task.getDriverTaskId().getFullId(), reason).getMessage(),
        task.getAbortCause().get().getMessage());
  }

  private boolean hasWarning() {
    return appender.list.stream().anyMatch(event -> event.getLevel().isGreaterOrEqual(Level.WARN));
  }
}
