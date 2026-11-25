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

package org.apache.iotdb.commons.external.caffeine.cache;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.external.caffeine.cache.testing.ConcurrentTestHarness.executor;
import static org.apache.iotdb.commons.external.caffeine.cache.testing.ConcurrentTestHarness.scheduledExecutor;
import static org.apache.iotdb.commons.external.caffeine.cache.testing.TruthAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/** Ported unit tests for {@link Scheduler} and {@link DisabledFuture}. */
public final class SchedulerTest {

  @Test
  public void schedulerNullArguments() {
    for (Scheduler scheduler : schedulers()) {
      assertThrows(
          NullPointerException.class,
          () -> scheduler.schedule(null, () -> {}, 1L, TimeUnit.NANOSECONDS));
      assertThrows(
          NullPointerException.class,
          () -> scheduler.schedule(Runnable::run, null, 1L, TimeUnit.NANOSECONDS));
      assertThrows(
          NullPointerException.class, () -> scheduler.schedule(Runnable::run, () -> {}, 1L, null));
    }
  }

  @Test
  public void schedulerExceptionInExecutor() {
    for (Scheduler scheduler : runnableSchedulers()) {
      AtomicReference<Thread> thread = new AtomicReference<>();
      Executor failingExecutor =
          task -> {
            thread.set(Thread.currentThread());
            throw new IllegalStateException("executor failure");
          };
      Future<?> future = scheduler.schedule(failingExecutor, () -> {}, 1L, TimeUnit.NANOSECONDS);
      assertThat(future).isNotNull();
      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(() -> assertThat(thread.get()).isNotNull());
    }
  }

  @Test
  public void schedulerExecutesTask() {
    for (Scheduler scheduler : runnableSchedulers()) {
      AtomicBoolean executed = new AtomicBoolean();
      Future<?> future =
          scheduler.schedule(executor, () -> executed.set(true), 1L, TimeUnit.NANOSECONDS);
      assertThat(future).isNotNull();
      Awaitility.await().atMost(Duration.ofSeconds(5)).untilTrue(executed);
    }
  }

  @Test
  public void disabledScheduler() {
    Future<?> future =
        Scheduler.disabledScheduler().schedule(Runnable::run, () -> {}, 1, TimeUnit.MINUTES);
    assertThat(future).isSameInstanceAs(DisabledFuture.instance());
    for (DisabledScheduler val : DisabledScheduler.values()) {
      assertThat(DisabledScheduler.valueOf(val.name()))
          .isSameInstanceAs(DisabledScheduler.INSTANCE);
    }
    for (SystemScheduler val : SystemScheduler.values()) {
      assertThat(SystemScheduler.valueOf(val.name())).isSameInstanceAs(SystemScheduler.INSTANCE);
    }
  }

  @Test
  public void disabledFuture() throws InterruptedException, ExecutionException, TimeoutException {
    assertThat(DisabledFuture.instance().get(0, TimeUnit.SECONDS)).isNull();
    assertThat(DisabledFuture.instance().isCancelled()).isFalse();
    assertThat(DisabledFuture.instance().cancel(false)).isFalse();
    assertThat(DisabledFuture.instance().cancel(true)).isFalse();
    assertThat(DisabledFuture.instance().isDone()).isTrue();
    assertThat(DisabledFuture.instance().get()).isNull();
    assertThrows(NullPointerException.class, () -> DisabledFuture.instance().get(0, null));
    for (DisabledFuture val : DisabledFuture.values()) {
      assertThat(DisabledFuture.valueOf(val.name())).isSameInstanceAs(DisabledFuture.INSTANCE);
    }
  }

  @Test
  public void guardedSchedulerNull() {
    assertThrows(NullPointerException.class, () -> Scheduler.guardedScheduler(null));
  }

  @Test
  public void guardedSchedulerTwice() {
    Scheduler scheduler = Scheduler.guardedScheduler(Scheduler.systemScheduler());
    assertThat(scheduler).isSameInstanceAs(Scheduler.guardedScheduler(scheduler));
  }

  @Test
  public void guardedSchedulerNullFuture() {
    ScheduledExecutorService mockScheduled = Mockito.mock(ScheduledExecutorService.class);
    Scheduler scheduler = Scheduler.forScheduledExecutorService(mockScheduled);
    Executor mockExecutor = Mockito.mock(Executor.class);
    Runnable command = () -> {};

    Future<?> future =
        Scheduler.guardedScheduler(scheduler).schedule(mockExecutor, command, 1L, TimeUnit.MINUTES);
    verify(mockScheduled).schedule(any(Runnable.class), eq(1L), eq(TimeUnit.MINUTES));
    assertThat(future).isSameInstanceAs(DisabledFuture.instance());
  }

  @Test
  public void guardedSchedulerNormal() {
    CompletableFuture<Void> expected = CompletableFuture.completedFuture(null);
    Future<?> future =
        Scheduler.guardedScheduler((r, e, d, u) -> expected)
            .schedule(Runnable::run, () -> {}, 1, TimeUnit.MINUTES);
    assertThat(future).isSameInstanceAs(expected);
  }

  @Test
  public void guardedSchedulerException() {
    Future<?> future =
        Scheduler.guardedScheduler(
                (r, e, d, u) -> {
                  throw new IllegalStateException("scheduler error");
                })
            .schedule(Runnable::run, () -> {}, 1, TimeUnit.MINUTES);
    assertThat(future).isSameInstanceAs(DisabledFuture.instance());
  }

  @Test
  public void scheduledExecutorServiceNull() {
    assertThrows(NullPointerException.class, () -> Scheduler.forScheduledExecutorService(null));
  }

  @Test
  public void scheduledExecutorServiceSchedule() {
    ScheduledExecutorService mockScheduled = Mockito.mock(ScheduledExecutorService.class);
    ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
    Executor mockExecutor = Mockito.mock(Executor.class);
    Runnable command = () -> {};

    Scheduler scheduler = Scheduler.forScheduledExecutorService(mockScheduled);
    Future<?> future = scheduler.schedule(mockExecutor, command, 1L, TimeUnit.MINUTES);
    assertThat(future).isNotSameInstanceAs(DisabledFuture.instance());

    verify(mockScheduled).isShutdown();
    verify(mockScheduled).schedule(task.capture(), eq(1L), eq(TimeUnit.MINUTES));
    verifyNoMoreInteractions(mockScheduled);

    task.getValue().run();
    verify(mockExecutor).execute(command);
    verifyNoMoreInteractions(mockExecutor);
  }

  @Test
  public void scheduledExecutorServiceShutdown() {
    ScheduledExecutorService mockScheduled = Mockito.mock(ScheduledExecutorService.class);
    Executor mockExecutor = Mockito.mock(Executor.class);

    when(mockScheduled.isShutdown()).thenReturn(true);
    Scheduler scheduler = Scheduler.forScheduledExecutorService(mockScheduled);
    Future<?> future = scheduler.schedule(mockExecutor, () -> {}, 1L, TimeUnit.MINUTES);
    assertThat(future).isSameInstanceAs(DisabledFuture.instance());

    verify(mockScheduled).isShutdown();
    verifyNoMoreInteractions(mockScheduled);
    verifyZeroInteractions(mockExecutor);
  }

  private static List<Scheduler> schedulers() {
    return Arrays.asList(
        Scheduler.forScheduledExecutorService(scheduledExecutor),
        Scheduler.disabledScheduler(),
        Scheduler.systemScheduler());
  }

  private static List<Scheduler> runnableSchedulers() {
    return Arrays.asList(
        Scheduler.forScheduledExecutorService(scheduledExecutor), Scheduler.systemScheduler());
  }
}
