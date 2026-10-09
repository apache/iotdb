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

package org.apache.iotdb.calc.execution.operator.process;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.junit.Assert.assertTrue;

public class AnyChildBlockedTest {
  @Test(timeout = 30000)
  public void testFastPath() throws Exception {
    CountingFuture ready = new CountingFuture();
    ready.complete();
    CountingFuture slow = new CountingFuture();
    CountingFuture[] inputs = {ready, slow};
    int[] polls = {0, 0};
    AnyChildBlocked blocker =
        new AnyChildBlocked(
            2,
            i -> {
              polls[i]++;
              return inputs[i];
            });
    for (int i = 0; i < 100; i++) {
      check(blocker.isBlocked().isDone(), "Ready input should avoid waiting");
    }
    check(polls[0] == 1 && polls[1] == 1, "Initialize every input and reuse cached futures");
    check(ready.registrations == 0 && slow.registrations == 0, "Fast path registers no listeners");
    check(blocker.takeReadyChildIndex() == 0, "Ready index");
    blocker.close();
  }

  @Test(timeout = 30000)
  public void testRepeatedWaiting() throws Exception {
    CountingFuture first = new CountingFuture();
    CountingFuture second = new CountingFuture();
    CountingFuture[] inputs = {first, second};
    AnyChildBlocked blocker = new AnyChildBlocked(2, i -> inputs[i]);
    ListenableFuture<?> round = blocker.isBlocked();
    for (int i = 0; i < 100; i++) {
      check(blocker.isBlocked() == round, "Reuse pending aggregate");
    }
    check(first.registrations == 1 && second.registrations == 1, "Register once per child");
    second.complete();
    first.complete();
    check(blocker.takeReadyChildIndex() == 1, "Later completion must not overwrite winner");
    inputs[1] = null;
    check(blocker.takeReadyChildIndex() == 0, "Do not lose another completed child");
    blocker.close();
  }

  @Test(timeout = 30000)
  public void testLongBlockedChild() throws Exception {
    CountingFuture slow = new CountingFuture();
    CountingFuture[] inputs = {slow, new CountingFuture()};
    int[] polls = {0, 0};
    AnyChildBlocked blocker =
        new AnyChildBlocked(
            2,
            i -> {
              polls[i]++;
              return inputs[i];
            });
    for (int i = 0; i < 2000; i++) {
      ListenableFuture<?> round = blocker.isBlocked();
      check(!round.isDone(), "All inputs block");
      inputs[1].complete();
      check(round.isDone(), "One child wakes the collector");
      check(blocker.takeReadyChildIndex() == 1, "Identify the fast child");
      inputs[1] = new CountingFuture();
    }
    check(
        slow.registrations == 1 && polls[0] == 1,
        "Slow child has one future and listener across rounds");
    ListenableFuture<?> last = blocker.isBlocked();
    slow.complete();
    check(
        last.isDone() && blocker.takeReadyChildIndex() == 0, "Slow child wakes the current round");
    blocker.close();
  }

  @Test(timeout = 30000)
  public void testRearmRaceAndStaleCallback() throws Exception {
    CountingFuture slow = new CountingFuture();
    slow.defer = true;
    CountingFuture[] inputs = {slow, new CountingFuture()};
    AnyChildBlocked blocker = new AnyChildBlocked(2, i -> inputs[i]);
    blocker.isBlocked();
    inputs[1].complete();
    check(blocker.takeReadyChildIndex() == 1, "Consume other child first");
    inputs[1] = new CountingFuture();
    slow.afterProbe = slow::complete;
    ListenableFuture<?> next = blocker.isBlocked();
    check(next.isDone(), "Recheck after retargeting must recover a completion after the scan");
    check(blocker.takeReadyChildIndex() == 0, "Recover the correct child");
    inputs[0] = new CountingFuture();
    ListenableFuture<?> following = blocker.isBlocked();
    slow.flushCallbacks();
    check(!following.isDone(), "Delayed old callback must not wake a new child generation");
    inputs[1].complete();
    check(
        following.isDone() && blocker.takeReadyChildIndex() == 1, "Current generation still wakes");
    check(slow.registrations == 1, "Rearming adds no listener");
    blocker.close();
  }

  @Test(timeout = 30000)
  public void testCompletionDuringRegistration() throws Exception {
    CountingFuture input = new CountingFuture();
    input.beforeRegister = input::complete;
    AnyChildBlocked blocker = new AnyChildBlocked(1, i -> input);
    check(blocker.isBlocked().isDone(), "Completion between scan and addListener must not be lost");
    check(blocker.takeReadyChildIndex() == 0, "Identify raced completion");
    blocker.close();
  }

  @Test(timeout = 30000)
  public void testFailureAndCancellation() throws Exception {
    CountingFuture input = new CountingFuture();
    AnyChildBlocked blocker = new AnyChildBlocked(1, i -> input);
    ListenableFuture<?> abandoned = blocker.isBlocked();
    abandoned.cancel(false);
    check(!input.isCancelled(), "Canceling a wait must not cancel the child");
    ListenableFuture<?> replacement = blocker.isBlocked();
    check(
        replacement != abandoned && input.registrations == 1,
        "Canceled waits rearm existing listeners");
    IllegalArgumentException cause = new IllegalArgumentException();
    input.delegate.setException(cause);
    check(replacement.isDone(), "Failure wakes the collector");
    try {
      blocker.takeReadyChildIndex();
      throw new AssertionError("Expected child failure");
    } catch (ExecutionException expected) {
      check(expected.getCause() == cause, "Preserve original cause");
    }
    blocker.close();
    CountingFuture canceled = new CountingFuture();
    AnyChildBlocked second = new AnyChildBlocked(1, i -> canceled);
    second.isBlocked();
    canceled.cancel(false);
    try {
      second.takeReadyChildIndex();
      throw new AssertionError("Expected child cancellation");
    } catch (CancellationException expected) {
      // Expected.
    }
    second.close();
  }

  @Test(timeout = 30000)
  public void testEmptyAndClosed() throws Exception {
    AnyChildBlocked empty = new AnyChildBlocked(0, i -> null);
    check(
        empty.isBlocked().isDone() && empty.takeReadyChildIndex() == -1, "Empty input is finished");
    CountingFuture input = new CountingFuture();
    AnyChildBlocked blocker = new AnyChildBlocked(2, i -> i == 0 ? null : input);
    ListenableFuture<?> pending = blocker.isBlocked();
    check(blocker.takeReadyChildIndex() == -1, "Never block the driver in takeReadyChildIndex");
    blocker.close();
    check(pending.isDone() && blocker.takeReadyChildIndex() == -1, "Close releases waiters");
    check(!input.isCancelled(), "Close does not cancel child futures");
    input.complete();
    check(blocker.takeReadyChildIndex() == -1, "Completion after close is harmless");
  }

  @Test(timeout = 30000)
  public void testConcurrentCompletion() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 1000; round++) {
        CountingFuture[] inputs = {new CountingFuture(), new CountingFuture()};
        CountingFuture firstInput = inputs[0];
        CountingFuture secondInput = inputs[1];
        AnyChildBlocked blocker = new AnyChildBlocked(2, i -> inputs[i]);
        CountDownLatch start = new CountDownLatch(1);
        Future<?> first = workers.submit(() -> completeAfter(start, firstInput));
        Future<?> second = workers.submit(() -> completeAfter(start, secondInput));
        start.countDown();
        blocker.isBlocked().get(5, TimeUnit.SECONDS);
        int winner = blocker.takeReadyChildIndex();
        check(winner == 0 || winner == 1, "Concurrent completion has a valid winner");
        inputs[winner] = null;
        first.get(5, TimeUnit.SECONDS);
        second.get(5, TimeUnit.SECONDS);
        check(
            blocker.takeReadyChildIndex() == 1 - winner, "Retain the other concurrent completion");
        blocker.close();
      }
    } finally {
      workers.shutdownNow();
    }
  }

  private static void completeAfter(CountDownLatch start, CountingFuture input) {
    try {
      start.await();
      input.complete();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void check(boolean condition, String message) {
    assertTrue(message, condition);
  }

  private static final class CountingFuture extends ForwardingListenableFuture<Void> {
    private final SettableFuture<Void> delegate = SettableFuture.create();
    private int registrations;
    private boolean defer;
    private final List<Runnable> deferred = new ArrayList<>();
    private Runnable afterProbe;
    private Runnable beforeRegister;

    @Override
    protected ListenableFuture<Void> delegate() {
      return delegate;
    }

    @Override
    public boolean isDone() {
      boolean done = delegate.isDone();
      Runnable hook = afterProbe;
      afterProbe = null;
      if (hook != null) {
        hook.run();
      }
      return done;
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      registrations++;
      if (beforeRegister != null) {
        beforeRegister.run();
      }
      delegate.addListener(
          () -> {
            if (defer) {
              deferred.add(() -> executor.execute(listener));
            } else {
              executor.execute(listener);
            }
          },
          directExecutor());
    }

    private void complete() {
      delegate.set(null);
    }

    private void flushCallbacks() {
      deferred.forEach(Runnable::run);
      deferred.clear();
    }
  }
}
