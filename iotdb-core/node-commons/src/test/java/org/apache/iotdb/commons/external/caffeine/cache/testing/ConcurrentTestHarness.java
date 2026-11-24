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

package org.apache.iotdb.commons.external.caffeine.cache.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

/** Multi-threaded test harness for ported Caffeine concurrency unit tests. */
public final class ConcurrentTestHarness {

  public static final ExecutorService executor =
      Executors.newCachedThreadPool(
          runnable -> {
            Thread thread = new Thread(runnable, "caffeine-test-worker");
            thread.setDaemon(true);
            return thread;
          });

  public static final ScheduledExecutorService scheduledExecutor =
      Executors.newSingleThreadScheduledExecutor(
          runnable -> {
            Thread thread = new Thread(runnable, "caffeine-test-scheduler");
            thread.setDaemon(true);
            return thread;
          });

  private ConcurrentTestHarness() {}

  public static long timeTasks(int nThreads, Runnable task) {
    return timeTasks(
        nThreads,
        () -> {
          task.run();
          return null;
        });
  }

  public static <T> long timeTasks(int nThreads, Callable<T> task) {
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch readyGate = new CountDownLatch(nThreads);
    CountDownLatch endGate = new CountDownLatch(nThreads);
    List<Future<T>> futures = new ArrayList<>(nThreads);

    for (int i = 0; i < nThreads; i++) {
      futures.add(
          executor.submit(
              () -> {
                readyGate.countDown();
                awaitUninterruptibly(startGate);
                try {
                  return task.call();
                } finally {
                  endGate.countDown();
                }
              }));
    }

    awaitUninterruptibly(readyGate);
    long start = System.nanoTime();
    startGate.countDown();
    awaitUninterruptibly(endGate);
    long end = System.nanoTime();

    for (Future<T> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted waiting for task result", e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        }
        if (e.getCause() instanceof Error) {
          throw (Error) e.getCause();
        }
        throw new IllegalStateException("Task execution failed", e.getCause());
      }
    }
    return end - start;
  }

  public static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          latch.await();
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
