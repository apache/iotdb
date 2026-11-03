/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.metrics.metricsets.jvm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JvmThreadMetricsTest {

  @Before
  @After
  public void expireCache() throws Exception {
    Field lastUpdateTime = JvmThreadMetrics.class.getDeclaredField("lastUpdateTime");
    lastUpdateTime.setAccessible(true);
    synchronized (JvmThreadMetrics.class) {
      lastUpdateTime.setLong(null, 0L);
    }
  }

  // Count two live threads, ignore a terminated thread's null info, and return zero for an absent
  // state without resampling within the cache interval.
  @Test
  public void testThreadStateCountsAndMissingState() {
    ThreadInfo info =
        ManagementFactory.getThreadMXBean().getThreadInfo(Thread.currentThread().getId());
    AtomicInteger samples = new AtomicInteger();
    ThreadMXBean threadBean =
        threadBean(
            () -> {
              samples.incrementAndGet();
              return new ThreadInfo[] {info, info, null};
            });

    assertEquals(2L, JvmThreadMetrics.getThreadStateCount(threadBean, info.getThreadState()));
    assertEquals(0L, JvmThreadMetrics.getThreadStateCount(threadBean, Thread.State.TERMINATED));
    assertEquals(1, samples.get());
  }

  // Pause a refresh after clearing the cache, then read the same state concurrently. Both calls
  // must return the complete sample rather than a temporarily absent entry.
  @Test
  public void testConcurrentReadDuringRefresh() throws Exception {
    ThreadInfo info =
        ManagementFactory.getThreadMXBean().getThreadInfo(Thread.currentThread().getId());
    CountDownLatch refreshStarted = new CountDownLatch(1);
    CountDownLatch resumeRefresh = new CountDownLatch(1);
    CountDownLatch readStarted = new CountDownLatch(1);
    AtomicReference<Thread> readerThread = new AtomicReference<>();
    ThreadMXBean threadBean =
        threadBean(
            () -> {
              refreshStarted.countDown();
              try {
                assertTrue(resumeRefresh.await(10, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
              return new ThreadInfo[] {info};
            });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Long> refresh =
          executor.submit(
              () -> JvmThreadMetrics.getThreadStateCount(threadBean, info.getThreadState()));
      assertTrue(refreshStarted.await(5, TimeUnit.SECONDS));
      Future<Long> read =
          executor.submit(
              () -> {
                readerThread.set(Thread.currentThread());
                readStarted.countDown();
                return JvmThreadMetrics.getThreadStateCount(threadBean, info.getThreadState());
              });
      assertTrue(readStarted.await(5, TimeUnit.SECONDS));

      // Ensure the reader reaches the cache lookup before allowing the refresh to finish.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!read.isDone()
          && readerThread.get().getState() != Thread.State.BLOCKED
          && System.nanoTime() < deadline) {
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      }
      assertTrue(read.isDone() || readerThread.get().getState() == Thread.State.BLOCKED);
      resumeRefresh.countDown();

      assertEquals(1L, refresh.get(5, TimeUnit.SECONDS).longValue());
      assertEquals(1L, read.get(5, TimeUnit.SECONDS).longValue());
    } finally {
      resumeRefresh.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static ThreadMXBean threadBean(Supplier<ThreadInfo[]> threadInfos) {
    return (ThreadMXBean)
        Proxy.newProxyInstance(
            JvmThreadMetricsTest.class.getClassLoader(),
            new Class<?>[] {ThreadMXBean.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "getAllThreadIds" -> new long[] {1L, 2L, 3L};
                  case "getThreadInfo" -> threadInfos.get();
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }
}
