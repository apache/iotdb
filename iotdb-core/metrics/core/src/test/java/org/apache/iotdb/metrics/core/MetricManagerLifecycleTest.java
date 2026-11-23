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

package org.apache.iotdb.metrics.core;

import org.apache.iotdb.metrics.core.type.IoTDBAutoGauge;
import org.apache.iotdb.metrics.core.type.IoTDBCounter;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToDoubleFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MetricManagerLifecycleTest {
  @Test
  public void testResetWaitsForRemovalButCachedMetricsRemainAccessible() throws Exception {
    TestMetricManager manager = new TestMetricManager();
    RecordingReporter reporter = new RecordingReporter(manager);
    manager.setBindJmxReporter(reporter);
    AtomicInteger value = new AtomicInteger(3);
    AutoGauge closing =
        manager.createAutoGauge("closing", MetricLevel.IMPORTANT, value, AtomicInteger::get);
    Object[] cached = cachedMetrics(manager);
    CountDownLatch removed = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    Map<MetricInfo, IMetric> original =
        new ConcurrentHashMap<MetricInfo, IMetric>(manager.getAllMetrics()) {
          @Override
          public IMetric remove(Object key) {
            IMetric metric = super.remove(key);
            if (metric != null && ((MetricInfo) key).getName().equals("closing")) {
              removed.countDown();
              await(resume);
            }
            return metric;
          }
        };
    manager.useRegistry(original);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    AtomicReference<Thread> removingThread = new AtomicReference<>();
    AtomicReference<Thread> resettingThread = new AtomicReference<>();
    CountDownLatch resetting = new CountDownLatch(1);
    try {
      Future<?> removal =
          executor.submit(
              () -> {
                removingThread.set(Thread.currentThread());
                manager.remove(MetricType.AUTO_GAUGE, "closing");
              });
      assertTrue(removed.await(5, TimeUnit.SECONDS));
      Future<Counter> reset =
          executor.submit(
              () -> {
                resettingThread.set(Thread.currentThread());
                resetting.countDown();
                manager.reset();
                return manager.getOrCreateCounter(
                    "closing", MetricLevel.IMPORTANT, "generation", "new");
              });
      assertTrue(resetting.await(5, TimeUnit.SECONDS));
      assertBlockedBy(resettingThread.get(), removingThread.get(), reset);
      assertSame(original, manager.getAllMetrics());
      Object[] actual = executor.submit(() -> cachedMetrics(manager)).get(5, TimeUnit.SECONDS);
      for (int i = 0; i < cached.length; i++) {
        assertSame(cached[i], actual[i]);
      }
      ((Counter) actual[0]).inc();
      assertEquals(1, ((Counter) cached[0]).getCount());
      assertEquals(3, executor.submit(closing::getValue).get(5, TimeUnit.SECONDS), 0);
      resume.countDown();
      removal.get(5, TimeUnit.SECONDS);
      Counter replacement = reset.get(5, TimeUnit.SECONDS);
      assertEquals(1, reporter.removals.get());
      assertSame(closing, reporter.removed.get());
      assertEquals(1, manager.getAllMetrics().size());
      assertSame(
          replacement,
          manager.getOrCreateCounter("closing", MetricLevel.IMPORTANT, "generation", "new"));
      assertTrue(manager.hasMetadata("closing"));
    } finally {
      resume.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testConcurrentRemovalsNotifyOnlyOnce() throws Exception {
    TestMetricManager manager = new TestMetricManager();
    RecordingReporter reporter = new RecordingReporter(manager);
    manager.setBindJmxReporter(reporter);
    Counter counter = manager.getOrCreateCounter("counter", MetricLevel.IMPORTANT);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<?>> removals = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        removals.add(
            executor.submit(
                () -> {
                  await(start);
                  manager.remove(MetricType.COUNTER, "counter");
                }));
      }
      start.countDown();
      for (Future<?> removal : removals) {
        removal.get(5, TimeUnit.SECONDS);
      }
      manager.remove(MetricType.COUNTER, "counter");
      assertEquals(1, reporter.removals.get());
      assertSame(counter, reporter.removed.get());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testConcurrentCreationPublishesOneMetricBeforeReporting() throws Exception {
    TestMetricManager manager = new TestMetricManager();
    RecordingReporter reporter = new RecordingReporter(manager);
    manager.setBindJmxReporter(reporter);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<Counter>> counters = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        counters.add(
            executor.submit(
                () -> {
                  await(start);
                  return manager.getOrCreateCounter("counter", MetricLevel.IMPORTANT);
                }));
      }
      start.countDown();
      Counter first = counters.get(0).get(5, TimeUnit.SECONDS);
      for (Future<Counter> counter : counters) {
        assertSame(first, counter.get(5, TimeUnit.SECONDS));
      }
      assertEquals(1, manager.counterCreations.get());
      assertEquals(1, reporter.registrations.get());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testReporterCallbackDoesNotHoldRegistryLock() throws Exception {
    TestMetricManager manager = new TestMetricManager();
    RecordingReporter reporter = new RecordingReporter(manager);
    manager.setBindJmxReporter(reporter);
    Counter old = manager.getOrCreateCounter("counter", MetricLevel.IMPORTANT);
    CountDownLatch callback = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    reporter.beforeRemoval =
        () -> {
          callback.countDown();
          await(resume);
        };
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> removal = executor.submit(() -> manager.remove(MetricType.COUNTER, "counter"));
      assertTrue(callback.await(5, TimeUnit.SECONDS));
      // A reporter/MBean-server callback can run after reset without pinning the registry lock.
      Counter next =
          executor
              .submit(
                  () -> {
                    manager.reset();
                    return manager.getOrCreateCounter(
                        "counter", MetricLevel.IMPORTANT, "generation", "new");
                  })
              .get(5, TimeUnit.SECONDS);
      resume.countDown();
      removal.get(5, TimeUnit.SECONDS);
      assertSame(old, reporter.removed.get());
      assertSame(
          next, manager.getOrCreateCounter("counter", MetricLevel.IMPORTANT, "generation", "new"));
      assertTrue(manager.hasMetadata("counter"));
    } finally {
      resume.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static Object[] cachedMetrics(TestMetricManager manager) {
    return new Object[] {
      manager.getOrCreateCounter("cached_counter", MetricLevel.IMPORTANT),
      manager.getOrCreateGauge("cached_gauge", MetricLevel.IMPORTANT),
      manager.getOrCreateRate("cached_rate", MetricLevel.IMPORTANT),
      manager.getOrCreateHistogram("cached_histogram", MetricLevel.IMPORTANT),
      manager.getOrCreateTimer("cached_timer", MetricLevel.IMPORTANT)
    };
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void assertBlockedBy(Thread thread, Thread owner, Future<?> task) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!task.isDone() && System.nanoTime() < deadline) {
      ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
      if (info != null
          && info.getThreadState() == Thread.State.BLOCKED
          && info.getLockOwnerId() == owner.getId()) {
        return;
      }
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    fail("Registry reset did not wait for the active removal");
  }

  private static class TestMetricManager extends DoNothingMetricManager {
    private final AtomicInteger counterCreations = new AtomicInteger();

    @Override
    public boolean isEnableMetricInGivenLevel(MetricLevel level) {
      return true;
    }

    @Override
    public Counter createCounter() {
      counterCreations.incrementAndGet();
      return new IoTDBCounter();
    }

    @Override
    public <T> AutoGauge createAutoGauge(T object, ToDoubleFunction<T> mapper) {
      return new IoTDBAutoGauge<>(object, mapper);
    }

    void useRegistry(Map<MetricInfo, IMetric> registry) {
      metrics = registry;
    }

    void reset() {
      stop();
    }

    boolean hasMetadata(String name) {
      return nameToMetaInfo.containsKey(name);
    }
  }

  private static class RecordingReporter implements JmxReporter {
    private final TestMetricManager manager;
    private final AtomicInteger registrations = new AtomicInteger();
    private final AtomicInteger removals = new AtomicInteger();
    private final AtomicReference<IMetric> removed = new AtomicReference<>();
    private Runnable beforeRemoval = () -> {};

    private RecordingReporter(TestMetricManager manager) {
      this.manager = manager;
    }

    @Override
    public void registerMetric(IMetric metric, MetricInfo info) {
      assertSame(metric, manager.getAllMetrics().get(info));
      registrations.incrementAndGet();
    }

    @Override
    public void unregisterMetric(IMetric metric, MetricInfo info) {
      assertNotNull(metric);
      beforeRemoval.run();
      removed.set(metric);
      removals.incrementAndGet();
    }

    @Override
    public boolean start() {
      return true;
    }

    @Override
    public boolean stop() {
      return true;
    }

    @Override
    public ReporterType getReporterType() {
      return ReporterType.JMX;
    }
  }
}
