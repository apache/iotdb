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

package org.apache.iotdb.metrics.reporter.prometheus;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.impl.DoNothingAutoGauge;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;
import reactor.netty.DisposableServer;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.DoubleSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrometheusReporterTest {

  @Test
  public void testManagedExecutorRecreatedAfterRestart() {
    MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
    boolean originalAsyncUpdate = metricConfig.isPrometheusReporterAsyncUpdate();
    Integer originalPort = metricConfig.getPrometheusReporterPort();
    metricConfig.setPrometheusReporterAsyncUpdate(true);
    metricConfig.setPrometheusReporterPort(0);

    AtomicInteger factoryCalls = new AtomicInteger();
    List<ScheduledExecutorService> executors = new ArrayList<>();
    PrometheusReporter reporter =
        new PrometheusReporter(
            new DoNothingMetricManager(),
            () -> {
              factoryCalls.incrementAndGet();
              ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
              executors.add(executor);
              return executor;
            });
    try {
      assertTrue(reporter.start());
      assertEquals(1, factoryCalls.get());
      assertTrue(reporter.stop());
      assertTrue(executors.get(0).isShutdown());

      assertTrue(reporter.start());
      assertEquals(2, factoryCalls.get());
      assertTrue(reporter.stop());
      assertTrue(executors.get(1).isShutdown());
    } finally {
      reporter.stop();
      metricConfig.setPrometheusReporterAsyncUpdate(originalAsyncUpdate);
      metricConfig.setPrometheusReporterPort(originalPort);
      executors.forEach(ScheduledExecutorService::shutdownNow);
    }
  }

  @Test
  public void testFirstRequestWaitsForBackgroundSnapshot() throws Exception {
    assertSerializedScrapes("updateSnapshot", "getMetricsSnapshot", 1);
  }

  @Test
  public void testConcurrentFirstRequestsReuseSnapshot() throws Exception {
    assertSerializedScrapes("getMetricsSnapshot", "getMetricsSnapshot", 1);
  }

  @Test
  public void testBackgroundRefreshWaitsForFirstRequest() throws Exception {
    assertSerializedScrapes("getMetricsSnapshot", "updateSnapshot", 2);
  }

  @Test
  public void testSynchronousScrapeWaitsForFirstRequest() throws Exception {
    assertSerializedScrapes("getMetricsSnapshot", "scrape", 2);
  }

  @Test
  public void testEmptySnapshotBeforeAndAfterMetricRegistration() throws Exception {
    Map<MetricInfo, IMetric> registeredMetrics = new HashMap<>();
    PrometheusReporter reporter =
        new PrometheusReporter(
            new DoNothingMetricManager() {
              @Override
              public Map<MetricInfo, IMetric> getAllMetrics() {
                return registeredMetrics;
              }
            });
    assertEquals("", invoke(reporter, "getMetricsSnapshot"));
    registeredMetrics.put(new MetricInfo(MetricType.AUTO_GAUGE, "sample"), sampleGauge(() -> 1.0));
    String snapshot = (String) invoke(reporter, "getMetricsSnapshot");
    assertTrue(snapshot.contains(" 1.0\n"));
    registeredMetrics.clear();
    assertEquals(snapshot, invoke(reporter, "getMetricsSnapshot"));
    invoke(reporter, "updateSnapshot");
    assertEquals("", invoke(reporter, "getMetricsSnapshot"));
  }

  @Test
  public void testMetricsResponseChunksPreserveContentAndSurrogatePairs() {
    String metrics = "x".repeat(8 * 1024 - 1) + "\uD83D\uDE00" + "y".repeat(8 * 1024 + 1);

    List<String> chunks = PrometheusReporter.splitMetricsIntoChunks(metrics).collectList().block();

    assertEquals(metrics, String.join("", chunks));
    assertTrue(chunks.stream().allMatch(chunk -> chunk.length() <= 8 * 1024));
    assertEquals(8 * 1024 - 1, chunks.get(0).length());
    assertTrue(chunks.get(1).startsWith("\uD83D\uDE00"));
  }

  @Test
  public void testLargeMetricsSnapshotCanBeScraped() throws Exception {
    MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
    boolean originalAsyncUpdate = metricConfig.isPrometheusReporterAsyncUpdate();
    Integer originalPort = metricConfig.getPrometheusReporterPort();
    metricConfig.setPrometheusReporterAsyncUpdate(true);
    metricConfig.setPrometheusReporterPort(0);
    PrometheusReporter reporter = new PrometheusReporter(new DoNothingMetricManager());
    HttpURLConnection connection = null;
    try {
      assertTrue(reporter.start());
      // Also run this regression with -XX:MaxDirectMemorySize=32m: a single response buffer for
      // this snapshot cannot fit, whereas streaming it requires only bounded direct memory.
      String metrics = "x".repeat(8 * 1024 - 1) + "\uD83D\uDE00" + "y".repeat(24 * 1024 * 1024);
      Field snapshotField = PrometheusReporter.class.getDeclaredField("metricsSnapshot");
      snapshotField.setAccessible(true);
      snapshotField.set(reporter, metrics);
      Field serverField = PrometheusReporter.class.getDeclaredField("httpServer");
      serverField.setAccessible(true);
      int port = ((DisposableServer) serverField.get(reporter)).port();
      connection =
          (HttpURLConnection)
              URI.create("http://127.0.0.1:" + port + "/metrics").toURL().openConnection();
      connection.setConnectTimeout(5_000);
      connection.setReadTimeout(10_000);
      assertEquals(200, connection.getResponseCode());
      try (InputStream response = connection.getInputStream()) {
        assertEquals(metrics, new String(response.readAllBytes(), StandardCharsets.UTF_8));
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
      reporter.stop();
      metricConfig.setPrometheusReporterAsyncUpdate(originalAsyncUpdate);
      metricConfig.setPrometheusReporterPort(originalPort);
    }
  }

  private void assertSerializedScrapes(String firstMethod, String secondMethod, int expectedReads)
      throws Exception {
    CountDownLatch collecting = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger reads = new AtomicInteger();
    DoubleSupplier supplier =
        () -> {
          int value = reads.incrementAndGet();
          if (value == 1) {
            collecting.countDown();
            try {
              assertTrue(release.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
          }
          return value;
        };
    PrometheusReporter reporter =
        new PrometheusReporter(
            new DoNothingMetricManager() {
              @Override
              public Map<MetricInfo, IMetric> getAllMetrics() {
                return Collections.singletonMap(
                    new MetricInfo(MetricType.AUTO_GAUGE, "sample"), sampleGauge(supplier));
              }
            });
    FutureTask<Object> first = new FutureTask<>(() -> invoke(reporter, firstMethod));
    FutureTask<Object> second = new FutureTask<>(() -> invoke(reporter, secondMethod));
    Thread firstThread = new Thread(first, "first-scrape");
    Thread secondThread = new Thread(second, "second-scrape");
    try {
      firstThread.start();
      assertTrue(collecting.await(10, TimeUnit.SECONDS));
      secondThread.start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (secondThread.getState() != Thread.State.BLOCKED && !second.isDone()) {
        if (System.nanoTime() > deadline) {
          fail("The second scrape did not wait for the active collection");
        }
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
      }
      assertEquals(Thread.State.BLOCKED, secondThread.getState());
      assertEquals(1, reads.get());
      release.countDown();
      first.get(10, TimeUnit.SECONDS);
      second.get(10, TimeUnit.SECONDS);
      String snapshot = (String) invoke(reporter, "getMetricsSnapshot");
      assertTrue(snapshot.endsWith(" " + expectedReads + ".0\n"));
      assertEquals(expectedReads, reads.get());
    } finally {
      release.countDown();
      first.cancel(true);
      second.cancel(true);
      firstThread.join(TimeUnit.SECONDS.toMillis(10));
      secondThread.join(TimeUnit.SECONDS.toMillis(10));
    }
  }

  private static AutoGauge sampleGauge(DoubleSupplier supplier) {
    return new DoNothingAutoGauge() {
      @Override
      public double getValue() {
        return supplier.getAsDouble();
      }
    };
  }

  private static Object invoke(PrometheusReporter reporter, String methodName) throws Exception {
    Method method = PrometheusReporter.class.getDeclaredMethod(methodName);
    method.setAccessible(true);
    return method.invoke(reporter);
  }
}
