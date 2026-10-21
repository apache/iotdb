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
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;

import org.junit.Test;
import reactor.netty.DisposableServer;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
}
