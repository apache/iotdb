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
}
