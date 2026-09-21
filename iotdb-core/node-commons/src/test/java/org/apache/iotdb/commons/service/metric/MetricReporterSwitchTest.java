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

package org.apache.iotdb.commons.service.metric;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.core.reporter.IoTDBJmxReporter;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetricReporterSwitchTest {
  private static final String METRIC = "reporter_switch_value";
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final MetricService service = MetricService.getInstance();
  private final AtomicInteger oldValue = new AtomicInteger(1);
  private final AtomicInteger newValue = new AtomicInteger(2);
  private MetricLevel previousLevel;
  private String previousReporters;

  @Before
  public void setUp() {
    previousLevel = config.getMetricLevel();
    previousReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("JMX");
    service.startService();
  }

  @After
  public void tearDown() {
    service.getMetricManager().setBindJmxReporter(null);
    service.stopService();
    config.setMetricLevel(previousLevel);
    config.setMetricReporterList(previousReporters);
  }

  @Test
  public void testDisablingReporterDuringGaugeReplacement() throws Exception {
    replaceGaugeWhileReloading("");
  }

  @Test
  public void testReloadingReporterKeepsCapturedNotificationTarget() throws Exception {
    replaceGaugeWhileReloading("JMX");
  }

  private void replaceGaugeWhileReloading(String reporters) throws Exception {
    AbstractMetricManager manager = service.getMetricManager();
    IoTDBJmxReporter delegate = IoTDBJmxReporter.getInstance();
    CountDownLatch removalCallback = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    AtomicInteger registrations = new AtomicInteger();
    AtomicInteger removals = new AtomicInteger();
    // Preserve real JMX behavior while pausing between replacement's remove/add notifications.
    manager.setBindJmxReporter(
        new JmxReporter() {
          @Override
          public void registerMetric(IMetric metric, MetricInfo info) {
            registrations.incrementAndGet();
            delegate.registerMetric(metric, info);
          }

          @Override
          public void unregisterMetric(IMetric metric, MetricInfo info) {
            removals.incrementAndGet();
            delegate.unregisterMetric(metric, info);
            removalCallback.countDown();
            try {
              assertTrue(resume.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
          }

          @Override
          public boolean start() {
            return delegate.start();
          }

          @Override
          public boolean stop() {
            return delegate.stop();
          }

          @Override
          public ReporterType getReporterType() {
            return ReporterType.JMX;
          }
        });
    manager.createAutoGauge(METRIC, MetricLevel.IMPORTANT, oldValue, AtomicInteger::get);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<AutoGauge> replacement =
          executor.submit(
              () ->
                  manager.createAutoGauge(
                      METRIC, MetricLevel.IMPORTANT, newValue, AtomicInteger::get));
      assertTrue(removalCallback.await(5, TimeUnit.SECONDS));
      config.setMetricReporterList(reporters);
      service.reloadService(ReloadLevel.RESTART_REPORTER);
      resume.countDown();
      assertEquals(2, replacement.get(5, TimeUnit.SECONDS).getValue(), 0);
      // Reload either clears the binding or replaces our wrapper with the real JMX reporter.
      // Both notifications for the in-flight replacement must still use the captured wrapper.
      assertEquals(2, registrations.get());
      assertEquals(1, removals.get());
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      ObjectName name =
          new ObjectName("org.apache.iotdb.metrics:name=" + METRIC + ",type=IoTDBAutoGauge");
      if (reporters.isEmpty()) {
        assertFalse(server.isRegistered(name));
      } else {
        assertEquals(2, (double) server.getAttribute(name, "Value"), 0);
      }
    } finally {
      resume.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
