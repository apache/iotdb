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

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.metricsets.jvm.JvmGcMetrics;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.SystemMetric;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricServiceRestartTest {

  private static final String METRIC = "metric_service_restart_test";

  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final List<IMetricSet> boundMetricSets = new ArrayList<>();
  private MetricLevel originalLevel;
  private String originalReporters;

  @Before
  public void setUp() {
    originalLevel = config.getMetricLevel();
    originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("");
    service.startService();
  }

  @After
  public void tearDown() {
    boundMetricSets.forEach(service::removeMetricSet);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testFailedMetricSetDoesNotStopOthersFromRebinding() {
    // The hash codes make the failed set iterated before the other one
    final FailingMetricSet failing = new FailingMetricSet();
    final GaugeMetricSet gauge = new GaugeMetricSet();
    bind(failing);
    bind(gauge);
    assertTrue(service.getAllMetrics().keySet().stream().anyMatch(this::isTestMetric));

    failing.fail = true;
    service.restartService();
    failing.fail = false;
    assertTrue(service.getAllMetrics().keySet().stream().anyMatch(this::isTestMetric));
  }

  @Test
  public void testSystemDiskMetricsSurviveRestart() {
    final SystemMetrics metrics = new SystemMetrics();
    metrics.setDiskDirs(Collections.singletonList(System.getProperty("java.io.tmpdir")));
    bind(metrics);
    assertTrue(metrics.getSystemDiskTotalSpace() > 0);

    service.restartService();
    assertTrue(metrics.getSystemDiskTotalSpace() > 0);
  }

  @Test
  public void testSystemDiskMetricsAreCollectedWhenLevelIsEnabledByRestart() {
    config.setMetricLevel(MetricLevel.OFF);
    final SystemMetrics metrics = new SystemMetrics();
    metrics.setDiskDirs(Collections.singletonList(System.getProperty("java.io.tmpdir")));
    bind(metrics);
    assertEquals(0, metrics.getSystemDiskTotalSpace());

    config.setMetricLevel(MetricLevel.IMPORTANT);
    service.restartService();
    assertTrue(metrics.getSystemDiskTotalSpace() > 0);
  }

  @Test
  public void testGcPauseTimersKeepCountingAfterRestart() {
    bind(new JvmGcMetrics());
    service.restartService();
    service.restartService();

    // The listeners of previous bindings used to reset the timers on every GC
    System.gc();
    await().atMost(30, TimeUnit.SECONDS).until(() -> getExplicitGcCount() >= 1);
    System.gc();
    await().atMost(30, TimeUnit.SECONDS).until(() -> getExplicitGcCount() >= 2);
  }

  @Test
  public void testGcPauseTimersAreBoundAgainBeforeTheNextGc() {
    bind(new JvmGcMetrics());
    System.gc();
    await().atMost(30, TimeUnit.SECONDS).until(() -> getExplicitGcCount() >= 1);

    service.restartService();
    // The timers used to be created again only by the next GC with the same cause
    assertTrue(
        service.getAllMetrics().keySet().stream()
            .anyMatch(
                info ->
                    SystemMetric.JVM_GC_PAUSE.toString().equals(info.getName())
                        && "System.gc()".equals(info.getTags().get("cause"))));
  }

  private void bind(final IMetricSet metricSet) {
    service.addMetricSet(metricSet);
    boundMetricSets.add(metricSet);
  }

  private boolean isTestMetric(final MetricInfo info) {
    return METRIC.equals(info.getName());
  }

  private long getExplicitGcCount() {
    long count = 0;
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (SystemMetric.JVM_GC_PAUSE.toString().equals(entry.getKey().getName())
          && "System.gc()".equals(entry.getKey().getTags().get("cause"))) {
        count += ((Timer) entry.getValue()).getCount();
      }
    }
    return count;
  }

  private static class FailingMetricSet implements IMetricSet {

    private volatile boolean fail = false;

    @Override
    public void bindTo(final AbstractMetricService metricService) {
      if (fail) {
        throw new IllegalStateException("bind failure for test");
      }
    }

    @Override
    public void unbindFrom(final AbstractMetricService metricService) {
      // do nothing
    }

    @Override
    public boolean equals(final Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private static class GaugeMetricSet implements IMetricSet {

    private final AtomicLong value = new AtomicLong(1);

    @Override
    public void bindTo(final AbstractMetricService metricService) {
      metricService.createAutoGauge(METRIC, MetricLevel.IMPORTANT, value, AtomicLong::get);
    }

    @Override
    public void unbindFrom(final AbstractMetricService metricService) {
      metricService.remove(MetricType.AUTO_GAUGE, METRIC);
    }

    @Override
    public boolean equals(final Object o) {
      return this == o;
    }

    @Override
    public int hashCode() {
      return 1;
    }
  }
}
