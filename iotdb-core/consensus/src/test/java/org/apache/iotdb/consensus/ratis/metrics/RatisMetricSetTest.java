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

package org.apache.iotdb.consensus.ratis.metrics;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.Timekeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RatisMetricSetTest {

  private static final String COMPONENT = "ratis_metric_set_test";

  private final MetricRegistryInfo info =
      new MetricRegistryInfo("test", "ratis", COMPONENT, "RatisMetricSetTest");
  private final RatisMetricSet metricSet = new RatisMetricSet();
  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private MetricLevel originalLevel;
  private String originalReporters;
  private RatisMetricRegistry registry;

  @Before
  public void setUp() {
    originalLevel = config.getMetricLevel();
    originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("");
    service.startService();
    service.addMetricSet(metricSet);
    registry = MetricRegistries.global().create(info);
  }

  @After
  public void tearDown() {
    MetricRegistries.global().remove(info);
    service.removeMetricSet(metricSet);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testRatisMetricsSurviveMetricServiceRestart() {
    // Ratis creates these metrics once and keeps using the returned instances.
    final LongCounter counter = registry.counter("counter");
    final Timekeeper timer = registry.timer("timer");
    registry.gauge("gauge", () -> () -> 42);
    counter.inc();

    for (final MetricLevel level :
        new MetricLevel[] {MetricLevel.ALL, MetricLevel.OFF, MetricLevel.IMPORTANT}) {
      config.setMetricLevel(level);
      service.restartService();
      if (level == MetricLevel.OFF) {
        assertNull(getMetric("counter"));
        assertNull(getMetric("timer"));
        assertNull(getMetric("gauge"));
      } else {
        assertNotNull(getMetric("counter"));
        assertNotNull(getMetric("timer"));
        assertEquals(42, ((AutoGauge) getMetric("gauge")).getValue(), 0);
      }
    }

    counter.inc();
    timer.time().stop();
    assertEquals(1, ((Counter) getMetric("counter")).getCount());
    assertEquals(1, ((Timer) getMetric("timer")).getCount());
  }

  @Test
  public void testRemovedGaugeIsNotRestoredAndCanBeAddedAgain() {
    registry.gauge("gauge", () -> () -> 1);
    registry.remove("gauge");
    assertNull(getMetric("gauge"));
    service.restartService();
    assertNull(getMetric("gauge"));

    // Ratis adds a gauge again with a new supplier, e.g. when a server becomes leader again.
    registry.gauge("gauge", () -> () -> 2);
    assertEquals(2, ((AutoGauge) getMetric("gauge")).getValue(), 0);
    service.restartService();
    assertEquals(2, ((AutoGauge) getMetric("gauge")).getValue(), 0);
  }

  private IMetric getMetric(final String name) {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (entry.getKey().getName().endsWith(COMPONENT + name)) {
        return entry.getValue();
      }
    }
    return null;
  }
}
