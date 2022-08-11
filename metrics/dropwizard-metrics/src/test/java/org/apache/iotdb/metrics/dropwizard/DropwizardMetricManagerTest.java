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

package org.apache.iotdb.metrics.dropwizard;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.DoNothingMetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricFrameworkType;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DropwizardMetricManagerTest {
  static MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  static AbstractMetricService metricService = new DoNothingMetricService();
  static DropwizardMetricManager metricManager;

  @BeforeClass
  public static void init() {
    metricConfig.setEnableMetric(true);
    metricConfig.setMetricFrameworkType(MetricFrameworkType.DROPWIZARD);
    metricConfig.setMetricLevel(MetricLevel.IMPORTANT);
    metricConfig.setPredefinedMetrics(new ArrayList<>());
    metricService.startService();
    metricManager = (DropwizardMetricManager) metricService.getMetricManager();
  }

  private void getOrCreateDifferentMetricsWithSameName() {
    Timer timer = metricManager.getOrCreateTimer("metric", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(timer);
    metricManager.getOrCreateCounter("metric", MetricLevel.IMPORTANT, "tag1", "tag2");
  }

  @Test
  public void getOrCreateDifferentMetricsWithSameNameTest() {
    assertThrows(IllegalArgumentException.class, this::getOrCreateDifferentMetricsWithSameName);
  }

  @Test
  public void removeHistogram() {
    Histogram histogram1 =
        metricManager.getOrCreateHistogram(
            "histogram_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.removeHistogram("histogram_remove", "tag1", "tag2");
    Histogram histogram2 =
        metricManager.getOrCreateHistogram(
            "histogram_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotEquals(histogram1, histogram2);
  }

  @Test
  public void removeTimer() {
    Timer timer1 =
        metricManager.getOrCreateTimer("timer_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.removeTimer("timer_remove", "tag1", "tag2");
    Timer timer2 =
        metricManager.getOrCreateTimer("timer_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotEquals(timer1, timer2);
  }

  @Test
  public void getAllMetricKeys() {
    metricManager.getOrCreateCounter("metric_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    List<String[]> result = metricManager.getAllMetricKeys();
    assertNotNull(result);
    boolean isContains = false;
    for (String[] res : result) {
      if (String.join(",", res).equals("metric_test,tag1,tag2")) {
        isContains = true;
        break;
      }
    }
    assertTrue(isContains);
  }

  @Test
  public void getAllCounters() {
    metricManager.getOrCreateCounter("counters", MetricLevel.IMPORTANT);
    Map<String[], Counter> counters = metricManager.getAllCounters();
    assertNotNull(counters);
    assertTrue(counters.size() > 0);
  }

  @Test
  public void getAllGauges() {
    metricManager.getOrCreateGauge("gauges", MetricLevel.IMPORTANT);
    Map<String[], Gauge> gauges = metricManager.getAllGauges();
    assertNotNull(gauges);
    assertTrue(gauges.size() > 0);
  }

  @Test
  public void getAllRates() {
    metricManager.getOrCreateRate("rates", MetricLevel.IMPORTANT);
    Map<String[], Rate> rates = metricManager.getAllRates();
    assertNotNull(rates);
    assertTrue(rates.size() > 0);
  }

  @Test
  public void getAllHistograms() {
    metricManager.getOrCreateHistogram("histograms", MetricLevel.IMPORTANT);
    Map<String[], Histogram> histograms = metricManager.getAllHistograms();
    assertNotNull(histograms);
    assertTrue(histograms.size() > 0);
  }

  @Test
  public void getAllTimers() {
    metricManager.getOrCreateTimer("timers", MetricLevel.IMPORTANT);
    Map<String[], Timer> timers = metricManager.getAllTimers();
    assertNotNull(timers);
    assertTrue(timers.size() > 0);
  }

  @Test
  public void isEnable() {
    assertTrue(metricManager.isEnableMetric());
    assertTrue(metricManager.isEnableMetricInGivenLevel(MetricLevel.IMPORTANT));
  }

  @AfterClass
  public static void stop() {
    metricManager.stop();
  }
}
