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

import org.apache.iotdb.metrics.DoNothingMetricService;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MonitorType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DropwizardMetricManagerTest {
  static MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  static MetricService metricService = new DoNothingMetricService();
  static MetricManager metricManager;

  @BeforeClass
  public static void init() {
    metricConfig.setEnableMetric(true);
    metricConfig.setMonitorType(MonitorType.DROPWIZARD);
    metricConfig.setMetricLevel(MetricLevel.IMPORTANT);
    metricConfig.setPredefinedMetrics(new ArrayList<>());
    metricService.startService();
    metricManager = metricService.getMetricManager();
  }

  @Test
  public void getOrCreateCounter() {
    Counter counter1 =
        metricManager.getOrCreateCounter("counter_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(counter1);
    Counter counter2 =
        metricManager.getOrCreateCounter("counter_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(counter1, counter2);
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
  public void getOrCreateGauge() {
    Gauge gauge1 =
        metricManager.getOrCreateGauge("gauge_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(gauge1);
    Gauge gauge2 =
        metricManager.getOrCreateGauge("gauge_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(gauge1, gauge2);
  }

  @Test
  public void testAutoGauge() {
    List<Integer> list = new ArrayList<>();
    Gauge autoGauge =
        metricManager.getOrCreateAutoGauge(
            "autoGaugeMetric", MetricLevel.IMPORTANT, list, List::size, "tagk", "tagv");
    assertEquals(0L, autoGauge.value());
    list.add(1);
    assertEquals(1L, autoGauge.value());
    list.clear();
    assertEquals(0L, autoGauge.value());
    list.add(1);
    assertEquals(1L, autoGauge.value());
    list = null;
    System.gc();
    assertEquals(0L, autoGauge.value());
  }

  @Test
  public void getOrCreateRate() {
    Rate rate1 = metricManager.getOrCreateRate("rate_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(rate1);
    Rate rate2 = metricManager.getOrCreateRate("rate_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(rate1, rate2);
  }

  @Test
  public void getOrCreateHistogram() {
    Histogram histogram1 =
        metricManager.getOrCreateHistogram("histogram_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(histogram1);
    Histogram histogram2 =
        metricManager.getOrCreateHistogram("histogram_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(histogram1, histogram2);
  }

  @Test
  public void getOrCreateTimer() {
    Timer timer1 =
        metricManager.getOrCreateTimer("timer_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(timer1);
    Timer timer2 =
        metricManager.getOrCreateTimer("timer_test", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(timer1, timer2);
  }

  @Test
  public void count() {
    Counter counter =
        metricManager.getOrCreateCounter("count_inc", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(counter);
    metricManager.count(10, "count_inc", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(counter.count(), 10);
    metricManager.count(10L, "count_inc", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(counter.count(), 20);
  }

  @Test
  public void gauge() {
    Gauge gauge1 =
        metricManager.getOrCreateGauge("gauge_set1", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(gauge1);
    metricManager.gauge(10, "gauge_set1", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(10, gauge1.value());
    Gauge gauge2 =
        metricManager.getOrCreateGauge("gauge_set2", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.gauge(20L, "gauge_set2", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(20, gauge2.value());
  }

  @Test
  public void rate() {
    Rate rate = metricManager.getOrCreateRate("rate_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(rate);
    metricManager.rate(10, "rate_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(10, rate.getCount());
    metricManager.rate(20L, "rate_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(30, rate.getCount());
  }

  @Test
  public void histogram() {
    Histogram histogram =
        metricManager.getOrCreateHistogram("history_count", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotNull(histogram);
    metricManager.histogram(10, "history_count", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.histogram(20L, "history_count", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.histogram(30, "history_count", MetricLevel.IMPORTANT, "tag1", "tag2");
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
      // do nothing
    }
    metricManager.histogram(40L, "history_count", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.histogram(50, "history_count", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(5, histogram.count());
    assertEquals(5, histogram.takeSnapshot().size());
    assertEquals(10, histogram.takeSnapshot().getMin());
    assertEquals(30.0, histogram.takeSnapshot().getMedian(), 1e-5);
    assertEquals(30.0, histogram.takeSnapshot().getMean(), 1e-5);
    assertEquals(50, histogram.takeSnapshot().getMax());
  }

  @Test
  public void timer() {
    Timer timer =
        metricManager.getOrCreateTimer("timer_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.timer(2L, TimeUnit.MINUTES, "timer_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.timer(
        4L, TimeUnit.MINUTES, "timer_" + "mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.timer(6L, TimeUnit.MINUTES, "timer_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
      // do nothing
    }
    metricManager.timer(8L, TimeUnit.MINUTES, "timer_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.timer(10L, TimeUnit.MINUTES, "timer_mark", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertEquals(5, timer.getImmutableRate().getCount());
    assertEquals(5, timer.takeSnapshot().size());
    assertEquals(120000000000L, timer.takeSnapshot().getMin());
    assertEquals(360000000000L, timer.takeSnapshot().getMedian(), 1e-5);
    assertEquals(360000000000L, timer.takeSnapshot().getMean(), 1e-5);
    assertEquals(600000000000L, timer.takeSnapshot().getMax());
  }

  @Test
  public void removeCounter() {
    Counter counter1 =
        metricManager.getOrCreateCounter("counter_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.removeCounter("counter_remove", "tag1", "tag2");
    Counter counter2 =
        metricManager.getOrCreateCounter("counter_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotEquals(counter1, counter2);
  }

  @Test
  public void removeGauge() {
    Gauge gauge1 =
        metricManager.getOrCreateGauge("gauge_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.removeGauge("gauge_remove", "tag1", "tag2");
    Gauge gauge2 =
        metricManager.getOrCreateGauge("gauge_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotEquals(gauge1, gauge2);
  }

  @Test
  public void removeRate() {
    Rate rate1 =
        metricManager.getOrCreateRate("rate_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    metricManager.removeRate("rate_remove", "tag1", "tag2");
    Rate rate2 =
        metricManager.getOrCreateRate("rate_remove", MetricLevel.IMPORTANT, "tag1", "tag2");
    assertNotEquals(rate1, rate2);
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
    assertTrue(metricManager.isEnable());
    assertTrue(metricManager.isEnable(MetricLevel.IMPORTANT));
  }

  @AfterClass
  public static void stop() {
    metricManager.stop();
  }
}
