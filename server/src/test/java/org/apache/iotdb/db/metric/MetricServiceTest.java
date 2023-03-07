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

package org.apache.iotdb.db.metric;

import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.DoNothingMetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricFrameType;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MetricServiceTest {

  private static final double DELTA = 0.000001;

  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private static AbstractMetricService metricService = new DoNothingMetricService();

  @Test
  public void testMetricService() {
    for (MetricFrameType type : MetricFrameType.values()) {
      // init metric service
      metricConfig.setMetricFrameType(type);
      metricConfig.setMetricLevel(MetricLevel.IMPORTANT);
      metricService = new DoNothingMetricService();
      metricService.startService();

      // test metric service
      assertTrue(metricService.getMetricManager().isEnableMetricInGivenLevel(MetricLevel.CORE));
      assertTrue(
          metricService.getMetricManager().isEnableMetricInGivenLevel(MetricLevel.IMPORTANT));
      assertFalse(metricService.getMetricManager().isEnableMetricInGivenLevel(MetricLevel.NORMAL));
      assertFalse(metricService.getMetricManager().isEnableMetricInGivenLevel(MetricLevel.ALL));

      testNormalSituation();

      testOtherSituation();

      // stop metric module
      metricService.stopService();
    }
  }

  private void testNormalSituation() {
    // test counter
    Counter counter1 =
        metricService.getOrCreateCounter("counter1", MetricLevel.IMPORTANT, "tag", "value");
    assertNotNull(counter1);
    metricService.count(10, "counter1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(10, counter1.count());
    metricService.count(20, "counter1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(30, counter1.count());
    Counter counter2 =
        metricService.getOrCreateCounter("counter1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(counter1, counter2);
    counter2 = metricService.getOrCreateCounter("counter2", MetricLevel.IMPORTANT);
    assertNotEquals(counter1, counter2);
    counter2 = metricService.getOrCreateCounter("counter3", MetricLevel.IMPORTANT, "tag", "value");
    assertNotEquals(counter1, counter2);
    counter2 =
        metricService.getOrCreateCounter(
            "counter4", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
    assertNotEquals(counter1, counter2);
    counter2 = metricService.getOrCreateCounter("counter5", MetricLevel.NORMAL, "tag", "value");
    assertEquals(DoNothingMetricManager.DO_NOTHING_COUNTER, counter2);
    assertEquals(4, metricService.getMetricsByType(MetricType.COUNTER).size());
    metricService.count(10, "counter6", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(5, metricService.getMetricsByType(MetricType.COUNTER).size());
    metricService.remove(MetricType.COUNTER, "counter6");
    assertEquals(5, metricService.getMetricsByType(MetricType.COUNTER).size());
    metricService.remove(MetricType.COUNTER, "counter6", "tag", "value");
    assertEquals(4, metricService.getMetricsByType(MetricType.COUNTER).size());
    assertEquals(4, metricService.getAllMetricKeys().size());

    // test gauge
    Gauge gauge1 = metricService.getOrCreateGauge("gauge1", MetricLevel.IMPORTANT, "tag", "value");
    assertNotNull(gauge1);
    metricService.gauge(10, "gauge1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(10, gauge1.value());
    Gauge gauge2 = metricService.getOrCreateGauge("gauge1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(gauge1, gauge2);
    gauge2 = metricService.getOrCreateGauge("gauge2", MetricLevel.IMPORTANT);
    assertNotEquals(gauge1, gauge2);
    gauge2 = metricService.getOrCreateGauge("gauge3", MetricLevel.IMPORTANT, "tag", "value");
    assertNotEquals(gauge1, gauge2);
    gauge2 =
        metricService.getOrCreateGauge(
            "gauge4", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
    assertNotEquals(gauge1, gauge2);
    gauge2 = metricService.getOrCreateGauge("gauge5", MetricLevel.NORMAL, "tag", "value");
    assertEquals(DoNothingMetricManager.DO_NOTHING_GAUGE, gauge2);
    assertEquals(4, metricService.getMetricsByType(MetricType.GAUGE).size());
    metricService.gauge(10, "gauge6", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(5, metricService.getMetricsByType(MetricType.GAUGE).size());
    metricService.remove(MetricType.GAUGE, "gauge6");
    assertEquals(5, metricService.getMetricsByType(MetricType.GAUGE).size());
    metricService.remove(MetricType.GAUGE, "gauge6", "tag", "value");
    assertEquals(4, metricService.getMetricsByType(MetricType.GAUGE).size());
    assertEquals(8, metricService.getAllMetricKeys().size());

    // test auto gauge
    List<Integer> list = new ArrayList<>();
    AutoGauge autoGauge =
        metricService.createAutoGauge(
            "autoGauge", MetricLevel.IMPORTANT, list, List::size, "tag", "value");
    assertEquals(0d, autoGauge.value(), DELTA);
    list.add(1);
    assertEquals(1d, autoGauge.value(), DELTA);
    list.clear();
    assertEquals(0d, autoGauge.value(), DELTA);
    list.add(1);
    assertEquals(1d, autoGauge.value(), DELTA);
    list = null;
    System.gc();
    assertEquals(0d, autoGauge.value(), DELTA);
    assertEquals(4, metricService.getMetricsByType(MetricType.GAUGE).size());
    assertEquals(1, metricService.getMetricsByType(MetricType.AUTO_GAUGE).size());
    metricService.remove(MetricType.AUTO_GAUGE, "autoGauge", "tag", "value");
    assertEquals(4, metricService.getMetricsByType(MetricType.GAUGE).size());
    assertEquals(8, metricService.getAllMetricKeys().size());

    // test rate
    Rate rate1 = metricService.getOrCreateRate("rate1", MetricLevel.IMPORTANT, "tag", "value");
    assertNotNull(rate1);
    metricService.rate(10, "rate1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(10, rate1.getCount());
    metricService.rate(20, "rate1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(30, rate1.getCount());
    Rate rate2 = metricService.getOrCreateRate("rate1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(rate1, rate2);
    rate2 = metricService.getOrCreateRate("rate2", MetricLevel.IMPORTANT);
    assertNotEquals(rate1, rate2);
    rate2 = metricService.getOrCreateRate("rate3", MetricLevel.IMPORTANT, "tag", "value");
    assertNotEquals(rate1, rate2);
    rate2 =
        metricService.getOrCreateRate(
            "rate4", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
    assertNotEquals(rate1, rate2);
    rate2 = metricService.getOrCreateRate("rate5", MetricLevel.NORMAL, "tag", "value");
    assertEquals(4, metricService.getMetricsByType(MetricType.RATE).size());
    assertEquals(DoNothingMetricManager.DO_NOTHING_RATE, rate2);
    metricService.rate(10, "rate6", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(5, metricService.getMetricsByType(MetricType.RATE).size());
    metricService.remove(MetricType.RATE, "rate6");
    assertEquals(5, metricService.getMetricsByType(MetricType.RATE).size());
    metricService.remove(MetricType.RATE, "rate6", "tag", "value");
    assertEquals(4, metricService.getMetricsByType(MetricType.RATE).size());
    assertEquals(12, metricService.getAllMetricKeys().size());

    // test histogram
    Histogram histogram1 =
        metricService.getOrCreateHistogram("histogram1", MetricLevel.IMPORTANT, "tag", "value");
    assertNotNull(histogram1);
    metricService.histogram(10, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.histogram(20, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.histogram(30, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.histogram(40, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.histogram(50, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(5, histogram1.count());
    assertEquals(5, histogram1.takeSnapshot().size());
    assertEquals(10.0D, histogram1.takeSnapshot().getMin(), 0.00001);
    assertEquals(50.0D, histogram1.takeSnapshot().getMax(), 0.00001);
    Histogram histogram2 =
        metricService.getOrCreateHistogram("histogram1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(histogram1, histogram2);
    histogram2 = metricService.getOrCreateHistogram("histogram2", MetricLevel.IMPORTANT);
    assertNotEquals(histogram1, histogram2);
    histogram2 =
        metricService.getOrCreateHistogram("histogram3", MetricLevel.IMPORTANT, "tag", "value");
    assertNotEquals(histogram1, histogram2);
    histogram2 =
        metricService.getOrCreateHistogram(
            "histogram4", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
    assertNotEquals(histogram1, histogram2);
    histogram2 =
        metricService.getOrCreateHistogram("histogram5", MetricLevel.NORMAL, "tag", "value");
    assertEquals(DoNothingMetricManager.DO_NOTHING_HISTOGRAM, histogram2);
    assertEquals(4, metricService.getMetricsByType(MetricType.HISTOGRAM).size());
    metricService.histogram(10, "histogram6", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(5, metricService.getMetricsByType(MetricType.HISTOGRAM).size());
    metricService.remove(MetricType.HISTOGRAM, "histogram6");
    assertEquals(5, metricService.getMetricsByType(MetricType.HISTOGRAM).size());
    metricService.remove(MetricType.HISTOGRAM, "histogram6", "tag", "value");
    assertEquals(4, metricService.getMetricsByType(MetricType.HISTOGRAM).size());
    assertEquals(16, metricService.getAllMetricKeys().size());

    // test timer
    Timer timer1 = metricService.getOrCreateTimer("timer1", MetricLevel.IMPORTANT, "tag", "value");
    assertNotNull(timer1);
    metricService.timer(2, TimeUnit.MILLISECONDS, "timer1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.timer(4, TimeUnit.MILLISECONDS, "timer1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.timer(6, TimeUnit.MILLISECONDS, "timer1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.timer(8, TimeUnit.MILLISECONDS, "timer1", MetricLevel.IMPORTANT, "tag", "value");
    metricService.timer(10, TimeUnit.MILLISECONDS, "timer1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(5, timer1.getImmutableRate().getCount());
    assertEquals(5, timer1.takeSnapshot().size());
    Timer timer2 = metricService.getOrCreateTimer("timer1", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(timer1, timer2);
    timer2 = metricService.getOrCreateTimer("timer2", MetricLevel.IMPORTANT);
    assertNotEquals(timer1, timer2);
    timer2 = metricService.getOrCreateTimer("timer3", MetricLevel.IMPORTANT, "tag", "value");
    assertNotEquals(timer1, timer2);
    timer2 =
        metricService.getOrCreateTimer(
            "timer4", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
    assertNotEquals(timer1, timer2);
    timer2 = metricService.getOrCreateTimer("timer5", MetricLevel.NORMAL, "tag", "value");
    assertNotEquals(timer1, timer2);
    assertEquals(4, metricService.getMetricsByType(MetricType.TIMER).size());
    metricService.timer(10, TimeUnit.MILLISECONDS, "timer6", MetricLevel.IMPORTANT, "tag", "value");
    assertEquals(5, metricService.getMetricsByType(MetricType.TIMER).size());
    metricService.remove(MetricType.TIMER, "timer6");
    assertEquals(5, metricService.getMetricsByType(MetricType.TIMER).size());
    metricService.remove(MetricType.TIMER, "timer6", "tag", "value");
    assertEquals(4, metricService.getMetricsByType(MetricType.TIMER).size());
    assertEquals(20, metricService.getAllMetricKeys().size());

    // test remove same key and different value counter
    Counter removeCounter1 =
        metricService.getOrCreateCounter("remove", MetricLevel.IMPORTANT, "tag", "value1");
    assertNotNull(removeCounter1);
    Counter removeCounter2 =
        metricService.getOrCreateCounter("remove", MetricLevel.IMPORTANT, "tag", "value2");
    assertNotNull(removeCounter2);
    assertEquals(6, metricService.getMetricsByType(MetricType.COUNTER).size());
    assertEquals(22, metricService.getAllMetricKeys().size());
    metricService.remove(MetricType.COUNTER, "remove", "tag", "value1");
    assertEquals(5, metricService.getMetricsByType(MetricType.COUNTER).size());
    assertEquals(21, metricService.getAllMetricKeys().size());
    removeCounter2 =
        metricService.getOrCreateCounter("remove", MetricLevel.IMPORTANT, "tag", "value1");
    assertNotNull(removeCounter2);
    assertEquals(6, metricService.getMetricsByType(MetricType.COUNTER).size());
    assertEquals(22, metricService.getAllMetricKeys().size());
  }

  private void testOtherSituation() {
    assertThrows(IllegalArgumentException.class, this::getOrCreateDifferentMetricsWithSameName);

    // forbidden to register same name but different type metrics
    Timer timer =
        metricService.getOrCreateTimer("same_name", MetricLevel.IMPORTANT, "tag", "value");
    assertNotNull(timer);
    assertNotEquals(DoNothingMetricManager.DO_NOTHING_TIMER, timer);
    Counter counter = metricService.getOrCreateCounter("same_name", MetricLevel.IMPORTANT);
    assertNotNull(counter);
    assertEquals(DoNothingMetricManager.DO_NOTHING_COUNTER, counter);
  }

  private void getOrCreateDifferentMetricsWithSameName() {
    Timer timer =
        metricService.getOrCreateTimer("same_name", MetricLevel.IMPORTANT, "tag", "value");
    assertNotNull(timer);
    metricService.getOrCreateCounter("same_name", MetricLevel.IMPORTANT, "tag", "value");
  }
}
