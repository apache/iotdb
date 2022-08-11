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
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricFrameworkType;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class MetricServiceTest {

  private static final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  private static AbstractMetricService metricService = new DoNothingMetricService();

  // TODD write test logical
  @Test
  public void testNormalVersion() {
    for(MetricFrameworkType type: MetricFrameworkType.values()) {
      // init metric module
      initMetric(type);

      // test counter
      Counter counter1 =
          metricService.getOrCreateCounter("counter1", MetricLevel.IMPORTANT, "tag", "value");
      assertNotNull(counter1);
      metricService.count(10, "count1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(10, counter1.count());
      metricService.count(20, "count1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(30, counter1.count());
      Counter counter2 =
          metricService.getOrCreateCounter("counter1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(counter1, counter2);
      counter2 = metricService.getOrCreateCounter("counter1", MetricLevel.IMPORTANT);
      assertNotEquals(counter1, counter2);
      counter2 = metricService.getOrCreateCounter("counter2", MetricLevel.IMPORTANT, "tag", "value");
      assertNotEquals(counter1, counter2);
      counter2 = metricService.getOrCreateCounter("counter3", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
      assertNotEquals(counter1, counter2);
      counter2 = metricService.getOrCreateCounter("counter4", MetricLevel.NORMAL, "tag", "value");
      assertEquals(DoNothingMetricManager.doNothingCounter, counter2);
      assertEquals(4, metricService.getAllCounters().size());
      metricService.count(10, "counter5", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(5, metricService.getAllCounters().size());
      metricService.removeCounter("counter5");
      assertEquals(5, metricService.getAllCounters().size());
      metricService.removeCounter("counter5", "tag", "value");
      assertEquals(4, metricService.getAllCounters().size());

      // test gauge
      Gauge gauge1 =
          metricService.getOrCreateGauge("gauge1", MetricLevel.IMPORTANT, "tag", "value");
      assertNotNull(gauge1);
      metricService.gauge(10, "gauge1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(10, gauge1.value());
      Gauge gauge2 =
          metricService.getOrCreateGauge("gauge1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(gauge1, gauge2);
      gauge2 = metricService.getOrCreateGauge("gauge1", MetricLevel.IMPORTANT);
      assertNotEquals(gauge1, gauge2);
      gauge2 = metricService.getOrCreateGauge("gauge2", MetricLevel.IMPORTANT, "tag", "value");
      assertNotEquals(gauge1, gauge2);
      gauge2 = metricService.getOrCreateGauge("gauge3", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
      assertNotEquals(gauge1, gauge2);
      gauge2 = metricService.getOrCreateGauge("gauge4", MetricLevel.NORMAL, "tag", "value");
      assertEquals(DoNothingMetricManager.doNothingGauge, gauge2);
      assertEquals(4, metricService.getAllGauges().size());
      metricService.gauge(10, "gauge5", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(5, metricService.getAllGauges().size());
      metricService.removeGauge("gauge5");
      assertEquals(5, metricService.getAllGauges().size());
      metricService.removeGauge("gauge5", "tag", "value");
      assertEquals(4, metricService.getAllGauges().size());

      // test auto gauge
      List<Integer> list = new ArrayList<>();
      Gauge autoGauge =
          metricService.getOrCreateAutoGauge(
              "autoGauge1", MetricLevel.IMPORTANT, list, List::size, "tag", "value");
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
      assertEquals(5, metricService.getAllGauges().size());
      metricService.removeGauge("autoGauge1", "tag", "value");
      assertEquals(4, metricService.getAllGauges().size());

      // test rate
      Rate rate1 = metricService.getOrCreateRate("rate1", MetricLevel.IMPORTANT, "tag", "value");
      assertNotNull(rate1);
      metricService.rate(10, "rate1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(10, rate1.getCount());
      metricService.rate(20, "rate1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(30, rate1.getCount());
      Rate rate2 = metricService.getOrCreateRate("rate1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(rate1, rate2);
      rate2 = metricService.getOrCreateRate("rate1", MetricLevel.IMPORTANT);
      assertNotEquals(rate1, rate2);
      rate2 = metricService.getOrCreateRate("rate2", MetricLevel.IMPORTANT, "tag", "value");
      assertNotEquals(rate1, rate2);
      rate2 = metricService.getOrCreateRate("rate3", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
      assertNotEquals(rate1, rate2);
      rate2 = metricService.getOrCreateRate("rate4", MetricLevel.NORMAL, "tag", "value");
      assertEquals(4, metricService.getAllRates().size());
      assertEquals(DoNothingMetricManager.doNothingRate, rate2);
      metricService.rate(10, "rate5", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(5, metricService.getAllRates().size());
      metricService.removeRate("rate5");
      assertEquals(5, metricService.getAllRates().size());
      metricService.removeRate("rate5", "tag", "value");
      assertEquals(4, metricService.getAllRates().size());

      // test histogram
      Histogram histogram1 =
          metricService.getOrCreateHistogram("histogram1", MetricLevel.IMPORTANT, "tag", "value");
      assertNotNull(histogram1);
      metricService.histogram(10, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
      metricService.histogram(20, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
      metricService.histogram(30, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        // do nothing
      }
      metricService.histogram(40, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
      metricService.histogram(50, "histogram1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(5, histogram1.count());
      assertEquals(5, histogram1.takeSnapshot().size());
      assertEquals(10, histogram1.takeSnapshot().getMin());
      assertEquals(30.0, histogram1.takeSnapshot().getMedian(), 1e-5);
      assertEquals(30.0, histogram1.takeSnapshot().getMean(), 1e-5);
      assertEquals(50, histogram1.takeSnapshot().getMax());

      Histogram histogram2 =
          metricService.getOrCreateHistogram("histogram1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(histogram1, histogram2);
      histogram2 =
          metricService.getOrCreateHistogram("histogram1", MetricLevel.IMPORTANT);
      assertNotEquals(histogram1, histogram2);
      histogram2 =
          metricService.getOrCreateHistogram("histogram2", MetricLevel.IMPORTANT, "tag", "value");
      assertNotEquals(histogram1, histogram2);
      histogram2 =
          metricService.getOrCreateHistogram("histogram3", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
      assertNotEquals(histogram1, histogram2);
      histogram2 =
          metricService.getOrCreateHistogram("histogram4", MetricLevel.NORMAL, "tag", "value");
      assertEquals(DoNothingMetricManager.doNothingHistogram, histogram2);



      // test timer
      Timer timer1 =
          metricService.getOrCreateTimer("timer1", MetricLevel.IMPORTANT, "tag", "value");
      assertNotNull(timer1);
      metricService.timer(2, TimeUnit.MINUTES, "timer1", MetricLevel.IMPORTANT, "tag", "value");
      metricService.timer(4, TimeUnit.MINUTES, "timer1", MetricLevel.IMPORTANT, "tag", "value");
      metricService.timer(6, TimeUnit.MINUTES, "timer1", MetricLevel.IMPORTANT, "tag", "value");
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        // do nothing
      }
      metricService.timer(8, TimeUnit.MINUTES, "timer1", MetricLevel.IMPORTANT, "tag", "value");
      metricService.timer(10, TimeUnit.MINUTES, "timer1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(5, timer1.getImmutableRate().getCount());
      assertEquals(5, timer1.takeSnapshot().size());
      assertEquals(120000000000L, timer1.takeSnapshot().getMin());
      assertEquals(360000000000L, timer1.takeSnapshot().getMedian(), 1e-5);
      assertEquals(360000000000L, timer1.takeSnapshot().getMean(), 1e-5);
      assertEquals(600000000000L, timer1.takeSnapshot().getMax());
      Timer timer2 =
          metricService.getOrCreateTimer("timer1", MetricLevel.IMPORTANT, "tag", "value");
      assertEquals(timer1, timer2);
      timer2 =
          metricService.getOrCreateTimer("timer1", MetricLevel.IMPORTANT);
      assertNotEquals(timer1, timer2);
      timer2 =
          metricService.getOrCreateTimer("timer2", MetricLevel.IMPORTANT, "tag", "value");
      assertNotEquals(timer1, timer2);
      timer2 =
          metricService.getOrCreateTimer("timer3", MetricLevel.IMPORTANT, "tag", "value", "tag2", "value");
      assertNotEquals(timer1, timer2);
      timer2 =
          metricService.getOrCreateTimer("timer4", MetricLevel.NORMAL, "tag", "value");
      assertNotEquals(timer1, timer2);

      // stop metric module
      metricService.stopService();
    }
  }

  private void initMetric(MetricFrameworkType type) {
    metricService = new DoNothingMetricService();
    metricConfig.setEnableMetric(true);
    metricConfig.setMetricFrameworkType(type);
    metricConfig.setMetricLevel(MetricLevel.IMPORTANT);
    metricService.startService();
    metricService.startAllReporter();
  }
}
