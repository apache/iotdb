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

package org.apache.iotdb.metrics;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public abstract class AbstractMetricManager {
  protected static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  /** Is metric service enabled */
  protected static boolean isEnableMetric;

  // region get or create metric
  /**
   * Get counter. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Counter getOrCreateCounter(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnableMetricInGivenLevel(metricLevel)) {
      return DoNothingMetricManager.doNothingCounter;
    }
    return getOrCreateCounter(metric, tags);
  }

  protected abstract Counter getOrCreateCounter(String metric, String... tags);

  /**
   * Get autoGauge. return if exists, create if not.
   *
   * <p>AutoGauge keep a weak reference of the obj, so it will not prevent gc of the obj. Notice: if
   * you call this gauge's value() when the obj has already been cleared by gc, then you will get
   * 0L.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param obj which will be monitored automatically
   * @param mapper use which to map the obj to a long value
   */
  public <T> Gauge getOrCreateAutoGauge(
      String metric, MetricLevel metricLevel, T obj, ToLongFunction<T> mapper, String... tags) {
    if (!isEnableMetricInGivenLevel(metricLevel)) {
      return DoNothingMetricManager.doNothingGauge;
    }
    return getOrCreateAutoGauge(metric, obj, mapper, tags);
  }

  protected abstract <T> Gauge getOrCreateAutoGauge(
      String metric, T obj, ToLongFunction<T> mapper, String... tags);

  /**
   * Get counter. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Gauge getOrCreateGauge(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnableMetricInGivenLevel(metricLevel)) {
      return DoNothingMetricManager.doNothingGauge;
    }
    return getOrCreateGauge(metric, tags);
  }

  protected abstract Gauge getOrCreateGauge(String metric, String... tags);

  /**
   * Get rate. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Rate getOrCreateRate(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnableMetricInGivenLevel(metricLevel)) {
      return DoNothingMetricManager.doNothingRate;
    }
    return getOrCreateRate(metric, tags);
  }

  protected abstract Rate getOrCreateRate(String metric, String... tags);

  /**
   * Get histogram. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Histogram getOrCreateHistogram(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnableMetricInGivenLevel(metricLevel)) {
      return DoNothingMetricManager.doNothingHistogram;
    }
    return getOrCreateHistogram(metric, tags);
  }

  protected abstract Histogram getOrCreateHistogram(String metric, String... tags);

  /**
   * Get timer. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Timer getOrCreateTimer(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnableMetricInGivenLevel(metricLevel)) {
      return DoNothingMetricManager.doNothingTimer;
    }
    return getOrCreateTimer(metric, tags);
  }

  protected abstract Timer getOrCreateTimer(String metric, String... tags);

  // endregion

  // region update metric

  /**
   * Update counter. if exists, then update counter by delta. if not, then create and update.
   *
   * @param delta the value to update
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void count(long delta, String metric, MetricLevel metricLevel, String... tags) {
    if (isEnableMetricInGivenLevel(metricLevel)) {
      count(delta, metric, tags);
    }
  }

  protected abstract void count(long delta, String metric, String... tags);

  /**
   * Set value of gauge. if exists, then set gauge by value. if not, then create and set.
   *
   * @param value the value of gauge
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void gauge(long value, String metric, MetricLevel metricLevel, String... tags) {
    if (isEnableMetricInGivenLevel(metricLevel)) {
      gauge(value, metric, tags);
    }
  }

  protected abstract void gauge(long value, String metric, String... tags);

  /**
   * Mark rate. if exists, then mark rate by value. if not, then create and mark.
   *
   * @param value the value to mark
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void rate(long value, String metric, MetricLevel metricLevel, String... tags) {
    if (isEnableMetricInGivenLevel(metricLevel)) {
      rate(value, metric, tags);
    }
  }

  protected abstract void rate(long value, String metric, String... tags);

  /**
   * Update histogram. if exists, then update histogram by value. if not, then create and update
   *
   * @param value the value to update
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void histogram(long value, String metric, MetricLevel metricLevel, String... tags) {
    if (isEnableMetricInGivenLevel(metricLevel)) {
      histogram(value, metric, tags);
    }
  }

  protected abstract void histogram(long value, String metric, String... tags);

  /**
   * Update timer. if exists, then update timer by delta and timeUnit. if not, then create and
   * update
   *
   * @param delta the value to update
   * @param timeUnit the unit of delta
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void timer(
      long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags) {
    if (isEnableMetricInGivenLevel(metricLevel)) {
      timer(delta, timeUnit, metric, tags);
    }
  }

  protected abstract void timer(long delta, TimeUnit timeUnit, String metric, String... tags);

  // endregion

  // region get metric

  /**
   * Get all metric keys.
   *
   * @return [[name, tags...], ..., [name, tags...]]
   */
  protected abstract List<String[]> getAllMetricKeys();

  /**
   * Get all counters
   *
   * @return [name, tags...] -> counter
   */
  protected abstract Map<String[], Counter> getAllCounters();

  /**
   * Get all gauges
   *
   * @return [name, tags...] -> gauge
   */
  protected abstract Map<String[], Gauge> getAllGauges();

  /**
   * Get all rates
   *
   * @return [name, tags...] -> rate
   */
  protected abstract Map<String[], Rate> getAllRates();

  /**
   * Get all histograms
   *
   * @return [name, tags...] -> histogram
   */
  protected abstract Map<String[], Histogram> getAllHistograms();

  /**
   * Get all timers
   *
   * @return [name, tags...] -> timer
   */
  protected abstract Map<String[], Timer> getAllTimers();

  // endregion

  // region remove metric

  /**
   * Remove counter
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  protected abstract void removeCounter(String metric, String... tags);

  /**
   * Remove gauge
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  protected abstract void removeGauge(String metric, String... tags);

  /**
   * Remove rate
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  protected abstract void removeRate(String metric, String... tags);

  /**
   * Remove histogram
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  protected abstract void removeHistogram(String metric, String... tags);

  /**
   * Update timer
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  protected abstract void removeTimer(String metric, String... tags);

  // endregion

  /** Is metric service enabled */
  public boolean isEnableMetric() {
    return isEnableMetric;
  }

  /** Is metric service enabled in specific level */
  public boolean isEnableMetricInGivenLevel(MetricLevel metricLevel) {
    return isEnableMetric()
        && MetricLevel.higherOrEqual(metricLevel, METRIC_CONFIG.getMetricLevel());
  }

  /** Stop and clear metric manager */
  protected abstract boolean stop();
}
