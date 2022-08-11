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
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public abstract class AbstractMetricManager {
  protected static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  /** Is metric service enabled */
  protected static boolean isEnableMetric;
  /** metric name -> tag keys */
  protected Map<MetricInfo, MetricInfo.MetaInfo> nameToTagInfo;
  /** metric type -> metric name -> metric info */
  protected Map<MetricInfo, IMetric> metrics;

  public AbstractMetricManager() {
    isEnableMetric = METRIC_CONFIG.getEnableMetric();
    nameToTagInfo = new ConcurrentHashMap<>();
    metrics = new ConcurrentHashMap<>();
  }

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
    MetricInfo metricInfo = new MetricInfo(MetricType.COUNTER, metric, tags);
    IMetric counter = metrics.computeIfAbsent(metricInfo, key -> createCounter(metricInfo));
    if (counter instanceof Counter) {
      add(metricInfo, counter);
      return (Counter) counter;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of metric");
  }

  protected abstract Counter createCounter(MetricInfo metricInfo);

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
    MetricInfo metricInfo = new MetricInfo(MetricType.GAUGE, metric, tags);
    IMetric gauge =
        metrics.computeIfAbsent(metricInfo, key -> createAutoGauge(metricInfo, obj, mapper));
    if (gauge instanceof Gauge) {
      add(metricInfo, gauge);
      return (Gauge) gauge;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of metric");
  }

  protected abstract <T> Gauge createAutoGauge(
      MetricInfo metricInfo, T obj, ToLongFunction<T> mapper);

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
    MetricInfo metricInfo = new MetricInfo(MetricType.GAUGE, metric, tags);
    IMetric gauge = metrics.computeIfAbsent(metricInfo, key -> createGauge(metricInfo));
    if (gauge instanceof Gauge) {
      add(metricInfo, gauge);
      return (Gauge) gauge;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of metric");
  }

  protected abstract Gauge createGauge(MetricInfo metricInfo);

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
    MetricInfo metricInfo = new MetricInfo(MetricType.RATE, metric, tags);
    IMetric rate = metrics.computeIfAbsent(metricInfo, key -> createRate(metricInfo));
    if (rate instanceof Rate) {
      add(metricInfo, rate);
      return (Rate) rate;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of metric");
  }

  protected abstract Rate createRate(MetricInfo metricInfo);

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
    MetricInfo metricInfo = new MetricInfo(MetricType.HISTOGRAM, metric, tags);
    IMetric histogram = metrics.computeIfAbsent(metricInfo, key -> createHistogram(metricInfo));
    if (histogram instanceof Histogram) {
      add(metricInfo, histogram);
      return (Histogram) histogram;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of metric");
  }

  protected abstract Histogram createHistogram(MetricInfo metricInfo);

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
    MetricInfo metricInfo = new MetricInfo(MetricType.TIMER, metric, tags);
    IMetric timer = metrics.computeIfAbsent(metricInfo, key -> createTimer(metricInfo));
    if (timer instanceof Timer) {
      add(metricInfo, timer);
      return (Timer) timer;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of metric");
  }

  protected abstract Timer createTimer(MetricInfo metricInfo);

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
    Counter counter = getOrCreateCounter(metric, metricLevel, tags);
    counter.inc(delta);
  }

  /**
   * Set value of gauge. if exists, then set gauge by value. if not, then create and set.
   *
   * @param value the value of gauge
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void gauge(long value, String metric, MetricLevel metricLevel, String... tags) {
    Gauge gauge = getOrCreateGauge(metric, metricLevel, tags);
    gauge.set(value);
  }

  /**
   * Mark rate. if exists, then mark rate by value. if not, then create and mark.
   *
   * @param value the value to mark
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void rate(long value, String metric, MetricLevel metricLevel, String... tags) {
    Rate rate = getOrCreateRate(metric, metricLevel, tags);
    rate.mark(value);
  }

  /**
   * Update histogram. if exists, then update histogram by value. if not, then create and update
   *
   * @param value the value to update
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void histogram(long value, String metric, MetricLevel metricLevel, String... tags) {
    Histogram histogram = getOrCreateHistogram(metric, metricLevel, tags);
    histogram.update(value);
  }

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
    Timer timer = getOrCreateTimer(metric, metricLevel, tags);
    timer.update(delta, timeUnit);
  }

  // endregion

  // region get metric

  /**
   * Get all metric keys.
   *
   * @return [[name, tags...], ..., [name, tags...]]
   */
  protected List<String[]> getAllMetricKeys() {
    List<String[]> keys = new ArrayList<>(metrics.size());
    metrics.keySet().forEach(k -> keys.add(k.toStringArray()));
    return keys;
  }

  /**
   * Get all counters
   *
   * @return [name, tags...] -> counter
   */
  protected Map<String[], Counter> getAllCounters() {
    Map<String[], Counter> counterMap = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> entry : metrics.entrySet()) {
      if (entry.getValue() instanceof Counter) {
        counterMap.put(entry.getKey().toStringArray(), (Counter) entry.getValue());
      }
    }
    return counterMap;
  }

  /**
   * Get all gauges
   *
   * @return [name, tags...] -> gauge
   */
  protected Map<String[], Gauge> getAllGauges() {
    Map<String[], Gauge> gaugeMap = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> entry : metrics.entrySet()) {
      if (entry.getValue() instanceof Gauge) {
        gaugeMap.put(entry.getKey().toStringArray(), (Gauge) entry.getValue());
      }
    }
    return gaugeMap;
  }

  /**
   * Get all rates
   *
   * @return [name, tags...] -> rate
   */
  protected Map<String[], Rate> getAllRates() {
    Map<String[], Rate> rateMap = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> entry : metrics.entrySet()) {
      if (entry.getValue() instanceof Rate) {
        rateMap.put(entry.getKey().toStringArray(), (Rate) entry.getValue());
      }
    }
    return rateMap;
  }

  /**
   * Get all histograms
   *
   * @return [name, tags...] -> histogram
   */
  protected Map<String[], Histogram> getAllHistograms() {
    Map<String[], Histogram> histogramMap = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> entry : metrics.entrySet()) {
      if (entry.getValue() instanceof Histogram) {
        histogramMap.put(entry.getKey().toStringArray(), (Histogram) entry.getValue());
      }
    }
    return histogramMap;
  }

  /**
   * Get all timers
   *
   * @return [name, tags...] -> timer
   */
  protected Map<String[], Timer> getAllTimers() {
    Map<String[], Timer> timerMap = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> entry : metrics.entrySet()) {
      if (entry.getValue() instanceof Timer) {
        timerMap.put(entry.getKey().toStringArray(), (Timer) entry.getValue());
      }
    }
    return timerMap;
  }

  // endregion

  // region remove metric

  /**
   * remove metric
   *
   * @param type the type of metric
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public void remove(MetricType type, String metric, String... tags) {
    if (isEnableMetric()) {
      MetricInfo metricInfo = new MetricInfo(type, metric, tags);
      MetricInfo.MetaInfo metaInfo = nameToTagInfo.get(metricInfo);
      if (type == metaInfo.getType()) {
        removeFromMap(metricInfo);
        remove(type, metricInfo);
      } else {
        throw new IllegalArgumentException(
            metricInfo + " failed to remove because the mismatch of type. ");
      }
    }
  }

  protected abstract void remove(MetricType type, MetricInfo metricInfo);

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
  protected boolean stop() {
    isEnableMetric = METRIC_CONFIG.getEnableMetric();
    metrics = new ConcurrentHashMap<>();
    nameToTagInfo = new ConcurrentHashMap<>();
    return stopFramework();
  }

  protected abstract boolean stopFramework();

  private void add(MetricInfo metricInfo, IMetric metric) {
    metrics.put(metricInfo, metric);
    nameToTagInfo.put(metricInfo, metricInfo.getTagMetaInfo());
  }

  private void removeFromMap(MetricInfo metricInfo) {
    nameToTagInfo.remove(metricInfo.getName());
    metrics.remove(metricInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractMetricManager that = (AbstractMetricManager) o;
    return Objects.equals(metrics, that.metrics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metrics);
  }
}
