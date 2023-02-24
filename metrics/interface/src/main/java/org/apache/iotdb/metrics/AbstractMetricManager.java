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
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

public abstract class AbstractMetricManager {
  protected static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  /** The map from metric name to metric metaInfo. */
  protected Map<String, MetricInfo.MetaInfo> nameToMetaInfo;
  /** The map from metricInfo to metric. */
  protected Map<MetricInfo, IMetric> metrics;

  public AbstractMetricManager() {
    nameToMetaInfo = new ConcurrentHashMap<>();
    metrics = new ConcurrentHashMap<>();
  }

  /**
   * Get counter. return if exists, create if not.
   *
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   * @throws IllegalArgumentException when there has different type metric with same name
   */
  public Counter getOrCreateCounter(String name, MetricLevel metricLevel, String... tags) {
    if (invalid(metricLevel, name, tags)) {
      return DoNothingMetricManager.DO_NOTHING_COUNTER;
    }
    MetricInfo metricInfo = new MetricInfo(MetricType.COUNTER, name, tags);
    IMetric metric =
        metrics.computeIfAbsent(
            metricInfo,
            key -> {
              Counter counter = createCounter(metricInfo);
              nameToMetaInfo.put(name, metricInfo.getMetaInfo());
              return counter;
            });
    if (metric instanceof Counter) {
      return (Counter) metric;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of name");
  }

  protected abstract Counter createCounter(MetricInfo metricInfo);

  /**
   * Create autoGauge
   *
   * <p>AutoGauge keep a weak reference of the obj, so it will not prevent gc of the obj. Notice: if
   * you call this gauge's value() when the obj has already been cleared by gc, then you will get
   * 0L.
   *
   * @param name the name of name
   * @param metricLevel the level of name
   * @param obj which will be monitored automatically
   * @param mapper use which to map the obj to a double value
   */
  public <T> AutoGauge createAutoGauge(
      String name, MetricLevel metricLevel, T obj, ToDoubleFunction<T> mapper, String... tags) {
    if (invalid(metricLevel, name, tags)) {
      return DoNothingMetricManager.DO_NOTHING_AUTO_GAUGE;
    }
    MetricInfo metricInfo = new MetricInfo(MetricType.AUTO_GAUGE, name, tags);
    AutoGauge gauge = createAutoGauge(metricInfo, obj, mapper);
    nameToMetaInfo.put(name, metricInfo.getMetaInfo());
    metrics.put(metricInfo, gauge);
    return gauge;
  }

  /**
   * Create autoGauge according to metric framework.
   *
   * @param metricInfo the metricInfo of autoGauge
   * @param obj which will be monitored automatically
   * @param mapper use which to map the obj to a double value
   */
  protected abstract <T> AutoGauge createAutoGauge(
      MetricInfo metricInfo, T obj, ToDoubleFunction<T> mapper);

  /**
   * Get autoGauge.
   *
   * @param name the name of name
   * @param metricLevel the level of name
   * @throws IllegalArgumentException when there has different type metric with same name
   */
  public AutoGauge getAutoGauge(String name, MetricLevel metricLevel, String... tags) {
    if (invalid(metricLevel, name, tags)) {
      return DoNothingMetricManager.DO_NOTHING_AUTO_GAUGE;
    }
    MetricInfo metricInfo = new MetricInfo(MetricType.AUTO_GAUGE, name, tags);
    IMetric metric = metrics.get(metricInfo);
    if (metric == null) {
      return DoNothingMetricManager.DO_NOTHING_AUTO_GAUGE;
    } else if (metric instanceof AutoGauge) {
      return (AutoGauge) metric;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of name");
  }

  /**
   * Get counter. return if exists, create if not.
   *
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   * @throws IllegalArgumentException when there has different type metric with same name
   */
  public Gauge getOrCreateGauge(String name, MetricLevel metricLevel, String... tags) {
    if (invalid(metricLevel, name, tags)) {
      return DoNothingMetricManager.DO_NOTHING_GAUGE;
    }
    MetricInfo metricInfo = new MetricInfo(MetricType.GAUGE, name, tags);
    IMetric metric =
        metrics.computeIfAbsent(
            metricInfo,
            key -> {
              Gauge gauge = createGauge(metricInfo);
              nameToMetaInfo.put(name, metricInfo.getMetaInfo());
              return gauge;
            });
    if (metric instanceof Gauge) {
      return (Gauge) metric;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of name");
  }

  /**
   * Create gauge according to metric framework.
   *
   * @param metricInfo the metricInfo of gauge
   */
  protected abstract Gauge createGauge(MetricInfo metricInfo);

  /**
   * Get rate. return if exists, create if not.
   *
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   * @throws IllegalArgumentException when there has different type metric with same name
   */
  public Rate getOrCreateRate(String name, MetricLevel metricLevel, String... tags) {
    if (invalid(metricLevel, name, tags)) {
      return DoNothingMetricManager.DO_NOTHING_RATE;
    }
    MetricInfo metricInfo = new MetricInfo(MetricType.RATE, name, tags);
    IMetric metric =
        metrics.computeIfAbsent(
            metricInfo,
            key -> {
              Rate rate = createRate(metricInfo);
              nameToMetaInfo.put(name, metricInfo.getMetaInfo());
              return rate;
            });
    if (metric instanceof Rate) {
      return (Rate) metric;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of name");
  }

  /**
   * Create rate according to metric framework.
   *
   * @param metricInfo the metricInfo of rate
   */
  protected abstract Rate createRate(MetricInfo metricInfo);

  /**
   * Get histogram. return if exists, create if not.
   *
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   * @throws IllegalArgumentException when there has different type metric with same name
   */
  public Histogram getOrCreateHistogram(String name, MetricLevel metricLevel, String... tags) {
    if (invalid(metricLevel, name, tags)) {
      return DoNothingMetricManager.DO_NOTHING_HISTOGRAM;
    }
    MetricInfo metricInfo = new MetricInfo(MetricType.HISTOGRAM, name, tags);
    IMetric metric =
        metrics.computeIfAbsent(
            metricInfo,
            key -> {
              Histogram histogram = createHistogram(metricInfo);
              nameToMetaInfo.put(name, metricInfo.getMetaInfo());
              return histogram;
            });
    if (metric instanceof Histogram) {
      return (Histogram) metric;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of name");
  }

  /**
   * Create histogram according to metric framework.
   *
   * @param metricInfo the metricInfo of metric
   */
  protected abstract Histogram createHistogram(MetricInfo metricInfo);

  /**
   * Get timer. return if exists, create if not.
   *
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   * @throws IllegalArgumentException when there has different type metric with same name
   */
  public Timer getOrCreateTimer(String name, MetricLevel metricLevel, String... tags) {
    if (invalid(metricLevel, name, tags)) {
      return DoNothingMetricManager.DO_NOTHING_TIMER;
    }
    MetricInfo metricInfo = new MetricInfo(MetricType.TIMER, name, tags);
    IMetric metric =
        metrics.computeIfAbsent(
            metricInfo,
            key -> {
              Timer timer = createTimer(metricInfo);
              nameToMetaInfo.put(name, metricInfo.getMetaInfo());
              return timer;
            });
    if (metric instanceof Timer) {
      return (Timer) metric;
    }
    throw new IllegalArgumentException(
        metricInfo + " is already used for a different type of name");
  }

  /**
   * Create timer according to metric framework.
   *
   * @param metricInfo the metricInfo of metric
   */
  protected abstract Timer createTimer(MetricInfo metricInfo);

  // endregion

  // region update metric

  /**
   * Update counter. if exists, then update counter by delta. if not, then create and update.
   *
   * @param delta the value to update
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Counter count(long delta, String name, MetricLevel metricLevel, String... tags) {
    Counter counter = getOrCreateCounter(name, metricLevel, tags);
    counter.inc(delta);
    return counter;
  }

  /**
   * Set value of gauge. if exists, then set gauge by value. if not, then create and set.
   *
   * @param value the value of gauge
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Gauge gauge(long value, String name, MetricLevel metricLevel, String... tags) {
    Gauge gauge = getOrCreateGauge(name, metricLevel, tags);
    gauge.set(value);
    return gauge;
  }

  /**
   * Mark rate. if exists, then mark rate by value. if not, then create and mark.
   *
   * @param value the value to mark
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Rate rate(long value, String name, MetricLevel metricLevel, String... tags) {
    Rate rate = getOrCreateRate(name, metricLevel, tags);
    rate.mark(value);
    return rate;
  }

  /**
   * Update histogram. if exists, then update histogram by value. if not, then create and update
   *
   * @param value the value to update
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Histogram histogram(long value, String name, MetricLevel metricLevel, String... tags) {
    Histogram histogram = getOrCreateHistogram(name, metricLevel, tags);
    histogram.update(value);
    return histogram;
  }

  /**
   * Update timer. if exists, then update timer by delta and timeUnit. if not, then create and
   * update
   *
   * @param delta the value to update
   * @param timeUnit the unit of delta
   * @param name the name of name
   * @param metricLevel the level of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  public Timer timer(
      long delta, TimeUnit timeUnit, String name, MetricLevel metricLevel, String... tags) {
    Timer timer = getOrCreateTimer(name, metricLevel, tags);
    timer.update(delta, timeUnit);
    return timer;
  }

  // endregion

  // region get metric

  /**
   * Get all metric keys.
   *
   * @return [[name, [tags...]], ..., [name, [tags...]]]
   */
  public List<Pair<String, String[]>> getAllMetricKeys() {
    List<Pair<String, String[]>> keys = new ArrayList<>(metrics.size());
    metrics.keySet().forEach(k -> keys.add(k.toStringArray()));
    return keys;
  }

  /**
   * Get all metrics.
   *
   * @return [name, [tags...]] -> metric
   */
  public Map<MetricInfo, IMetric> getAllMetrics() {
    return metrics;
  }

  /**
   * Get metrics by type.
   *
   * @return [name, [tags...]] -> metric
   */
  public Map<MetricInfo, IMetric> getMetricsByType(MetricType metricType) {
    Map<MetricInfo, IMetric> result = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> entry : metrics.entrySet()) {
      if (entry.getKey().getMetaInfo().getType() == metricType) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  // endregion

  // region remove metric

  /**
   * remove name.
   *
   * @param type the type of name
   * @param name the name of name
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   * @throws IllegalArgumentException when there has different type metric with same name
   */
  public void remove(MetricType type, String name, String... tags) {
    MetricInfo metricInfo = new MetricInfo(type, name, tags);
    if (metrics.containsKey(metricInfo)) {
      if (type == metricInfo.getMetaInfo().getType()) {
        nameToMetaInfo.remove(metricInfo.getName());
        metrics.remove(metricInfo);
        removeMetric(type, metricInfo);
      } else {
        throw new IllegalArgumentException(
            metricInfo + " failed to remove because the mismatch of type. ");
      }
    }
  }

  protected abstract void removeMetric(MetricType type, MetricInfo metricInfo);

  // endregion

  /** Is metric service enabled in specific level. */
  public boolean isEnableMetricInGivenLevel(MetricLevel metricLevel) {
    return MetricLevel.higherOrEqual(metricLevel, METRIC_CONFIG.getMetricLevel());
  }

  /** Stop and clear metric manager. */
  protected boolean stop() {
    metrics = new ConcurrentHashMap<>();
    nameToMetaInfo = new ConcurrentHashMap<>();
    return stopFramework();
  }

  protected abstract boolean stopFramework();

  private boolean invalid(MetricLevel metricLevel, String name, String... tags) {
    if (!isEnableMetricInGivenLevel(metricLevel)) {
      return true;
    }
    if (!nameToMetaInfo.containsKey(name)) {
      return false;
    }
    MetricInfo.MetaInfo metaInfo = nameToMetaInfo.get(name);
    return !metaInfo.hasSameKey(tags);
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
