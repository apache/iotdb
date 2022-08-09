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

public interface MetricManager {

  // region get or create metric

  /**
   * Get counter. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  Counter getOrCreateCounter(String metric, MetricLevel metricLevel, String... tags);

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
  <T> Gauge getOrCreateAutoGauge(
      String metric, MetricLevel metricLevel, T obj, ToLongFunction<T> mapper, String... tags);

  /**
   * Get counter. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  Gauge getOrCreateGauge(String metric, MetricLevel metricLevel, String... tags);

  /**
   * Get rate. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  Rate getOrCreateRate(String metric, MetricLevel metricLevel, String... tags);

  /**
   * Get histogram. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  Histogram getOrCreateHistogram(String metric, MetricLevel metricLevel, String... tags);

  /**
   * Get timer. return if exists, create if not.
   *
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  Timer getOrCreateTimer(String metric, MetricLevel metricLevel, String... tags);

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
  void count(long delta, String metric, MetricLevel metricLevel, String... tags);

  /**
   * Set value of gauge. if exists, then set gauge by value. if not, then create and set.
   *
   * @param value the value of gauge
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void gauge(long value, String metric, MetricLevel metricLevel, String... tags);

  /**
   * Mark rate. if exists, then mark rate by value. if not, then create and mark.
   *
   * @param value the value to mark
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void rate(long value, String metric, MetricLevel metricLevel, String... tags);

  /**
   * Update histogram. if exists, then update histogram by value. if not, then create and update
   *
   * @param value the value to update
   * @param metric the name of metric
   * @param metricLevel the level of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void histogram(long value, String metric, MetricLevel metricLevel, String... tags);

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
  void timer(long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags);

  // endregion

  // region get metric

  /**
   * Get all metric keys.
   *
   * @return [[name, tags...], ..., [name, tags...]]
   */
  List<String[]> getAllMetricKeys();

  /**
   * Get all counters
   *
   * @return [name, tags...] -> counter
   */
  Map<String[], Counter> getAllCounters();

  /**
   * Get all gauges
   *
   * @return [name, tags...] -> gauge
   */
  Map<String[], Gauge> getAllGauges();

  /**
   * Get all rates
   *
   * @return [name, tags...] -> rate
   */
  Map<String[], Rate> getAllRates();

  /**
   * Get all histograms
   *
   * @return [name, tags...] -> histogram
   */
  Map<String[], Histogram> getAllHistograms();

  /**
   * Get all timers
   *
   * @return [name, tags...] -> timer
   */
  Map<String[], Timer> getAllTimers();

  // endregion

  // region remove metric

  /**
   * Remove counter
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void removeCounter(String metric, String... tags);

  /**
   * Remove gauge
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void removeGauge(String metric, String... tags);

  /**
   * Remove rate
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void removeRate(String metric, String... tags);

  /**
   * Remove histogram
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void removeHistogram(String metric, String... tags);

  /**
   * Update timer
   *
   * @param metric the name of metric
   * @param tags string pairs, like sg="ln" will be "sg", "ln"
   */
  void removeTimer(String metric, String... tags);

  // endregion

  /** Is metric service enabled */
  boolean isEnable();

  /** Is metric service enabled in specific level */
  boolean isEnable(MetricLevel metricLevel);

  /** Stop and clear metric manager */
  boolean stop();
}
