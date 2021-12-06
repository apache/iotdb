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
import org.apache.iotdb.metrics.utils.PredefinedMetric;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public interface MetricManager {
  /**
   * Get Counter If exists, then return or create one to return
   *
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   */
  Counter getOrCreateCounter(String metric, String... tags);

  /**
   * Get Gauge If exists, then return or create one to return
   *
   * <p>This type of gauge will keep a weak reference of the obj, so it will not prevent the obj's
   * gc. NOTICE: When the obj has already been cleared by gc when you call the gauge's value(), then
   * you will get 0L;
   *
   * @param obj which will be monitored automatically
   * @param mapper use which to map the obj to a long value
   */
  <T> Gauge getOrCreateAutoGauge(String metric, T obj, ToLongFunction<T> mapper, String... tags);

  /**
   * Get Gauge If exists, then return or create one to return
   *
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   */
  Gauge getOrCreateGauge(String metric, String... tags);

  /**
   * Get Rate If exists, then return or create one to return
   *
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   */
  Rate getOrCreateRate(String metric, String... tags);

  /**
   * Get Histogram If exists, then return or create one to return
   *
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   */
  Histogram getOrCreateHistogram(String metric, String... tags);

  /**
   * Get Timer If exists, then return or create one to return
   *
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   */
  Timer getOrCreateTimer(String metric, String... tags);

  /** Update Counter */
  void count(int delta, String metric, String... tags);

  /** Update Counter */
  void count(long delta, String metric, String... tags);

  /** update Gauge */
  void gauge(int value, String metric, String... tags);

  /** update Gauge */
  void gauge(long value, String metric, String... tags);

  /** update Rate */
  void rate(int value, String metric, String... tags);

  /** update Rate */
  void rate(long value, String metric, String... tags);

  /** update Histogram */
  void histogram(int value, String metric, String... tags);

  /** update Histogram */
  void histogram(long value, String metric, String... tags);

  /** update Timer */
  void timer(long delta, TimeUnit timeUnit, String metric, String... tags);

  /** remove counter */
  void removeCounter(String metric, String... tags);

  /** remove gauge */
  void removeGauge(String metric, String... tags);

  /** remove rate */
  void removeRate(String metric, String... tags);

  /** remove histogram */
  void removeHistogram(String metric, String... tags);

  /** update timer */
  void removeTimer(String metric, String... tags);

  /**
   * get all metric keys.
   *
   * @return all MetricKeys, key is metric name, value is tags, which is a string array.
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
   * Get all histogram
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

  /** whether is enabled monitor */
  boolean isEnable();

  /**
   * enable pre-defined metric set.
   *
   * @param metric which metric set we want to collect
   */
  void enablePredefinedMetric(PredefinedMetric metric);

  /**
   * init something.
   *
   * @return whether success
   */
  boolean init();

  /** clear metrics */
  boolean stop();
}
