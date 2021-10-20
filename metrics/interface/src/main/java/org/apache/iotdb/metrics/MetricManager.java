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

import org.apache.iotdb.metrics.type.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface MetricManager {
  /**
   * Get Counter If exists, then return or create one to return
   *
   * @param metric
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   * @return
   */
  Counter getOrCreateCounter(String metric, String... tags);

  /**
   * Get Guage If exists, then return or create one to return
   *
   * @param metric
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   * @return
   */
  Gauge getOrCreatGauge(String metric, String... tags);

  /**
   * Get Rate If exists, then return or create one to return
   *
   * @param metric
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   * @return
   */
  Rate getOrCreatRate(String metric, String... tags);

  /**
   * Get Histogram If exists, then return or create one to return
   *
   * @param metric
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   * @return
   */
  Histogram getOrCreateHistogram(String metric, String... tags);

  /**
   * Get Timer If exists, then return or create one to return
   *
   * @param metric
   * @param tags string appear in pairs, like sg="ln" will be "sg", "ln"
   * @return
   */
  Timer getOrCreateTimer(String metric, String... tags);

  /**
   * Update Counter
   *
   * @param delta
   * @param metric
   * @param tags
   */
  void count(int delta, String metric, String... tags);

  /**
   * Update Counter
   *
   * @param delta
   * @param metric
   * @param tags
   */
  void count(long delta, String metric, String... tags);

  /**
   * update Gauge
   *
   * @param value
   * @param metric
   * @param tags
   */
  void gauge(int value, String metric, String... tags);

  /**
   * update Gauge
   *
   * @param value
   * @param metric
   * @param tags
   */
  void gauge(long value, String metric, String... tags);

  /**
   * update Rate
   *
   * @param value
   * @param metric
   * @param tags
   */
  void rate(int value, String metric, String... tags);

  /**
   * update Rate
   *
   * @param value
   * @param metric
   * @param tags
   */
  void rate(long value, String metric, String... tags);

  /**
   * update Histogram
   *
   * @param value
   * @param metric
   * @param tags
   */
  void histogram(int value, String metric, String... tags);

  /**
   * update Histogram
   *
   * @param value
   * @param metric
   * @param tags
   */
  void histogram(long value, String metric, String... tags);

  /**
   * update Timer
   *
   * @param delta
   * @param timeUnit
   * @param metric
   * @param tags
   */
  void timer(long delta, TimeUnit timeUnit, String metric, String... tags);

  /**
   * remove counter
   *
   * @param metric
   * @param tags
   */
  void removeCounter(String metric, String... tags);

  /**
   * remove gauge
   *
   * @param metric
   * @param tags
   */
  void removeGauge(String metric, String... tags);

  /**
   * remove rate
   *
   * @param metric
   * @param tags
   */
  void removeRate(String metric, String... tags);

  /**
   * remove histogram
   *
   * @param metric
   * @param tags
   */
  void removeHistogram(String metric, String... tags);

  /**
   * update timer
   *
   * @param metric
   * @param tags
   */
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

  /**
   * whether is enable monitor
   *
   * @return
   */
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

  /**
   * stop everything and clear
   *
   * @return
   */
  boolean stop();

  /**
   * Get name of manager
   *
   * @return
   */
  String getName();
}
