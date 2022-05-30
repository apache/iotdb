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

package org.apache.iotdb.metrics.impl;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

public class DoNothingMetricManager implements MetricManager {

  public static final DoNothingCounter doNothingCounter = new DoNothingCounter();
  public static final DoNothingHistogram doNothingHistogram = new DoNothingHistogram();
  public static final DoNothingGauge doNothingGauge = new DoNothingGauge();
  public static final DoNothingRate doNothingRate = new DoNothingRate();
  public static final DoNothingTimer doNothingTimer = new DoNothingTimer();

  @Override
  public Counter getOrCreateCounter(String metric, MetricLevel metricLevel, String... tags) {
    return doNothingCounter;
  }

  @Override
  public <T> Gauge getOrCreateAutoGauge(
      String metric, MetricLevel metricLevel, T obj, ToLongFunction<T> mapper, String... tags) {
    return doNothingGauge;
  }

  @Override
  public Gauge getOrCreateGauge(String metric, MetricLevel metricLevel, String... tags) {
    return doNothingGauge;
  }

  @Override
  public Histogram getOrCreateHistogram(String metric, MetricLevel metricLevel, String... tags) {
    return doNothingHistogram;
  }

  @Override
  public Rate getOrCreateRate(String metric, MetricLevel metricLevel, String... tags) {
    return doNothingRate;
  }

  @Override
  public Timer getOrCreateTimer(String metric, MetricLevel metricLevel, String... tags) {
    return doNothingTimer;
  }

  @Override
  public void count(long delta, String metric, MetricLevel metricLevel, String... tags) {
    // do nothing
  }

  @Override
  public void histogram(long value, String metric, MetricLevel metricLevel, String... tags) {
    // do nothing
  }

  @Override
  public void gauge(long value, String metric, MetricLevel metricLevel, String... tags) {
    // do nothing
  }

  @Override
  public void rate(long value, String metric, MetricLevel metricLevel, String... tags) {
    // do nothing
  }

  @Override
  public void timer(
      long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags) {
    // do nothing
  }

  @Override
  public List<String[]> getAllMetricKeys() {
    return Collections.emptyList();
  }

  @Override
  public Map<String[], Counter> getAllCounters() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String[], Rate> getAllRates() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String[], Histogram> getAllHistograms() {
    return Collections.emptyMap();
  }

  @Override
  public Map<String[], Timer> getAllTimers() {
    return Collections.emptyMap();
  }

  @Override
  public boolean isEnable() {
    return false;
  }

  @Override
  public boolean isEnable(MetricLevel metricLevel) {
    return false;
  }

  @Override
  public boolean init() {
    return false;
  }

  @Override
  public void removeCounter(String metric, String... tags) {}

  @Override
  public void removeGauge(String metric, String... tags) {}

  @Override
  public void removeRate(String metric, String... tags) {}

  @Override
  public void removeHistogram(String metric, String... tags) {}

  @Override
  public void removeTimer(String metric, String... tags) {}

  /**
   * stop everything and clear
   *
   * @return
   */
  @Override
  public boolean stop() {
    return false;
  }
}
