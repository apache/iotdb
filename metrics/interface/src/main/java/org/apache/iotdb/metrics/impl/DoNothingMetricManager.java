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

import org.apache.iotdb.metrics.KnownMetric;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DoNothingMetricManager implements MetricManager {

  private final DoNothingCounter doNothingCounter = new DoNothingCounter();
  private final DoNothingHistogram doNothingHistogram = new DoNothingHistogram();
  private final DoNothingGauge doNothingGauge = new DoNothingGauge();
  private final DoNothingRate doNothingRate = new DoNothingRate();
  private final DoNothingTimer doNothingTimer = new DoNothingTimer();

  @Override
  public Counter counter(String metric, String... tags) {
    return doNothingCounter;
  }

  @Override
  public Gauge gauge(String metric, String... tags) {
    return doNothingGauge;
  }

  @Override
  public Histogram histogram(String metric, String... tags) {
    return doNothingHistogram;
  }

  @Override
  public Rate rate(String metric, String... tags) {
    return doNothingRate;
  }

  @Override
  public Timer timer(String metric, String... tags) {
    return doNothingTimer;
  }

  @Override
  public void count(int delta, String metric, String... tags) {}

  @Override
  public void count(long delta, String metric, String... tags) {}

  @Override
  public void histogram(int value, String metric, String... tags) {}

  @Override
  public void histogram(long value, String metric, String... tags) {}

  @Override
  public void gauge(int value, String metric, String... tags) {}

  @Override
  public void gauge(long value, String metric, String... tags) {}

  @Override
  public void rate(int value, String metric, String... tags) {}

  @Override
  public void rate(long value, String metric, String... tags) {}

  @Override
  public void timer(long delta, TimeUnit timeUnit, String metric, String... tags) {}

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
  public void enableKnownMetric(KnownMetric metric) {}

  @Override
  public boolean init() {
    return false;
  }

  @Override
  public String getName() {
    return "DoNothingMetricManager";
  }
}
