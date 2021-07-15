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

package org.apache.iotdb.metrics.opentelemetry;

import org.apache.iotdb.metrics.KnownMetric;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricReporter;
import org.apache.iotdb.metrics.type.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OpenTelemetryMetricManager implements MetricManager {
  /**
   * The following functions will create or get a exist Metric
   *
   * @param metric : the metric name
   * @param tags : string appear in pairs, like sg="ln",user="user1" will be "sg", "ln", "user",
   *     "user1"
   * @return Metric Instance
   */
  @Override
  public Counter getOrCreateCounter(String metric, String... tags) {
    return null;
  }

  @Override
  public Gauge getOrCreatGauge(String metric, String... tags) {
    return null;
  }

  @Override
  public Rate getOrCreatRate(String metric, String... tags) {
    return null;
  }

  @Override
  public Histogram getOrCreateHistogram(String metric, String... tags) {
    return null;
  }

  @Override
  public Timer getOrCreateTimer(String metric, String... tags) {
    return null;
  }

  @Override
  public void count(int delta, String metric, String... tags) {}

  @Override
  public void count(long delta, String metric, String... tags) {}

  @Override
  public void gauge(int value, String metric, String... tags) {}

  @Override
  public void gauge(long value, String metric, String... tags) {}

  @Override
  public void rate(int value, String metric, String... tags) {}

  @Override
  public void rate(long value, String metric, String... tags) {}

  @Override
  public void histogram(int value, String metric, String... tags) {}

  @Override
  public void histogram(long value, String metric, String... tags) {}

  @Override
  public void timer(long delta, TimeUnit timeUnit, String metric, String... tags) {}

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
   * get all metric keys.
   *
   * @return all MetricKeys, key is metric name, value is tags, which is a string array.
   */
  @Override
  public List<String[]> getAllMetricKeys() {
    return null;
  }

  @Override
  public Map<String[], Counter> getAllCounters() {
    return null;
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    return null;
  }

  @Override
  public Map<String[], Rate> getAllRates() {
    return null;
  }

  @Override
  public Map<String[], Histogram> getAllHistograms() {
    return null;
  }

  @Override
  public Map<String[], Timer> getAllTimers() {
    return null;
  }

  @Override
  public boolean isEnable() {
    return false;
  }

  /**
   * enable pre-defined metric set.
   *
   * @param metric which metric set we want to collect
   */
  @Override
  public void enableKnownMetric(KnownMetric metric) {}

  /**
   * init something.
   *
   * @return whether success
   */
  @Override
  public boolean init() {
    return false;
  }

  /**
   * stop everything and clear
   *
   * @return
   */
  @Override
  public boolean stop() {
    return false;
  }

  @Override
  public boolean startReporter(String reporterName) {
    return false;
  }

  @Override
  public boolean stopReporter(String reporterName) {
    return false;
  }

  @Override
  public void setReporter(MetricReporter metricReporter) {}

  @Override
  public String getName() {
    return null;
  }
}
