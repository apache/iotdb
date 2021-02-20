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

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DoNothingMetricManager implements MetricManager {

  @Override
  public Counter counter(String metric, String... tags) {
    return null;
  }

  @Override
  public Gauge gauge(String metric, String... tags) {
    return null;
  }

  @Override
  public Histogram histogram(String metric, String... tags) {
    return null;
  }

  @Override
  public Rate rate(String metric, String... tags) {
    return null;
  }

  @Override
  public Timer timer(String metric, String... tags) {
    return null;
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
  public void meter(int value, String metric, String... tags) {}

  @Override
  public void meter(long value, String metric, String... tags) {}

  @Override
  public void timer(long delta, TimeUnit timeUnit, String metric, String... tags) {}

  @Override
  public void timerStart(String metric, String... tags) {}

  @Override
  public void timerEnd(String metric, String... tags) {}

  @Override
  public Map<String, String[]> getAllMetricKeys() {
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
  public Map<String[], Rate> getAllMeters() {
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
}
