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

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.function.ToDoubleFunction;

public class DoNothingMetricManager extends AbstractMetricManager {

  public static final DoNothingCounter DO_NOTHING_COUNTER = new DoNothingCounter();
  public static final DoNothingHistogram DO_NOTHING_HISTOGRAM = new DoNothingHistogram();
  public static final DoNothingAutoGauge DO_NOTHING_AUTO_GAUGE = new DoNothingAutoGauge();
  public static final DoNothingGauge DO_NOTHING_GAUGE = new DoNothingGauge();
  public static final DoNothingRate DO_NOTHING_RATE = new DoNothingRate();
  public static final DoNothingTimer DO_NOTHING_TIMER = new DoNothingTimer();

  @Override
  public Counter createCounter(MetricInfo metricInfo) {
    return DO_NOTHING_COUNTER;
  }

  @Override
  public <T> AutoGauge createAutoGauge(MetricInfo metricInfo, T obj, ToDoubleFunction<T> mapper) {
    return DO_NOTHING_AUTO_GAUGE;
  }

  @Override
  public Gauge createGauge(MetricInfo metricInfo) {
    return DO_NOTHING_GAUGE;
  }

  @Override
  public Histogram createHistogram(MetricInfo metricInfo) {
    return DO_NOTHING_HISTOGRAM;
  }

  @Override
  public Rate createRate(MetricInfo metricInfo) {
    return DO_NOTHING_RATE;
  }

  @Override
  public Timer createTimer(MetricInfo metricInfo) {
    return DO_NOTHING_TIMER;
  }

  @Override
  public boolean isEnableMetricInGivenLevel(MetricLevel metricLevel) {
    return false;
  }

  @Override
  protected void removeMetric(MetricType type, MetricInfo metricInfo) {
    // do nothing
  }

  @Override
  protected boolean stopFramework() {
    return true;
  }
}
