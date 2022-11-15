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
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.function.ToLongFunction;

public class DoNothingMetricManager extends AbstractMetricManager {

  public static final DoNothingCounter doNothingCounter = new DoNothingCounter();
  public static final DoNothingHistogram doNothingHistogram = new DoNothingHistogram();
  public static final DoNothingGauge doNothingGauge = new DoNothingGauge();
  public static final DoNothingRate doNothingRate = new DoNothingRate();
  public static final DoNothingTimer doNothingTimer = new DoNothingTimer();

  @Override
  public Counter createCounter(MetricInfo metricInfo) {
    return doNothingCounter;
  }

  @Override
  public <T> Gauge createAutoGauge(MetricInfo metricInfo, T obj, ToLongFunction<T> mapper) {
    return doNothingGauge;
  }

  @Override
  public Gauge createGauge(MetricInfo metricInfo) {
    return doNothingGauge;
  }

  @Override
  public Histogram createHistogram(MetricInfo metricInfo) {
    return doNothingHistogram;
  }

  @Override
  public Rate createRate(MetricInfo metricInfo) {
    return doNothingRate;
  }

  @Override
  public Timer createTimer(MetricInfo metricInfo) {
    return doNothingTimer;
  }

  @Override
  public boolean isEnableMetric() {
    return false;
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
