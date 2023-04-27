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

package org.apache.iotdb.metrics.dropwizard;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardAutoGauge;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardCounter;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardGauge;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardHistogram;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardRate;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardTimer;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import java.util.function.ToDoubleFunction;

/**
 * Metric manager based on dropwizard metrics. More details in https://metrics.dropwizard.io/4.1.2/.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class DropwizardMetricManager extends AbstractMetricManager {
  com.codahale.metrics.MetricRegistry metricRegistry;

  public DropwizardMetricManager() {
    metricRegistry = new MetricRegistry();
  }

  @Override
  public Counter createCounter(MetricInfo metricInfo) {
    return new DropwizardCounter(
        metricRegistry.counter(DropwizardMetricNameTool.toFlatString(metricInfo)));
  }

  @Override
  public <T> AutoGauge createAutoGauge(MetricInfo metricInfo, T obj, ToDoubleFunction<T> mapper) {
    DropwizardAutoGauge<T> dropwizardGauge = new DropwizardAutoGauge<>(obj, mapper);
    metricRegistry.register(DropwizardMetricNameTool.toFlatString(metricInfo), dropwizardGauge);
    return dropwizardGauge;
  }

  @Override
  public Gauge createGauge(MetricInfo metricInfo) {
    DropwizardGauge dropwizardGauge = new DropwizardGauge();
    metricRegistry.register(
        DropwizardMetricNameTool.toFlatString(metricInfo),
        dropwizardGauge.getDropwizardCachedGauge());
    return dropwizardGauge;
  }

  @Override
  public Rate createRate(MetricInfo metricInfo) {
    return new DropwizardRate(
        metricRegistry.meter(DropwizardMetricNameTool.toFlatString(metricInfo)));
  }

  @Override
  public Histogram createHistogram(MetricInfo metricInfo) {
    return new DropwizardHistogram(
        metricRegistry.histogram(DropwizardMetricNameTool.toFlatString(metricInfo)));
  }

  @Override
  public Timer createTimer(MetricInfo metricInfo) {
    return new DropwizardTimer(
        metricRegistry.timer(DropwizardMetricNameTool.toFlatString(metricInfo)));
  }

  @Override
  protected void removeMetric(MetricType type, MetricInfo metricInfo) {
    metricRegistry.remove(DropwizardMetricNameTool.toFlatString(metricInfo));
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public boolean stopFramework() {
    metricRegistry.removeMatching(MetricFilter.ALL);
    return true;
  }
}
