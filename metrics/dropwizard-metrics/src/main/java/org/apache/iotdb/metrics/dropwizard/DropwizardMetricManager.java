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
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricType;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;

import java.util.function.ToLongFunction;

/**
 * Metric manager based on dropwizard metrics. More details in https://metrics.dropwizard.io/4.1.2/.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class DropwizardMetricManager extends AbstractMetricManager {
  com.codahale.metrics.MetricRegistry metricRegistry;

  MetricRegistry.MetricSupplier<com.codahale.metrics.Timer> timerMetricSupplier =
      () -> new com.codahale.metrics.Timer(new UniformReservoir());
  MetricRegistry.MetricSupplier<com.codahale.metrics.Histogram> histogramMetricSupplier =
      () -> new com.codahale.metrics.Histogram(new UniformReservoir());

  public DropwizardMetricManager() {
    metricRegistry = new MetricRegistry();
  }

  @Override
  public Counter createCounter(MetricInfo metricInfo) {
    DropwizardMetricName name = new DropwizardMetricName(metricInfo);
    return new DropwizardCounter(metricRegistry.counter(name.toFlatString()));
  }

  @Override
  public <T> Gauge createAutoGauge(MetricInfo metricInfo, T obj, ToLongFunction<T> mapper) {
    DropwizardMetricName name = new DropwizardMetricName(metricInfo);
    DropwizardAutoGauge<T> dropwizardGauge = new DropwizardAutoGauge<>(obj, mapper);
    metricRegistry.register(name.toFlatString(), dropwizardGauge);
    return dropwizardGauge;
  }

  @Override
  public Gauge createGauge(MetricInfo metricInfo) {
    DropwizardMetricName name = new DropwizardMetricName(metricInfo);
    DropwizardGauge dropwizardGauge = new DropwizardGauge();
    metricRegistry.register(name.toFlatString(), dropwizardGauge.getDropwizardCachedGauge());
    return dropwizardGauge;
  }

  @Override
  public Rate createRate(MetricInfo metricInfo) {
    DropwizardMetricName name = new DropwizardMetricName(metricInfo);
    return new DropwizardRate(metricRegistry.meter(name.toFlatString()));
  }

  @Override
  public Histogram createHistogram(MetricInfo metricInfo) {
    DropwizardMetricName name = new DropwizardMetricName(metricInfo);
    return new DropwizardHistogram(
        metricRegistry.histogram(name.toFlatString(), histogramMetricSupplier));
  }

  @Override
  public Timer createTimer(MetricInfo metricInfo) {
    DropwizardMetricName name = new DropwizardMetricName(metricInfo);
    return new DropwizardTimer(metricRegistry.timer(name.toFlatString(), timerMetricSupplier));
  }

  @Override
  protected void remove(MetricType type, MetricInfo metricInfo) {
    DropwizardMetricName name = new DropwizardMetricName(metricInfo);
    metricRegistry.remove(name.toFlatString());
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
