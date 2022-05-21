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

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardAutoGauge;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardCounter;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardGauge;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardHistogram;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardRate;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardTimer;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

/**
 * Metric manager based on dropwizard metrics. More details in https://metrics.dropwizard.io/4.1.2/.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class DropwizardMetricManager implements MetricManager {
  private static final Logger logger = LoggerFactory.getLogger(DropwizardMetricManager.class);

  Map<MetricName, IMetric> currentMeters;
  /** whether is able to monitor */
  boolean isEnable;

  com.codahale.metrics.MetricRegistry metricRegistry;
  MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  MetricRegistry.MetricSupplier<com.codahale.metrics.Timer> timerMetricSupplier =
      () -> new com.codahale.metrics.Timer(new UniformReservoir());
  MetricRegistry.MetricSupplier<com.codahale.metrics.Histogram> histogramMetricSupplier =
      () -> new com.codahale.metrics.Histogram(new UniformReservoir());

  /** init the field with dropwizard library. */
  public DropwizardMetricManager() {
    metricRegistry = new MetricRegistry();
    isEnable = metricConfig.getEnableMetric();
    currentMeters = new ConcurrentHashMap<>();
  }

  @Override
  public Counter getOrCreateCounter(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return DoNothingMetricManager.doNothingCounter;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardCounter(metricRegistry.counter(name.toFlatString())));
    if (m instanceof Counter) {
      return (Counter) m;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public <T> Gauge getOrCreateAutoGauge(
      String metric, MetricLevel metricLevel, T obj, ToLongFunction<T> mapper, String... tags) {
    if (!isEnable(metricLevel)) {
      return DoNothingMetricManager.doNothingGauge;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name,
            key -> {
              DropwizardAutoGauge<T> dropwizardGauge = new DropwizardAutoGauge<>(obj, mapper);
              metricRegistry.register(name.toFlatString(), dropwizardGauge);
              return dropwizardGauge;
            });
    if (m instanceof Gauge) {
      return (Gauge) m;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public Gauge getOrCreateGauge(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return DoNothingMetricManager.doNothingGauge;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name,
            key -> {
              DropwizardGauge dropwizardGauge = new DropwizardGauge();
              metricRegistry.register(
                  name.toFlatString(), dropwizardGauge.getDropwizardCachedGauge());
              return dropwizardGauge;
            });
    if (m instanceof Gauge) {
      return (Gauge) m;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public Rate getOrCreateRate(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return DoNothingMetricManager.doNothingRate;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardRate(metricRegistry.meter(name.toFlatString())));
    if (m instanceof Rate) {
      return (Rate) m;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public Histogram getOrCreateHistogram(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return DoNothingMetricManager.doNothingHistogram;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name,
            key ->
                new DropwizardHistogram(
                    metricRegistry.histogram(name.toFlatString(), histogramMetricSupplier)));
    if (m instanceof Histogram) {
      return (Histogram) m;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public Timer getOrCreateTimer(String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable()) {
      return DoNothingMetricManager.doNothingTimer;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name,
            key ->
                new DropwizardTimer(
                    metricRegistry.timer(name.toFlatString(), timerMetricSupplier)));
    if (m instanceof Timer) {
      return (Timer) m;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public void count(long delta, String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardCounter(metricRegistry.counter(name.toFlatString())));
    if (m instanceof Counter) {
      ((Counter) m).inc(delta);
      return;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public void gauge(long value, String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name,
            key -> {
              DropwizardGauge dropwizardGauge = new DropwizardGauge();
              metricRegistry.register(
                  name.toFlatString(), dropwizardGauge.getDropwizardCachedGauge());
              return dropwizardGauge;
            });
    if (m instanceof Gauge) {
      ((Gauge) m).set(value);
      return;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public void rate(long value, String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardRate(metricRegistry.meter(name.toFlatString())));
    if (m instanceof Rate) {
      ((Rate) m).mark(value);
      return;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public void histogram(long value, String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name,
            key ->
                new DropwizardHistogram(
                    metricRegistry.histogram(name.toFlatString(), histogramMetricSupplier)));
    if (m instanceof Histogram) {
      ((Histogram) m).update(value);
      return;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public void timer(
      long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags) {
    if (!isEnable(metricLevel)) {
      return;
    }
    MetricName name = new MetricName(metric, metricLevel, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            name,
            key ->
                new DropwizardTimer(
                    metricRegistry.timer(name.toFlatString(), timerMetricSupplier)));

    if (m instanceof Timer) {
      ((Timer) m).update(delta, timeUnit);
      return;
    }
    throw new IllegalArgumentException(name + " is already used for a different type of metric");
  }

  @Override
  public void removeCounter(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    metricRegistry.remove(name.toFlatString());
    currentMeters.remove(name);
  }

  @Override
  public void removeGauge(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    metricRegistry.remove(name.toFlatString());
    currentMeters.remove(name);
  }

  @Override
  public void removeRate(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    metricRegistry.remove(name.toFlatString());
    currentMeters.remove(name);
  }

  @Override
  public void removeHistogram(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    metricRegistry.remove(name.toFlatString());
    currentMeters.remove(name);
  }

  @Override
  public void removeTimer(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    metricRegistry.remove(name.toFlatString());
    currentMeters.remove(name);
  }

  @Override
  public List<String[]> getAllMetricKeys() {
    if (!isEnable()) {
      return Collections.emptyList();
    }
    List<String[]> keys = new ArrayList<>(currentMeters.size());
    currentMeters.keySet().forEach(k -> keys.add(k.toStringArray()));
    return keys;
  }

  @Override
  public Map<String[], Counter> getAllCounters() {
    Map<String[], Counter> counterMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Counter) {
        counterMap.put(entry.getKey().toStringArray(), (Counter) entry.getValue());
      }
    }
    return counterMap;
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    Map<String[], Gauge> gaugeMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Gauge) {
        gaugeMap.put(entry.getKey().toStringArray(), (Gauge) entry.getValue());
      }
    }
    return gaugeMap;
  }

  @Override
  public Map<String[], Rate> getAllRates() {
    Map<String[], Rate> rateMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Rate) {
        rateMap.put(entry.getKey().toStringArray(), (Rate) entry.getValue());
      }
    }
    return rateMap;
  }

  @Override
  public Map<String[], Histogram> getAllHistograms() {
    Map<String[], Histogram> histogramMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Histogram) {
        histogramMap.put(entry.getKey().toStringArray(), (Histogram) entry.getValue());
      }
    }
    return histogramMap;
  }

  @Override
  public Map<String[], Timer> getAllTimers() {
    Map<String[], Timer> timerMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Timer) {
        timerMap.put(entry.getKey().toStringArray(), (Timer) entry.getValue());
      }
    }
    return timerMap;
  }

  @Override
  public boolean isEnable() {
    return isEnable;
  }

  @Override
  public boolean isEnable(MetricLevel metricLevel) {
    return isEnable() && MetricLevel.higherOrEqual(metricLevel, metricConfig.getMetricLevel());
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public boolean init() {
    // init something
    return true;
  }

  @Override
  public boolean stop() {
    isEnable = metricConfig.getEnableMetric();
    metricRegistry.removeMatching(MetricFilter.ALL);
    currentMeters = new ConcurrentHashMap<>();
    return true;
  }
}
