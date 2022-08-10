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

package org.apache.iotdb.metrics.micrometer;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.micrometer.type.MicrometerAutoGauge;
import org.apache.iotdb.metrics.micrometer.type.MicrometerCounter;
import org.apache.iotdb.metrics.micrometer.type.MicrometerGauge;
import org.apache.iotdb.metrics.micrometer.type.MicrometerHistogram;
import org.apache.iotdb.metrics.micrometer.type.MicrometerRate;
import org.apache.iotdb.metrics.micrometer.type.MicrometerTimer;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

/** Metric manager based on micrometer. More details in https://micrometer.io/. */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class MicrometerMetricManager extends AbstractMetricManager {
  private static final Logger logger = LoggerFactory.getLogger(MicrometerMetricManager.class);

  Map<MicrometerMetricName, IMetric> currentMeters;
  io.micrometer.core.instrument.MeterRegistry meterRegistry;

  /** init the field with micrometer library. */
  public MicrometerMetricManager() {
    meterRegistry = Metrics.globalRegistry;
    currentMeters = new ConcurrentHashMap<>();
  }

  @Override
  public Counter getOrCreateCounter(String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.COUNTER, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName, key -> new MicrometerCounter(meterRegistry.counter(metric, tags)));
    if (m instanceof Counter) {
      return (Counter) m;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public <T> Gauge getOrCreateAutoGauge(
      String metric, T obj, ToLongFunction<T> mapper, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.GAUGE, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName,
            key -> new MicrometerAutoGauge<T>(meterRegistry, metric, obj, mapper, tags));
    if (m instanceof Gauge) {
      return (Gauge) m;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public Gauge getOrCreateGauge(String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.GAUGE, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName, key -> new MicrometerGauge(meterRegistry, metric, tags));
    if (m instanceof Gauge) {
      return (Gauge) m;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public Histogram getOrCreateHistogram(String metric, String... tags) {
    MicrometerMetricName metricName =
        new MicrometerMetricName(metric, Meter.Type.DISTRIBUTION_SUMMARY, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName,
            key -> {
              io.micrometer.core.instrument.DistributionSummary distributionSummary =
                  io.micrometer.core.instrument.DistributionSummary.builder(metric)
                      .tags(tags)
                      .register(meterRegistry);
              return new MicrometerHistogram(distributionSummary);
            });
    if (m instanceof Histogram) {
      return (Histogram) m;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public Rate getOrCreateRate(String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.GAUGE, tags);

    IMetric m =
        currentMeters.computeIfAbsent(
            metricName,
            key ->
                new MicrometerRate(meterRegistry.gauge(metric, Tags.of(tags), new AtomicLong(0))));
    if (m instanceof Rate) {
      return (Rate) m;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public Timer getOrCreateTimer(String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.TIMER, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName,
            key -> {
              io.micrometer.core.instrument.Timer timer =
                  io.micrometer.core.instrument.Timer.builder(metric)
                      .tags(tags)
                      .register(meterRegistry);
              logger.info("create getOrCreateTimer {}", metric);
              return new MicrometerTimer(timer);
            });
    if (m instanceof Timer) {
      return (Timer) m;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public void count(long delta, String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.COUNTER, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName, key -> new MicrometerCounter(meterRegistry.counter(metric, tags)));
    if (m instanceof Counter) {
      ((Counter) m).inc(delta);
    }
  }

  @Override
  public void histogram(long value, String metric, String... tags) {
    MicrometerMetricName metricName =
        new MicrometerMetricName(metric, Meter.Type.DISTRIBUTION_SUMMARY, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName,
            key -> {
              io.micrometer.core.instrument.DistributionSummary distributionSummary =
                  io.micrometer.core.instrument.DistributionSummary.builder(metric)
                      .tags(tags)
                      .publishPercentileHistogram()
                      .publishPercentiles(0)
                      .register(meterRegistry);
              return new MicrometerHistogram(distributionSummary);
            });
    if (m instanceof Histogram) {
      ((Histogram) m).update(value);
      return;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public void gauge(long value, String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.GAUGE, tags);
    IMetric m =
        (currentMeters.computeIfAbsent(
            metricName, key -> new MicrometerGauge(meterRegistry, metric, tags)));
    if (m instanceof Gauge) {
      ((Gauge) m).set(value);
      return;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public void rate(long value, String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.GAUGE, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName,
            key ->
                new MicrometerRate(meterRegistry.gauge(metric, Tags.of(tags), new AtomicLong(0))));

    if (m instanceof Rate) {
      ((Rate) m).mark(value);
      return;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public synchronized void timer(long delta, TimeUnit timeUnit, String metric, String... tags) {
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.TIMER, tags);
    IMetric m =
        currentMeters.computeIfAbsent(
            metricName,
            key -> {
              io.micrometer.core.instrument.Timer timer =
                  io.micrometer.core.instrument.Timer.builder(metric)
                      .tags(tags)
                      .register(meterRegistry);
              return new MicrometerTimer(timer);
            });
    if (m instanceof Timer) {
      ((Timer) m).update(delta, timeUnit);
      return;
    }
    throw new IllegalArgumentException(
        metricName + " is already used for a different type of metric");
  }

  @Override
  public List<String[]> getAllMetricKeys() {
    List<String[]> keys = new ArrayList<>(currentMeters.size());
    List<Meter> meterList = meterRegistry.getMeters();
    for (Meter meter : meterList) {
      List<String> tags = new ArrayList<>(meter.getId().getTags().size() * 2 + 1);
      tags.add(meter.getId().getName());
      for (Tag tag : meter.getId().getTags()) {
        tags.add(tag.getKey());
        tags.add(tag.getValue());
      }
      keys.add(tags.toArray(new String[0]));
    }
    return keys;
  }

  @Override
  public Map<String[], Counter> getAllCounters() {
    Map<String[], IMetric> metricMap = getMetricByType(Meter.Type.COUNTER);
    Map<String[], Counter> counterMap = new HashMap<>();
    metricMap.forEach((k, v) -> counterMap.put(k, (Counter) v));
    return counterMap;
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    Map<String[], IMetric> metricMap = getMetricByType(Meter.Type.GAUGE);
    Map<String[], Gauge> gaugeMap = new HashMap<>();
    metricMap.forEach((k, v) -> gaugeMap.put(k, (Gauge) v));
    return gaugeMap;
  }

  @Override
  public Map<String[], Rate> getAllRates() {
    Map<String[], IMetric> metricMap = getMetricByType(Meter.Type.OTHER);
    Map<String[], Rate> rateMap = new HashMap<>();
    metricMap.forEach((k, v) -> rateMap.put(k, (Rate) v));
    return rateMap;
  }

  @Override
  public Map<String[], Histogram> getAllHistograms() {
    Map<String[], IMetric> metricMap = getMetricByType(Meter.Type.DISTRIBUTION_SUMMARY);
    Map<String[], Histogram> histogramMap = new HashMap<>();
    metricMap.forEach((k, v) -> histogramMap.put(k, (Histogram) v));
    return histogramMap;
  }

  @Override
  public Map<String[], Timer> getAllTimers() {
    Map<String[], IMetric> metricMap = getMetricByType(Meter.Type.TIMER);
    Map<String[], Timer> timerMap = new HashMap<>();
    metricMap.forEach((k, v) -> timerMap.put(k, (Timer) v));
    return timerMap;
  }

  private Map<String[], IMetric> getMetricByType(Meter.Type type) {
    Map<String[], IMetric> metricMap = new HashMap<>();
    for (Map.Entry<MicrometerMetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getKey().getId().getType() == type) {
        List<String> tags = new ArrayList<>(entry.getKey().getId().getTags().size() * 2);
        tags.add(entry.getKey().getId().getName());
        for (Tag tag : entry.getKey().getId().getTags()) {
          tags.add(tag.getKey());
          tags.add(tag.getValue());
        }
        metricMap.put(tags.toArray(new String[0]), entry.getValue());
      }
    }
    return metricMap;
  }

  @Override
  public void removeCounter(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.COUNTER, tags);
    currentMeters.remove(metricName);
  }

  @Override
  public void removeGauge(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.GAUGE, tags);
    currentMeters.remove(metricName);
  }

  @Override
  public void removeRate(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.GAUGE, tags);
    currentMeters.remove(metricName);
  }

  @Override
  public void removeHistogram(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MicrometerMetricName metricName =
        new MicrometerMetricName(metric, Meter.Type.DISTRIBUTION_SUMMARY, tags);
    currentMeters.remove(metricName);
  }

  @Override
  public void removeTimer(String metric, String... tags) {
    if (!isEnable()) {
      return;
    }
    MicrometerMetricName metricName = new MicrometerMetricName(metric, Meter.Type.TIMER, tags);
    currentMeters.remove(metricName);
  }

  /** stop everything and clear */
  @Override
  public boolean stop() {
    isEnable = METRIC_CONFIG.getEnableMetric();
    meterRegistry.clear();
    currentMeters = new ConcurrentHashMap<>();
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MicrometerMetricManager that = (MicrometerMetricManager) o;
    return Objects.equals(currentMeters, that.currentMeters);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(currentMeters);
  }
}
