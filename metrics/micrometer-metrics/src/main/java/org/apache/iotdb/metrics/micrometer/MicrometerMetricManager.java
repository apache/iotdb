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

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.micrometer.type.*;
import org.apache.iotdb.metrics.type.*;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.PredefinedMetric;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.jvm.*;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Metric manager based on micrometer. More details in https://micrometer.io/. */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class MicrometerMetricManager implements MetricManager {
  private static final Logger logger = LoggerFactory.getLogger(MicrometerMetricManager.class);

  Map<Meter.Id, IMetric> currentMeters;
  boolean isEnable;
  io.micrometer.core.instrument.MeterRegistry meterRegistry;

  MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

  /** init the field with micrometer library. */
  public MicrometerMetricManager() {
    meterRegistry = Metrics.globalRegistry;
    currentMeters = new ConcurrentHashMap<>();
    isEnable = metricConfig.getEnableMetric();
  }

  /**
   * init of manager
   *
   * @see org.apache.iotdb.metrics.MetricService
   */
  @Override
  public boolean init() {
    logger.info("micrometer init registry");
    List<ReporterType> reporters = metricConfig.getMetricReporterList();
    if (reporters == null) {
      return false;
    }
    for (ReporterType report : reporters) {
      if (!addMeterRegistry(report)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Counter getOrCreateCounter(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingCounter;
    }
    io.micrometer.core.instrument.Counter innerCounter = meterRegistry.counter(metric, tags);
    return (Counter)
        currentMeters.computeIfAbsent(
            innerCounter.getId(), key -> new MicrometerCounter(innerCounter));
  }

  @Override
  public Gauge getOrCreatGauge(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingGauge;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    return (Gauge)
        currentMeters.computeIfAbsent(id, key -> new MicrometerGauge(meterRegistry, metric, tags));
  }

  @Override
  public Histogram getOrCreateHistogram(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingHistogram;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.DISTRIBUTION_SUMMARY, tags);
    return (Histogram)
        currentMeters.computeIfAbsent(
            id,
            key -> {
              io.micrometer.core.instrument.DistributionSummary distributionSummary =
                  io.micrometer.core.instrument.DistributionSummary.builder(metric)
                      .tags(tags)
                      .register(meterRegistry);
              return new MicrometerHistogram(distributionSummary);
            });
  }

  /**
   * We only create a gauge(AtomicLong) to record the raw value, because we assume that the backend
   * metrics system has the ability to calculate getOrCreatRate.
   *
   * @param metric the name
   * @param tags tags to describe some attribute
   * @return Rate instance
   */
  @Override
  public Rate getOrCreatRate(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingRate;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);

    return (Rate)
        currentMeters.computeIfAbsent(
            id,
            key ->
                new MicrometerRate(meterRegistry.gauge(metric, Tags.of(tags), new AtomicLong(0))));
  }

  @Override
  public Timer getOrCreateTimer(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingTimer;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.TIMER, tags);
    return (Timer)
        currentMeters.computeIfAbsent(
            id,
            key -> {
              io.micrometer.core.instrument.Timer timer =
                  io.micrometer.core.instrument.Timer.builder(metric)
                      .tags(tags)
                      .register(meterRegistry);
              logger.info("create getOrCreateTimer {}", metric);
              return new MicrometerTimer(timer);
            });
  }

  @Override
  public void count(int delta, String metric, String... tags) {
    io.micrometer.core.instrument.Counter innerCounter = meterRegistry.counter(metric, tags);
    innerCounter.increment(delta);
  }

  @Override
  public void count(long delta, String metric, String... tags) {
    io.micrometer.core.instrument.Counter innerCounter = meterRegistry.counter(metric, tags);
    innerCounter.increment(delta);
  }

  @Override
  public void histogram(int value, String metric, String... tags) {
    this.histogram(Long.valueOf(value), metric, tags);
  }

  @Override
  public void histogram(long value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.DISTRIBUTION_SUMMARY, tags);
    ((Histogram)
            currentMeters.computeIfAbsent(
                id,
                key -> {
                  io.micrometer.core.instrument.DistributionSummary distributionSummary =
                      io.micrometer.core.instrument.DistributionSummary.builder(metric)
                          .tags(tags)
                          .publishPercentileHistogram()
                          .publishPercentiles(0)
                          .register(meterRegistry);
                  return new MicrometerHistogram(distributionSummary);
                }))
        .update(value);
  }

  @Override
  public void gauge(int value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    ((Gauge)
            (currentMeters.computeIfAbsent(
                id, key -> new MicrometerGauge(meterRegistry, metric, tags))))
        .set(value);
  }

  @Override
  public void gauge(long value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    ((Gauge)
            (currentMeters.computeIfAbsent(
                id, key -> new MicrometerGauge(meterRegistry, metric, tags))))
        .set(value);
  }

  @Override
  public void rate(int value, String metric, String... tags) {
    this.rate(Long.valueOf(value), metric, tags);
  }

  @Override
  public void rate(long value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    ((Rate)
            currentMeters.computeIfAbsent(
                id,
                key ->
                    new MicrometerRate(
                        meterRegistry.gauge(metric, Tags.of(tags), new AtomicLong(0)))))
        .mark(value);
  }

  @Override
  public synchronized void timer(long delta, TimeUnit timeUnit, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.TIMER, tags);
    ((Timer)
            currentMeters.computeIfAbsent(
                id,
                key -> {
                  io.micrometer.core.instrument.Timer timer =
                      io.micrometer.core.instrument.Timer.builder(metric)
                          .tags(tags)
                          .register(meterRegistry);
                  return new MicrometerTimer(timer);
                }))
        .update(delta, timeUnit);
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
    for (Map.Entry<Meter.Id, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getKey().getType() == type) {
        List<String> tags = new ArrayList<>(entry.getKey().getTags().size() * 2);
        tags.add(entry.getKey().getName());
        for (Tag tag : entry.getKey().getTags()) {
          tags.add(tag.getKey());
          tags.add(tag.getValue());
        }
        metricMap.put(tags.toArray(new String[0]), entry.getValue());
      }
    }
    return metricMap;
  }

  @Override
  public void enablePredefinedMetric(PredefinedMetric metric) {
    if (!isEnable) {
      return;
    }
    switch (metric) {
      case JVM:
        enableJvmMetrics();
        break;
      default:
        logger.warn("Unsupported metric type {}", metric);
    }
  }

  /** bind default metric to registry(or reporter */
  private void enableJvmMetrics() {
    if (!isEnable) {
      return;
    }
    ClassLoaderMetrics classLoaderMetrics = new ClassLoaderMetrics();
    classLoaderMetrics.bindTo(meterRegistry);
    JvmCompilationMetrics jvmCompilationMetrics = new JvmCompilationMetrics();
    jvmCompilationMetrics.bindTo(meterRegistry);
    try (JvmGcMetrics jvmGcMetrics = new JvmGcMetrics();
        JvmHeapPressureMetrics jvmHeapPressureMetrics = new JvmHeapPressureMetrics()) {
      jvmGcMetrics.bindTo(meterRegistry);
      jvmHeapPressureMetrics.bindTo(meterRegistry);
    }
    JvmMemoryMetrics jvmMemoryMetrics = new JvmMemoryMetrics();
    jvmMemoryMetrics.bindTo(meterRegistry);
    JvmThreadMetrics jvmThreadMetrics = new JvmThreadMetrics();
    jvmThreadMetrics.bindTo(meterRegistry);
  }

  @Override
  public void removeCounter(String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.COUNTER, tags);
    currentMeters.remove(id);
  }

  @Override
  public void removeGauge(String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    currentMeters.remove(id);
  }

  @Override
  public void removeRate(String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    currentMeters.remove(id);
  }

  @Override
  public void removeHistogram(String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.DISTRIBUTION_SUMMARY, tags);
    currentMeters.remove(id);
  }

  @Override
  public void removeTimer(String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.TIMER, tags);
    currentMeters.remove(id);
  }

  /** stop everything and clear */
  @Override
  public boolean stop() {
    // do nothing
    return true;
  }

  private boolean addMeterRegistry(ReporterType reporter) {
    switch (reporter) {
      case jmx:
        Metrics.addRegistry(new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM));
        break;
      case prometheus:
        Metrics.addRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
        break;
      default:
        logger.warn("Unsupported report type {}, please check the config.", reporter);
        return false;
    }
    return true;
  }

  @Override
  public boolean isEnable() {
    return isEnable;
  }
}
