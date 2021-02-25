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

import org.apache.iotdb.metrics.KnownMetric;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
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
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmCompilationMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MicrometerMetricManager implements MetricManager {
  private static final Logger logger = LoggerFactory.getLogger(MicrometerMetricManager.class);

  Map<Meter.Id, IMetric> currentMeters;
  boolean isEnable;

  io.micrometer.core.instrument.MeterRegistry meterRegistry;
  MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

  public MicrometerMetricManager() {
    meterRegistry = Metrics.globalRegistry;
    currentMeters = new ConcurrentHashMap<>();
    isEnable = metricConfig.isEnabled();
  }

  @Override
  public boolean init() {
    logger.debug("micrometer init registry");
    List<String> reporters = metricConfig.getReporterList();
    for (String reporter : reporters) {
      switch (ReporterType.get(reporter)) {
        case JMX:
          Metrics.addRegistry(new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM));
          break;
        case PROMETHEUS:
          Metrics.addRegistry(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
          break;
        case IOTDB:
          break;
        default:
          logger.warn("Unsupported report type {}, please check the config.", reporter);
          return false;
      }
    }
    return true;
  }

  public MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }

  public PrometheusMeterRegistry getPrometheusMeterRegistry() {
    Set<MeterRegistry> meterRegistrySet = Metrics.globalRegistry.getRegistries();
    for (MeterRegistry childMeterRegistry : meterRegistrySet) {
      if (childMeterRegistry instanceof PrometheusMeterRegistry) {
        return (PrometheusMeterRegistry) childMeterRegistry;
      }
    }
    return null;
  }

  public JmxMeterRegistry getJmxMeterRegistry() {
    Set<MeterRegistry> meterRegistrySet = Metrics.globalRegistry.getRegistries();
    for (MeterRegistry childMeterRegistry : meterRegistrySet) {
      if (childMeterRegistry instanceof JmxMeterRegistry) {
        return (JmxMeterRegistry) childMeterRegistry;
      }
    }
    return null;
  }

  @Override
  public Counter counter(String metric, String... tags) {
    if (!isEnable) {
      return null;
    }
    io.micrometer.core.instrument.Counter innerCounter = meterRegistry.counter(metric, tags);
    return (Counter)
        currentMeters.computeIfAbsent(
            innerCounter.getId(), key -> new MicrometerCounter(innerCounter));
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
  public Gauge gauge(String metric, String... tags) {
    if (!isEnable) {
      return null;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    return (Gauge)
        currentMeters.computeIfAbsent(id, key -> new MicrometerGauge(meterRegistry, metric, tags));
  }

  @Override
  public Histogram histogram(String metric, String... tags) {
    if (!isEnable) {
      return null;
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
   * metrics system has the ability to calculate rate
   *
   * @param metric
   * @param tags
   * @return
   */
  @Override
  @Deprecated
  public Rate rate(String metric, String... tags) {
    if (!isEnable) {
      return null;
    }
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.GAUGE, tags);
    return (Rate)
        currentMeters.computeIfAbsent(
            id,
            key ->
                new MicrometerRate(meterRegistry.gauge(metric, Tags.of(tags), new AtomicLong(0))));
  }

  @Override
  public Timer timer(String metric, String... tags) {
    if (!isEnable) {
      return null;
    }
    logger.info(metric + Arrays.toString(tags));
    Meter.Id id = MeterIdUtils.fromMetricName(metric, Meter.Type.TIMER, tags);
    return (Timer)
        currentMeters.computeIfAbsent(
            id,
            key -> {
              io.micrometer.core.instrument.Timer timer =
                  io.micrometer.core.instrument.Timer.builder(metric)
                      .tags(tags)
                      .register(meterRegistry);
              logger.info("create timer {}", metric);
              return new MicrometerTimer(timer);
            });
  }

  @Override
  public void histogram(int value, String metric, String... tags) {
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
    Map<String[], IMetric> iMetricMap = getMetricByType(Meter.Type.COUNTER);
    Map<String[], Counter> counterMap = new HashMap<>();
    iMetricMap.forEach((k, v) -> counterMap.put(k, (Counter) v));
    return counterMap;
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    Map<String[], IMetric> iMetricMap = getMetricByType(Meter.Type.GAUGE);
    Map<String[], Gauge> gaugeMap = new HashMap<>();
    iMetricMap.forEach((k, v) -> gaugeMap.put(k, (Gauge) v));
    return gaugeMap;
  }

  @Override
  public Map<String[], Rate> getAllRates() {
    Map<String[], IMetric> iMetricMap = getMetricByType(Meter.Type.OTHER);
    Map<String[], Rate> rateMap = new HashMap<>();
    iMetricMap.forEach((k, v) -> rateMap.put(k, (Rate) v));
    return rateMap;
  }

  @Override
  public Map<String[], Histogram> getAllHistograms() {
    Map<String[], IMetric> iMetricMap = getMetricByType(Meter.Type.DISTRIBUTION_SUMMARY);
    Map<String[], Histogram> histogramMap = new HashMap<>();
    iMetricMap.forEach((k, v) -> histogramMap.put(k, (Histogram) v));
    return histogramMap;
  }

  @Override
  public Map<String[], Timer> getAllTimers() {
    Map<String[], IMetric> iMetricMap = getMetricByType(Meter.Type.TIMER);
    Map<String[], Timer> timerMap = new HashMap<>();
    iMetricMap.forEach((k, v) -> timerMap.put(k, (Timer) v));
    return timerMap;
  }

  private Map<String[], IMetric> getMetricByType(Meter.Type type) {
    Map<String[], IMetric> iMetricMap = new HashMap<>();
    for (Map.Entry<Meter.Id, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getKey().getType() == type) {
        List<String> tags = new ArrayList<>(entry.getKey().getTags().size() * 2);
        tags.add(entry.getKey().getName());
        for (Tag tag : entry.getKey().getTags()) {
          tags.add(tag.getKey());
          tags.add(tag.getValue());
        }
        iMetricMap.put(tags.toArray(new String[0]), entry.getValue());
      }
    }
    return iMetricMap;
  }

  @Override
  public void enableKnownMetric(KnownMetric metric) {
    if (!isEnable) {
      return;
    }
    switch (metric) {
      case JVM:
        enableJVMMetrics();
        break;
      case SYSTEM:
        break;
      case THREAD:
        break;
      default:
        // ignore;
    }
  }

  private void enableJVMMetrics() {
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
  public boolean isEnable() {
    return isEnable;
  }
}
