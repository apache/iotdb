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

package org.apache.iotdb.metrics;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalMemoryReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBSessionReporter;
import org.apache.iotdb.metrics.reporter.prometheus.PrometheusReporter;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;

/** MetricService is the entry to get all metric features. */
public abstract class AbstractMetricService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMetricService.class);
  /** The config of metric service. */
  protected static final MetricConfig METRIC_CONFIG =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  /** The metric manager of metric service. */
  protected AbstractMetricManager metricManager = new DoNothingMetricManager();
  /** The metric reporter of metric service. */
  protected CompositeReporter compositeReporter = new CompositeReporter();
  /** The internal reporter of metric service. */
  protected IoTDBInternalReporter internalReporter = new IoTDBInternalMemoryReporter();

  /** The list of metric sets. */
  protected Set<IMetricSet> metricSets = new HashSet<>();

  public AbstractMetricService() {
    // empty constructor
  }

  /** Start metric service. */
  public void startService() {
    startCoreModule();
    synchronized (this) {
      for (IMetricSet metricSet : metricSets) {
        metricSet.bindTo(this);
      }
    }
  }

  /** Stop metric service. */
  public void stopService() {
    synchronized (this) {
      for (IMetricSet metricSet : metricSets) {
        metricSet.unbindFrom(this);
      }
    }
    stopCoreModule();
  }

  /** Start metric core module. */
  protected void startCoreModule() {
    LOGGER.info("Start metric service at level: {}", METRIC_CONFIG.getMetricLevel().name());
    // load metric manager
    loadManager();
    // load metric reporter
    loadReporter();
    // do start all reporter without first time
    startAllReporter();
  }

  /** Stop metric core module. */
  protected void stopCoreModule() {
    stopAllReporter();
    metricManager.stop();
    metricManager = new DoNothingMetricManager();
    compositeReporter = new CompositeReporter();
  }

  /** Load metric manager according to configuration. */
  private void loadManager() {
    LOGGER.info("Load metricManager, type: {}", METRIC_CONFIG.getMetricFrameType());
    ServiceLoader<AbstractMetricManager> metricManagers =
        ServiceLoader.load(AbstractMetricManager.class);
    int size = 0;
    for (AbstractMetricManager mf : metricManagers) {
      size++;
      if (mf.getClass()
          .getName()
          .toLowerCase()
          .contains(METRIC_CONFIG.getMetricFrameType().name().toLowerCase())) {
        metricManager = mf;
        break;
      }
    }

    // if no more implementations, we use nothingManager.
    if (size == 0 || metricManager == null) {
      metricManager = new DoNothingMetricManager();
    } else if (size > 1) {
      LOGGER.info(
          "Detect more than one MetricManager, will use {}", metricManager.getClass().getName());
    }
  }

  /** Load metric reporters according to configuration. */
  protected void loadReporter() {
    LOGGER.info("Load metric reporters, type: {}", METRIC_CONFIG.getMetricReporterList());
    compositeReporter.clearReporter();
    if (METRIC_CONFIG.getMetricReporterList() == null) {
      return;
    }
    for (ReporterType reporterType : METRIC_CONFIG.getMetricReporterList()) {
      Reporter reporter = null;
      switch (reporterType) {
        case JMX:
          ServiceLoader<JmxReporter> reporters = ServiceLoader.load(JmxReporter.class);
          for (JmxReporter jmxReporter : reporters) {
            if (jmxReporter
                .getClass()
                .getName()
                .toLowerCase()
                .contains(METRIC_CONFIG.getMetricFrameType().name().toLowerCase())) {
              jmxReporter.setMetricManager(metricManager);
              reporter = jmxReporter;
            }
          }
          break;
        case PROMETHEUS:
          reporter = new PrometheusReporter(metricManager);
          break;
        case IOTDB:
          reporter = new IoTDBSessionReporter(metricManager);
          break;
        default:
          continue;
      }
      if (reporter == null) {
        LOGGER.warn("Failed to load reporter which type is {}", reporterType);
        continue;
      }
      compositeReporter.addReporter(reporter);
    }
  }

  /**
   * Reload internal reporter.
   *
   * @param internalReporter the new internal reporter
   */
  public abstract void reloadInternalReporter(IoTDBInternalReporter internalReporter);

  /**
   * Reload metric service.
   *
   * @param reloadLevel the level of reload
   */
  protected abstract void reloadService(ReloadLevel reloadLevel);

  // region interface from metric reporter

  /** Start all reporters. */
  public void startAllReporter() {
    compositeReporter.startAll();
  }

  /** Stop all reporters. */
  public void stopAllReporter() {
    compositeReporter.stopAll();
  }

  /** Start reporter according to type. */
  public void start(ReporterType type) {
    compositeReporter.start(type);
  }

  /** Stop reporter according to type. */
  public void stop(ReporterType type) {
    compositeReporter.stop(type);
  }

  // endregion

  // region interface from metric manager

  public Counter getOrCreateCounter(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getOrCreateCounter(metric, metricLevel, tags);
  }

  public <T> AutoGauge createAutoGauge(
      String metric, MetricLevel metricLevel, T obj, ToDoubleFunction<T> mapper, String... tags) {
    return metricManager.createAutoGauge(metric, metricLevel, obj, mapper, tags);
  }

  public AutoGauge getAutoGauge(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getAutoGauge(metric, metricLevel, tags);
  }

  public Gauge getOrCreateGauge(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getOrCreateGauge(metric, metricLevel, tags);
  }

  public Rate getOrCreateRate(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getOrCreateRate(metric, metricLevel, tags);
  }

  public Histogram getOrCreateHistogram(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getOrCreateHistogram(metric, metricLevel, tags);
  }

  public Timer getOrCreateTimer(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getOrCreateTimer(metric, metricLevel, tags);
  }

  public void count(long delta, String metric, MetricLevel metricLevel, String... tags) {
    metricManager.count(delta, metric, metricLevel, tags);
  }

  public void gauge(long value, String metric, MetricLevel metricLevel, String... tags) {
    metricManager.gauge(value, metric, metricLevel, tags);
  }

  public void rate(long value, String metric, MetricLevel metricLevel, String... tags) {
    metricManager.rate(value, metric, metricLevel, tags);
  }

  public void histogram(long value, String metric, MetricLevel metricLevel, String... tags) {
    metricManager.histogram(value, metric, metricLevel, tags);
  }

  public void timer(
      long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags) {
    metricManager.timer(delta, timeUnit, metric, metricLevel, tags);
  }

  /** GetOrCreateCounter with internal report. */
  public Counter getOrCreateCounterWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Counter counter = metricManager.getOrCreateCounter(metric, metricLevel, tags);
    internalReporter.writeMetricToIoTDB(counter, metric, tags);
    return counter;
  }

  /** GetOrCreateAutoGauge with internal report. */
  public <T> AutoGauge createAutoGaugeWithInternalReport(
      String metric, MetricLevel metricLevel, T obj, ToDoubleFunction<T> mapper, String... tags) {
    AutoGauge gauge = metricManager.createAutoGauge(metric, metricLevel, obj, mapper, tags);
    internalReporter.addAutoGauge(gauge, metric, tags);
    return gauge;
  }

  /** GetOrCreateGauge with internal report. */
  public Gauge getOrCreateGaugeWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Gauge gauge = metricManager.getOrCreateGauge(metric, metricLevel, tags);
    internalReporter.writeMetricToIoTDB(gauge, metric, tags);
    return gauge;
  }

  /** GetOrCreateRate with internal report. */
  public Rate getOrCreateRateWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Rate rate = metricManager.getOrCreateRate(metric, metricLevel, tags);
    internalReporter.writeMetricToIoTDB(rate, metric, tags);
    return rate;
  }

  /** GetOrCreateHistogram with internal report. */
  public Histogram getOrCreateHistogramWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Histogram histogram = metricManager.getOrCreateHistogram(metric, metricLevel, tags);
    internalReporter.writeMetricToIoTDB(histogram, metric, tags);
    return histogram;
  }

  /** GetOrCreateTimer with internal report. */
  public Timer getOrCreateTimerWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Timer timer = metricManager.getOrCreateTimer(metric, metricLevel, tags);
    internalReporter.writeMetricToIoTDB(timer, metric, tags);
    return timer;
  }

  /** Count with internal report. */
  public void countWithInternalReport(
      long delta, String metric, MetricLevel metricLevel, String... tags) {
    internalReporter.writeMetricToIoTDB(
        metricManager.count(delta, metric, metricLevel, tags), metric, tags);
  }

  /** Gauge value with internal report */
  public void gaugeWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    internalReporter.writeMetricToIoTDB(
        metricManager.gauge(value, metric, metricLevel, tags), metric, tags);
  }

  /** Rate with internal report. */
  public void rateWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    internalReporter.writeMetricToIoTDB(
        metricManager.rate(value, metric, metricLevel, tags), metric, tags);
  }

  /** Histogram with internal report. */
  public void histogramWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    internalReporter.writeMetricToIoTDB(
        metricManager.histogram(value, metric, metricLevel, tags), metric, tags);
  }

  /** Timer with internal report. */
  public void timerWithInternalReport(
      long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags) {
    internalReporter.writeMetricToIoTDB(
        metricManager.timer(delta, timeUnit, metric, metricLevel, tags), metric, tags);
  }

  public List<Pair<String, String[]>> getAllMetricKeys() {
    return metricManager.getAllMetricKeys();
  }

  public Map<MetricInfo, IMetric> getAllMetrics() {
    return metricManager.getAllMetrics();
  }

  public Map<MetricInfo, IMetric> getMetricsByType(MetricType metricType) {
    return metricManager.getMetricsByType(metricType);
  }

  public void remove(MetricType type, String metric, String... tags) {
    metricManager.remove(type, metric, tags);
  }

  // endregion

  public AbstractMetricManager getMetricManager() {
    return metricManager;
  }

  /** Bind metrics and store metric set. */
  public synchronized void addMetricSet(IMetricSet metricSet) {
    if (!metricSets.contains(metricSet)) {
      metricSet.bindTo(this);
      metricSets.add(metricSet);
    }
  }

  /** Remove metrics. */
  public synchronized void removeMetricSet(IMetricSet metricSet) {
    if (metricSets.contains(metricSet)) {
      metricSet.unbindFrom(this);
      metricSets.remove(metricSet);
    }
  }
}
