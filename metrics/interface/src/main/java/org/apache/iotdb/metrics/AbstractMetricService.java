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
import org.apache.iotdb.metrics.impl.DoNothingMetric;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.reporter.CompositeReporter;
import org.apache.iotdb.metrics.reporter.InternalReporter;
import org.apache.iotdb.metrics.reporter.MemoryInternalReporter;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.ReporterType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

/** MetricService is the entry to get all metric features. */
public abstract class AbstractMetricService {
  private static final Logger logger = LoggerFactory.getLogger(AbstractMetricService.class);
  /** The config of metric service */
  protected final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  /** The metric manager of metric service */
  protected AbstractMetricManager metricManager = new DoNothingMetricManager();
  /** The metric reporter of metric service */
  protected CompositeReporter compositeReporter = new CompositeReporter();
  /** The internal reporter of metric service */
  protected InternalReporter internalReporter = new MemoryInternalReporter();

  /** The list of metric sets */
  protected List<IMetricSet> metricSets = new ArrayList<>();

  public AbstractMetricService() {}

  /** Start metric service */
  public void startService() {
    startCoreModule();
    for (IMetricSet metricSet : metricSets) {
      metricSet.bindTo(this);
    }
  }

  /** Stop metric service */
  public void stopService() {
    for (IMetricSet metricSet : metricSets) {
      metricSet.unbindFrom(this);
    }
    stopCoreModule();
  }

  /** Start metric core module */
  protected void startCoreModule() {
    logger.info("Start metric service at level: {}", metricConfig.getMetricLevel().name());
    // load metric manager
    loadManager();
    // load metric reporter
    loadReporter();
    // do start all reporter without first time
    startAllReporter();
  }

  /** Stop metric core module */
  protected void stopCoreModule() {
    stopAllReporter();
    metricManager.stop();
    metricManager = new DoNothingMetricManager();
    compositeReporter = new CompositeReporter();
  }

  /** Load metric manager according to configuration */
  private void loadManager() {
    logger.info("Load metricManager, type: {}", metricConfig.getMetricFrameType());
    ServiceLoader<AbstractMetricManager> metricManagers =
        ServiceLoader.load(AbstractMetricManager.class);
    int size = 0;
    for (AbstractMetricManager mf : metricManagers) {
      size++;
      if (mf.getClass()
          .getName()
          .toLowerCase()
          .contains(metricConfig.getMetricFrameType().name().toLowerCase())) {
        metricManager = mf;
        break;
      }
    }

    // if no more implementations, we use nothingManager.
    if (size == 0 || metricManager == null) {
      metricManager = new DoNothingMetricManager();
    } else if (size > 1) {
      logger.warn(
          "Detect more than one MetricManager, will use {}", metricManager.getClass().getName());
    }
  }

  /** Load metric reporters according to configuration */
  protected void loadReporter() {
    logger.info("Load metric reporters, type: {}", metricConfig.getMetricReporterList());
    compositeReporter.clearReporter();
    ServiceLoader<Reporter> reporters = ServiceLoader.load(Reporter.class);
    for (Reporter reporter : reporters) {
      if (metricConfig.getMetricReporterList() != null
          && metricConfig.getMetricReporterList().contains(reporter.getReporterType())
          && reporter
              .getClass()
              .getName()
              .toLowerCase()
              .contains(metricConfig.getMetricFrameType().name().toLowerCase())) {
        reporter.setMetricManager(metricManager);
        compositeReporter.addReporter(reporter);
      }
    }
  }

  /**
   * Reload internal reporter
   *
   * @param internalReporter the new internal reporter
   */
  public abstract void reloadInternalReporter(InternalReporter internalReporter);

  /**
   * Reload metric service
   *
   * @param reloadLevel the level of reload
   */
  protected abstract void reloadService(ReloadLevel reloadLevel);

  // region interface from metric reporter

  /** Start all reporters */
  public void startAllReporter() {
    compositeReporter.startAll();
  }

  /** Stop all reporters */
  public void stopAllReporter() {
    compositeReporter.stopAll();
  }

  /** Start reporter according to type */
  public void start(ReporterType type) {
    compositeReporter.start(type);
  }

  /** Stop reporter according to type */
  public void stop(ReporterType type) {
    compositeReporter.stop(type);
  }

  // endregion

  // region interface from metric manager

  public Counter getOrCreateCounter(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getOrCreateCounter(metric, metricLevel, tags);
  }

  public <T> AutoGauge getOrCreateAutoGauge(
      String metric, MetricLevel metricLevel, T obj, ToLongFunction<T> mapper, String... tags) {
    return metricManager.getOrCreateAutoGauge(metric, metricLevel, obj, mapper, tags);
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

  /** GetOrCreateCounter with internal report */
  public Counter getOrCreateCounterWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Counter counter = metricManager.getOrCreateCounter(metric, metricLevel, tags);
    report(counter, metric, tags);
    return counter;
  }

  /** GetOrCreateAutoGauge with internal report */
  public <T> AutoGauge getOrCreateAutoGaugeWithInternalReport(
      String metric, MetricLevel metricLevel, T obj, ToLongFunction<T> mapper, String... tags) {
    AutoGauge gauge = metricManager.getOrCreateAutoGauge(metric, metricLevel, obj, mapper, tags);
    report(gauge, metric, tags);
    return gauge;
  }

  /** GetOrCreateGauge with internal report */
  public Gauge getOrCreateGaugeWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Gauge gauge = metricManager.getOrCreateGauge(metric, metricLevel, tags);
    report(gauge, metric, tags);
    return gauge;
  }

  /** GetOrCreateRate with internal report */
  public Rate getOrCreateRateWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Rate rate = metricManager.getOrCreateRate(metric, metricLevel, tags);
    report(rate, metric, tags);
    return rate;
  }

  /** GetOrCreateHistogram with internal report */
  public Histogram getOrCreateHistogramWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Histogram histogram = metricManager.getOrCreateHistogram(metric, metricLevel, tags);
    report(histogram, metric, tags);
    return histogram;
  }

  /** GetOrCreateTimer with internal report */
  public Timer getOrCreateTimerWithInternalReport(
      String metric, MetricLevel metricLevel, String... tags) {
    Timer timer = metricManager.getOrCreateTimer(metric, metricLevel, tags);
    report(timer, metric, tags);
    return timer;
  }

  /** Count with internal report */
  public void countWithInternalReport(
      long delta, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.count(delta, metric, metricLevel, tags), metric, tags);
  }

  /** Gauge value with internal report */
  public void gaugeWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.gauge(value, metric, metricLevel, tags), metric, tags);
  }

  /** Rate with internal report */
  public void rateWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.rate(value, metric, metricLevel, tags), metric, tags);
  }

  /** Histogram with internal report */
  public void histogramWithInternalReport(
      long value, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.histogram(value, metric, metricLevel, tags), metric, tags);
  }

  /** Timer with internal report */
  public void timerWithInternalReport(
      long delta, TimeUnit timeUnit, String metric, MetricLevel metricLevel, String... tags) {
    report(metricManager.timer(delta, timeUnit, metric, metricLevel, tags), metric, tags);
  }

  /**
   * Reporter metric internally
   *
   * @param metric the metric that need to report
   * @param name the name of metric
   * @param tags the tags of metric
   */
  protected void report(IMetric metric, String name, String... tags) {
    if (metric instanceof DoNothingMetric) {
      return;
    }
    if (metric instanceof Counter) {
      Counter counter = (Counter) metric;
      internalReporter.updateValue(name, counter.count(), TSDataType.INT64, tags);
    } else if (metric instanceof AutoGauge) {
      internalReporter.addAutoGauge((AutoGauge) metric, name, tags);
    } else if (metric instanceof Gauge) {
      internalReporter.updateValue(name, ((Gauge) metric).value(), TSDataType.INT64, tags);
    } else if (metric instanceof Rate) {
      Rate rate = (Rate) metric;
      Long time = System.currentTimeMillis();
      internalReporter.updateValue(name + "_count", rate.getCount(), TSDataType.INT64, time, tags);
      internalReporter.updateValue(
          name + "_mean", rate.getMeanRate(), TSDataType.DOUBLE, time, tags);
      internalReporter.updateValue(
          name + "_1min", rate.getOneMinuteRate(), TSDataType.DOUBLE, time, tags);
      internalReporter.updateValue(
          name + "_5min", rate.getFiveMinuteRate(), TSDataType.DOUBLE, time, tags);
      internalReporter.updateValue(
          name + "_15min", rate.getFifteenMinuteRate(), TSDataType.DOUBLE, time, tags);
    } else if (metric instanceof Histogram) {
      Histogram histogram = (Histogram) metric;
      internalReporter.writeSnapshotAndCount(name, histogram.takeSnapshot(), tags);
    } else if (metric instanceof Timer) {
      Timer timer = (Timer) metric;
      internalReporter.writeSnapshotAndCount(name, timer.takeSnapshot(), tags);
    }
  }

  public List<String[]> getAllMetricKeys() {
    return metricManager.getAllMetricKeys();
  }

  public Map<String[], Counter> getAllCounters() {
    return metricManager.getAllCounters();
  }

  public Map<String[], Gauge> getAllGauges() {
    return metricManager.getAllGauges();
  }

  public Map<String[], AutoGauge> getAllAutoGauges() {
    return metricManager.getAllAutoGauges();
  }

  public Map<String[], Rate> getAllRates() {
    return metricManager.getAllRates();
  }

  public Map<String[], Histogram> getAllHistograms() {
    return metricManager.getAllHistograms();
  }

  public Map<String[], Timer> getAllTimers() {
    return metricManager.getAllTimers();
  }

  public void remove(MetricType type, String metric, String... tags) {
    metricManager.remove(type, metric, tags);
  }

  // endregion

  public AbstractMetricManager getMetricManager() {
    return metricManager;
  }

  /** Bind metrics and store metric set */
  public void addMetricSet(IMetricSet metricSet) {
    if (!metricSets.contains(metricSet)) {
      metricSet.bindTo(this);
      metricSets.add(metricSet);
    }
  }

  /** Remove metrics */
  public void removeMetricSet(IMetricSet metricSet) {
    if (metricSets.contains(metricSet)) {
      metricSet.unbindFrom(this);
      metricSets.remove(metricSet);
    }
  }
}
