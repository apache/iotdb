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
import org.apache.iotdb.metrics.metricsets.predefined.PredefinedMetric;
import org.apache.iotdb.metrics.reporter.CompositeReporter;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToLongFunction;

/** MetricService is the entry to get all metric features. */
public abstract class AbstractMetricService {

  private static final Logger logger = LoggerFactory.getLogger(AbstractMetricService.class);
  /** The config of metric service */
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  /** Is the first initialization of metric service */
  protected AtomicBoolean isFirstInitialization = new AtomicBoolean(true);
  /** The metric manager of metric service */
  protected AbstractMetricManager metricManager = new DoNothingMetricManager();
  /** The metric reporter of metric service */
  protected CompositeReporter compositeReporter = new CompositeReporter();
  /** Is metric service enabled */
  protected boolean isEnableMetric = metricConfig.getEnableMetric();
  /** The list of metric sets */
  protected List<IMetricSet> metricSets = new ArrayList<>();

  public AbstractMetricService() {}

  /** start metric service */
  public void startService() {
    startCoreModule();
    for (IMetricSet metricSet : metricSets) {
      metricSet.bindTo(this);
    }
  }

  /** restart metric service */
  public void restartService() {
    logger.info("Restart Core Module");
    stopCoreModule();
    startCoreModule();
    for (IMetricSet metricSet : metricSets) {
      logger.info("Restart metricSet: {}", metricSet.getClass().getName());
      metricSet.unbindFrom(this);
      metricSet.bindTo(this);
    }
  }

  /** stop metric service */
  public void stopService() {
    for (IMetricSet metricSet : metricSets) {
      metricSet.unbindFrom(this);
    }
    stopCoreModule();
  }
  /** start metric core module */
  private void startCoreModule() {
    logger.info("Start metric service at level: {}", metricConfig.getMetricLevel().name());
    // load metric manager
    loadManager();
    // load metric reporter
    loadReporter();
    // do start all reporter without first time
    startAllReporter();
    logger.info("Start predefined metrics: {}", metricConfig.getPredefinedMetrics());
    for (PredefinedMetric predefinedMetric : metricConfig.getPredefinedMetrics()) {
      enablePredefinedMetrics(predefinedMetric);
    }
  }

  /** stop metric core module */
  private void stopCoreModule() {
    stopAllReporter();
    metricManager.stop();
    metricManager = new DoNothingMetricManager();
    compositeReporter = new CompositeReporter();
  }

  /** Load metric manager according to configuration */
  private void loadManager() {
    logger.info("Load metricManager, type: {}", metricConfig.getMonitorType());
    ServiceLoader<AbstractMetricManager> metricManagers =
        ServiceLoader.load(AbstractMetricManager.class);
    int size = 0;
    for (AbstractMetricManager mf : metricManagers) {
      size++;
      if (mf.getClass()
          .getName()
          .toLowerCase()
          .contains(metricConfig.getMonitorType().name().toLowerCase())) {
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
              .contains(metricConfig.getMonitorType().name().toLowerCase())) {
        reporter.setMetricManager(metricManager);
        compositeReporter.addReporter(reporter);
      }
    }
  }

  /** Enable predefined Metrics */
  protected abstract void enablePredefinedMetrics(PredefinedMetric metric);

  /** Reload metric service according to reloadLevel */
  protected abstract void reloadProperties(ReloadLevel reloadLevel);

  // region interface from metric reporter

  /** Start all reporters */
  public void startAllReporter() {
    if (!isEnable()) {
      return;
    }
    compositeReporter.startAll();
  }

  /** Stop all reporters */
  public void stopAllReporter() {
    if (!isEnable()) {
      return;
    }
    compositeReporter.stopAll();
  }

  /** Start reporter according to type */
  public void start(ReporterType type) {
    if (!isEnable()) {
      return;
    }
    compositeReporter.start(type);
  }

  /** Stop reporter according to type */
  public void stop(ReporterType type) {
    if (!isEnable()) {
      return;
    }
    compositeReporter.stop(type);
  }

  // endregion

  // region interface from metric manager

  public Counter getOrCreateCounter(String metric, MetricLevel metricLevel, String... tags) {
    return metricManager.getOrCreateCounter(metric, metricLevel, tags);
  }

  public <T> Gauge getOrCreateAutoGauge(
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

  public List<String[]> getAllMetricKeys() {
    return metricManager.getAllMetricKeys();
  }

  public Map<String[], Counter> getAllCounters() {
    return metricManager.getAllCounters();
  }

  public Map<String[], Gauge> getAllGauges() {
    return metricManager.getAllGauges();
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

  public boolean isEnable() {
    return isEnableMetric;
  }

  /** bind metrics and store metric set */
  public void addMetricSet(IMetricSet metricSet) {
    if (!metricSets.contains(metricSet)) {
      metricSet.bindTo(this);
      metricSets.add(metricSet);
    }
  }
}
