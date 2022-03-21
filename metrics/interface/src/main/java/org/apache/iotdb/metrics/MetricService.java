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
import org.apache.iotdb.metrics.reporter.CompositeReporter;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.utils.PredefinedMetric;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MetricService is the entry to manage all Metric system, include MetricManager and MetricReporter.
 */
public abstract class MetricService {

  private static final Logger logger = LoggerFactory.getLogger(MetricService.class);
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

  protected MetricManager metricManager = new DoNothingMetricManager();

  protected CompositeReporter compositeReporter = new CompositeReporter();

  protected boolean isEnableMetric = metricConfig.getEnableMetric();

  private AtomicBoolean firstInit = new AtomicBoolean(true);

  public MetricService() {}

  /** Start metric service without start reporter. if is disabled, do nothing */
  public void startService() {
    // load manager
    loadManager();
    // load reporter
    loadReporter();
    // do some init work
    metricManager.init();
    // do start all reporter without first time
    if (!firstInit.getAndSet(false)) {
      startAllReporter();
    }

    logger.info("Start predefined metric:" + metricConfig.getPredefinedMetrics());
    for (PredefinedMetric predefinedMetric : metricConfig.getPredefinedMetrics()) {
      enablePredefinedMetric(predefinedMetric);
    }
    logger.info("Start metric at level: " + metricConfig.getMetricLevel().name());

    collectFileSystemInfo();
  }

  /** Stop metric service. if is disabled, do nothing */
  public void stopService() {
    metricManager.stop();
    compositeReporter.stopAll();
    metricManager = new DoNothingMetricManager();
    compositeReporter = new CompositeReporter();
  }

  private void loadManager() {
    logger.info("Load metricManager, type: {}", metricConfig.getMonitorType());
    ServiceLoader<MetricManager> metricManagers = ServiceLoader.load(MetricManager.class);
    int size = 0;
    for (MetricManager mf : metricManagers) {
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
          "detect more than one MetricManager, will use {}", metricManager.getClass().getName());
    }
  }

  private void loadReporter() {
    logger.info("Load metric reporter, reporters: {}", metricConfig.getMetricReporterList());

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

  public void startAllReporter() {
    // start reporter
    compositeReporter.startAll();
  }

  /** start reporter by name, values in jmx, prometheus, internal. if is disabled, do nothing */
  public void start(ReporterType reporter) {
    if (!isEnable()) {
      return;
    }
    compositeReporter.start(reporter);
  }

  /** stop reporter by name, values in jmx, prometheus, internal. if is disabled, do nothing */
  public void stop(ReporterType reporter) {
    if (!isEnable()) {
      return;
    }
    compositeReporter.stop(reporter);
  }

  /**
   * Enable some predefined metric, now support jvm, logback. Notice: In dropwizard mode, logback
   * metrics are not supported
   */
  public void enablePredefinedMetric(PredefinedMetric metric) {
    metricManager.enablePredefinedMetric(metric);
  }

  /** collect file system info in metric way */
  protected abstract void collectFileSystemInfo();

  /**
   * support hot load of some properties
   *
   * @param reloadLevel
   */
  protected abstract void reloadProperties(ReloadLevel reloadLevel);

  public MetricManager getMetricManager() {
    return metricManager;
  }

  public boolean isEnable() {
    return isEnableMetric;
  }
}
