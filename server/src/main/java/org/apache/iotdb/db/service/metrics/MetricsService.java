/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.service.metrics;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.metrics.CompositeReporter;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.Reporter;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.utils.PredefinedMetric;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

public class MetricsService implements MetricsServiceMBean, IService {
  private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();
  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private static MetricManager metricManager;

  private static CompositeReporter compositeReporter;

  private MetricsService() {}

  public static void loadManager() {
    logger.info("Load metricManager, type: {}", metricConfig.getMonitorType());
    ServiceLoader<MetricManager> metricManagers = ServiceLoader.load(MetricManager.class);
    int size = 0;
    for (MetricManager mf : metricManagers) {
      size++;
      if (mf.getClass()
          .getName()
          .toLowerCase()
          .contains(metricConfig.getMonitorType().getName().toLowerCase())) {
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

  public static void loadReporter() {
    logger.info("Load metric reporter, reporters: {}", metricConfig.getMetricReporterList());
    compositeReporter = new CompositeReporter();

    ServiceLoader<Reporter> reporters = ServiceLoader.load(Reporter.class);
    for (Reporter reporter : reporters) {
      if (metricConfig.getMetricReporterList() != null
          && metricConfig.getMetricReporterList().contains(reporter.getReporterType())
          && reporter
              .getClass()
              .getName()
              .toLowerCase()
              .contains(metricConfig.getMonitorType().getName().toLowerCase())) {
        reporter.setMetricManager(metricManager);
        compositeReporter.addReporter(reporter);
      }
    }
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

  @Override
  public void start() throws StartupException {
    try {
      if (metricConfig.getEnableMetric()) {
        JMXService.registerMBean(getInstance(), mbeanName);
        startService();
      }
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    if (metricConfig.getEnableMetric()) {
      stopService();
      JMXService.deregisterMBean(mbeanName);
    }
  }

  @Override
  /** Start all reporter. if is disabled, do nothing */
  public void startService() {
    logger.info("Init metric service");
    // load manager
    loadManager();
    // load reporter
    loadReporter();
    // do some init work
    metricManager.init();
    // start reporter
    compositeReporter.startAll();
  }

  @Override
  public void restartService() throws StartupException {
    stopService();
    startService();
  }

  @Override
  /** Stop metric service. if is disabled, do nothing */
  public void stopService() {
    compositeReporter.stopAll();
  }

  /** Enable some predefined metric, now support jvm */
  public void enablePredefinedMetric(PredefinedMetric metric) {
    metricManager.enablePredefinedMetric(metric);
  }

  public MetricManager getMetricManager() {
    return metricManager;
  }

  public boolean isEnable() {
    return metricConfig.getEnableMetric();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.METRICS_SERVICE;
  }

  public static MetricsService getInstance() {
    return MetricsServiceHolder.INSTANCE;
  }

  private static class MetricsServiceHolder {

    private static final MetricsService INSTANCE = new MetricsService();

    private MetricsServiceHolder() {}
  }
}
