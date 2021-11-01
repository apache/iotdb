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
import org.apache.iotdb.metrics.impl.DoNothingCompositeReporter;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.utils.PredefinedMetric;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ServiceLoader;

/**
 * MetricService is the entry to manage all Metric system, include MetricManager and MetricReporter.
 */
public class MetricService {

  private static final Logger logger = LoggerFactory.getLogger(MetricService.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  static {
    init();
  }

  private static final MetricService INSTANCE = new MetricService();

  private static MetricManager metricManager;

  private static CompositeReporter compositeReporter;

  public static MetricService getInstance() {
    return INSTANCE;
  }

  private MetricService() {}

  /** init config, manager and reporter */
  private static void init() {
    logger.info("init metric service");
    ServiceLoader<MetricManager> metricManagers = ServiceLoader.load(MetricManager.class);
    int size = 0;
    MetricManager nothingManager = new DoNothingMetricManager();

    for (MetricManager mf : metricManagers) {
      size++;
      if (mf.getName()
          .toLowerCase()
          .contains(metricConfig.getMonitorType().getName().toLowerCase())) {
        metricManager = mf;
        break;
      }
    }

    // if no more implementations, we use nothingManager.
    if (size == 0 || metricManager == null) {
      metricManager = nothingManager;
    } else if (size > 1) {
      logger.warn(
          "detect more than one MetricManager, will use {}", metricManager.getClass().getName());
    }

    compositeReporter = new CompositeReporter();

    ServiceLoader<Reporter> reporters = ServiceLoader.load(Reporter.class);
    size = 0;
    for(Reporter reporter: reporters){

    }

    ServiceLoader<CompositeReporter> reporter = ServiceLoader.load(CompositeReporter.class);
    size = 0;
    for (CompositeReporter r : reporter) {
      size++;
      if (r.getName()
          .toLowerCase()
          .contains(metricConfig.getMonitorType().getName().toLowerCase())) {
        compositeReporter = r;
        logger.info("detect MetricReporter {}", r.getClass().getName());
      }
    }

    // if no more implementations, we use nothingReporter.
    if (size == 0 || compositeReporter == null) {
      compositeReporter = new DoNothingCompositeReporter();
    } else if (size > 1) {
      logger.warn(
          "detect more than one MetricReporter, will use {}",
          compositeReporter.getClass().getName());
    }
    // do some init work
    metricManager.init();
    compositeReporter.setMetricManager(metricManager);
    List<ReporterType> reporters = metricConfig.getMetricReporterList();
    for (ReporterType report : reporters) {
      if (!compositeReporter.start(report)) {
        logger.warn("fail to start {}", report);
      }
    }
  }

  /**
   * start reporter by name name values in jmx, prometheus, iotdb, internal
   *
   * @param reporter
   */
  public static void start(ReporterType reporter) {
    compositeReporter.start(reporter);
  }

  /**
   * stop reporter by name name values in jmx, prometheus, iotdb, internal
   *
   * @param reporter
   */
  public static void stop(ReporterType reporter) {
    compositeReporter.stop(reporter);
  }

  /** Stop metric service. If disable, do nothing. */
  public static void stop() {
    if (!isEnable()) {
      return;
    }
    compositeReporter.stop();
  }

  public static MetricManager getMetricManager() {
    return metricManager;
  }

  public static void enablePredefinedMetric(PredefinedMetric metric) {
    metricManager.enablePredefinedMetric(metric);
  }

  public static boolean isEnable() {
    return metricConfig.getEnableMetric();
  }
}
