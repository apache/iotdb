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

import org.apache.iotdb.metrics.impl.DoNothingMetricManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** MetricService is the entry to manage all Metric system */
public class MetricService {

  private static final Logger logger = LoggerFactory.getLogger(MetricService.class);

  private static final List<MetricReporter> reporters = new ArrayList<>();

  static {
    init();
  }

  private static final MetricService INSTANCE = new MetricService();

  private static MetricManager metricManager;

  public static MetricService getINSTANCE() {
    return INSTANCE;
  }

  private MetricService() {}

  private static void init() {
    logger.debug("init metric service");
    ServiceLoader<MetricManager> metricManagers = ServiceLoader.load(MetricManager.class);
    int size = 0;
    MetricManager nothingManager = new DoNothingMetricManager();

    for (MetricManager mf : metricManagers) {
      if (mf instanceof DoNothingMetricManager) {
        nothingManager = mf;
        continue;
      }
      size++;
      metricManager = mf;
    }

    // if no more implementations, we use nothingFactory.
    if (size == 0) {
      metricManager = nothingManager;
    } else if (size > 1) {
      logger.warn(
          "detect more than one MetricManager, will use {}", metricManager.getClass().getName());
    }
    // do some init work
    metricManager.init();

    ServiceLoader<MetricReporter> reporter = ServiceLoader.load(MetricReporter.class);
    for (MetricReporter r : reporter) {
      reporters.add(r);
      r.setMetricManager(metricManager);
      r.start();
      logger.info("detect MetricReporter {}", r.getClass().getName());
    }
  }

  public static void stop() {
    for (MetricReporter r : reporters) {
      logger.info("detect MetricReporter {}", r.getClass().getName());
      r.stop();
    }
  }

  public static MetricManager getMetricManager() {
    return metricManager;
  }

  public static void enableKnownMetric(KnownMetric metric) {
    metricManager.enableKnownMetric(metric);
  }

  public static boolean isEnable() {
    return metricManager.isEnable();
  }
}
