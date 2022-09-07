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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.service.metrics.predefined.FileMetrics;
import org.apache.iotdb.db.service.metrics.predefined.ProcessMetrics;
import org.apache.iotdb.db.service.metrics.predefined.SystemMetrics;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.metricsets.predefined.PredefinedMetric;
import org.apache.iotdb.metrics.metricsets.predefined.jvm.JvmMetrics;
import org.apache.iotdb.metrics.metricsets.predefined.logback.LogbackMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricService extends AbstractMetricService implements MetricServiceMBean, IService {
  private static final Logger logger = LoggerFactory.getLogger(MetricService.class);
  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private MetricService() {}

  @Override
  public void start() throws StartupException {
    try {
      if (isEnable()) {
        logger.info("Start to start metric Service.");
        JMXService.registerMBean(getInstance(), mbeanName);
        startService();
        logger.info("Finish start metric Service");
      }
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  public void restart() {
    logger.info("Restart metric Service.");
    restartService();
    logger.info("Finish restart metric Service");
  }

  @Override
  public void stop() {
    if (isEnable()) {
      logger.info("Stop metric Service.");
      stopService();
      JMXService.deregisterMBean(mbeanName);
      logger.info("Finish stop metric Service");
    }
  }

  @Override
  public void enablePredefinedMetrics(PredefinedMetric metric) {
    IMetricSet metricSet;
    switch (metric) {
      case JVM:
        metricSet = new JvmMetrics();
        break;
      case LOGBACK:
        metricSet = new LogbackMetrics();
        break;
      case FILE:
        metricSet = new FileMetrics();
        break;
      case PROCESS:
        metricSet = new ProcessMetrics();
        break;
      case SYSTEM:
        metricSet = new SystemMetrics();
        break;
      default:
        logger.error("Unknown predefined metrics: {}", metric);
        return;
    }
    metricSet.bindTo(this);
  }

  @Override
  public void reloadProperties(ReloadLevel reloadLevel) {
    logger.info("Reload properties of metric service");
    synchronized (this) {
      try {
        switch (reloadLevel) {
          case START_METRIC:
            isEnableMetric = true;
            start();
            break;
          case STOP_METRIC:
            stop();
            isEnableMetric = false;
            break;
          case RESTART_METRIC:
            isEnableMetric = true;
            restart();
            break;
          case RESTART_REPORTER:
            stopAllReporter();
            loadReporter();
            startAllReporter();
            logger.info("Finish restart metric reporters.");
            break;
          case NOTHING:
            logger.debug("There are nothing change in metric module.");
            break;
          default:
            break;
        }
      } catch (StartupException startupException) {
        logger.error("Failed to start metric when reload properties");
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.METRIC_SERVICE;
  }

  public static MetricService getInstance() {
    return MetricsServiceHolder.INSTANCE;
  }

  private static class MetricsServiceHolder {

    private static final MetricService INSTANCE = new MetricService();

    private MetricsServiceHolder() {}
  }
}
