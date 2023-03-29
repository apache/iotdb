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

package org.apache.iotdb.commons.service.metric;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalMemoryReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricService extends AbstractMetricService implements MetricServiceMBean, IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricService.class);
  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private MetricService() {
    // empty constructor
  }

  @Override
  public void start() throws StartupException {
    try {
      LOGGER.info("MetricService start to init.");
      JMXService.registerMBean(getInstance(), mbeanName);
      startService();
      LOGGER.info("MetricService start successfully.");
    } catch (Exception e) {
      LOGGER.error("MetricService failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  /** Restart metric service. */
  public void restartService() {
    LOGGER.info("MetricService try to restart.");
    stopCoreModule();
    internalReporter.clear();
    startCoreModule();
    synchronized (this) {
      for (IMetricSet metricSet : metricSets) {
        LOGGER.info("MetricService rebind metricSet: {}", metricSet.getClass().getName());
        metricSet.unbindFrom(this);
        metricSet.bindTo(this);
      }
    }
    LOGGER.info("MetricService restart successfully.");
  }

  @Override
  public void stop() {
    LOGGER.info("MetricService try to stop.");
    internalReporter.stop();
    internalReporter = new IoTDBInternalMemoryReporter();
    stopService();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("MetricService stop successfully.");
  }

  @Override
  public void reloadInternalReporter(IoTDBInternalReporter internalReporter) {
    LOGGER.info("MetricService reload internal reporter.");
    internalReporter.addAutoGauge(this.internalReporter.getAllAutoGauge());
    this.internalReporter.stop();
    this.internalReporter = internalReporter;
    startInternalReporter();
    LOGGER.info("MetricService reload internal reporter successfully.");
  }

  @Override
  public void reloadService(ReloadLevel reloadLevel) {
    synchronized (this) {
      switch (reloadLevel) {
        case RESTART_METRIC:
          restartService();
          break;
        case RESTART_REPORTER:
          stopAllReporter();
          loadReporter();
          startAllReporter();
          LOGGER.info("MetricService restart reporters successfully.");
          break;
        case NOTHING:
          LOGGER.debug("There are nothing change in metric config.");
          break;
        default:
          break;
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.METRIC_SERVICE;
  }

  public void updateInternalReporter(IoTDBInternalReporter internalReporter) {
    this.internalReporter = internalReporter;
  }

  public void startInternalReporter() {
    if (!this.internalReporter.start()) {
      LOGGER.warn("Internal Reporter failed to start!");
      this.internalReporter = new IoTDBInternalMemoryReporter();
    }
  }

  public static MetricService getInstance() {
    return MetricsServiceHolder.INSTANCE;
  }

  private static class MetricsServiceHolder {

    private static final MetricService INSTANCE = new MetricService();

    private MetricsServiceHolder() {
      // empty constructor
    }
  }
}
