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
import org.apache.iotdb.metrics.reporter.iotdb.InternalIoTDBReporter;
import org.apache.iotdb.metrics.reporter.iotdb.MemoryInternalIoTDBReporter;

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
      logger.info("Start to start metric Service.");
      JMXService.registerMBean(getInstance(), mbeanName);
      startService();
      logger.info("Finish start metric Service");
    } catch (Exception e) {
      logger.error("Failed to start {} because: ", this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  /** Restart metric service */
  public void restartService() {
    logger.info("Restart metric service");
    stopCoreModule();
    internalReporter.clear();
    startCoreModule();
    for (IMetricSet metricSet : metricSets) {
      logger.info("Restart metricSet: {}", metricSet.getClass().getName());
      metricSet.unbindFrom(this);
      metricSet.bindTo(this);
    }
    logger.info("Finish restarting metric service");
  }

  @Override
  public void stop() {
    logger.info("Stop metric service");
    internalReporter.stop();
    internalReporter = new MemoryInternalIoTDBReporter();
    stopService();
    JMXService.deregisterMBean(mbeanName);
    logger.info("Finish stopping metric service");
  }

  @Override
  public void reloadInternalReporter(InternalIoTDBReporter internalReporter) {
    logger.info("Reload internal reporter");
    internalReporter.addAutoGauge(this.internalReporter.getAllAutoGauge());
    this.internalReporter.stop();
    this.internalReporter = internalReporter;
    this.internalReporter.start();
    logger.info("Finish reloading internal reporter");
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
          logger.info("Finish restart metric reporters.");
          break;
        case NOTHING:
          logger.debug("There are nothing change in metric module.");
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

  public void updateInternalReporter(InternalIoTDBReporter InternalReporter) {
    this.internalReporter = InternalReporter;
  }

  public void startInternalReporter() {
    this.internalReporter.start();
  }

  public static MetricService getInstance() {
    return MetricsServiceHolder.INSTANCE;
  }

  private static class MetricsServiceHolder {

    private static final MetricService INSTANCE = new MetricService();

    private MetricsServiceHolder() {}
  }
}
