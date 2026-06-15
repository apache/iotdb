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
import org.apache.iotdb.commons.i18n.ServiceMessages;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.config.ReloadLevel;
import org.apache.iotdb.metrics.core.IoTDBMetricManager;
import org.apache.iotdb.metrics.core.reporter.IoTDBJmxReporter;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.reporter.Reporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalMemoryReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBInternalReporter;
import org.apache.iotdb.metrics.reporter.iotdb.IoTDBSessionReporter;
import org.apache.iotdb.metrics.reporter.prometheus.PrometheusReporter;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricService extends AbstractMetricService implements MetricServiceMBean, IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricService.class);
  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          IoTDBConstant.IOTDB_SERVICE_JMX_NAME, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private MetricService() {
    // empty constructor
  }

  @Override
  protected void loadManager() {
    metricManager = IoTDBMetricManager.getInstance();
  }

  @Override
  protected void loadReporter() {
    LOGGER.info(ServiceMessages.LOAD_METRIC_REPORTERS, METRIC_CONFIG.getMetricReporterList());
    compositeReporter.clearReporter();
    if (METRIC_CONFIG.getMetricReporterList() == null) {
      return;
    }
    boolean hasJmxReporter = false;
    for (ReporterType reporterType : METRIC_CONFIG.getMetricReporterList()) {
      Reporter reporter = null;
      switch (reporterType) {
        case JMX:
          reporter = IoTDBJmxReporter.getInstance();
          metricManager.setBindJmxReporter((JmxReporter) reporter);
          hasJmxReporter = true;
          break;
        case PROMETHEUS:
          reporter = new PrometheusReporter(metricManager);
          break;
        case IOTDB:
          reporter = new IoTDBSessionReporter(metricManager);
          break;
        default:
          break;
      }
      if (reporter == null) {
        LOGGER.warn(ServiceMessages.FAILED_TO_LOAD_REPORTER, reporterType);
        continue;
      }
      compositeReporter.addReporter(reporter);
    }
    if (!hasJmxReporter) {
      // in hot-loading scenario, we need to ensure that JmxReporter is null
      metricManager.setBindJmxReporter(null);
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      LOGGER.info(ServiceMessages.METRIC_SERVICE_START_TO_INIT);
      JMXService.registerMBean(getInstance(), mbeanName);
      startService();
      LOGGER.info(ServiceMessages.METRIC_SERVICE_START_SUCCESSFULLY);
    } catch (Exception e) {
      LOGGER.error(ServiceMessages.METRIC_SERVICE_FAILED_TO_START, this.getID().getName(), e);
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  /** Restart metric service. */
  public void restartService() {
    LOGGER.info(ServiceMessages.METRIC_SERVICE_TRY_TO_RESTART);
    stopCoreModule();
    internalReporter.clear();
    startCoreModule();
    synchronized (this) {
      for (IMetricSet metricSet : metricSets) {
        LOGGER.info(
            ServiceMessages.METRIC_SERVICE_REBIND_METRIC_SET, metricSet.getClass().getName());
        metricSet.unbindFrom(this);
        metricSet.bindTo(this);
      }
    }
    LOGGER.info(ServiceMessages.METRIC_SERVICE_RESTART_SUCCESSFULLY);
  }

  @Override
  public void stop() {
    LOGGER.info(ServiceMessages.METRIC_SERVICE_TRY_TO_STOP);
    internalReporter.stop();
    internalReporter = new IoTDBInternalMemoryReporter();
    stopService();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info(ServiceMessages.METRIC_SERVICE_STOP_SUCCESSFULLY);
  }

  @Override
  public void reloadInternalReporter(IoTDBInternalReporter internalReporter) {
    LOGGER.info(ServiceMessages.METRIC_SERVICE_RELOAD_INTERNAL_REPORTER);
    internalReporter.addAutoGauge(this.internalReporter.getAllAutoGauge());
    this.internalReporter.stop();
    this.internalReporter = internalReporter;
    startInternalReporter();
    LOGGER.info(ServiceMessages.METRIC_SERVICE_RELOAD_INTERNAL_REPORTER_SUCCESSFULLY);
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
          LOGGER.info(ServiceMessages.METRIC_SERVICE_RESTART_REPORTERS_SUCCESSFULLY);
          break;
        case NOTHING:
          LOGGER.debug(ServiceMessages.NOTHING_CHANGED_IN_METRIC_CONFIG);
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
      LOGGER.warn(ServiceMessages.INTERNAL_REPORTER_FAILED_TO_START);
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
