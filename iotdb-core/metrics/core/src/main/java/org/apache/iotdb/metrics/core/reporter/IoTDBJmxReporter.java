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

package org.apache.iotdb.metrics.core.reporter;

import org.apache.iotdb.metrics.AbstractMetricManager;
import org.apache.iotdb.metrics.core.IoTDBMetricManager;
import org.apache.iotdb.metrics.core.i18n.MetricsCoreMessages;
import org.apache.iotdb.metrics.core.utils.IoTDBMetricObjNameFactory;
import org.apache.iotdb.metrics.core.utils.ObjectNameFactory;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.ReporterType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class IoTDBJmxReporter implements JmxReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBJmxReporter.class);

  /** Domain name of IoTDB Metrics */
  private static final String DOMAIN = "org.apache.iotdb.metrics";

  /** The metricManager of IoTDB */
  private final AbstractMetricManager metricManager;

  /** The objectNameFactory used to create objectName for metrics */
  private final ObjectNameFactory objectNameFactory;

  /** Registrations owned by this reporter, guarded by the map's monitor. */
  private final Map<ObjectName, Registration> registered;

  private boolean started;

  /** The JMX MBeanServer */
  private final MBeanServer mBeanServer;

  private void registerMBean(IMetric metric, ObjectName objectName) throws JMException {
    Registration previous = registered.get(objectName);
    if (previous != null) {
      if (previous.metric == metric && mBeanServer.isRegistered(previous.actualName)) {
        return;
      }
      unregisterMBean(previous);
      registered.remove(objectName);
    }
    if (!mBeanServer.isRegistered(objectName)) {
      ObjectInstance objectInstance = mBeanServer.registerMBean(metric, objectName);
      // Some MBean servers rewrite ObjectNames. Keep the actual name together with its owner.
      registered.put(
          objectName,
          new Registration(
              metric, objectInstance == null ? objectName : objectInstance.getObjectName()));
    }
  }

  private void unregisterMBean(Registration registration) throws MBeanRegistrationException {
    try {
      mBeanServer.unregisterMBean(registration.actualName);
    } catch (InstanceNotFoundException ignored) {
      // An externally removed MBean is already unregistered.
    }
  }

  @Override
  public void registerMetric(IMetric metric, MetricInfo metricInfo) {
    String metricName = metric.getClass().getSimpleName();
    try {
      final ObjectName objectName = createName(metricName, metricInfo);
      synchronized (registered) {
        // Ignore callbacks from a stopped reporter or a superseded registry entry.
        if (!started || metricManager.getAllMetrics().get(metricInfo) != metric) {
          return;
        }
        metric.setObjectName(objectName);
        registerMBean(metric, objectName);
      }
    } catch (Exception e) {
      LOGGER.warn(MetricsCoreMessages.JMX_REGISTER_FAILED + metricName, e);
    }
  }

  @Override
  public void unregisterMetric(IMetric metric, MetricInfo metricInfo) {
    if (metric == null) {
      return;
    }
    String metricName = metric.getClass().getSimpleName();
    try {
      final ObjectName objectName = createName(metricName, metricInfo);
      synchronized (registered) {
        Registration registration = registered.get(objectName);
        // A delayed callback for an old metric must not delete its replacement.
        if (registration != null && registration.metric == metric) {
          unregisterMBean(registration);
          registered.remove(objectName);
        }
      }
    } catch (MBeanRegistrationException e) {
      LOGGER.warn(MetricsCoreMessages.JMX_UNREGISTER_FAILED, e);
    }
  }

  private ObjectName createName(String type, MetricInfo metricInfo) {
    String name = metricInfo.getName();
    return objectNameFactory.createName(type, DOMAIN, name, metricInfo.getTags());
  }

  void unregisterAll() throws MBeanRegistrationException {
    synchronized (registered) {
      Iterator<Registration> iterator = registered.values().iterator();
      while (iterator.hasNext()) {
        unregisterMBean(iterator.next());
        iterator.remove();
      }
    }
  }

  IoTDBJmxReporter(
      AbstractMetricManager metricManager,
      MBeanServer mBeanServer,
      ObjectNameFactory objectNameFactory) {
    this.metricManager = metricManager;
    this.mBeanServer = mBeanServer;
    this.objectNameFactory = objectNameFactory;
    this.registered = new HashMap<>();
  }

  @Override
  public boolean start() {
    try {
      boolean alreadyStarted;
      synchronized (registered) {
        alreadyStarted = started;
        started = true;
      }
      if (alreadyStarted) {
        LOGGER.warn(MetricsCoreMessages.JMX_REPORTER_ALREADY_START);
        return false;
      }
      // register all existed metrics into JmxReporter
      metricManager.getAllMetrics().forEach((key, value) -> registerMetric(value, key));
    } catch (Exception e) {
      stop();
      LOGGER.warn(MetricsCoreMessages.JMX_REPORTER_START_FAILED, e);
      return false;
    }
    LOGGER.info(MetricsCoreMessages.JMX_REPORTER_START);
    return true;
  }

  @Override
  public boolean stop() {
    try {
      synchronized (registered) {
        started = false;
        unregisterAll();
      }
    } catch (Exception e) {
      LOGGER.warn(MetricsCoreMessages.JMX_REPORTER_STOP_FAILED, e);
      return false;
    }
    LOGGER.info(MetricsCoreMessages.JMX_REPORTER_STOP);
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.JMX;
  }

  private static class Registration {
    private final IMetric metric;
    private final ObjectName actualName;

    private Registration(IMetric metric, ObjectName actualName) {
      this.metric = metric;
      this.actualName = actualName;
    }
  }

  private static class IoTDBJmxReporterHolder {
    private static final IoTDBJmxReporter INSTANCE =
        new IoTDBJmxReporter(
            IoTDBMetricManager.getInstance(),
            ManagementFactory.getPlatformMBeanServer(),
            IoTDBMetricObjNameFactory.getInstance());

    private IoTDBJmxReporterHolder() {
      // empty constructor
    }
  }

  public static IoTDBJmxReporter getInstance() {
    return IoTDBJmxReporterHolder.INSTANCE;
  }
}
