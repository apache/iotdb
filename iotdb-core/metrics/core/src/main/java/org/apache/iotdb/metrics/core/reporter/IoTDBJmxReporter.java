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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IoTDBJmxReporter implements JmxReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBJmxReporter.class);

  /** Domain name of IoTDB Metrics */
  private static final String DOMAIN = "org.apache.iotdb.metrics";

  /** The metricManager of IoTDB */
  private final AbstractMetricManager metricManager;

  /** The objectNameFactory used to create objectName for metrics */
  private final ObjectNameFactory objectNameFactory;

  /** The map that stores all registered metrics */
  private final Map<ObjectName, ObjectName> registered;

  /** The JMX MBeanServer */
  private final MBeanServer mBeanServer;

  private void registerMBean(Object mBean, ObjectName objectName) throws JMException {
    if (!mBeanServer.isRegistered(objectName)) {
      ObjectInstance objectInstance = mBeanServer.registerMBean(mBean, objectName);
      if (objectInstance != null) {
        // the websphere mbeanserver rewrites the objectname to include
        // cell, node & server info
        // make sure we capture the new objectName for unregistration
        registered.put(objectName, objectInstance.getObjectName());
      } else {
        registered.put(objectName, objectName);
      }
    }
  }

  private void unregisterMBean(ObjectName originalObjectName)
      throws InstanceNotFoundException, MBeanRegistrationException {
    ObjectName storedObjectName = registered.remove(originalObjectName);
    if (storedObjectName != null) {
      if (mBeanServer.isRegistered(storedObjectName)) {
        mBeanServer.unregisterMBean(storedObjectName);
      }
    } else {
      if (mBeanServer.isRegistered(originalObjectName)) {
        mBeanServer.unregisterMBean(originalObjectName);
      }
    }
  }

  @Override
  public void registerMetric(IMetric metric, MetricInfo metricInfo) {
    String metricName = metric.getClass().getSimpleName();
    try {
      final ObjectName objectName = createName(metricName, metricInfo);
      metric.setObjectName(objectName);
      registerMBean(metric, objectName);
    } catch (Exception e) {
      LOGGER.warn("IoTDB Metric: Unable to register " + metricName, e);
    }
  }

  @Override
  public void unregisterMetric(IMetric metric, MetricInfo metricInfo) {
    String metricName = metric.getClass().getSimpleName();
    try {
      final ObjectName objectName = createName(metricName, metricInfo);
      unregisterMBean(objectName);
    } catch (InstanceNotFoundException e) {
      LOGGER.debug("IoTDB Metric: Unable to unregister: ", e);
    } catch (MBeanRegistrationException e) {
      LOGGER.warn("IoTDB Metric: Unable to unregister: ", e);
    }
  }

  private ObjectName createName(String type, MetricInfo metricInfo) {
    String name = metricInfo.getName();
    return objectNameFactory.createName(type, DOMAIN, name);
  }

  void unregisterAll() throws InstanceNotFoundException, MBeanRegistrationException {
    for (ObjectName name : registered.keySet()) {
      unregisterMBean(name);
    }
    // clear registered
    registered.clear();
  }

  private IoTDBJmxReporter(
      AbstractMetricManager metricManager,
      MBeanServer mBeanServer,
      ObjectNameFactory objectNameFactory) {
    this.metricManager = metricManager;
    this.mBeanServer = mBeanServer;
    this.objectNameFactory = objectNameFactory;
    this.registered = new ConcurrentHashMap<>();
  }

  @Override
  public boolean start() {
    try {
      if (!registered.isEmpty()) {
        LOGGER.warn("IoTDB Metric: JmxReporter already start!");
        return false;
      }
      // register all existed metrics into JmxReporter
      metricManager.getAllMetrics().forEach((key, value) -> registerMetric(value, key));
    } catch (Exception e) {
      LOGGER.warn("IoTDB Metric: JmxReporter failed to start, because ", e);
      return false;
    }
    LOGGER.info("IoTDB Metric: JmxReporter start!");
    return true;
  }

  @Override
  public boolean stop() {
    try {
      unregisterAll();
    } catch (Exception e) {
      LOGGER.warn("IoTDB Metric: JmxReporter failed to stop, because ", e);
      return false;
    }
    LOGGER.info("IoTDB Metric: JmxReporter stop!");
    return true;
  }

  @Override
  public ReporterType getReporterType() {
    return ReporterType.JMX;
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
