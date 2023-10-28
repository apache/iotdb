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
import org.apache.iotdb.metrics.core.uitls.IoTDBMetricObjNameFactory;
import org.apache.iotdb.metrics.reporter.JmxReporter;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.codahale.metrics.jmx.ObjectNameFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class IoTDBJmxReporter implements JmxReporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBJmxReporter.class);
  private static final String DOMAIN = "org.apache.iotdb.metrics";

  private final AbstractMetricManager metricManager;

  private final MBeanServer mBeanServer;

  private final ObjectNameFactory objectNameFactory;

  private final Map<ObjectName, ObjectName> registered;

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxGaugeMBean {
    ObjectName objectName();

    Number getValue();
  }

  public abstract static class AbstractJmxGaugeBean implements JmxGaugeMBean {
    private ObjectName objectName;

    public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxCounterMBean {
    ObjectName objectName();

    long getCount();
  }

  public abstract static class AbstractJmxCounterBean implements JmxCounterMBean {
    private ObjectName objectName;

    public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxHistogramMBean {
    ObjectName objectName();

    long getCount();

    double getMax();

    double getMean();

    int getSize();

    double get50thPercentile();

    double get99thPercentile();
  }

  public abstract static class AbstractJmxHistogramBean implements JmxHistogramMBean {
    private ObjectName objectName;

    public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxRateMBean {
    ObjectName objectName();

    long getCount();

    double getMeanRate();

    double getOneMinuteRate();
  }

  public abstract static class AbstractJmxRateBean implements JmxRateMBean {
    private ObjectName objectName;

    public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }
  }

  @SuppressWarnings("UnusedDeclaration")
  public interface JmxTimerMBean {
    ObjectName objectName();

    long getCount();

    double getSum();

    double getMax();

    double getMean();

    int getSize();

    double get50thPercentile();

    double get99thPercentile();
  }

  public abstract static class AbstractJmxTimerBean implements JmxTimerMBean {
    private ObjectName objectName;

    public void setObjectName(ObjectName objectName) {
      this.objectName = objectName;
    }

    @Override
    public ObjectName objectName() {
      return objectName;
    }
  }

  private void registerMBean(Object mBean, ObjectName objectName)
      throws InstanceAlreadyExistsException, JMException {
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

  private void unregisterMBean(ObjectName originalObjectName)
      throws InstanceNotFoundException, MBeanRegistrationException {
    ObjectName storedObjectName = registered.remove(originalObjectName);
    if (storedObjectName != null) {
      mBeanServer.unregisterMBean(storedObjectName);
    } else {
      mBeanServer.unregisterMBean(originalObjectName);
    }
  }

  @Override
  public void onMetricCreate(IMetric metric, MetricInfo metricInfo) {
    String metricName = metric.getClass().getSimpleName();
    try {
      final ObjectName objectName = createName(metricName, metricInfo);
      metric.setObjectName(objectName);
      registerMBean(metric, objectName);
    } catch (InstanceAlreadyExistsException e) {
      LOGGER.debug("Unable to register " + metricName, e);
    } catch (JMException e) {
      LOGGER.warn("Unable to register " + metricName, e);
    }
  }

  @Override
  public void onMetricRemove(IMetric metric, MetricInfo metricInfo) {
    String metricName = metric.getClass().getSimpleName();
    try {
      final ObjectName objectName = createName(metricName, metricInfo);
      unregisterMBean(objectName);
    } catch (InstanceNotFoundException e) {
      LOGGER.debug("Unable to unregister timer", e);
    } catch (MBeanRegistrationException e) {
      LOGGER.warn("Unable to unregister timer", e);
    }
  }

  private ObjectName createName(String type, MetricInfo metricInfo) {
    String name = null;
    return objectNameFactory.createName(type, DOMAIN, name);
  }

  void unregisterAll() {
    for (ObjectName name : registered.keySet()) {
      try {
        unregisterMBean(name);
      } catch (InstanceNotFoundException e) {
        LOGGER.debug("Unable to unregister metric", e);
      } catch (MBeanRegistrationException e) {
        LOGGER.warn("Unable to unregister metric", e);
      }
    }
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
      Set<MeterRegistry> meterRegistrySet =
          Metrics.globalRegistry.getRegistries().stream()
              .filter(JmxMeterRegistry.class::isInstance)
              .collect(Collectors.toSet());
      for (MeterRegistry meterRegistry : meterRegistrySet) {
        if (!meterRegistry.isClosed()) {
          ((JmxMeterRegistry) meterRegistry).stop();
          meterRegistry.close();
          Metrics.removeRegistry(meterRegistry);
        }
      }
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
            IoTDBMetricObjNameFactory.getInstance());;

    private IoTDBJmxReporterHolder() {
      // empty constructor
    }
  }

  public static IoTDBJmxReporter getInstance() {
    return IoTDBJmxReporterHolder.INSTANCE;
  }
}
