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

package org.apache.iotdb.metrics.core.reporter;

import org.apache.iotdb.metrics.core.type.IoTDBAutoGauge;
import org.apache.iotdb.metrics.core.type.IoTDBCounter;
import org.apache.iotdb.metrics.core.utils.IoTDBMetricObjNameFactory;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class IoTDBJmxReporterTest {
  private TestMetricManager manager;
  private MBeanServer server;
  private IoTDBJmxReporter reporter;
  private final List<AtomicInteger> values = new ArrayList<>();

  @Before
  public void setUp() {
    manager = new TestMetricManager();
    server = MBeanServerFactory.newMBeanServer();
    reporter = new IoTDBJmxReporter(manager, server, IoTDBMetricObjNameFactory.getInstance());
    manager.setBindJmxReporter(reporter);
    assertTrue(reporter.start());
  }

  @After
  public void tearDown() {
    assertTrue(reporter.stop());
  }

  @Test
  public void testTaggedMetricsAreIndependent() throws Exception {
    AtomicInteger firstValue = new AtomicInteger(1);
    AtomicInteger secondValue = new AtomicInteger(2);
    IoTDBAutoGauge<?> first = gauge(firstValue, "first");
    IoTDBAutoGauge<?> second = gauge(secondValue, "second");
    assertNotEquals(first.objectName(), second.objectName());
    assertEquals(1, (double) server.getAttribute(first.objectName(), "Value"), 0);
    assertEquals(2, (double) server.getAttribute(second.objectName(), "Value"), 0);

    manager.remove(MetricType.AUTO_GAUGE, "client_manager", "name", "num_active", "type", "second");
    assertFalse(server.isRegistered(second.objectName()));
    assertEquals(1, (double) server.getAttribute(first.objectName(), "Value"), 0);
  }

  @Test
  public void testDelayedUnregisterPreservesReplacement() throws Exception {
    AtomicInteger firstValue = new AtomicInteger(1);
    AtomicInteger nextValue = new AtomicInteger(2);
    IoTDBAutoGauge<?> first = gauge(firstValue, "pool");
    IoTDBAutoGauge<?> next = gauge(nextValue, "pool");
    assertEquals(first.objectName(), next.objectName());
    reporter.unregisterMetric(first, info("pool"));
    assertEquals(2, (double) server.getAttribute(next.objectName(), "Value"), 0);

    assertTrue(reporter.stop());
    assertTrue(reporter.start());
    reporter.unregisterMetric(first, info("pool"));
    assertEquals(2, (double) server.getAttribute(next.objectName(), "Value"), 0);
  }

  @Test
  public void testUnregisterDoesNotRemoveUnownedMBean() throws Exception {
    AtomicInteger value = new AtomicInteger(3);
    values.add(value);
    IoTDBAutoGauge<AtomicInteger> external = new IoTDBAutoGauge<>(value, AtomicInteger::get);
    ObjectName name =
        IoTDBMetricObjNameFactory.getInstance()
            .createName(
                "IoTDBAutoGauge",
                "org.apache.iotdb.metrics",
                "client_manager",
                info("pool").getTags());
    server.registerMBean(external, name);
    reporter.unregisterMetric(external, info("pool"));
    assertEquals(3, (double) server.getAttribute(name, "Value"), 0);
    assertTrue(reporter.stop());
    assertTrue(server.isRegistered(name));
  }

  @Test
  public void testRepeatedAndMissingUnregistration() {
    AtomicInteger value = new AtomicInteger(1);
    IoTDBAutoGauge<?> gauge = gauge(value, "pool");
    manager.remove(MetricType.AUTO_GAUGE, "client_manager", "name", "num_active", "type", "pool");
    reporter.unregisterMetric(gauge, info("pool"));
    reporter.unregisterMetric(null, info("pool"));
    assertFalse(server.isRegistered(gauge.objectName()));
  }

  @Test
  public void testCounterRegistration() throws Exception {
    IoTDBCounter counter =
        (IoTDBCounter)
            manager.getOrCreateCounter("requests", MetricLevel.IMPORTANT, "name", "client");
    counter.inc(7);
    assertEquals(7L, server.getAttribute(counter.objectName(), "Count"));
  }

  @Test
  public void testDelayedRegistrationCannotResurrectRemovedMetric() {
    IoTDBAutoGauge<?> gauge = gauge(new AtomicInteger(1), "pool");
    manager.remove(MetricType.AUTO_GAUGE, "client_manager", "name", "num_active", "type", "pool");
    reporter.registerMetric(gauge, info("pool"));
    assertFalse(server.isRegistered(gauge.objectName()));
  }

  @Test
  public void testDelayedRegistrationCannotReplaceNewMetric() throws Exception {
    IoTDBAutoGauge<?> old = gauge(new AtomicInteger(1), "pool");
    IoTDBAutoGauge<?> next = gauge(new AtomicInteger(2), "pool");
    reporter.registerMetric(old, info("pool"));
    assertEquals(2, (double) server.getAttribute(next.objectName(), "Value"), 0);
  }

  @Test
  public void testRegistrationWhileStoppedIsDeferred() throws Exception {
    assertTrue(reporter.stop());
    IoTDBAutoGauge<?> gauge = gauge(new AtomicInteger(1), "pool");
    ObjectName pattern = new ObjectName("org.apache.iotdb.metrics:*");
    assertTrue(server.queryNames(pattern, null).isEmpty());
    for (int i = 0; i < 3; i++) {
      assertTrue(reporter.start());
      assertEquals(1, server.queryNames(pattern, null).size());
      assertEquals(1, (double) server.getAttribute(gauge.objectName(), "Value"), 0);
      assertTrue(reporter.stop());
      assertTrue(server.queryNames(pattern, null).isEmpty());
    }
  }

  private IoTDBAutoGauge<?> gauge(AtomicInteger value, String pool) {
    values.add(value);
    return (IoTDBAutoGauge<?>)
        manager.createAutoGauge(
            "client_manager",
            MetricLevel.IMPORTANT,
            value,
            AtomicInteger::get,
            "name",
            "num_active",
            "type",
            pool);
  }

  private MetricInfo info(String pool) {
    return new MetricInfo(
        MetricType.AUTO_GAUGE, "client_manager", "name", "num_active", "type", pool);
  }

  private static class TestMetricManager extends DoNothingMetricManager {
    @Override
    public boolean isEnableMetricInGivenLevel(MetricLevel level) {
      return true;
    }

    @Override
    public <T> AutoGauge createAutoGauge(T object, ToDoubleFunction<T> mapper) {
      return new IoTDBAutoGauge<>(object, mapper);
    }

    @Override
    public Counter createCounter() {
      return new IoTDBCounter();
    }
  }
}
