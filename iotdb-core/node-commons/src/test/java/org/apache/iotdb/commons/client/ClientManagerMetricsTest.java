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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.DoNothingMetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.core.IoTDBMetricManager;
import org.apache.iotdb.metrics.core.reporter.IoTDBJmxReporter;
import org.apache.iotdb.metrics.core.type.IoTDBAutoGauge;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClientManagerMetricsTest {

  private final ClientManagerMetrics metrics = ClientManagerMetrics.getInstance();
  private final List<ClientManager<String, Object>> managers = new ArrayList<>();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private TestMetricService service;
  private MetricLevel originalLevel;
  private String originalReporters;

  @Before
  public void setUp() {
    originalLevel = config.getMetricLevel();
    originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("");
    service = new TestMetricService();
    service.startService();
    service.addMetricSet(metrics);
  }

  @After
  public void tearDown() {
    managers.forEach(ClientManager::close);
    service.removeMetricSet(metrics);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testInFlightGaugesAndRepeatedRebind() throws Exception {
    ClientManager<String, Object> manager = createManager("pool");
    Object client = manager.borrowClient("node");
    manager.returnClient("node", client);
    manager.borrowClient("node");
    manager.getPool().addObject("node");
    Map<MetricInfo, IMetric> inFlight = new HashMap<>(service.getAllMetrics());
    assertEquals(8, inFlight.size());

    for (int i = 0; i < 3; i++) {
      service.stopService();
      assertTrue(service.getAllMetrics().isEmpty());
      assertMetrics(inFlight, "pool", manager.getPool());
      service.startService();
      assertEquals(8, service.getAllMetrics().size());
      assertMetrics(service.getAllMetrics(), "pool", manager.getPool());
    }

    manager.borrowClient("node");
    assertMetrics(inFlight, "pool", manager.getPool());
  }

  @Test
  public void testRegistrationWhileUnbound() {
    ClientManager<String, Object> first = createManager("first");
    service.removeMetricSet(metrics);
    ClientManager<String, Object> second = createManager("second");
    assertTrue(service.getAllMetrics().isEmpty());

    service.addMetricSet(metrics);
    assertEquals(16, service.getAllMetrics().size());
    assertMetrics(service.getAllMetrics(), "first", first.getPool());
    assertMetrics(service.getAllMetrics(), "second", second.getPool());
  }

  @Test
  public void testMetricServiceRestartAndLevelChanges() throws Exception {
    ClientManager<String, Object> manager = createManager("pool");
    manager.borrowClient("node");
    service.removeMetricSet(metrics);
    MetricService metricService = MetricService.getInstance();
    metricService.startService();
    try {
      metricService.addMetricSet(metrics);
      for (MetricLevel level :
          new MetricLevel[] {MetricLevel.ALL, MetricLevel.CORE, MetricLevel.IMPORTANT}) {
        config.setMetricLevel(level);
        metricService.restartService();
        if (level == MetricLevel.CORE) {
          assertTrue(metricService.getAllMetrics().isEmpty());
        } else {
          assertEquals(8, metricService.getAllMetrics().size());
          assertMetrics(metricService.getAllMetrics(), "pool", manager.getPool());
        }
      }
    } finally {
      metricService.removeMetricSet(metrics);
      metricService.stopService();
    }
  }

  @Test
  public void testClosedPoolsAreNotRetainedAcrossRebinds() {
    for (int i = 0; i < 10; i++) {
      ClientManager<String, Object> manager = createManager("pool" + i);
      assertEquals(8, service.getAllMetrics().size());
      manager.close();
      assertTrue(service.getAllMetrics().isEmpty());
      service.stopService();
      service.startService();
      assertTrue(service.getAllMetrics().isEmpty());
    }
  }

  @Test
  public void testCloseWhileUnboundRemovesOnlyClosedPool() {
    ClientManager<String, Object> first = createManager("first");
    ClientManager<String, Object> second = createManager("second");
    service.removeMetricSet(metrics);
    first.close();
    service.addMetricSet(metrics);

    assertEquals(8, service.getAllMetrics().size());
    assertMetrics(service.getAllMetrics(), "second", second.getPool());
  }

  @Test
  public void testClosingSupersededPoolDoesNotRemoveReplacement() throws Exception {
    ClientManager<String, Object> first = createManager("pool");
    first.borrowClient("node");
    Map<MetricInfo, IMetric> inFlight = new HashMap<>(service.getAllMetrics());
    service.removeMetricSet(metrics);
    ClientManager<String, Object> replacement = createManager("pool");
    replacement.borrowClient("node");
    replacement.borrowClient("node");
    service.addMetricSet(metrics);

    assertMetrics(inFlight, "pool", first.getPool());
    first.close();
    assertEquals(8, service.getAllMetrics().size());
    assertMetrics(service.getAllMetrics(), "pool", replacement.getPool());
    assertMetrics(inFlight, "pool", first.getPool());
  }

  @Test
  public void testClosingUnregisteredDuplicateDoesNotRemoveOriginal() throws Exception {
    ClientManager<String, Object> first = createManager("pool");
    first.borrowClient("node");
    ClientManager<String, Object> duplicate = createManager("pool");
    duplicate.close();

    assertEquals(8, service.getAllMetrics().size());
    assertMetrics(service.getAllMetrics(), "pool", first.getPool());
  }

  @Test
  public void testClosedPoolCanBeReplacedBeforeUnregistration() throws Exception {
    IoTDBJmxReporter reporter = IoTDBJmxReporter.getInstance();
    service.getMetricManager().setBindJmxReporter(reporter);
    try {
      ClientManager<String, Object> first = createManager("pool");
      first.borrowClient("node");
      ObjectName objectName =
          ((IoTDBAutoGauge<?>)
                  service.getAutoGauge(
                      Metric.CLIENT_MANAGER.toString(),
                      MetricLevel.IMPORTANT,
                      Tag.NAME.toString(),
                      "client_manager_num_active",
                      Tag.TYPE.toString(),
                      "pool"))
              .objectName();
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      assertEquals(1, (double) mBeanServer.getAttribute(objectName, "Value"), 0);
      // The pool is closed, but its owner's close has not reached metric unregistration yet.
      first.getPool().close();
      ClientManager<String, Object> replacement = createManager("pool");
      replacement.borrowClient("node");
      replacement.borrowClient("node");
      first.close();

      assertEquals(8, service.getAllMetrics().size());
      assertMetrics(service.getAllMetrics(), "pool", replacement.getPool());
      assertEquals(2, (double) mBeanServer.getAttribute(objectName, "Value"), 0);
    } finally {
      service.getMetricManager().setBindJmxReporter(null);
      reporter.stop();
    }
  }

  @Test
  public void testRegistrationAndSamplingDuringUnbind() throws Exception {
    ClientManager<String, Object> first = createManager("first");
    first.borrowClient("node");
    createManager("second");
    AutoGauge inFlight =
        service.getAutoGauge(
            Metric.CLIENT_MANAGER.toString(),
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            "client_manager_num_active",
            Tag.TYPE.toString(),
            "first");
    CountDownLatch removing = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    AtomicBoolean pauseOnce = new AtomicBoolean(true);
    service.beforeRemove =
        () -> {
          if (pauseOnce.compareAndSet(true, false)) {
            removing.countDown();
            try {
              assertTrue(resume.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
          }
        };
    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      Future<?> stop = executor.submit(service::stopService);
      assertTrue(removing.await(5, TimeUnit.SECONDS));
      AtomicReference<Thread> registrationThread = new AtomicReference<>();
      CountDownLatch registering = new CountDownLatch(1);
      Future<ClientManager<String, Object>> registration =
          executor.submit(
              () -> {
                registrationThread.set(Thread.currentThread());
                registering.countDown();
                return createManager("third");
              });
      assertTrue(registering.await(5, TimeUnit.SECONDS));
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () ->
                  registration.isDone()
                      || registrationThread.get().getState() == Thread.State.BLOCKED);
      assertFalse(registration.isDone());
      // Sampling must not acquire the lifecycle lock held by the paused unbind.
      assertEquals(1, executor.submit(inFlight::getValue).get(5, TimeUnit.SECONDS), 0);
      resume.countDown();
      stop.get(5, TimeUnit.SECONDS);
      registration.get(5, TimeUnit.SECONDS);
      assertTrue(service.getAllMetrics().isEmpty());

      service.startService();
      assertEquals(24, service.getAllMetrics().size());
      assertMetrics(service.getAllMetrics(), "first", first.getPool());
    } finally {
      resume.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private ClientManager<String, Object> createManager(String name) {
    ClientManager<String, Object> manager =
        new ClientManager<>(
            owner -> {
              GenericKeyedObjectPoolConfig<Object> poolConfig =
                  new GenericKeyedObjectPoolConfig<>();
              poolConfig.setJmxEnabled(false);
              GenericKeyedObjectPool<String, Object> pool =
                  new GenericKeyedObjectPool<>(
                      new BaseKeyedPooledObjectFactory<String, Object>() {
                        @Override
                        public Object create(String key) {
                          return new Object();
                        }

                        @Override
                        public PooledObject<Object> wrap(Object value) {
                          return new DefaultPooledObject<>(value);
                        }
                      },
                      poolConfig);
              metrics.registerClientManager(name, pool);
              return pool;
            });
    managers.add(manager);
    return manager;
  }

  private void assertMetrics(
      Map<MetricInfo, IMetric> actual, String poolName, GenericKeyedObjectPool<?, ?> pool) {
    Map<String, Double> expected = new HashMap<>();
    expected.put("client_manager_num_active", (double) pool.getNumActive());
    expected.put("client_manager_num_idle", (double) pool.getNumIdle());
    expected.put("client_manager_borrowed_count", (double) pool.getBorrowedCount());
    expected.put("client_manager_created_count", (double) pool.getCreatedCount());
    expected.put("client_manager_destroyed_count", (double) pool.getDestroyedCount());
    expected.put("client_manager_mean_active_time", (double) pool.getMeanActiveTimeMillis());
    expected.put(
        "client_manager_mean_borrow_wait_time", (double) pool.getMeanBorrowWaitTimeMillis());
    expected.put("client_manager_mean_idle_time", (double) pool.getMeanIdleTimeMillis());
    expected.forEach(
        (name, value) -> {
          MetricInfo key =
              new MetricInfo(
                  MetricType.AUTO_GAUGE,
                  Metric.CLIENT_MANAGER.toString(),
                  Tag.NAME.toString(),
                  name,
                  Tag.TYPE.toString(),
                  poolName);
          assertTrue(actual.get(key) instanceof AutoGauge);
          assertEquals(value, ((AutoGauge) actual.get(key)).getValue(), 0);
        });
  }

  private static class TestMetricService extends DoNothingMetricService {
    private Runnable beforeRemove = () -> {};

    @Override
    protected void loadManager() {
      metricManager = IoTDBMetricManager.getInstance();
    }

    @Override
    public void remove(MetricType type, String metric, String... tags) {
      beforeRemove.run();
      super.remove(type, metric, tags);
    }
  }
}
