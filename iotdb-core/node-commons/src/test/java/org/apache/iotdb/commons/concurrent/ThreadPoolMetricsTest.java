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

package org.apache.iotdb.commons.concurrent;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.SystemMetric;
import org.apache.iotdb.metrics.utils.SystemTag;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ThreadPoolMetricsTest {

  private static final String POOL_PREFIX = "ThreadPoolMetricsTest-";

  private final ThreadPoolMetrics metrics = ThreadPoolMetrics.getInstance();
  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final List<ExecutorService> pools = new ArrayList<>();
  private MetricLevel originalLevel;
  private String originalReporters;

  @Before
  public void setUp() {
    originalLevel = config.getMetricLevel();
    originalReporters =
        config.getMetricReporterList().stream().map(Enum::name).collect(Collectors.joining(","));
    config.setMetricLevel(MetricLevel.IMPORTANT);
    config.setMetricReporterList("");
    service.startService();
    service.addMetricSet(metrics);
  }

  @After
  public void tearDown() {
    pools.forEach(ExecutorService::shutdownNow);
    service.removeMetricSet(metrics);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testMetricsSurviveMetricServiceRestart() {
    createPool("restart", 2);
    assertPoolMetrics("restart", 2);

    for (MetricLevel level :
        new MetricLevel[] {
          MetricLevel.ALL, MetricLevel.CORE, MetricLevel.IMPORTANT, MetricLevel.IMPORTANT
        }) {
      config.setMetricLevel(level);
      service.restartService();
      if (level == MetricLevel.CORE) {
        assertTrue(getPoolMetrics("restart").isEmpty());
      } else {
        assertPoolMetrics("restart", 2);
      }
    }
  }

  @Test
  public void testShutdownPoolIsNotRestoredByRestart() {
    ExecutorService pool = createPool("shutdown", 1);
    assertPoolMetrics("shutdown", 1);

    pool.shutdown();
    assertTrue(getPoolMetrics("shutdown").isEmpty());
    service.restartService();
    assertTrue(getPoolMetrics("shutdown").isEmpty());
  }

  @Test
  public void testRegistrationWhileUnbound() {
    service.removeMetricSet(metrics);
    createPool("unbound", 3);
    assertTrue(getPoolMetrics("unbound").isEmpty());

    service.addMetricSet(metrics);
    assertPoolMetrics("unbound", 3);
  }

  @Test
  public void testShuttingDownSupersededPoolKeepsReplacementMetrics() {
    ExecutorService first = createPool("replaced", 1);
    ExecutorService replacement = createPool("replaced", 4);

    first.shutdown();
    assertPoolMetrics("replaced", 4);
    service.restartService();
    assertPoolMetrics("replaced", 4);

    replacement.shutdown();
    assertTrue(getPoolMetrics("replaced").isEmpty());
  }

  private ExecutorService createPool(String name, int size) {
    ExecutorService pool = IoTDBThreadPoolFactory.newFixedThreadPool(size, POOL_PREFIX + name);
    pools.add(pool);
    return pool;
  }

  private Map<String, Double> getPoolMetrics(String name) {
    String poolName =
        String.format(
            "%s:%s=%s",
            IoTDBConstant.IOTDB_THREADPOOL_JMX_NAME, IoTDBConstant.JMX_TYPE, POOL_PREFIX + name);
    Map<String, Double> poolMetrics = new HashMap<>();
    for (Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (poolName.equals(entry.getKey().getTags().get(SystemTag.POOL_NAME.toString()))) {
        poolMetrics.put(entry.getKey().getName(), ((AutoGauge) entry.getValue()).getValue());
      }
    }
    return poolMetrics;
  }

  private void assertPoolMetrics(String name, int coreSize) {
    Map<String, Double> poolMetrics = getPoolMetrics(name);
    assertEquals(5, poolMetrics.size());
    assertEquals(coreSize, poolMetrics.get(SystemMetric.THREAD_POOL_CORE_SIZE.toString()), 0);
  }
}
