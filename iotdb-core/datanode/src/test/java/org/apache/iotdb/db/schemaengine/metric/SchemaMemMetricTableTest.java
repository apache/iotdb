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

package org.apache.iotdb.db.schemaengine.metric;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The per-table device number gauges must be exported next to the other schema engine and schema
 * region metrics, and bound again when the metric service restarts.
 */
public class SchemaMemMetricTableTest {

  private static final String TABLE = "t1";

  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
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
  }

  @After
  public void tearDown() {
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testSchemaEngineTableMetrics() {
    final Map<String, Long> table2DevicesNum = new ConcurrentHashMap<>();
    final MemSchemaEngineStatistics statistics = mock(MemSchemaEngineStatistics.class);
    when(statistics.getTable2DevicesNumMap()).thenReturn(table2DevicesNum);
    when(statistics.getTableDeviceNumber(TABLE)).thenReturn(3L);
    final SchemaEngineMemMetric metric = new SchemaEngineMemMetric(statistics);
    checkTableMetrics(metric, metric::bindTableMetrics, table2DevicesNum);
  }

  @Test
  public void testSchemaRegionTableMetrics() {
    final Map<String, Long> table2DevicesNum = new ConcurrentHashMap<>();
    final MemSchemaRegionStatistics statistics = mock(MemSchemaRegionStatistics.class);
    when(statistics.getSchemaRegionId()).thenReturn(1);
    when(statistics.getTable2DevicesNumMap()).thenReturn(table2DevicesNum);
    when(statistics.getTableDevicesNumber(TABLE)).thenReturn(3L);
    final SchemaRegionMemMetric metric = new SchemaRegionMemMetric(statistics, "db");
    checkTableMetrics(metric, metric::bindTableMetrics, table2DevicesNum);
  }

  private void checkTableMetrics(
      final IMetricSet metric,
      final Consumer<String> bindTableMetrics,
      final Map<String, Long> table2DevicesNum) {
    service.addMetricSet(metric);
    try {
      // A table binds its metrics when its first device is added
      table2DevicesNum.put(TABLE, 3L);
      bindTableMetrics.accept(TABLE);
      assertEquals(3, getTableGauge().getValue(), 0);

      service.restartService();
      assertEquals(3, getTableGauge().getValue(), 0);
    } finally {
      service.removeMetricSet(metric);
    }
    assertNull(findTableGauge());
  }

  private AutoGauge getTableGauge() {
    final IMetric metric = findTableGauge();
    if (metric == null) {
      throw new AssertionError("The gauge of table " + TABLE + " is not registered");
    }
    return (AutoGauge) metric;
  }

  private IMetric findTableGauge() {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (TABLE.equals(entry.getKey().getTags().get(SchemaEngineMemMetric.TABLE))) {
        return entry.getValue();
      }
    }
    return null;
  }
}
