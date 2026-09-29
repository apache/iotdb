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

package org.apache.iotdb.db.pipe.metric.schema;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.source.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.type.AutoGauge;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PipeSchemaRegionListenerMetricsTest {

  private static final int SCHEMA_REGION_ID = 7;

  private final PipeSchemaRegionListenerMetrics metrics =
      PipeSchemaRegionListenerMetrics.getInstance();
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
    service.addMetricSet(metrics);
  }

  @After
  public void tearDown() {
    metrics.deregister(SCHEMA_REGION_ID);
    service.removeMetricSet(metrics);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testQueueSizeSurvivesMetricServiceRestart() {
    final SchemaRegionListeningQueue queue = Mockito.mock(SchemaRegionListeningQueue.class);
    Mockito.when(queue.getSize()).thenReturn(3L);
    metrics.register(queue, SCHEMA_REGION_ID);
    assertQueueSize(3);

    for (final MetricLevel level :
        new MetricLevel[] {MetricLevel.ALL, MetricLevel.CORE, MetricLevel.IMPORTANT}) {
      config.setMetricLevel(level);
      service.restartService();
      if (level == MetricLevel.CORE) {
        assertNull(getQueueSizeGauge());
      } else {
        assertQueueSize(3);
      }
    }

    metrics.deregister(SCHEMA_REGION_ID);
    assertNull(getQueueSizeGauge());
    service.restartService();
    assertNull(getQueueSizeGauge());
  }

  private AutoGauge getQueueSizeGauge() {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (Metric.PIPE_SCHEMA_LINKED_QUEUE_SIZE.toString().equals(entry.getKey().getName())
          && String.valueOf(SCHEMA_REGION_ID)
              .equals(entry.getKey().getTags().get(Tag.REGION.toString()))) {
        return (AutoGauge) entry.getValue();
      }
    }
    return null;
  }

  private void assertQueueSize(final double expected) {
    final AutoGauge gauge = getQueueSizeGauge();
    assertNotNull(gauge);
    assertEquals(expected, gauge.getValue(), 0);
  }
}
