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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipeMetricsLifecycleTest {

  private static final String PIPE = "pipe";
  private static final long CREATION_TIME = 1L;
  private static final String PIPE_ID = PIPE + "_" + CREATION_TIME;
  private static final int REGION_ID = 1;

  private final MetricService service = MetricService.getInstance();
  private final MetricConfig config = MetricConfigDescriptor.getInstance().getMetricConfig();
  private final List<IMetricSet> boundMetricSets = new ArrayList<>();
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
    boundMetricSets.forEach(service::removeMetricSet);
    service.stopService();
    config.setMetricLevel(originalLevel);
    config.setMetricReporterList(originalReporters);
  }

  @Test
  public void testSinglePipeMetricsKeepOperatorsAfterRestart() {
    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    bind(metrics);
    metrics.register(mockDataRegionSource());
    try {
      metrics.increaseTsFileEventCount(PIPE, CREATION_TIME);
      assertEquals(1, count(Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT));

      service.restartService();
      assertEquals(1, count(Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT));
      assertEquals(1L, (long) metrics.getRemainingEventAndTime(PIPE, CREATION_TIME).getLeft());
      metrics.updateTsFileTransferTimer(PIPE, CREATION_TIME, 10);
      assertEquals(1, ((Timer) get(Metric.PIPE_TSFILE_EVENT_TRANSFER_TIME)).getCount());
    } finally {
      metrics.deregister(PIPE_ID);
    }
    assertEquals(0, count(Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT));
    assertTrue(metrics.remainingEventAndTimeOperatorMap.isEmpty());
  }

  @Test
  public void testTsFileToTabletsMetricsRebindAfterRestart() {
    final PipeTsFileToTabletsMetrics metrics = PipeTsFileToTabletsMetrics.getInstance();
    bind(metrics);
    metrics.register(mockDataRegionSource());
    try {
      service.restartService();
      metrics.recordTsFileToTabletTime(PIPE_ID, 10);
      assertEquals(1, ((Timer) get(Metric.PIPE_TSFILE_TO_TABLETS_TIME)).getCount());
    } finally {
      metrics.deregister(PIPE_ID);
    }
    assertEquals(0, count(Metric.PIPE_TSFILE_TO_TABLETS_TIME));
  }

  private void bind(final IMetricSet metricSet) {
    service.addMetricSet(metricSet);
    boundMetricSets.add(metricSet);
  }

  private static IoTDBDataRegionSource mockDataRegionSource() {
    final IoTDBDataRegionSource source = mock(IoTDBDataRegionSource.class);
    when(source.getPipeName()).thenReturn(PIPE);
    when(source.getCreationTime()).thenReturn(CREATION_TIME);
    when(source.getRegionId()).thenReturn(REGION_ID);
    return source;
  }

  private long count(final Metric metric) {
    return service.getAllMetrics().keySet().stream()
        .filter(info -> metric.toString().equals(info.getName()))
        .count();
  }

  private IMetric get(final Metric metric) {
    for (final Map.Entry<MetricInfo, IMetric> entry : service.getAllMetrics().entrySet()) {
      if (metric.toString().equals(entry.getKey().getName())) {
        return entry.getValue();
      }
    }
    throw new AssertionError(metric + " is not registered");
  }
}
