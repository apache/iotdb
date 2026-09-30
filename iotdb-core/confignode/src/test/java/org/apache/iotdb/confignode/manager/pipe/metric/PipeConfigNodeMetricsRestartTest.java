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

package org.apache.iotdb.confignode.manager.pipe.metric;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTemporaryMetaInCoordinator;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeConfigNodeSubtask;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeConfigNodeRemainingTimeMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeTemporaryMetaInCoordinatorMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.sink.PipeConfigRegionSinkMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.source.PipeConfigRegionSourceMetrics;
import org.apache.iotdb.confignode.manager.pipe.source.IoTDBConfigRegionSource;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/** Pipe metrics must survive a metric service restart, which drops and rebinds all metrics. */
public class PipeConfigNodeMetricsRestartTest {

  private static final String PIPE = "pipe";
  private static final long CREATION_TIME = 1L;
  private static final String PIPE_ID = PIPE + "_" + CREATION_TIME;
  private static final String TASK_ID = "task";

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
  public void testConfigRegionSourceMetrics() {
    final PipeConfigRegionSourceMetrics metrics = PipeConfigRegionSourceMetrics.getInstance();
    bind(metrics);
    metrics.register(mockConfigRegionSource());
    try {
      service.restartService();
      assertEquals(1, count(Metric.UNTRANSFERRED_CONFIG_COUNT));
    } finally {
      metrics.deregister(TASK_ID);
    }
    assertEquals(0, count(Metric.UNTRANSFERRED_CONFIG_COUNT));
  }

  @Test
  public void testConfigRegionSinkMetrics() {
    final PipeConfigRegionSinkMetrics metrics = PipeConfigRegionSinkMetrics.getInstance();
    bind(metrics);
    final PipeConfigNodeSubtask subtask = Mockito.mock(PipeConfigNodeSubtask.class);
    when(subtask.getTaskID()).thenReturn(TASK_ID);
    when(subtask.getPipeName()).thenReturn(PIPE);
    when(subtask.getCreationTime()).thenReturn(CREATION_TIME);
    metrics.register(subtask);
    try {
      service.restartService();
      metrics.markConfigEvent(TASK_ID);
      assertEquals(1, ((Rate) get(Metric.PIPE_CONNECTOR_CONFIG_TRANSFER)).getCount());
    } finally {
      metrics.deregister(TASK_ID);
    }
    assertEquals(0, count(Metric.PIPE_CONNECTOR_CONFIG_TRANSFER));
  }

  @Test
  public void testRemainingTimeMetrics() {
    final PipeConfigNodeRemainingTimeMetrics metrics =
        PipeConfigNodeRemainingTimeMetrics.getInstance();
    bind(metrics);
    metrics.register(mockConfigRegionSource());
    try {
      service.restartService();
      assertEquals(1, count(Metric.PIPE_CONFIGNODE_REMAINING_TIME));
    } finally {
      metrics.deregister(PIPE_ID);
    }
    assertEquals(0, count(Metric.PIPE_CONFIGNODE_REMAINING_TIME));
  }

  @Test
  public void testTemporaryMetaInCoordinatorMetrics() {
    final PipeTemporaryMetaInCoordinatorMetrics metrics =
        PipeTemporaryMetaInCoordinatorMetrics.getInstance();
    bind(metrics);
    final PipeStaticMeta staticMeta = Mockito.mock(PipeStaticMeta.class);
    when(staticMeta.getPipeName()).thenReturn(PIPE);
    when(staticMeta.getCreationTime()).thenReturn(CREATION_TIME);
    final PipeMeta pipeMeta = Mockito.mock(PipeMeta.class);
    when(pipeMeta.getStaticMeta()).thenReturn(staticMeta);
    final PipeTemporaryMetaInCoordinator temporaryMeta =
        Mockito.mock(PipeTemporaryMetaInCoordinator.class);
    when(pipeMeta.getTemporaryMeta()).thenReturn(temporaryMeta);
    metrics.register(pipeMeta);
    try {
      service.restartService();
      assertEquals(1, count(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT));
    } finally {
      metrics.deregister(PIPE_ID);
    }
    assertEquals(0, count(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT));
  }

  private void bind(final IMetricSet metricSet) {
    service.addMetricSet(metricSet);
    boundMetricSets.add(metricSet);
  }

  private static IoTDBConfigRegionSource mockConfigRegionSource() {
    final IoTDBConfigRegionSource source = Mockito.mock(IoTDBConfigRegionSource.class);
    when(source.getTaskID()).thenReturn(TASK_ID);
    when(source.getPipeName()).thenReturn(PIPE);
    when(source.getCreationTime()).thenReturn(CREATION_TIME);
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
