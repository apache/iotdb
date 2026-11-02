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
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.confignode.manager.pipe.agent.task.PipeConfigNodeSubtask;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeConfigNodeRemainingTimeMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.overview.PipeTemporaryMetaInCoordinatorMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.sink.PipeConfigRegionSinkMetrics;
import org.apache.iotdb.confignode.manager.pipe.metric.source.PipeConfigRegionSourceMetrics;
import org.apache.iotdb.confignode.manager.pipe.source.IoTDBConfigRegionSource;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.AutoGauge;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
    final IoTDBConfigRegionSource source = mockConfigRegionSource();
    when(source.getTaskID()).thenReturn(PIPE + "_0_" + CREATION_TIME);
    when(source.getUnTransferredEventCount()).thenReturn(7L);
    metrics.register(source);
    try {
      assertEquals(7L, metrics.getRemainingEventCount(PIPE, CREATION_TIME));
      assertEquals(0L, metrics.getRemainingEventCount(PIPE, CREATION_TIME + 1));
      assertEquals(0L, metrics.getRemainingEventCount("other_pipe", CREATION_TIME));
      service.restartService();
      assertEquals(1, count(Metric.UNTRANSFERRED_CONFIG_COUNT));
      when(source.getUnTransferredEventCount()).thenReturn(3L);
      assertEquals(3L, metrics.getRemainingEventCount(PIPE, CREATION_TIME));
    } finally {
      metrics.deregister(source.getTaskID());
    }
    assertEquals(0, count(Metric.UNTRANSFERRED_CONFIG_COUNT));
    assertEquals(0L, metrics.getRemainingEventCount(PIPE, CREATION_TIME));
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

  @Test
  public void testTemporaryMetaMetricsWithUnderscores() {
    final PipeTemporaryMetaInCoordinatorMetrics metrics =
        PipeTemporaryMetaInCoordinatorMetrics.getInstance();
    bind(metrics);
    final String[] pipeNames = {
      "pipe_with_underscores", "pipe_with_underscores", "pipe_with_another_suffix_", "_pipe"
    };
    final long[] creationTimes = {CREATION_TIME, CREATION_TIME + 1, CREATION_TIME, CREATION_TIME};
    final List<String> registeredPipeIds = new ArrayList<>();
    final List<PipeTemporaryMetaInCoordinator> temporaryMetas = new ArrayList<>();
    try {
      for (int i = 0; i < pipeNames.length; i++) {
        final PipeStaticMeta staticMeta = Mockito.mock(PipeStaticMeta.class);
        when(staticMeta.getPipeName()).thenReturn(pipeNames[i]);
        when(staticMeta.getCreationTime()).thenReturn(creationTimes[i]);
        final PipeMeta pipeMeta = Mockito.mock(PipeMeta.class);
        when(pipeMeta.getStaticMeta()).thenReturn(staticMeta);
        final PipeTemporaryMetaInCoordinator temporaryMeta =
            Mockito.mock(PipeTemporaryMetaInCoordinator.class);
        when(temporaryMeta.getGlobalRemainingEvents()).thenReturn(7L + i);
        when(temporaryMeta.getGlobalRemainingTime()).thenReturn(10.0 + i);
        when(pipeMeta.getTemporaryMeta()).thenReturn(temporaryMeta);
        registeredPipeIds.add(pipeNames[i] + "_" + creationTimes[i]);
        temporaryMetas.add(temporaryMeta);
        metrics.register(pipeMeta);
      }
      assertEquals(pipeNames.length, count(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT));
      assertEquals(pipeNames.length, count(Metric.PIPE_GLOBAL_REMAINING_TIME));
      for (int i = 0; i < pipeNames.length; i++) {
        assertTemporaryMetaValues(pipeNames[i], creationTimes[i], 7L + i, 10.0 + i);
      }
      service.restartService();
      when(temporaryMetas.get(0).getGlobalRemainingEvents()).thenReturn(3L);
      when(temporaryMetas.get(0).getGlobalRemainingTime()).thenReturn(4.0);
      assertTemporaryMetaValues(pipeNames[0], creationTimes[0], 3L, 4.0);
      metrics.deregister(registeredPipeIds.remove(0));
      assertEquals(pipeNames.length - 1, count(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT));
      assertEquals(pipeNames.length - 1, count(Metric.PIPE_GLOBAL_REMAINING_TIME));
      assertNull(get(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT, pipeNames[0], creationTimes[0]));
      assertNull(get(Metric.PIPE_GLOBAL_REMAINING_TIME, pipeNames[0], creationTimes[0]));
      for (int i = 1; i < pipeNames.length; i++) {
        assertTemporaryMetaValues(pipeNames[i], creationTimes[i], 7L + i, 10.0 + i);
      }
      service.restartService();
      assertEquals(pipeNames.length - 1, count(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT));
      assertEquals(pipeNames.length - 1, count(Metric.PIPE_GLOBAL_REMAINING_TIME));
      assertNull(get(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT, pipeNames[0], creationTimes[0]));
      assertNull(get(Metric.PIPE_GLOBAL_REMAINING_TIME, pipeNames[0], creationTimes[0]));
      for (int i = 1; i < pipeNames.length; i++) {
        assertTemporaryMetaValues(pipeNames[i], creationTimes[i], 7L + i, 10.0 + i);
      }
    } finally {
      registeredPipeIds.forEach(metrics::deregister);
    }
    assertEquals(0, count(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT));
    assertEquals(0, count(Metric.PIPE_GLOBAL_REMAINING_TIME));
  }

  private void assertTemporaryMetaValues(
      final String pipeName,
      final long creationTime,
      final long remainingEvents,
      final double remainingTime) {
    final IMetric eventGauge =
        get(Metric.PIPE_GLOBAL_REMAINING_EVENT_COUNT, pipeName, creationTime);
    final IMetric timeGauge = get(Metric.PIPE_GLOBAL_REMAINING_TIME, pipeName, creationTime);
    assertNotNull(eventGauge);
    assertNotNull(timeGauge);
    assertEquals(remainingEvents, ((AutoGauge) eventGauge).getValue(), 0.0);
    assertEquals(remainingTime, ((AutoGauge) timeGauge).getValue(), 0.0);
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

  private IMetric get(final Metric metric, final String pipeName, final long creationTime) {
    return service.getAllMetrics().entrySet().stream()
        .filter(entry -> metric.toString().equals(entry.getKey().getName()))
        .filter(entry -> pipeName.equals(entry.getKey().getTags().get(Tag.NAME.toString())))
        .filter(
            entry ->
                String.valueOf(creationTime)
                    .equals(entry.getKey().getTags().get(Tag.CREATION_TIME.toString())))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }
}
