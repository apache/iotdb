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

package org.apache.iotdb.db.pipe.metric.overview;

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Test;
import org.mockito.invocation.Invocation;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeDataNodeSinglePipeMetricsTest {

  @Test
  public void testCompletionGaugeLifecycleAndStaleCallbacksDoNotRecreateState() throws Exception {
    final String pipeName = "completion_metric_" + System.nanoTime();
    final long creationTime = 1L;
    final String pipeId = pipeName + "_" + creationTime;
    final TestSource realtimeSource =
        new TestSource(
            pipeName, creationTime, 1, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0));
    final IoTDBDataRegionSource outerSource = mock(IoTDBDataRegionSource.class);
    final PipeDataRegionAssigner assigner = mock(PipeDataRegionAssigner.class);
    final AbstractMetricService metricService = mock(AbstractMetricService.class);

    when(outerSource.getPipeName()).thenReturn(pipeName);
    when(outerSource.getCreationTime()).thenReturn(creationTime);
    when(outerSource.getRealtimeSourceForCompletion()).thenReturn(realtimeSource);

    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    final Field metricServiceField =
        PipeDataNodeSinglePipeMetrics.class.getDeclaredField("metricService");
    metricServiceField.setAccessible(true);
    reset(metrics, metricServiceField);

    try {
      metrics.register(outerSource);
      metrics.register(realtimeSource, assigner);
      metrics.bindTo(metricService);

      assertTrue(hasCompletionGaugeCreation(metricService, pipeName, creationTime));

      metrics.deregister(pipeId);
      assertFalse(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeId));
      assertFalse(metrics.completionOperatorMap.containsKey(pipeId));
      verify(metricService)
          .remove(
              MetricType.AUTO_GAUGE,
              Metric.PIPE_DATANODE_COMPLETION_READY.toString(),
              Tag.NAME.toString(),
              pipeName,
              Tag.CREATION_TIME.toString(),
              String.valueOf(creationTime));

      metrics.register(realtimeSource, assigner);
      metrics.markDataRegionCompleted(
          pipeName,
          creationTime,
          1,
          realtimeSource.getPipeTaskMeta(),
          1,
          1,
          realtimeSource.getCompletionSourceId(),
          mock(CommitterKey.class));
      metrics.markDataRegionInvalid(
          pipeName,
          creationTime,
          1,
          realtimeSource.getPipeTaskMeta(),
          realtimeSource.getCompletionSourceId());
      metrics.deregister(realtimeSource, assigner);
      metrics.decreaseHeartbeatEventCount(pipeName, creationTime);
      assertFalse(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeId));
      assertFalse(metrics.completionOperatorMap.containsKey(pipeId));
    } finally {
      reset(metrics, metricServiceField);
    }
  }

  @Test
  public void testDeregisterClearsOperatorsWhenMetricsAreUnbound() throws Exception {
    final String pipeName = "completion_unbound_" + System.nanoTime();
    final long creationTime = 2L;
    final String pipeId = pipeName + "_" + creationTime;
    final TestSource realtimeSource =
        new TestSource(
            pipeName, creationTime, 2, new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 0));
    final IoTDBDataRegionSource outerSource = mock(IoTDBDataRegionSource.class);
    when(outerSource.getPipeName()).thenReturn(pipeName);
    when(outerSource.getCreationTime()).thenReturn(creationTime);
    when(outerSource.getRealtimeSourceForCompletion()).thenReturn(realtimeSource);
    when(outerSource.getHistoricalTsFileInsertionEventCount()).thenReturn(1);

    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    final Field metricServiceField =
        PipeDataNodeSinglePipeMetrics.class.getDeclaredField("metricService");
    metricServiceField.setAccessible(true);
    reset(metrics, metricServiceField);

    try {
      metrics.register(outerSource);
      assertTrue(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeId));
      assertTrue(metrics.completionOperatorMap.containsKey(pipeId));
      assertEquals(
          1, metrics.remainingEventAndTimeOperatorMap.get(pipeId).getRemainingNonHeartbeatEvents());

      metrics.deregister(outerSource);
      assertEquals(
          0, metrics.remainingEventAndTimeOperatorMap.get(pipeId).getRemainingNonHeartbeatEvents());

      metrics.deregister(pipeId);
      assertFalse(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeId));
      assertFalse(metrics.completionOperatorMap.containsKey(pipeId));
    } finally {
      reset(metrics, metricServiceField);
    }
  }

  private static boolean hasCompletionGaugeCreation(
      final AbstractMetricService metricService, final String pipeName, final long creationTime) {
    for (final Invocation invocation : mockingDetails(metricService).getInvocations()) {
      final Object[] arguments = invocation.getRawArguments();
      if ("createAutoGauge".equals(invocation.getMethod().getName())
          && arguments.length == 5
          && Metric.PIPE_DATANODE_COMPLETION_READY.toString().equals(arguments[0])
          && MetricLevel.IMPORTANT.equals(arguments[1])
          && arguments[2] instanceof PipeDataNodeCompletionOperator
          && Arrays.equals(
              new String[] {
                Tag.NAME.toString(),
                pipeName,
                Tag.CREATION_TIME.toString(),
                String.valueOf(creationTime)
              },
              (String[]) arguments[4])) {
        return true;
      }
    }
    return false;
  }

  private static void reset(
      final PipeDataNodeSinglePipeMetrics metrics, final Field metricServiceField)
      throws IllegalAccessException {
    metrics.remainingEventAndTimeOperatorMap.clear();
    metrics.completionOperatorMap.clear();
    metricServiceField.set(metrics, null);
  }

  private static class TestSource extends PipeRealtimeDataRegionSource {

    private TestSource(
        final String pipeName,
        final long creationTime,
        final int dataRegionId,
        final PipeTaskMeta pipeTaskMeta) {
      this.pipeName = pipeName;
      this.creationTime = creationTime;
      this.dataRegionId = dataRegionId;
      this.pipeTaskMeta = pipeTaskMeta;
    }

    @Override
    protected void doExtract(final PipeRealtimeEvent event) {
      // Do nothing.
    }

    @Override
    public Event supply() {
      return null;
    }

    @Override
    public boolean isNeedListenToTsFile() {
      return false;
    }

    @Override
    public boolean isNeedListenToInsertNode() {
      return false;
    }
  }
}
