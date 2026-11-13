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
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
          realtimeSource.getSourceId(),
          mock(CommitterKey.class));
      metrics.markDataRegionCompletionInvalid(
          pipeName,
          creationTime,
          1,
          realtimeSource.getPipeTaskMeta(),
          realtimeSource.getSourceId());
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

  @Test
  public void testMetricCreationUsesExactPipeInstanceAcrossModels() throws Exception {
    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    final Field metricServiceField =
        PipeDataNodeSinglePipeMetrics.class.getDeclaredField("metricService");
    metricServiceField.setAccessible(true);
    reset(metrics, metricServiceField);

    final String pipeName = "shared_model_metric_" + System.nanoTime();
    final long treeCreationTime = System.nanoTime();
    final long tableCreationTime = treeCreationTime + 1;
    final PipeMeta treePipe =
        createPipeMeta(pipeName, treeCreationTime, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final PipeMeta tablePipe =
        createPipeMeta(pipeName, tableCreationTime, SystemConstant.SQL_DIALECT_TABLE_VALUE);
    final PipeMetaKeeper pipeMetaKeeper = getPipeMetaKeeper();

    try {
      pipeMetaKeeper.addPipeMeta(treePipe);
      pipeMetaKeeper.addPipeMeta(tablePipe);

      assertEquals(treeCreationTime, PipeDataNodeAgent.task().getPipeCreationTime(pipeName));
      assertTrue(PipeDataNodeAgent.task().isPipeExisted(pipeName, tableCreationTime));

      metrics.increaseTsFileEventCount(pipeName, tableCreationTime);

      assertTrue(
          metrics.remainingEventAndTimeOperatorMap.containsKey(pipeName + "_" + tableCreationTime));
    } finally {
      metrics.deregister(pipeName + "_" + tableCreationTime);
      pipeMetaKeeper.removePipeMeta(treePipe.getStaticMeta());
      pipeMetaKeeper.removePipeMeta(tablePipe.getStaticMeta());
      reset(metrics, metricServiceField);
    }
  }

  @Test
  public void testMetricCreationSerializesWithDeregister() throws Exception {
    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    final Field metricServiceField =
        PipeDataNodeSinglePipeMetrics.class.getDeclaredField("metricService");
    metricServiceField.setAccessible(true);
    reset(metrics, metricServiceField);

    final String pipeName = "metric_drop_race_" + System.nanoTime();
    final long creationTime = System.nanoTime();
    final String pipeID = pipeName + "_" + creationTime;
    final PipeMeta pipeMeta =
        createPipeMeta(pipeName, creationTime, SystemConstant.SQL_DIALECT_TREE_VALUE);
    final PipeMetaKeeper pipeMetaKeeper = getPipeMetaKeeper();
    final Field pipeMetaMapField = PipeMetaKeeper.class.getDeclaredField("pipeNameToPipeMetaMap");
    pipeMetaMapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    final Map<String, PipeMeta> originalPipeMetaMap =
        (Map<String, PipeMeta>) pipeMetaMapField.get(pipeMetaKeeper);
    final BlockingPipeMetaMap blockingPipeMetaMap =
        new BlockingPipeMetaMap(originalPipeMetaMap, pipeName, creationTime);
    pipeMetaMapField.set(pipeMetaKeeper, blockingPipeMetaMap);
    pipeMetaKeeper.addPipeMeta(pipeMeta);

    final CountDownLatch metadataRemoved = new CountDownLatch(1);
    final AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
    final Thread updateThread =
        new Thread(
            () -> {
              try {
                metrics.increaseTsFileEventCount(pipeName, creationTime);
              } catch (final Throwable e) {
                backgroundFailure.compareAndSet(null, e);
              }
            });
    final Thread deregisterThread =
        new Thread(
            () -> {
              try {
                pipeMetaKeeper.removePipeMeta(pipeMeta.getStaticMeta());
                metadataRemoved.countDown();
                metrics.deregister(pipeID);
              } catch (final Throwable e) {
                backgroundFailure.compareAndSet(null, e);
              }
            });

    try {
      try {
        updateThread.start();
        assertTrue(blockingPipeMetaMap.lookupPaused.await(10, TimeUnit.SECONDS));
        deregisterThread.start();
        assertTrue(metadataRemoved.await(10, TimeUnit.SECONDS));
        assertEquals(Thread.State.BLOCKED, waitForBlockedOrTerminated(deregisterThread));
      } finally {
        blockingPipeMetaMap.resumeLookup.countDown();
        updateThread.join(TimeUnit.SECONDS.toMillis(10));
        deregisterThread.join(TimeUnit.SECONDS.toMillis(10));
      }

      assertFalse(updateThread.isAlive());
      assertFalse(deregisterThread.isAlive());
      assertNull(backgroundFailure.get());
      assertFalse(metrics.remainingEventAndTimeOperatorMap.containsKey(pipeID));
    } finally {
      pipeMetaKeeper.removePipeMeta(pipeMeta.getStaticMeta());
      pipeMetaMapField.set(pipeMetaKeeper, originalPipeMetaMap);
      metrics.deregister(pipeID);
      reset(metrics, metricServiceField);
    }
  }

  private static PipeMeta createPipeMeta(
      final String pipeName, final long creationTime, final String sqlDialect) {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(SystemConstant.SQL_DIALECT_KEY, sqlDialect);
    return new PipeMeta(
        new PipeStaticMeta(
            pipeName, creationTime, sourceAttributes, new HashMap<>(), new HashMap<>()),
        new PipeRuntimeMeta(new ConcurrentHashMap<>()));
  }

  private static PipeMetaKeeper getPipeMetaKeeper() throws Exception {
    final Field pipeMetaKeeperField = PipeTaskAgent.class.getDeclaredField("pipeMetaKeeper");
    pipeMetaKeeperField.setAccessible(true);
    return (PipeMetaKeeper) pipeMetaKeeperField.get(PipeDataNodeAgent.task());
  }

  private static Thread.State waitForBlockedOrTerminated(final Thread thread)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    Thread.State state = thread.getState();
    while (state != Thread.State.BLOCKED
        && state != Thread.State.TERMINATED
        && System.nanoTime() < deadline) {
      Thread.sleep(1);
      state = thread.getState();
    }
    return state;
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

  private static class BlockingPipeMetaMap extends ConcurrentHashMap<String, PipeMeta> {

    private static final long serialVersionUID = 1L;

    private final String pipeName;
    private final long creationTime;
    private final AtomicBoolean pauseNextLookup = new AtomicBoolean(true);
    private final CountDownLatch lookupPaused = new CountDownLatch(1);
    private final CountDownLatch resumeLookup = new CountDownLatch(1);

    private BlockingPipeMetaMap(
        final Map<String, PipeMeta> originalMap, final String pipeName, final long creationTime) {
      putAll(originalMap);
      this.pipeName = pipeName;
      this.creationTime = creationTime;
    }

    @Override
    public PipeMeta get(final Object key) {
      final PipeMeta pipeMeta = super.get(key);
      pauseIfMatching(pipeMeta);
      return pipeMeta;
    }

    @Override
    public Collection<PipeMeta> values() {
      final Collection<PipeMeta> values = super.values();
      final ArrayList<PipeMeta> snapshot = new ArrayList<>(values);
      if (snapshot.stream().anyMatch(this::matches)) {
        pauseIfMatching(snapshot.stream().filter(this::matches).findFirst().orElse(null));
        return snapshot;
      }
      return values;
    }

    private boolean matches(final PipeMeta pipeMeta) {
      return pipeMeta != null
          && pipeName.equals(pipeMeta.getStaticMeta().getPipeName())
          && creationTime == pipeMeta.getStaticMeta().getCreationTime();
    }

    private void pauseIfMatching(final PipeMeta pipeMeta) {
      if (matches(pipeMeta) && pauseNextLookup.compareAndSet(true, false)) {
        lookupPaused.countDown();
        try {
          if (!resumeLookup.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timed out waiting to resume pipe metadata lookup");
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
    }
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
