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

package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSink;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtask;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.metric.processor.PipeProcessorMetrics;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionSinkMetrics;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionSourceMetrics;
import org.apache.iotdb.db.pipe.metric.sink.PipeDataRegionSinkMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeAssignerMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionSourceMetrics;
import org.apache.iotdb.db.pipe.sink.protocol.airgap.IoTDBDataRegionAirGapSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.db.pipe.source.dataregion.IoTDBDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricInfo;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Pipe metrics must survive a metric service restart, which drops and rebinds all metrics. */
public class PipeMetricsRestartTest {

  private static final String PIPE = "pipe";
  private static final long CREATION_TIME = 1L;
  private static final String PIPE_ID = PIPE + "_" + CREATION_TIME;
  private static final String TASK_ID = "task";
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
  public void testSinglePipeMetricsKeepOperators() {
    final PipeDataNodeSinglePipeMetrics metrics = PipeDataNodeSinglePipeMetrics.getInstance();
    bind(metrics);
    final IoTDBDataRegionSource source = mockDataRegionSource();
    metrics.register(source);
    try {
      metrics.increaseTsFileEventCount(PIPE, CREATION_TIME);
      assertEquals(1, count(Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT));

      service.restartService();
      assertEquals(1, count(Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT));
      // The operator holding the state of the pipe is kept
      assertEquals(1L, (long) metrics.getRemainingEventAndTime(PIPE, CREATION_TIME).getLeft());
      // and records into the timer of the current binding
      metrics.updateTsFileTransferTimer(PIPE, CREATION_TIME, 10);
      assertEquals(1, ((Timer) get(Metric.PIPE_TSFILE_EVENT_TRANSFER_TIME)).getCount());
    } finally {
      metrics.deregister(PIPE_ID);
    }
    assertEquals(0, count(Metric.PIPE_DATANODE_REMAINING_EVENT_COUNT));
    assertTrue(metrics.remainingEventAndTimeOperatorMap.isEmpty());
  }

  @Test
  public void testDataRegionSourceMetrics() {
    final PipeDataRegionSourceMetrics metrics = PipeDataRegionSourceMetrics.getInstance();
    bind(metrics);
    metrics.register(mockDataRegionSource());
    try {
      service.restartService();
      assertEquals(1, count(Metric.UNPROCESSED_TABLET_COUNT));
      metrics.markTabletEvent(TASK_ID);
      assertEquals(1, ((Rate) get(Metric.PIPE_EXTRACTOR_TABLET_SUPPLY)).getCount());
    } finally {
      metrics.deregister(TASK_ID);
    }
    assertEquals(0, count(Metric.UNPROCESSED_TABLET_COUNT));
  }

  @Test
  public void testDataRegionSinkMetrics() {
    final PipeDataRegionSinkMetrics metrics = PipeDataRegionSinkMetrics.getInstance();
    bind(metrics);
    final PipeSinkSubtask subtask = mockSinkSubtask();
    metrics.register(subtask);
    try {
      service.restartService();
      assertEquals(1, count(Metric.UNTRANSFERRED_TABLET_COUNT));
      assertSame(get(Metric.PIPE_COMPRESSION_TIME), metrics.getCompressionTimer(TASK_ID));
      // The histograms pushed into the subtask are replaced as well
      verify(subtask, times(2)).setTabletBatchSizeHistogram(any());
    } finally {
      metrics.deregister(TASK_ID);
    }
    assertEquals(0, count(Metric.UNTRANSFERRED_TABLET_COUNT));
  }

  @Test
  public void testSinksUseCompressionTimerOfCurrentBinding() throws Exception {
    final PipeDataRegionSinkMetrics metrics = PipeDataRegionSinkMetrics.getInstance();
    bind(metrics);
    metrics.register(mockSinkSubtask());
    try (final IoTDBDataRegionSyncSink syncSink = new IoTDBDataRegionSyncSink();
        final IoTDBDataRegionAsyncSink asyncSink = new IoTDBDataRegionAsyncSink();
        final IoTDBDataRegionAirGapSink airGapSink = new IoTDBDataRegionAirGapSink()) {
      final Method airGapCompress =
          IoTDBDataRegionAirGapSink.class.getDeclaredMethod("compressIfNeeded", byte[].class);
      airGapCompress.setAccessible(true);
      for (final IoTDBSink sink : new IoTDBSink[] {syncSink, asyncSink, airGapSink}) {
        setField(sink, "sinkTaskId", TASK_ID);
      }

      for (int i = 0; i < 2; i++) {
        final Timer timer = metrics.getCompressionTimer(TASK_ID);
        syncSink.compressIfNeeded(newReq());
        asyncSink.compressIfNeeded(newReq());
        airGapCompress.invoke(airGapSink, (Object) new byte[1]);
        for (final IoTDBSink sink : new IoTDBSink[] {syncSink, asyncSink, airGapSink}) {
          assertSame(timer, getField(sink, "compressionTimer"));
        }
        service.restartService();
        assertNotSame(timer, metrics.getCompressionTimer(TASK_ID));
      }
    } finally {
      metrics.deregister(TASK_ID);
    }
  }

  @Test
  public void testSchemaRegionSinkMetrics() {
    final PipeSchemaRegionSinkMetrics metrics = PipeSchemaRegionSinkMetrics.getInstance();
    bind(metrics);
    final PipeSinkSubtask subtask = mockSinkSubtask();
    metrics.register(subtask);
    try {
      service.restartService();
      assertEquals(1, count(Metric.PIPE_CONNECTOR_SCHEMA_TRANSFER));
      verify(subtask, times(2)).setSchemaBatchSizeHistogram(any());
    } finally {
      metrics.deregister(TASK_ID);
    }
    assertEquals(0, count(Metric.PIPE_CONNECTOR_SCHEMA_TRANSFER));
  }

  @Test
  public void testSchemaRegionSourceMetrics() {
    final PipeSchemaRegionSourceMetrics metrics = PipeSchemaRegionSourceMetrics.getInstance();
    bind(metrics);
    final IoTDBSchemaRegionSource source = Mockito.mock(IoTDBSchemaRegionSource.class);
    when(source.getTaskID()).thenReturn(TASK_ID);
    when(source.getPipeName()).thenReturn(PIPE);
    when(source.getRegionId()).thenReturn(REGION_ID);
    when(source.getCreationTime()).thenReturn(CREATION_TIME);
    metrics.register(source);
    try {
      service.restartService();
      assertEquals(1, count(Metric.UNTRANSFERRED_SCHEMA_COUNT));
    } finally {
      metrics.deregister(TASK_ID);
    }
    assertEquals(0, count(Metric.UNTRANSFERRED_SCHEMA_COUNT));
  }

  @Test
  public void testProcessorMetrics() {
    final PipeProcessorMetrics metrics = PipeProcessorMetrics.getInstance();
    bind(metrics);
    final PipeProcessorSubtask subtask = Mockito.mock(PipeProcessorSubtask.class);
    when(subtask.getTaskID()).thenReturn(TASK_ID);
    when(subtask.getPipeName()).thenReturn(PIPE);
    when(subtask.getRegionId()).thenReturn(REGION_ID);
    when(subtask.getCreationTime()).thenReturn(CREATION_TIME);
    metrics.register(subtask);
    try {
      service.restartService();
      metrics.markTabletEvent(TASK_ID);
      assertEquals(1, ((Rate) get(Metric.PIPE_PROCESSOR_TABLET_PROCESS)).getCount());
    } finally {
      metrics.deregister(TASK_ID);
    }
    assertEquals(0, count(Metric.PIPE_PROCESSOR_TABLET_PROCESS));
  }

  /**
   * A subtask may deregister while the metric service restart is unbinding the processor metrics.
   * The restart must neither fail on it nor drop the metrics of the other subtasks.
   */
  @Test
  public void testProcessorMetricsWithConcurrentDeregistration() throws Exception {
    final PipeProcessorMetrics metrics = PipeProcessorMetrics.getInstance();
    bind(metrics);
    final AtomicBoolean armed = new AtomicBoolean(false);
    final AtomicReference<PipeProcessorSubtask> paused = new AtomicReference<>();
    final CountDownLatch pausedLatch = new CountDownLatch(1);
    final CountDownLatch resumeLatch = new CountDownLatch(1);
    final PipeProcessorSubtask first =
        mockBlockingProcessorSubtask("first", armed, paused, pausedLatch, resumeLatch);
    final PipeProcessorSubtask second =
        mockBlockingProcessorSubtask("second", armed, paused, pausedLatch, resumeLatch);
    metrics.register(first);
    metrics.register(second);

    // Pause the restart while it unbinds the first subtask, and deregister the other one meanwhile
    armed.set(true);
    final Thread restart = new Thread(service::restartService);
    restart.start();
    Thread deregistration = null;
    try {
      assertTrue(pausedLatch.await(30, TimeUnit.SECONDS));
      final PipeProcessorSubtask other = paused.get() == first ? second : first;
      deregistration = new Thread(() -> metrics.deregister(other.getTaskID()));
      deregistration.start();
      // Either the deregistration completes at once, or it waits for the unbinding to complete
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (deregistration.getState() != Thread.State.TERMINATED
          && deregistration.getState() != Thread.State.BLOCKED
          && deregistration.getState() != Thread.State.WAITING
          && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    } finally {
      resumeLatch.countDown();
      restart.join(TimeUnit.SECONDS.toMillis(30));
      if (deregistration != null) {
        deregistration.join(TimeUnit.SECONDS.toMillis(30));
      }
    }

    final String remainingPipe = paused.get().getPipeName();
    try {
      assertEquals(
          1,
          service.getAllMetrics().keySet().stream()
              .filter(
                  info ->
                      Metric.PIPE_PROCESSOR_TABLET_PROCESS.toString().equals(info.getName())
                          && remainingPipe.equals(info.getTags().get("name")))
              .count());
      assertEquals(1, count(Metric.PIPE_PROCESSOR_TABLET_PROCESS));
    } finally {
      metrics.deregister(paused.get().getTaskID());
    }
  }

  private static PipeProcessorSubtask mockBlockingProcessorSubtask(
      final String name,
      final AtomicBoolean armed,
      final AtomicReference<PipeProcessorSubtask> paused,
      final CountDownLatch pausedLatch,
      final CountDownLatch resumeLatch) {
    final PipeProcessorSubtask subtask = Mockito.mock(PipeProcessorSubtask.class);
    when(subtask.getTaskID()).thenReturn(name);
    when(subtask.getRegionId()).thenReturn(REGION_ID);
    when(subtask.getCreationTime()).thenReturn(CREATION_TIME);
    when(subtask.getPipeName())
        .thenAnswer(
            invocation -> {
              // Only the first call after arming pauses, which is during the unbinding
              if (armed.get() && paused.compareAndSet(null, subtask)) {
                pausedLatch.countDown();
                resumeLatch.await(30, TimeUnit.SECONDS);
              }
              return name;
            });
    return subtask;
  }

  @Test
  public void testAssignerMetrics() {
    final PipeAssignerMetrics metrics = PipeAssignerMetrics.getInstance();
    bind(metrics);
    final PipeDataRegionAssigner assigner = Mockito.mock(PipeDataRegionAssigner.class);
    when(assigner.getDataRegionId()).thenReturn(REGION_ID);
    metrics.register(assigner);
    try {
      service.restartService();
      assertEquals(1, count(Metric.UNASSIGNED_TABLET_COUNT));
    } finally {
      metrics.deregister(REGION_ID);
    }
    assertEquals(0, count(Metric.UNASSIGNED_TABLET_COUNT));
  }

  @Test
  public void testTsFileToTabletsMetrics() {
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
    final IoTDBDataRegionSource source = Mockito.mock(IoTDBDataRegionSource.class);
    when(source.getTaskID()).thenReturn(TASK_ID);
    when(source.getPipeName()).thenReturn(PIPE);
    when(source.getRegionId()).thenReturn(REGION_ID);
    when(source.getCreationTime()).thenReturn(CREATION_TIME);
    return source;
  }

  private static PipeSinkSubtask mockSinkSubtask() {
    final PipeSinkSubtask subtask = Mockito.mock(PipeSinkSubtask.class);
    when(subtask.getTaskID()).thenReturn(TASK_ID);
    when(subtask.getPipeName()).thenReturn(PIPE);
    when(subtask.getAttributeSortedString()).thenReturn("sink");
    when(subtask.getCreationTime()).thenReturn(CREATION_TIME);
    return subtask;
  }

  private static TPipeTransferReq newReq() {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.body = ByteBuffer.wrap(new byte[1]);
    return req;
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

  private static void setField(final IoTDBSink sink, final String name, final Object value)
      throws ReflectiveOperationException {
    final Field field = IoTDBSink.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(sink, value);
  }

  private static Object getField(final IoTDBSink sink, final String name)
      throws ReflectiveOperationException {
    final Field field = IoTDBSink.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(sink);
  }
}
