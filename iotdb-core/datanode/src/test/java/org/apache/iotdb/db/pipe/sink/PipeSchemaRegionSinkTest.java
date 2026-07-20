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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeSyncClientManager;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeSchemaRegionWritePlanEventBatch;
import org.apache.iotdb.db.pipe.sink.protocol.airgap.IoTDBSchemaRegionAirGapSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBSchemaRegionSink;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateTimeSeriesNode;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipeSchemaRegionSinkTest {

  @Test
  public void testSyncSinkFlushesBatchedEventsOnHeartbeat() throws Exception {
    final TestIoTDBSchemaRegionSyncSink sink = new TestIoTDBSchemaRegionSyncSink();
    try {
      final IoTDBDataNodeSyncClientManager clientManager =
          Mockito.mock(IoTDBDataNodeSyncClientManager.class);
      final org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient client =
          Mockito.mock(org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient.class);
      when(clientManager.getClient()).thenAnswer(invocation -> new Pair<>(client, true));
      when(client.getEndPoint()).thenReturn(new TEndPoint("127.0.0.1", 6667));
      when(client.pipeTransfer(any(TPipeTransferReq.class))).thenReturn(createSuccessResp());

      setField(sink, "clientManager", clientManager);
      enableBatching(sink);

      final PipeSchemaRegionWritePlanEvent event =
          createBatchableEvent("root.db.d1.s1", "pipeA", 1L);

      sink.transfer(event);
      Assert.assertFalse(event.isReleased());
      verify(client, never()).pipeTransfer(any(TPipeTransferReq.class));

      sink.transfer(new PipeHeartbeatEvent("heartbeat", false));

      verify(client, times(1)).pipeTransfer(any(TPipeTransferReq.class));
      Assert.assertTrue(event.isReleased());
      Assert.assertTrue(getBatch(sink).isEmpty());
    } finally {
      sink.close();
    }
  }

  @Test
  public void testSyncSinkFlushesBufferedEventsBeforeStandaloneTransfer() throws Exception {
    final TestIoTDBSchemaRegionSyncSink sink = new TestIoTDBSchemaRegionSyncSink();
    try {
      final IoTDBDataNodeSyncClientManager clientManager =
          Mockito.mock(IoTDBDataNodeSyncClientManager.class);
      final org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient client =
          Mockito.mock(org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient.class);
      when(clientManager.getClient()).thenAnswer(invocation -> new Pair<>(client, true));
      when(client.getEndPoint()).thenReturn(new TEndPoint("127.0.0.1", 6667));
      when(client.pipeTransfer(any(TPipeTransferReq.class))).thenReturn(createSuccessResp());

      setField(sink, "clientManager", clientManager);
      enableBatching(sink);

      final PipeSchemaRegionWritePlanEvent batchedEvent =
          createBatchableEvent("root.db.d1.s1", "pipeA", 1L);
      final PipeSchemaRegionWritePlanEvent standaloneEvent =
          createNonBatchableEvent("root.db.d2.s1", "pipeA", 1L);

      sink.transfer(batchedEvent);
      sink.transfer(standaloneEvent);

      verify(client, times(2)).pipeTransfer(any(TPipeTransferReq.class));
      Assert.assertTrue(batchedEvent.isReleased());
      Assert.assertTrue(standaloneEvent.isReleased());
      Assert.assertTrue(getBatch(sink).isEmpty());
    } finally {
      sink.close();
    }
  }

  @Test
  public void testSyncSinkRetriesBatchingAfterFlushingIncompatibleBatch() throws Exception {
    final TestIoTDBSchemaRegionSyncSink sink = new TestIoTDBSchemaRegionSyncSink();
    try {
      final IoTDBDataNodeSyncClientManager clientManager =
          Mockito.mock(IoTDBDataNodeSyncClientManager.class);
      final org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient client =
          Mockito.mock(org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient.class);
      when(clientManager.getClient()).thenAnswer(invocation -> new Pair<>(client, true));
      when(client.getEndPoint()).thenReturn(new TEndPoint("127.0.0.1", 6667));
      when(client.pipeTransfer(any(TPipeTransferReq.class))).thenReturn(createSuccessResp());

      setField(sink, "clientManager", clientManager);
      enableBatching(sink);

      final PipeSchemaRegionWritePlanEvent firstEvent =
          createBatchableEvent("root.db.d1.s1", "pipeA", 1L);
      final PipeSchemaRegionWritePlanEvent secondEvent =
          createBatchableEvent("root.db.d2.s1", "pipeB", 1L);

      sink.transfer(firstEvent);
      sink.transfer(secondEvent);

      verify(client, times(1)).pipeTransfer(any(TPipeTransferReq.class));
      Assert.assertTrue(firstEvent.isReleased());
      Assert.assertFalse(secondEvent.isReleased());
      Assert.assertEquals(1, getBatch(sink).size());
      Assert.assertEquals("pipeB", getBatch(sink).getPipeName());

      sink.transfer(new PipeHeartbeatEvent("heartbeat", false));

      verify(client, times(2)).pipeTransfer(any(TPipeTransferReq.class));
      Assert.assertTrue(secondEvent.isReleased());
      Assert.assertTrue(getBatch(sink).isEmpty());
    } finally {
      sink.close();
    }
  }

  @Test
  public void testAirGapSinkFlushesBatchedEventsOnHeartbeat() throws Exception {
    final TestIoTDBSchemaRegionAirGapSink sink = new TestIoTDBSchemaRegionAirGapSink();
    try {
      enableBatching(sink);

      final PipeSchemaRegionWritePlanEvent event =
          createBatchableEvent("root.db.d1.s1", "pipeA", 1L);

      sink.transfer(event);
      Assert.assertFalse(event.isReleased());
      Assert.assertEquals(0, sink.getSendCount());

      sink.transfer(new PipeHeartbeatEvent("heartbeat", false));

      Assert.assertEquals(1, sink.getSendCount());
      Assert.assertTrue(event.isReleased());
      Assert.assertTrue(getBatch(sink).isEmpty());
    } finally {
      sink.close();
    }
  }

  @Test
  public void testAirGapSinkRetriesBatchingAfterFlushingIncompatibleBatch() throws Exception {
    final TestIoTDBSchemaRegionAirGapSink sink = new TestIoTDBSchemaRegionAirGapSink();
    try {
      enableBatching(sink);

      final PipeSchemaRegionWritePlanEvent firstEvent =
          createBatchableEvent("root.db.d1.s1", "pipeA", 1L);
      final PipeSchemaRegionWritePlanEvent secondEvent =
          createBatchableEvent("root.db.d2.s1", "pipeB", 1L);

      sink.transfer(firstEvent);
      sink.transfer(secondEvent);

      Assert.assertEquals(1, sink.getSendCount());
      Assert.assertTrue(firstEvent.isReleased());
      Assert.assertFalse(secondEvent.isReleased());
      Assert.assertEquals(1, getBatch(sink).size());
      Assert.assertEquals("pipeB", getBatch(sink).getPipeName());

      sink.transfer(new PipeHeartbeatEvent("heartbeat", false));

      Assert.assertEquals(2, sink.getSendCount());
      Assert.assertTrue(secondEvent.isReleased());
      Assert.assertTrue(getBatch(sink).isEmpty());
    } finally {
      sink.close();
    }
  }

  @Test
  public void testAirGapSinkFlushesBufferedEventsBeforeStandaloneTransfer() throws Exception {
    final TestIoTDBSchemaRegionAirGapSink sink = new TestIoTDBSchemaRegionAirGapSink();
    try {
      enableBatching(sink);

      final PipeSchemaRegionWritePlanEvent batchedEvent =
          createBatchableEvent("root.db.d1.s1", "pipeA", 1L);
      final PipeSchemaRegionWritePlanEvent standaloneEvent =
          createNonBatchableEvent("root.db.d2.s1", "pipeA", 1L);

      sink.transfer(batchedEvent);
      sink.transfer(standaloneEvent);

      Assert.assertEquals(2, sink.getSendCount());
      Assert.assertTrue(batchedEvent.isReleased());
      Assert.assertTrue(standaloneEvent.isReleased());
      Assert.assertTrue(getBatch(sink).isEmpty());
    } finally {
      sink.close();
    }
  }

  private PipeParameters createParameters() {
    return new PipeParameters(
        new HashMap<String, String>() {
          {
            put(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, "100000");
            put(CONNECTOR_IOTDB_BATCH_SIZE_KEY, "1048576");
          }
        });
  }

  private void enableBatching(final Object sink) throws Exception {
    setField(sink, "isTabletBatchModeEnabled", true);
    setField(
        sink,
        "schemaRegionWritePlanEventBatch",
        new PipeSchemaRegionWritePlanEventBatch(createParameters()));
  }

  private PipeSchemaRegionWritePlanEventBatch getBatch(final Object sink) throws Exception {
    return (PipeSchemaRegionWritePlanEventBatch)
        getFieldValue(sink, "schemaRegionWritePlanEventBatch");
  }

  private PipeSchemaRegionWritePlanEvent createBatchableEvent(
      final String path, final String pipeName, final long creationTime) throws Exception {
    return new PipeSchemaRegionWritePlanEvent(
        new CreateTimeSeriesNode(
            new org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId(path),
            new org.apache.iotdb.commons.path.MeasurementPath(path),
            TSDataType.INT64,
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            null,
            null,
            null,
            null),
        pipeName,
        creationTime,
        null,
        null,
        true);
  }

  private PipeSchemaRegionWritePlanEvent createNonBatchableEvent(
      final String path, final String pipeName, final long creationTime) throws Exception {
    return new PipeSchemaRegionWritePlanEvent(
        new CreateTimeSeriesNode(
            new org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId(path + "-p"),
            new org.apache.iotdb.commons.path.MeasurementPath(path),
            TSDataType.INT64,
            TSEncoding.PLAIN,
            CompressionType.LZ4,
            java.util.Collections.singletonMap("prop", "v1"),
            null,
            null,
            null),
        pipeName,
        creationTime,
        null,
        null,
        true);
  }

  private TPipeTransferResp createSuccessResp() {
    final TPipeTransferResp resp = new TPipeTransferResp();
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()).setMessage("success"));
    return resp;
  }

  private void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    Field field = findField(target.getClass(), fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private Object getFieldValue(final Object target, final String fieldName) throws Exception {
    Field field = findField(target.getClass(), fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private Field findField(final Class<?> clazz, final String fieldName)
      throws NoSuchFieldException {
    Class<?> current = clazz;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException ignored) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static class TestIoTDBSchemaRegionSyncSink extends IoTDBSchemaRegionSink {

    @Override
    public TPipeTransferReq compressIfNeeded(final TPipeTransferReq req) {
      return req;
    }

    @Override
    public void rateLimitIfNeeded(
        final String pipeName,
        final long creationTime,
        final TEndPoint endPoint,
        final long bytesLength) {
      // Do nothing in tests.
    }
  }

  private static class TestIoTDBSchemaRegionAirGapSink extends IoTDBSchemaRegionAirGapSink {

    private int sendCount = 0;

    private TestIoTDBSchemaRegionAirGapSink() {
      sockets.add(new AirGapSocket("127.0.0.1", 6667));
      isSocketAlive.add(true);
    }

    @Override
    protected int nextSocketIndex() {
      return 0;
    }

    @Override
    protected boolean send(
        final String pipeName,
        final long creationTime,
        final AirGapSocket socket,
        final byte[] bytes) {
      sendCount++;
      return true;
    }

    private int getSendCount() {
      return sendCount;
    }
  }
}
