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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletObjectBatchResp;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

// Covers async Object-tablet transfer regressions for client pinning and retry-queue ownership.
// These cases lock in the fixes for: multi-RPC Object handlers returning the async client too
// early, and failed Object batches releasing events before they enter the retry queue.
public class PipeTransferTabletObjectHandlerTest {

  // Verifies sequence-reset response round-trip and rejects malformed reset bodies.
  @Test
  public void testObjectBatchSequenceResetResponse() {
    Assert.assertEquals(
        2,
        PipeTransferTabletObjectBatchResp.fromTPipeTransferResp(
                PipeTransferTabletObjectBatchResp.toSequenceResetTPipeTransferResp(2))
            .getExpectedSequenceId());

    try {
      PipeTransferTabletObjectBatchResp.fromTPipeTransferResp(
          new TPipeTransferResp().setBody(ByteBuffer.allocate(0)));
      Assert.fail();
    } catch (final PipeException e) {
      // expected
    }
  }

  // Ensures a multi-request Object handler pins the async client with setShouldReturnSelf(false)
  // before the first RPC, so the client is not returned to the pool
  // between sequenced callbacks.
  @Test
  public void testMultiRequestHandlerPinsClientUntilAllRequestsFinish() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalSliceThreshold = commonConfig.getPipeSinkRequestSliceThresholdBytes();
    commonConfig.setPipeSinkRequestSliceThresholdBytes(Integer.MAX_VALUE);

    final PipeInsertNodeTabletInsertionEvent event =
        Mockito.mock(PipeInsertNodeTabletInsertionEvent.class);
    Mockito.when(event.convertToTablets())
        .thenReturn(Arrays.asList(createObjectTablet("s1"), createObjectTablet("s2")));
    Mockito.when(event.isAligned(Mockito.anyInt())).thenReturn(false);

    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    Mockito.when(sink.compressIfNeeded(Mockito.any(TPipeTransferReq.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));

    final AsyncPipeDataTransferServiceClient client =
        Mockito.mock(AsyncPipeDataTransferServiceClient.class);
    Mockito.when(client.getEndPoint()).thenReturn(new TEndPoint("127.0.0.1", 6667));

    final PipeTransferTabletObjectHandler handler =
        new PipeTransferTabletObjectHandler(event, sink);
    try {
      handler.transfer(client);

      final InOrder inOrder = Mockito.inOrder(client);
      inOrder.verify(client).setShouldReturnSelf(false);
      inOrder
          .verify(client)
          .pipeTransfer(Mockito.any(TPipeTransferReq.class), Mockito.same(handler));
    } finally {
      handler.clearEventsReferenceCount();
      commonConfig.setPipeSinkRequestSliceThresholdBytes(originalSliceThreshold);
    }
  }

  // Ensures a failed Object batch keeps an event reference when enqueueing retry, so the event is
  // not filtered out as already released and silently dropped.
  @Test
  public void testObjectBatchFailureRetainsEventsForRetry() {
    final PipeRawTabletInsertionEvent event =
        new PipeRawTabletInsertionEvent(
            true, "db", "db", "root.db", createTablet("s"), false, "pipe", 1L, null, null, false);
    final IoTDBDataRegionAsyncSink retrySink = new IoTDBDataRegionAsyncSink();
    try {
      Assert.assertTrue(event.increaseReferenceCount("object batch"));

      // Keep the batch reference while the event is handed over to the retry queue.
      retrySink.addFailureEventToRetryQueue(event, null);

      Assert.assertEquals(1, retrySink.getRetryEventQueueSize());
      Assert.assertFalse(event.isReleased());
    } finally {
      retrySink.clearRetryEventsReferenceCount();
      if (!event.isReleased()) {
        event.clearReferenceCount("test cleanup");
      }
    }
  }

  private static Tablet createTablet(final String measurement) {
    final Tablet tablet =
        new Tablet(
            "table",
            Collections.singletonList(new MeasurementSchema(measurement, TSDataType.INT32)),
            1);
    tablet.setColumnCategories(Collections.singletonList(ColumnCategory.FIELD));
    tablet.addTimestamp(0, 1L);
    tablet.addValue(measurement, 0, 1);
    tablet.setRowSize(1);
    return tablet;
  }

  private static Tablet createObjectTablet(final String measurement) {
    final Tablet tablet =
        new Tablet(
            "table",
            Collections.singletonList(new MeasurementSchema(measurement, TSDataType.OBJECT)),
            1);
    tablet.setColumnCategories(Collections.singletonList(ColumnCategory.FIELD));
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, true, 0L, new byte[] {1});
    tablet.setRowSize(1);
    return tablet;
  }
}
