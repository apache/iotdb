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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PipeTransferTrackableHandlerTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private int originalRequestSliceThresholdBytes;
  private long originalRetryMaxDurationMs;
  private long originalRetryProbeIntervalMs;
  private long originalSinkSubtaskSleepIntervalInitMs;
  private long originalSinkSubtaskSleepIntervalMaxMs;

  @Before
  public void setUp() {
    originalRequestSliceThresholdBytes = commonConfig.getPipeSinkRequestSliceThresholdBytes();
    originalRetryMaxDurationMs = commonConfig.getPipeAsyncSinkRetryMaxDurationMs();
    originalRetryProbeIntervalMs = commonConfig.getPipeAsyncSinkRetryProbeIntervalMs();
    originalSinkSubtaskSleepIntervalInitMs = commonConfig.getPipeSinkSubtaskSleepIntervalInitMs();
    originalSinkSubtaskSleepIntervalMaxMs = commonConfig.getPipeSinkSubtaskSleepIntervalMaxMs();
    commonConfig.setPipeSinkRequestSliceThresholdBytes(4);
  }

  @After
  public void tearDown() {
    commonConfig.setPipeSinkRequestSliceThresholdBytes(originalRequestSliceThresholdBytes);
    commonConfig.setPipeAsyncSinkRetryMaxDurationMs(originalRetryMaxDurationMs);
    commonConfig.setPipeAsyncSinkRetryProbeIntervalMs(originalRetryProbeIntervalMs);
    commonConfig.setPipeSinkSubtaskSleepIntervalInitMs(originalSinkSubtaskSleepIntervalInitMs);
    commonConfig.setPipeSinkSubtaskSleepIntervalMaxMs(originalSinkSubtaskSleepIntervalMaxMs);
  }

  @Test
  public void testLargeRequestWillBeSlicedForAsyncTransfer() throws Exception {
    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    final AsyncPipeDataTransferServiceClient client =
        Mockito.mock(AsyncPipeDataTransferServiceClient.class);
    Mockito.when(client.shouldReturnSelf()).thenReturn(true);

    final List<TPipeTransferReq> transferredRequests = new ArrayList<>();
    Mockito.doAnswer(
            invocation -> {
              final TPipeTransferReq req = invocation.getArgument(0);
              final AsyncMethodCallback<TPipeTransferResp> callback = invocation.getArgument(1);
              transferredRequests.add(req);
              callback.onComplete(successResp());
              return null;
            })
        .when(client)
        .pipeTransfer(Mockito.any(TPipeTransferReq.class), Mockito.any());

    final TestPipeTransferTrackableHandler handler = new TestPipeTransferTrackableHandler(sink);
    final TPipeTransferReq originalReq = createReq(10);

    handler.transfer(client, originalReq);

    Assert.assertEquals(3, transferredRequests.size());
    Assert.assertEquals(1, handler.completeCount);
    Assert.assertEquals(0, handler.errorCount);

    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReq.fromTPipeTransferReq(transferredRequests.get(0));
    final PipeTransferSliceReq secondSlice =
        PipeTransferSliceReq.fromTPipeTransferReq(transferredRequests.get(1));
    final PipeTransferSliceReq thirdSlice =
        PipeTransferSliceReq.fromTPipeTransferReq(transferredRequests.get(2));

    Assert.assertEquals(
        PipeRequestType.TRANSFER_SLICE.getType(), transferredRequests.get(0).getType());
    Assert.assertEquals(firstSlice.getOrderId(), secondSlice.getOrderId());
    Assert.assertEquals(firstSlice.getOrderId(), thirdSlice.getOrderId());
    Assert.assertEquals(originalReq.getType(), firstSlice.getOriginReqType());
    Assert.assertEquals(10, firstSlice.getOriginBodySize());
    Assert.assertEquals(3, firstSlice.getSliceCount());
    Assert.assertEquals(0, firstSlice.getSliceIndex());
    Assert.assertEquals(1, secondSlice.getSliceIndex());
    Assert.assertEquals(2, thirdSlice.getSliceIndex());
    Assert.assertEquals(4, firstSlice.getSliceBody().length);
    Assert.assertEquals(4, secondSlice.getSliceBody().length);
    Assert.assertEquals(2, thirdSlice.getSliceBody().length);

    final ArgumentCaptor<Boolean> shouldReturnSelfCaptor = ArgumentCaptor.forClass(Boolean.class);
    Mockito.verify(client, Mockito.times(3)).setShouldReturnSelf(shouldReturnSelfCaptor.capture());
    Assert.assertEquals(Arrays.asList(false, false, true), shouldReturnSelfCaptor.getAllValues());
  }

  @Test
  public void testLargeRequestFallsBackToWholeRequestWhenSliceTransferFails() throws Exception {
    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    final AsyncPipeDataTransferServiceClient client =
        Mockito.mock(AsyncPipeDataTransferServiceClient.class);
    Mockito.when(client.shouldReturnSelf()).thenReturn(true);

    final List<TPipeTransferReq> transferredRequests = new ArrayList<>();
    Mockito.doAnswer(
            invocation -> {
              final TPipeTransferReq req = invocation.getArgument(0);
              final AsyncMethodCallback<TPipeTransferResp> callback = invocation.getArgument(1);
              transferredRequests.add(req);
              if (req.getType() == PipeRequestType.TRANSFER_SLICE.getType()) {
                callback.onComplete(failedResp());
              } else {
                callback.onComplete(successResp());
              }
              return null;
            })
        .when(client)
        .pipeTransfer(Mockito.any(TPipeTransferReq.class), Mockito.any());

    final TestPipeTransferTrackableHandler handler = new TestPipeTransferTrackableHandler(sink);
    final TPipeTransferReq originalReq = createReq(10);

    handler.transfer(client, originalReq);

    Assert.assertEquals(2, transferredRequests.size());
    Assert.assertEquals(
        PipeRequestType.TRANSFER_SLICE.getType(), transferredRequests.get(0).getType());
    Assert.assertEquals(originalReq.getType(), transferredRequests.get(1).getType());
    Assert.assertEquals(originalReq.getVersion(), transferredRequests.get(1).getVersion());
    Assert.assertArrayEquals(originalReq.getBody(), transferredRequests.get(1).getBody());
    Assert.assertEquals(1, handler.completeCount);
    Assert.assertEquals(0, handler.errorCount);
  }

  @Test
  public void testTransferWaitsForReceiverBackoffAndRecordsStatus() throws Exception {
    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    final AsyncPipeDataTransferServiceClient client =
        Mockito.mock(AsyncPipeDataTransferServiceClient.class);
    final TEndPoint endPoint = new TEndPoint("127.0.0.1", 6667);
    final TSStatus status =
        new TSStatus()
            .setCode(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode());

    Mockito.when(client.getEndPoint()).thenReturn(endPoint);
    Mockito.doAnswer(
            invocation -> {
              final AsyncMethodCallback<TPipeTransferResp> callback = invocation.getArgument(1);
              callback.onComplete(resp(status));
              return null;
            })
        .when(client)
        .pipeTransfer(Mockito.any(TPipeTransferReq.class), Mockito.any());

    final TestPipeTransferTrackableHandler handler = new TestPipeTransferTrackableHandler(sink);

    handler.transfer(client, createReq(1));

    final InOrder inOrder = Mockito.inOrder(sink, client);
    inOrder.verify(sink).waitIfReceiverTemporarilyUnavailable(endPoint);
    inOrder.verify(client).pipeTransfer(Mockito.any(TPipeTransferReq.class), Mockito.any());
    Mockito.verify(sink).recordReceiverStatus(endPoint, status);
  }

  @Test
  public void testClientIsReturnedWhenReceiverProbeIsDelayed() throws Exception {
    final IoTDBDataRegionAsyncSink sink = Mockito.mock(IoTDBDataRegionAsyncSink.class);
    final AsyncPipeDataTransferServiceClient client =
        Mockito.mock(AsyncPipeDataTransferServiceClient.class);
    final TEndPoint endPoint = new TEndPoint("127.0.0.1", 6667);
    final PipeRuntimeSinkNonReportTimeConfigurableException exception =
        new PipeRuntimeSinkNonReportTimeConfigurableException("probe delayed", Long.MAX_VALUE);
    Mockito.when(client.getEndPoint()).thenReturn(endPoint);
    Mockito.doThrow(exception).when(sink).waitIfReceiverTemporarilyUnavailable(endPoint);

    final TestPipeTransferTrackableHandler handler = new TestPipeTransferTrackableHandler(sink);

    handler.transfer(client, createReq(1));
    Assert.assertEquals(1, handler.errorCount);
    Mockito.verify(client).setShouldReturnSelf(true);
    Mockito.verify(client).returnSelf(Mockito.any());
    Mockito.verify(client, Mockito.never())
        .pipeTransfer(Mockito.any(TPipeTransferReq.class), Mockito.any());
  }

  @Test
  public void testReceiverRetriesAreSerialized() {
    commonConfig.setPipeSinkSubtaskSleepIntervalInitMs(40);
    commonConfig.setPipeSinkSubtaskSleepIntervalMaxMs(40);
    commonConfig.setPipeAsyncSinkRetryMaxDurationMs(5000);

    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final TEndPoint endPoint = new TEndPoint("127.0.0.1", 6667);
    sink.recordReceiverStatus(endPoint, temporarilyUnavailableStatus());

    final long startTimeInMs = System.currentTimeMillis();
    sink.waitIfReceiverTemporarilyUnavailable(endPoint);
    sink.waitIfReceiverTemporarilyUnavailable(endPoint);

    Assert.assertTrue(System.currentTimeMillis() - startTimeInMs >= 60);
  }

  @Test
  public void testReceiverRetryFallsBackToSingleProbeAfterMaxDuration() {
    commonConfig.setPipeAsyncSinkRetryMaxDurationMs(0);
    commonConfig.setPipeAsyncSinkRetryProbeIntervalMs(1000);

    final IoTDBDataRegionAsyncSink sink = new IoTDBDataRegionAsyncSink();
    final TEndPoint endPoint = new TEndPoint("127.0.0.1", 6667);
    sink.recordReceiverStatus(endPoint, temporarilyUnavailableStatus());

    sink.waitIfReceiverTemporarilyUnavailable(endPoint);
    Assert.assertThrows(
        PipeRuntimeSinkNonReportTimeConfigurableException.class,
        () -> sink.waitIfReceiverTemporarilyUnavailable(endPoint));
    Assert.assertTrue(sink.peekSchedulingDelayMs() > 0);

    sink.recordReceiverStatus(
        endPoint, new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    sink.waitIfReceiverTemporarilyUnavailable(endPoint);
  }

  private static TSStatus temporarilyUnavailableStatus() {
    return new TSStatus()
        .setCode(TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode());
  }

  private static TPipeTransferReq createReq(final int bodySize) {
    final byte[] body = new byte[bodySize];
    for (int i = 0; i < body.length; ++i) {
      body[i] = (byte) i;
    }

    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = (short) 123;
    req.body = ByteBuffer.wrap(body);
    return req;
  }

  private static TPipeTransferResp successResp() {
    return resp(new TSStatus().setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  private static TPipeTransferResp resp(final TSStatus status) {
    final TPipeTransferResp resp = new TPipeTransferResp();
    resp.setStatus(status);
    return resp;
  }

  private static TPipeTransferResp failedResp() {
    final TPipeTransferResp resp = new TPipeTransferResp();
    resp.setStatus(
        new TSStatus().setCode(TSStatusCode.PIPE_TRANSFER_SLICE_OUT_OF_ORDER.getStatusCode()));
    return resp;
  }

  private static class TestPipeTransferTrackableHandler extends PipeTransferTrackableHandler {

    private int completeCount;
    private int errorCount;

    private TestPipeTransferTrackableHandler(final IoTDBDataRegionAsyncSink sink) {
      super(sink);
    }

    private void transfer(
        final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
        throws TException {
      tryTransfer(client, req);
    }

    @Override
    protected boolean onCompleteInternal(final TPipeTransferResp response) {
      completeCount++;
      return true;
    }

    @Override
    protected void onErrorInternal(final Exception exception) {
      errorCount++;
    }

    @Override
    protected void doTransfer(
        final AsyncPipeDataTransferServiceClient client, final TPipeTransferReq req)
        throws TException {
      transferWithOptionalRequestSlicing(client, req);
    }

    @Override
    public void clearEventsReferenceCount() {
      // Do nothing
    }
  }
}
