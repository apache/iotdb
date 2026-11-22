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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.common;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapUdpProtocol;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBAirGapSink;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTransferSliceReqBuilderTest {

  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  private int originalRequestSliceThresholdBytes;

  @Before
  public void setUp() {
    originalRequestSliceThresholdBytes = commonConfig.getPipeSinkRequestSliceThresholdBytes();
    commonConfig.setPipeSinkRequestSliceThresholdBytes(4);
  }

  @After
  public void tearDown() {
    commonConfig.setPipeSinkRequestSliceThresholdBytes(originalRequestSliceThresholdBytes);
  }

  @Test
  public void testBuildSliceReq() throws Exception {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 10);
    final int bodySizeLimit = PipeTransferSliceReqBuilder.getBodySizeLimit();

    Assert.assertTrue(PipeTransferSliceReqBuilder.shouldSlice(req, bodySizeLimit));
    Assert.assertEquals(3, PipeTransferSliceReqBuilder.getSliceCount(req, bodySizeLimit));

    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReqBuilder.buildSliceReq(req, 123, 0, 3, bodySizeLimit);
    final PipeTransferSliceReq secondSlice =
        PipeTransferSliceReqBuilder.buildSliceReq(req, 123, 1, 3, bodySizeLimit);
    final PipeTransferSliceReq thirdSlice =
        PipeTransferSliceReqBuilder.buildSliceReq(req, 123, 2, 3, bodySizeLimit);

    Assert.assertArrayEquals(new byte[] {0, 1, 2, 3}, firstSlice.getSliceBody());
    Assert.assertArrayEquals(new byte[] {4, 5, 6, 7}, secondSlice.getSliceBody());
    Assert.assertArrayEquals(new byte[] {8, 9}, thirdSlice.getSliceBody());
    Assert.assertEquals(0, firstSlice.getSliceIndex());
    Assert.assertEquals(1, secondSlice.getSliceIndex());
    Assert.assertEquals(2, thirdSlice.getSliceIndex());
    Assert.assertEquals(3, firstSlice.getSliceCount());
    Assert.assertEquals(req.getType(), firstSlice.getOriginReqType());
    Assert.assertEquals(10, firstSlice.getOriginBodySize());
  }

  @Test
  public void testShouldSliceOnlyForVersion1RequestsAboveThreshold() {
    final int bodySizeLimit = PipeTransferSliceReqBuilder.getBodySizeLimit();

    Assert.assertFalse(
        PipeTransferSliceReqBuilder.shouldSlice(
            createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 3), bodySizeLimit));
    Assert.assertFalse(
        PipeTransferSliceReqBuilder.shouldSlice(
            createReq((byte) (IoTDBSinkRequestVersion.VERSION_1.getVersion() + 1), 10),
            bodySizeLimit));
    Assert.assertTrue(
        PipeTransferSliceReqBuilder.shouldSlice(
            createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4), bodySizeLimit));
  }

  @Test
  public void testPipeTransferSliceReqFromLegacyV13Body() throws IOException {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.type = PipeRequestType.TRANSFER_SLICE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(7, outputStream);
      ReadWriteIOUtils.write(PipeRequestType.TRANSFER_TABLET_RAW.getType(), outputStream);
      ReadWriteIOUtils.write(6, outputStream);
      ReadWriteIOUtils.write(new Binary(new byte[] {2, 3, 4}), outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write(2, outputStream);
      req.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    final PipeTransferSliceReq sliceReq = PipeTransferSliceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(7, sliceReq.getOrderId());
    Assert.assertEquals(PipeRequestType.TRANSFER_TABLET_RAW.getType(), sliceReq.getOriginReqType());
    Assert.assertEquals(6, sliceReq.getOriginBodySize());
    Assert.assertArrayEquals(new byte[] {2, 3, 4}, sliceReq.getSliceBody());
    Assert.assertEquals(1, sliceReq.getSliceIndex());
    Assert.assertEquals(2, sliceReq.getSliceCount());
  }

  @Test
  public void testSliceReqHandlerRejectsOversizedSlices() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 6);
    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 2, req.body.duplicate(), 0, 4);
    final PipeTransferSliceReq oversizedSecondSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 1, 2, req.body.duplicate(), 2, 6);

    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();
    Assert.assertTrue(handler.receiveSlice(firstSlice));
    Assert.assertFalse(handler.receiveSlice(oversizedSecondSlice));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
  }

  @Test
  public void testSliceReqHandlerAssemblesCompleteRequest() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 6);
    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 2, req.body.duplicate(), 0, 4);
    final PipeTransferSliceReq secondSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 1, 2, req.body.duplicate(), 4, 6);

    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();
    Assert.assertTrue(handler.receiveSlice(firstSlice));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
    Assert.assertTrue(handler.receiveSlice(secondSlice));

    final Optional<TPipeTransferReq> completedReq = handler.makeReqIfComplete();
    Assert.assertTrue(completedReq.isPresent());

    final TPipeTransferReq assembledReq = completedReq.get();
    Assert.assertEquals(IoTDBSinkRequestVersion.VERSION_1.getVersion(), assembledReq.getVersion());
    Assert.assertEquals(req.getType(), assembledReq.getType());

    final byte[] assembledBody = new byte[assembledReq.body.remaining()];
    assembledReq.body.get(assembledBody);
    Assert.assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5}, assembledBody);
  }

  @Test
  public void testSliceReqHandlerRejectsInvalidMetadata() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 2);
    final PipeTransferSliceReq invalidSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 0, req.body.duplicate(), 0, 1);

    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();

    Assert.assertFalse(handler.receiveSlice(invalidSlice));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
  }

  @Test
  public void testUdpSliceReqHandlerReordersSlicesAndIgnoresDuplicates() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 6);
    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 3, req.body.duplicate(), 0, 2);
    final PipeTransferSliceReq middleSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 1, 3, req.body.duplicate(), 2, 4);
    final PipeTransferSliceReq lastSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 2, 3, req.body.duplicate(), 4, 6);
    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();

    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
        handler.receiveSliceWithReordering(lastSlice));
    Assert.assertFalse(handler.isComplete(7));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.DUPLICATE,
        handler.receiveSliceWithReordering(lastSlice));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.ACCEPTED,
        handler.receiveSliceWithReordering(firstSlice));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.ACCEPTED,
        handler.receiveSliceWithReordering(middleSlice));

    final TPipeTransferReq assembledReq = handler.makeReqIfComplete(7).get();
    final byte[] assembledBody = new byte[assembledReq.body.remaining()];
    assembledReq.body.get(assembledBody);
    Assert.assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5}, assembledBody);
  }

  @Test
  public void testUdpSliceReqHandlerKeepsInterleavedRequests() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4);
    final PipeTransferSliceReq olderSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 2, req.body.duplicate(), 0, 2);
    final PipeTransferSliceReq newerFirstSlice =
        PipeTransferSliceReq.toTPipeTransferReq(8, req.getType(), 0, 2, req.body.duplicate(), 0, 2);
    final PipeTransferSliceReq olderLastSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 1, 2, req.body.duplicate(), 2, 4);
    final PipeTransferSliceReq newerLastSlice =
        PipeTransferSliceReq.toTPipeTransferReq(8, req.getType(), 1, 2, req.body.duplicate(), 2, 4);
    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();

    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
        handler.receiveSliceWithReordering(olderSlice));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
        handler.receiveSliceWithReordering(newerFirstSlice));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.ACCEPTED,
        handler.receiveSliceWithReordering(olderLastSlice));
    Assert.assertTrue(handler.isComplete(7));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.ACCEPTED,
        handler.receiveSliceWithReordering(newerLastSlice));
    Assert.assertTrue(handler.isComplete(8));

    handler.completeUdpRequest(8, true);
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.STALE,
        handler.receiveSliceWithReordering(newerFirstSlice));
    handler.completeUdpRequest(7, true);
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.STALE,
        handler.receiveSliceWithReordering(olderSlice));
  }

  @Test
  public void testNonUdpSliceHandlerStillRejectsOutOfOrderSlices() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4);
    final PipeTransferSliceReq secondSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 1, 2, req.body.duplicate(), 2, 4);
    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();

    Assert.assertFalse(handler.receiveSlice(secondSlice));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
  }

  @Test
  public void testUdpSliceHandlerBoundsPendingRequests() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4);
    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();
    final List<Long> evictedIds = new ArrayList<>();
    for (int orderId = 0; orderId < 33; orderId++) {
      final PipeTransferSliceReq firstSlice =
          PipeTransferSliceReq.toTPipeTransferReq(
              orderId, req.getType(), 0, 2, req.body.duplicate(), 0, 2);
      Assert.assertEquals(
          PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
          handler.receiveSliceWithReordering(firstSlice, () -> {}, evictedIds::add));
    }

    Assert.assertEquals(Collections.singletonList(0L), evictedIds);
    final PipeTransferSliceReq delayedSlice =
        PipeTransferSliceReq.toTPipeTransferReq(0, req.getType(), 1, 2, req.body.duplicate(), 2, 4);
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.DROPPED,
        handler.receiveSliceWithReordering(delayedSlice, () -> Assert.fail(), evictedIds::add));
  }

  @Test
  public void testUdpSliceExpirationAndDuplicateMemoryReservation() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4);
    final PipeTransferSliceReq firstSlice =
        PipeTransferSliceReqBuilder.buildSliceReq(req, 7, 0, 2, 2);
    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();
    final AtomicInteger reservations = new AtomicInteger();
    final List<Long> evictedIds = new ArrayList<>();

    final long beforeReceive = System.nanoTime();
    handler.receiveSliceWithReordering(firstSlice, reservations::incrementAndGet, evictedIds::add);
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.DUPLICATE,
        handler.receiveSliceWithReordering(
            firstSlice, reservations::incrementAndGet, evictedIds::add));
    final long idleTimeout = TimeUnit.SECONDS.toNanos(1);
    handler.expireUdpRequests(beforeReceive, idleTimeout, evictedIds::add);
    Assert.assertTrue(evictedIds.isEmpty());
    handler.expireUdpRequests(System.nanoTime() + idleTimeout, idleTimeout, evictedIds::add);
    Assert.assertEquals(Collections.singletonList(7L), evictedIds);
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.DROPPED,
        handler.receiveSliceWithReordering(
            firstSlice, reservations::incrementAndGet, evictedIds::add));
    Assert.assertEquals(1, reservations.get());
  }

  @Test
  public void testUdpSliceOrderUsesUpperBitsAcrossSenderRestarts() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4);
    final long retiredOrderId = 0x9234567800000007L;
    final long restartedOrderId = 0xA234567800000007L;
    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();
    final PipeTransferSliceReq legacySlice =
        PipeTransferSliceReq.fromTPipeTransferReq(
            PipeTransferSliceReqBuilder.buildSliceReq(req, 7, 0, 2, 2), true);
    Assert.assertEquals(7, legacySlice.getUdpOrderId());
    handler.receiveSliceWithReordering(legacySlice);
    handler.receiveSliceWithReordering(
        PipeTransferSliceReq.fromTPipeTransferReq(
            PipeTransferSliceReqBuilder.buildSliceReq(req, 7, 1, 2, 2), true));
    Assert.assertTrue(handler.isComplete(7));
    handler.completeUdpRequest(legacySlice.getUdpOrderId(), true);
    final PipeTransferSliceReq retiredSlice =
        PipeTransferSliceReq.fromTPipeTransferReq(
            PipeTransferSliceReqBuilder.buildSliceReq(req, retiredOrderId, 1, 2, 2), true);
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
        handler.receiveSliceWithReordering(retiredSlice));
    handler.receiveSliceWithReordering(
        PipeTransferSliceReq.fromTPipeTransferReq(
            PipeTransferSliceReqBuilder.buildSliceReq(req, retiredOrderId, 0, 2, 2), true));
    Assert.assertTrue(handler.isComplete(retiredOrderId));
    handler.completeUdpRequest(retiredOrderId, true);

    final PipeTransferSliceReq restartedSlice =
        PipeTransferSliceReq.fromTPipeTransferReq(
            PipeTransferSliceReqBuilder.buildSliceReq(req, restartedOrderId, 0, 2, 2), true);
    Assert.assertEquals(retiredSlice.getOrderId(), restartedSlice.getOrderId());
    Assert.assertEquals(restartedOrderId, restartedSlice.getUdpOrderId());
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
        handler.receiveSliceWithReordering(restartedSlice));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.STALE,
        handler.receiveSliceWithReordering(retiredSlice));
    handler.receiveSliceWithReordering(
        PipeTransferSliceReq.fromTPipeTransferReq(
            PipeTransferSliceReqBuilder.buildSliceReq(req, restartedOrderId, 1, 2, 2), true));
    Assert.assertTrue(handler.isComplete(restartedOrderId));
  }

  @Test
  public void testInvalidUdpSliceDiscardsOnlyItsRequest() throws IOException {
    final TPipeTransferReq req = createReq(IoTDBSinkRequestVersion.VERSION_1.getVersion(), 4);
    final PipeTransferSliceReq validSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 2, req.body.duplicate(), 0, 2);
    final PipeTransferSliceReq invalidSlice =
        PipeTransferSliceReq.toTPipeTransferReq(7, req.getType(), 0, 0, req.body.duplicate(), 0, 2);
    final PipeTransferSliceReqHandler handler = new PipeTransferSliceReqHandler();

    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
        handler.receiveSliceWithReordering(validSlice));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.INVALID,
        handler.receiveSliceWithReordering(invalidSlice));
    Assert.assertEquals(
        PipeTransferSliceReqHandler.SliceReceiveResult.NEW_ORDER,
        handler.receiveSliceWithReordering(validSlice));
  }

  @Test
  public void testUdpRequestCopyCountDefaultsAndAcceptsReducedKeys() {
    Assert.assertEquals(2, TestAirGapSink.getCopyCount(new PipeParameters(Collections.emptyMap())));

    final Map<String, String> parameters = new HashMap<>();
    parameters.put("air-gap.udp.request-copy-count", "3");
    Assert.assertEquals(3, TestAirGapSink.getCopyCount(new PipeParameters(parameters)));
    parameters.clear();
    parameters.put(PipeSinkConstant.CONNECTOR_AIR_GAP_UDP_REQUEST_COPY_COUNT_KEY, "4");
    Assert.assertEquals(4, TestAirGapSink.getCopyCount(new PipeParameters(parameters)));
    parameters.clear();
    parameters.put(PipeSinkConstant.SINK_AIR_GAP_UDP_REQUEST_COPY_COUNT_KEY, "5");
    Assert.assertEquals(5, TestAirGapSink.getCopyCount(new PipeParameters(parameters)));
  }

  @Test
  public void testSlicedUdpRequestCopiesKeepIdsAndUseSeparateRounds() throws Exception {
    final TestAirGapSink sink = new TestAirGapSink();
    Assert.assertEquals(2, sink.getRequestCopyCount());
    sink.configureUdp(2, true);

    final byte[] body = new byte[2_500];
    for (int i = 0; i < body.length; i++) {
      body[i] = (byte) i;
    }
    final byte[] request =
        ByteBuffer.allocate(Byte.BYTES + Short.BYTES + body.length)
            .put((byte) 1)
            .putShort((short) 123)
            .put(body)
            .array();

    try (final DatagramSocket receiver = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      receiver.setSoTimeout(2_000);
      final long accountedBytes = sink.sendUdpToPort(receiver.getLocalPort(), request);

      final byte[][] receivedPackets = new byte[6][];
      for (int i = 0; i < receivedPackets.length; i++) {
        final DatagramPacket packet = new DatagramPacket(new byte[1_500], 1_500);
        receiver.receive(packet);
        receivedPackets[i] =
            Arrays.copyOfRange(
                packet.getData(), packet.getOffset(), packet.getOffset() + packet.getLength());
      }

      Assert.assertEquals(
          Arrays.stream(receivedPackets).mapToLong(packet -> packet.length).sum(), accountedBytes);
      for (int i = 0; i < 3; i++) {
        Assert.assertArrayEquals(receivedPackets[i], receivedPackets[i + 3]);
        Assert.assertEquals(0x9234567800000001L, getSliceOrderId(receivedPackets[i]));
        final long requestId = ByteBuffer.wrap(receivedPackets[i]).getLong();
        Assert.assertEquals(0x9234567800000002L + i, requestId);
      }
    }
  }

  @Test
  public void testUdpAckRetryReusesRequestIdAndStopsAfterSuccess() throws Exception {
    final TestAirGapSink sink = new TestAirGapSink();
    sink.configureUdp(3, false);
    final byte[] request = ByteBuffer.allocate(3).put((byte) 1).putShort((short) 123).array();

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try (final DatagramSocket receiver = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
      receiver.setSoTimeout(5_000);
      final Future<Long> sender =
          executor.submit(() -> sink.sendUdpToPort(receiver.getLocalPort(), request));
      final DatagramPacket first = new DatagramPacket(new byte[1_024], 1_024);
      receiver.receive(first);
      final long requestId = ByteBuffer.wrap(first.getData()).getLong();
      Assert.assertEquals(0, requestId & AirGapUdpProtocol.NO_ACK_REQUEST_ID_MASK);
      sendUdpAck(receiver, first, requestId + 1);

      final DatagramPacket retry = new DatagramPacket(new byte[1_024], 1_024);
      receiver.receive(retry);
      Assert.assertEquals(first.getLength(), retry.getLength());
      Assert.assertArrayEquals(first.getData(), retry.getData());
      sendUdpAck(receiver, retry, requestId);
      Assert.assertEquals(first.getLength() * 2L, sender.get(5, TimeUnit.SECONDS).longValue());
      receiver.setSoTimeout(100);
      Assert.assertThrows(SocketTimeoutException.class, () -> receiver.receive(retry));
    } finally {
      executor.shutdownNow();
    }
  }

  private static void sendUdpAck(
      final DatagramSocket receiver, final DatagramPacket request, final long requestId)
      throws IOException {
    final byte[] ack =
        ByteBuffer.allocate(Long.BYTES + 1)
            .putLong(requestId)
            .put(AirGapOneByteResponse.OK)
            .array();
    receiver.send(new DatagramPacket(ack, ack.length, request.getSocketAddress()));
  }

  private static class TestAirGapSink extends IoTDBAirGapSink {

    private static int getCopyCount(final PipeParameters parameters) {
      return getUdpRequestCopyCount(parameters);
    }

    private int getRequestCopyCount() throws Exception {
      final Field field = IoTDBAirGapSink.class.getDeclaredField("udpRequestCopyCount");
      field.setAccessible(true);
      return field.getInt(this);
    }

    private void configureUdp(final int requestCopyCount, final boolean strictUnidirectional)
        throws Exception {
      final Field requestIdGeneratorField =
          IoTDBAirGapSink.class.getDeclaredField("udpRequestIdGenerator");
      requestIdGeneratorField.setAccessible(true);
      ((AtomicLong) requestIdGeneratorField.get(this)).set(0x1234567800000000L);
      setField("strictUnidirectional", strictUnidirectional);
      setField("udpPacketSizeInBytes", 1_024);
      setField("strictUdpSendIntervalNanos", 0L);
      setField("udpRequestCopyCount", requestCopyCount);
    }

    private long sendUdpToPort(final int port, final byte[] request) throws Exception {
      final AirGapSocket socket = new AirGapSocket("127.0.0.1", port);
      socket.connectUdp(3_000);
      try {
        final AtomicLong accountedBytes = new AtomicLong();
        Assert.assertTrue(sendBytes(socket, request, accountedBytes::addAndGet));
        return accountedBytes.get();
      } finally {
        socket.close();
      }
    }

    private void setField(final String name, final Object value) throws Exception {
      final Field field = IoTDBAirGapSink.class.getDeclaredField(name);
      field.setAccessible(true);
      field.set(this, value);
    }

    @Override
    public void transfer(final Event event) {}

    @Override
    public void transfer(final TabletInsertionEvent tabletInsertionEvent) {}

    @Override
    protected byte[] generateHandShakeV1Payload() {
      return new byte[0];
    }

    @Override
    protected byte[] generateHandShakeV2Payload() {
      return new byte[0];
    }

    @Override
    protected void mayLimitRateAndRecordIO(final long requiredBytes) {}

    @Override
    protected boolean mayNeedHandshakeWhenFail() {
      return false;
    }

    @Override
    protected byte[] getTransferSingleFilePieceBytes(
        final String fileName, final long position, final byte[] payLoad) {
      return payLoad;
    }

    @Override
    protected byte[] getTransferMultiFilePieceBytes(
        final String fileName, final long position, final byte[] payLoad) {
      return payLoad;
    }
  }

  private static long getSliceOrderId(final byte[] datagram) {
    final int requestOffset = Long.BYTES + 2 * Integer.BYTES + Long.BYTES;
    final ByteBuffer requestBytes =
        ByteBuffer.wrap(datagram, requestOffset, datagram.length - requestOffset);
    final TPipeTransferReq request =
        new TPipeTransferReq(requestBytes.get(), requestBytes.getShort(), requestBytes.slice());
    return PipeTransferSliceReq.fromTPipeTransferReq(request, true).getUdpOrderId();
  }

  private static TPipeTransferReq createReq(final byte version, final int bodySize) {
    final byte[] body = new byte[bodySize];
    for (int i = 0; i < body.length; ++i) {
      body[i] = (byte) i;
    }

    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = version;
    req.type = (short) 123;
    req.body = ByteBuffer.wrap(body);
    return req;
  }
}
