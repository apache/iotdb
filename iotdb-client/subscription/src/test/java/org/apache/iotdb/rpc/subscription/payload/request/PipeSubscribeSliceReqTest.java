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

package org.apache.iotdb.rpc.subscription.payload.request;

import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;

public class PipeSubscribeSliceReqTest {

  @Test
  public void testBuildAndReassembleSlicesFromNonZeroPosition() throws Exception {
    final byte[] bytes = new byte[] {99, 98, 0, 1, 2, 3, 4, 5, 6, 97};
    final ByteBuffer body = ByteBuffer.wrap(bytes);
    body.position(2);
    body.limit(9);
    final TPipeSubscribeReq req = request(PipeSubscribeRequestType.POLL, body);
    final int originalPosition = body.position();
    final int originalLimit = body.limit();

    final int bodySizeLimit = 3;
    final int sliceCount = PipeSubscribeSliceReqBuilder.getSliceCount(req, bodySizeLimit);
    Assert.assertEquals(3, sliceCount);
    Assert.assertTrue(PipeSubscribeSliceReqBuilder.shouldSlice(req, bodySizeLimit));

    final PipeSubscribeSliceReqHandler handler = new PipeSubscribeSliceReqHandler(10);
    for (int sliceIndex = 0; sliceIndex < sliceCount; sliceIndex++) {
      final PipeSubscribeSliceReq thriftSlice =
          PipeSubscribeSliceReqBuilder.buildSliceReq(
              req, -1, sliceIndex, sliceCount, bodySizeLimit);
      final int sliceBodyPosition = thriftSlice.body.position();
      final PipeSubscribeSliceReq slice = PipeSubscribeSliceReq.fromTPipeSubscribeReq(thriftSlice);

      Assert.assertEquals(sliceBodyPosition, thriftSlice.body.position());
      Assert.assertEquals(-1, slice.getOrderId());
      Assert.assertEquals(sliceIndex, slice.getSliceIndex());
      Assert.assertEquals(sliceCount, slice.getSliceCount());
      Assert.assertTrue(handler.receiveSlice(slice));
      if (sliceIndex < sliceCount - 1) {
        Assert.assertFalse(handler.makeReqIfComplete().isPresent());
      }
    }

    final Optional<TPipeSubscribeReq> reassembled = handler.makeReqIfComplete();
    Assert.assertTrue(reassembled.isPresent());
    Assert.assertEquals(req.getVersion(), reassembled.get().getVersion());
    Assert.assertEquals(req.getType(), reassembled.get().getType());
    Assert.assertArrayEquals(
        new byte[] {0, 1, 2, 3, 4, 5, 6}, remainingBytes(reassembled.get().body));
    Assert.assertEquals(originalPosition, body.position());
    Assert.assertEquals(originalLimit, body.limit());
  }

  @Test
  public void testRejectOutOfOrderSliceAndAcceptFreshRequest() throws Exception {
    final TPipeSubscribeReq req =
        request(PipeSubscribeRequestType.SEEK, ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5}));
    final int sliceCount = PipeSubscribeSliceReqBuilder.getSliceCount(req, 2);
    final PipeSubscribeSliceReqHandler handler = new PipeSubscribeSliceReqHandler(10);

    Assert.assertTrue(
        handler.receiveSlice(PipeSubscribeSliceReqBuilder.buildSliceReq(req, 1, 0, sliceCount, 2)));
    Assert.assertFalse(
        handler.receiveSlice(PipeSubscribeSliceReqBuilder.buildSliceReq(req, 1, 2, sliceCount, 2)));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());

    for (int sliceIndex = 0; sliceIndex < sliceCount; sliceIndex++) {
      Assert.assertTrue(
          handler.receiveSlice(
              PipeSubscribeSliceReqBuilder.buildSliceReq(req, 2, sliceIndex, sliceCount, 2)));
    }
    Assert.assertArrayEquals(
        new byte[] {0, 1, 2, 3, 4, 5}, remainingBytes(handler.makeReqIfComplete().get().body));
  }

  @Test
  public void testRejectChangedMetadataAndBodySizeViolations() throws Exception {
    final PipeSubscribeSliceReqHandler handler = new PipeSubscribeSliceReqHandler(10);

    Assert.assertTrue(
        handler.receiveSlice(slice(1, PipeSubscribeRequestType.POLL, 4, 0, 2, new byte[] {0, 1})));
    Assert.assertFalse(
        handler.receiveSlice(slice(2, PipeSubscribeRequestType.POLL, 4, 1, 2, new byte[] {2, 3})));

    Assert.assertFalse(
        handler.receiveSlice(slice(1, PipeSubscribeRequestType.POLL, 11, 0, 2, new byte[] {0})));
    Assert.assertFalse(
        handler.receiveSlice(slice(1, PipeSubscribeRequestType.POLL, 2, 0, 2, new byte[] {0, 1})));

    Assert.assertTrue(
        handler.receiveSlice(slice(1, PipeSubscribeRequestType.POLL, 3, 0, 2, new byte[] {0})));
    Assert.assertFalse(
        handler.receiveSlice(slice(1, PipeSubscribeRequestType.POLL, 3, 1, 2, new byte[] {1})));
    Assert.assertFalse(handler.makeReqIfComplete().isPresent());
  }

  @Test
  public void testRejectUnsliceableOriginRequestTypes() {
    for (final PipeSubscribeRequestType type :
        new PipeSubscribeRequestType[] {
          PipeSubscribeRequestType.HANDSHAKE,
          PipeSubscribeRequestType.CLOSE,
          PipeSubscribeRequestType.SLICE
        }) {
      final TPipeSubscribeReq req = request(type, ByteBuffer.wrap(new byte[] {0, 1}));
      Assert.assertFalse(PipeSubscribeSliceReqBuilder.shouldSlice(req, 1));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> PipeSubscribeSliceReqBuilder.buildSliceReq(req, 1, 0, 2, 1));
    }

    final PipeSubscribeSliceReqHandler handler = new PipeSubscribeSliceReqHandler(10);
    Assert.assertFalse(
        handler.receiveSlice(slice(1, PipeSubscribeRequestType.SLICE, 2, 0, 2, new byte[] {0})));
  }

  @Test
  public void testRejectMalformedSliceEnvelopeBeforeAllocatingBody() throws Exception {
    final TPipeSubscribeReq req =
        request(PipeSubscribeRequestType.POLL, ByteBuffer.wrap(new byte[] {0, 1}));
    final PipeSubscribeSliceReq malformedSlice =
        PipeSubscribeSliceReqBuilder.buildSliceReq(req, 1, 0, 2, 1);
    malformedSlice.body.putInt(malformedSlice.body.position() + 19, Integer.MAX_VALUE);

    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> PipeSubscribeSliceReq.fromTPipeSubscribeReq(malformedSlice));

    final PipeSubscribeSliceReq invalidVersionSlice =
        PipeSubscribeSliceReqBuilder.buildSliceReq(req, 1, 0, 2, 1);
    invalidVersionSlice.version = (byte) -1;
    Assert.assertFalse(new PipeSubscribeSliceReqHandler(10).receiveSlice(invalidVersionSlice));
  }

  private static TPipeSubscribeReq request(
      final PipeSubscribeRequestType type, final ByteBuffer body) {
    final TPipeSubscribeReq req = new TPipeSubscribeReq();
    req.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    req.type = type.getType();
    req.body = body;
    return req;
  }

  private static PipeSubscribeSliceReq slice(
      final int orderId,
      final PipeSubscribeRequestType originType,
      final int originBodySize,
      final int sliceIndex,
      final int sliceCount,
      final byte[] sliceBody) {
    final TPipeSubscribeReq thriftReq = new TPipeSubscribeReq();
    thriftReq.version = PipeSubscribeRequestVersion.VERSION_1.getVersion();
    thriftReq.type = PipeSubscribeRequestType.SLICE.getType();
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(orderId, outputStream);
      ReadWriteIOUtils.write(PipeSubscribeRequestVersion.VERSION_1.getVersion(), outputStream);
      ReadWriteIOUtils.write(originType.getType(), outputStream);
      ReadWriteIOUtils.write(originBodySize, outputStream);
      ReadWriteIOUtils.write(sliceIndex, outputStream);
      ReadWriteIOUtils.write(sliceCount, outputStream);
      ReadWriteIOUtils.write(new Binary(sliceBody), outputStream);
      thriftReq.body =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    } catch (final IOException e) {
      throw new AssertionError(e);
    }
    return PipeSubscribeSliceReq.fromTPipeSubscribeReq(thriftReq);
  }

  private static byte[] remainingBytes(final ByteBuffer buffer) {
    final ByteBuffer duplicate = buffer.duplicate();
    final byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }
}
