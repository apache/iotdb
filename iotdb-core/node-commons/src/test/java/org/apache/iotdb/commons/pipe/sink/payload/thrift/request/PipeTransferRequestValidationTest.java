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

package org.apache.iotdb.commons.pipe.sink.payload.thrift.request;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class PipeTransferRequestValidationTest {

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeV1RejectsOversizedString() {
    new HandshakeV1Req()
        .translateFromTPipeTransferReq(
            request(ByteBuffer.allocate(5).putInt(Integer.MAX_VALUE).put((byte) 1).flip()));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeV2RejectsOversizedKey() {
    new HandshakeV2Req()
        .translateFromTPipeTransferReq(
            request(
                ByteBuffer.allocate(9).putInt(1).putInt(Integer.MAX_VALUE).put((byte) 1).flip()));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeV2RejectsOversizedKeyWithValuePrefix() {
    new HandshakeV2Req()
        .translateFromTPipeTransferReq(
            request(ByteBuffer.allocate(12).putInt(1).putInt(Integer.MAX_VALUE).putInt(0).flip()));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeV2RejectsOversizedValue() {
    new HandshakeV2Req()
        .translateFromTPipeTransferReq(
            request(
                ByteBuffer.allocate(13)
                    .putInt(1)
                    .putInt(0)
                    .putInt(Integer.MAX_VALUE)
                    .put((byte) 1)
                    .flip()));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeRejectsStringBeyondBufferLimit() {
    final ByteBuffer body = ByteBuffer.allocate(32);
    body.putInt(7).putInt(5).put((byte) 'a').flip();
    body.position(Integer.BYTES);
    new HandshakeV1Req().translateFromTPipeTransferReq(request(body.asReadOnlyBuffer()));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeRejectsTruncatedStringLength() {
    new HandshakeV1Req().translateFromTPipeTransferReq(request(ByteBuffer.allocate(3)));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeV2RejectsNegativeParameterCount() {
    new HandshakeV2Req()
        .translateFromTPipeTransferReq(request(ByteBuffer.allocate(4).putInt(-1).flip()));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testHandshakeV2RejectsParameterCountBeyondBody() {
    new HandshakeV2Req()
        .translateFromTPipeTransferReq(
            request(ByteBuffer.allocate(12).putInt(Integer.MAX_VALUE).putInt(0).putInt(0).flip()));
  }

  @Test
  public void testHandshakeV1RoundTripPreservesBody() throws IOException {
    final HandshakeV1Req original = new HandshakeV1Req();
    original.convertToTPipeTransferReq("ms");
    final ByteBuffer body = original.body.duplicate();
    final PipeTransferHandshakeV1Req decoded =
        new HandshakeV1Req().translateFromTPipeTransferReq(original);
    Assert.assertEquals("ms", decoded.getTimestampPrecision());
    Assert.assertEquals(body, original.body);
    Assert.assertEquals(original.version, decoded.version);
    Assert.assertEquals(original.type, decoded.type);
  }

  @Test
  public void testHandshakeV2RoundTripPreservesNullEmptyAndUnicode() throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put("", "");
    params.put(null, null);
    params.put("username", "用户");
    final HandshakeV2Req original = new HandshakeV2Req();
    original.convertToTPipeTransferReq(params);
    final ByteBuffer body = ByteBuffer.allocateDirect(original.body.remaining() + Integer.BYTES);
    body.putInt(7).put(original.body.duplicate()).flip();
    body.position(Integer.BYTES);
    original.body = body.asReadOnlyBuffer();

    final PipeTransferHandshakeV2Req decoded =
        new HandshakeV2Req().translateFromTPipeTransferReq(original);

    Assert.assertEquals(params, decoded.getParams());
    Assert.assertEquals(Integer.BYTES, original.body.position());
    Assert.assertEquals(original.version, decoded.version);
    Assert.assertEquals(original.type, decoded.type);
    Assert.assertSame(original.body, decoded.body);
  }

  @Test
  public void testHandshakeV2EmptyParameters() throws IOException {
    final HandshakeV2Req original = new HandshakeV2Req();
    original.convertToTPipeTransferReq(new HashMap<>());
    Assert.assertTrue(
        new HandshakeV2Req().translateFromTPipeTransferReq(original).getParams().isEmpty());
  }

  @Test(expected = BufferUnderflowException.class)
  public void testSliceRejectsOversizedBinary() {
    PipeTransferSliceReq.fromTPipeTransferReq(request(sliceBody(Integer.MAX_VALUE)));
  }

  @Test(expected = BufferUnderflowException.class)
  public void testSliceRejectsBinaryBeyondRemainingBody() {
    PipeTransferSliceReq.fromTPipeTransferReq(request(sliceBody(13)));
  }

  @Test
  public void testSliceRoundTrip() throws IOException {
    final PipeTransferSliceReq original =
        PipeTransferSliceReq.toTPipeTransferReq(
            7,
            PipeRequestType.HANDSHAKE_DATANODE_V1.getType(),
            0,
            1,
            ByteBuffer.wrap(new byte[] {1, 2, 3, 4}),
            0,
            4);
    final PipeTransferSliceReq decoded = PipeTransferSliceReq.fromTPipeTransferReq(original);
    Assert.assertEquals(original.getOrderId(), decoded.getOrderId());
    Assert.assertEquals(original.getOriginReqType(), decoded.getOriginReqType());
    Assert.assertEquals(original.getOriginBodySize(), decoded.getOriginBodySize());
    Assert.assertEquals(original.getSliceIndex(), decoded.getSliceIndex());
    Assert.assertEquals(original.getSliceCount(), decoded.getSliceCount());
    Assert.assertEquals(original.version, decoded.version);
    Assert.assertEquals(original.type, decoded.type);
    Assert.assertArrayEquals(new byte[] {1, 2, 3, 4}, decoded.getSliceBody());
  }

  @Test(expected = BufferUnderflowException.class)
  public void testCleanupRejectsOversizedPipeName() {
    PipeTransferPipeReceiverRuntimeInfoCleanupReq.fromTPipeTransferReq(
        request(ByteBuffer.allocate(12).putInt(Integer.MAX_VALUE).putLong(1).flip()));
  }

  @Test
  public void testCleanupRoundTripPreservesBody() throws IOException {
    final PipeTransferPipeReceiverRuntimeInfoCleanupReq original =
        PipeTransferPipeReceiverRuntimeInfoCleanupReq.toTPipeTransferReq("pipe", 1);
    final ByteBuffer body = original.body.duplicate();
    final PipeTransferPipeReceiverRuntimeInfoCleanupReq decoded =
        PipeTransferPipeReceiverRuntimeInfoCleanupReq.fromTPipeTransferReq(original);
    Assert.assertEquals(original, decoded);
    Assert.assertEquals(body, original.body);
  }

  private static ByteBuffer sliceBody(final int length) {
    return ByteBuffer.allocate(26)
        .putInt(0)
        .putShort(PipeRequestType.HANDSHAKE_DATANODE_V1.getType())
        .putInt(4)
        .putInt(length)
        .putInt(0)
        .putInt(0)
        .putInt(1)
        .flip();
  }

  private static TPipeTransferReq request(final ByteBuffer body) {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.version = IoTDBSinkRequestVersion.VERSION_1.getVersion();
    req.body = body;
    return req;
  }

  private static class HandshakeV1Req extends PipeTransferHandshakeV1Req {
    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.HANDSHAKE_DATANODE_V1;
    }
  }

  private static class HandshakeV2Req extends PipeTransferHandshakeV2Req {
    @Override
    protected PipeRequestType getPlanType() {
      return PipeRequestType.HANDSHAKE_DATANODE_V2;
    }
  }
}
