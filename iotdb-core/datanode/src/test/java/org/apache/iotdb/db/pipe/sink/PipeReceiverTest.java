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

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiver;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV1Req;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class PipeReceiverTest {

  @Test
  public void testMalformedPreHandshakePayloadsRejected() throws IOException {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final long minimumReceiverMemory = config.getPipeMinimumReceiverMemory();
    config.setPipeMinimumReceiverMemory(0);
    try {
      final IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();
      assertMalformedRequestRejected(
          receiver,
          PipeRequestType.HANDSHAKE_DATANODE_V1,
          ByteBuffer.allocate(5).putInt(Integer.MAX_VALUE).put((byte) 1).flip());
      assertMalformedRequestRejected(
          receiver,
          PipeRequestType.HANDSHAKE_DATANODE_V2,
          ByteBuffer.allocate(9).putInt(1).putInt(Integer.MAX_VALUE).put((byte) 1).flip());
      assertMalformedRequestRejected(
          receiver,
          PipeRequestType.TRANSFER_SLICE,
          ByteBuffer.allocate(26)
              .putInt(0)
              .putShort(PipeRequestType.HANDSHAKE_DATANODE_V1.getType())
              .putInt(0)
              .putInt(Integer.MAX_VALUE)
              .putInt(0)
              .putInt(0)
              .putInt(1)
              .flip());
      assertMalformedRequestRejected(
          receiver,
          PipeRequestType.TRANSFER_PIPE_RECEIVER_RUNTIME_INFO_CLEANUP,
          ByteBuffer.allocate(12).putInt(Integer.MAX_VALUE).putLong(1).flip());
    } finally {
      config.setPipeMinimumReceiverMemory(minimumReceiverMemory);
    }
  }

  private void assertMalformedRequestRejected(
      final IoTDBDataNodeReceiver receiver, final PipeRequestType type, final ByteBuffer body)
      throws IOException {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.setVersion(IoTDBSinkRequestVersion.VERSION_1.getVersion());
    req.setType(type.getType());
    req.setBody(body);
    final TPipeTransferReq compressedReq =
        PipeTransferCompressedReq.toTPipeTransferReq(req, Collections.emptyList());

    Assert.assertEquals(
        TSStatusCode.PIPE_ERROR.getStatusCode(), receiver.receive(req).getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.PIPE_ERROR.getStatusCode(),
        receiver.receive(compressedReq).getStatus().getCode());
    Assert.assertEquals(
        TSStatusCode.NOT_LOGIN.getStatusCode(),
        receiver.receive(buildEmptyRawTabletTransferReq()).getStatus().getCode());
  }

  @Test
  public void testUnauthenticatedPipeTransferRejected() {
    final IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();

    final TPipeTransferResp resp = receiver.receive(buildEmptyRawTabletTransferReq());

    Assert.assertEquals(TSStatusCode.NOT_LOGIN.getStatusCode(), resp.getStatus().getCode());
  }

  @Test
  public void testUnauthenticatedWrappedPipeTransferRejected() throws IOException {
    final IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();
    final TPipeTransferReq rawReq = buildEmptyRawTabletTransferReq();

    final TPipeTransferResp compressedResp =
        receiver.receive(
            PipeTransferCompressedReq.toTPipeTransferReq(rawReq, Collections.emptyList()));
    Assert.assertEquals(
        TSStatusCode.NOT_LOGIN.getStatusCode(), compressedResp.getStatus().getCode());

    final TPipeTransferReq sliceReq =
        PipeTransferSliceReq.toTPipeTransferReq(
            0,
            PipeRequestType.TRANSFER_TABLET_RAW.getType(),
            0,
            1,
            rawReq.body.duplicate(),
            0,
            rawReq.body.limit());
    final TPipeTransferResp sliceResp = receiver.receive(sliceReq);
    Assert.assertEquals(TSStatusCode.NOT_LOGIN.getStatusCode(), sliceResp.getStatus().getCode());
  }

  @Test
  public void testIoTDBThriftReceiverV1HandshakeDoesNotAuthenticateTransfer() {
    final IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();
    try {
      final TPipeTransferResp handshakeResp =
          receiver.receive(
              PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
                  CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), handshakeResp.getStatus().getCode());

      final TPipeTransferResp transferResp = receiver.receive(buildEmptyRawTabletTransferReq());
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(), transferResp.getStatus().getCode());
    } catch (IOException e) {
      Assert.fail();
    } finally {
      receiver.handleExit();
    }
  }

  private TPipeTransferReq buildEmptyRawTabletTransferReq() {
    final TPipeTransferReq req = new TPipeTransferReq();
    req.setVersion(IoTDBSinkRequestVersion.VERSION_1.getVersion());
    req.setType(PipeRequestType.TRANSFER_TABLET_RAW.getType());
    req.setBody(ByteBuffer.allocate(0));
    return req;
  }
}
