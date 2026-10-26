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
  public void testIoTDBThriftReceiverV1HandshakeRejected() {
    IoTDBDataNodeReceiver receiver = new IoTDBDataNodeReceiver();
    try {
      final TPipeTransferResp handshakeResp =
          receiver.receive(
              PipeTransferDataNodeHandshakeV1Req.toTPipeTransferReq(
                  CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
      Assert.assertEquals(
          TSStatusCode.PIPE_HANDSHAKE_ERROR.getStatusCode(), handshakeResp.getStatus().getCode());
    } catch (IOException e) {
      Assert.fail();
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
