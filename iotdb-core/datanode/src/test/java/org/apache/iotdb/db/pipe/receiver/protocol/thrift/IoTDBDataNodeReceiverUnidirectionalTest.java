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

package org.apache.iotdb.db.pipe.receiver.protocol.thrift;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressorFactory;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferHandshakeV2Req;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferSliceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferDataNodeHandshakeV2Req;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IoTDBDataNodeReceiverUnidirectionalTest {

  @Test
  public void testRepeatedHandshakePreservesFileAndPendingSlices() throws Exception {
    verifyRepeatedHandshakePreservesFileAndPendingSlices(false);
  }

  @Test
  public void testCompressedHandshakePreservesFileAndPendingSlices() throws Exception {
    verifyRepeatedHandshakePreservesFileAndPendingSlices(true);
  }

  private void verifyRepeatedHandshakePreservesFileAndPendingSlices(final boolean compressed)
      throws Exception {
    final Path baseDir = Files.createTempDirectory("unidirectional-receiver");
    final TestReceiver receiver = new TestReceiver(baseDir);
    try {
      assertSuccess(receiver.receive(handshakeForTransfer(compressed)));
      assertSuccess(
          receiver.receive(
              PipeTransferTsFilePieceReq.toTPipeTransferReq(
                  "test.tsfile", 0, new byte[] {1, 2, 3})));
      final File receiverDir = receiver.getReceiverDir();
      final TPipeTransferReq piece =
          PipeTransferTsFilePieceReq.toTPipeTransferReq("test.tsfile", 3, new byte[] {4, 5, 6});
      final int split = piece.body.remaining() / 2;
      assertSuccess(receiver.receive(slice(piece, 0, 0, split)));

      assertSuccess(receiver.receive(handshakeForTransfer(compressed)));

      Assert.assertEquals(receiverDir, receiver.getReceiverDir());
      Assert.assertTrue(receiverDir.exists());
      assertSuccess(receiver.receive(slice(piece, 1, split, piece.body.limit())));
      Assert.assertArrayEquals(
          new byte[] {1, 2, 3, 4, 5, 6},
          Files.readAllBytes(receiverDir.toPath().resolve("test.tsfile")));
      Assert.assertEquals(1, receiver.handshakeCount);
    } finally {
      receiver.handleExit();
      Files.deleteIfExists(baseDir);
    }
  }

  @Test
  public void testAcknowledgedAndChangedHandshakesStillResetSession() throws Exception {
    final Path baseDir = Files.createTempDirectory("unidirectional-receiver");
    final TestReceiver receiver = new TestReceiver(baseDir);
    try {
      assertSuccess(receiver.receive(handshake(true, true)));
      File oldDir = receiver.getReceiverDir();

      assertSuccess(receiver.receive(handshake(true, false)));
      Assert.assertFalse(oldDir.exists());
      oldDir = receiver.getReceiverDir();

      assertSuccess(receiver.receive(handshake(false, false)));
      Assert.assertFalse(oldDir.exists());
      oldDir = receiver.getReceiverDir();

      assertSuccess(receiver.receive(handshake(true, false)));
      Assert.assertFalse(oldDir.exists());
      Assert.assertEquals(4, receiver.handshakeCount);
    } finally {
      receiver.handleExit();
      Files.deleteIfExists(baseDir);
    }
  }

  @Test
  public void testFailedHandshakeIsRetriedAndAuthenticationIsRechecked() throws Exception {
    final Path baseDir = Files.createTempDirectory("unidirectional-receiver");
    final TestReceiver receiver = new TestReceiver(baseDir);
    try {
      receiver.loginStatus = TSStatusCode.NOT_LOGIN;
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          receiver.receive(handshake(true, true)).getStatus().getCode());
      receiver.loginStatus = TSStatusCode.SUCCESS_STATUS;
      assertSuccess(receiver.receive(handshake(true, true)));
      Assert.assertEquals(2, receiver.handshakeCount);

      receiver.loginStatus = TSStatusCode.NOT_LOGIN;
      Assert.assertEquals(
          TSStatusCode.NOT_LOGIN.getStatusCode(),
          receiver.receive(handshake(true, true)).getStatus().getCode());
      receiver.loginStatus = TSStatusCode.SUCCESS_STATUS;
      final File oldDir = receiver.getReceiverDir();
      assertSuccess(receiver.receive(handshake(true, true)));
      Assert.assertFalse(oldDir.exists());
      Assert.assertEquals(3, receiver.handshakeCount);
    } finally {
      receiver.handleExit();
      Files.deleteIfExists(baseDir);
    }
  }

  private static TPipeTransferReq handshake(
      final boolean unidirectional, final boolean validateTsFile) throws IOException {
    final Map<String, String> params = new HashMap<>();
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID, "sender-cluster");
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, "root");
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, "root");
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE,
        Boolean.toString(validateTsFile));
    final TPipeTransferReq req = PipeTransferDataNodeHandshakeV2Req.toTPipeTransferReq(params);
    return new AirGapPseudoTPipeTransferRequest()
        .setStrictUnidirectional(unidirectional)
        .setVersion(req.version)
        .setType(req.type)
        .setBody(req.body);
  }

  private static TPipeTransferReq handshakeForTransfer(final boolean compressed)
      throws IOException {
    final TPipeTransferReq handshake = handshake(true, true);
    if (!compressed) {
      return handshake;
    }
    final TPipeTransferReq req =
        PipeTransferCompressedReq.toTPipeTransferReq(
            handshake,
            Collections.singletonList(
                PipeCompressorFactory.getCompressor(
                    PipeCompressor.PipeCompressionType.GZIP.getIndex())));
    return new AirGapPseudoTPipeTransferRequest()
        .setStrictUnidirectional(true)
        .setVersion(req.version)
        .setType(req.type)
        .setBody(req.body);
  }

  private static TPipeTransferReq slice(
      final TPipeTransferReq req, final int index, final int start, final int end)
      throws IOException {
    final ByteBuffer body = req.body.duplicate();
    return PipeTransferSliceReq.toTPipeTransferReq(1, req.type, index, 2, body, start, end);
  }

  private static void assertSuccess(final TPipeTransferResp resp) {
    Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
  }

  private static class TestReceiver extends IoTDBDataNodeReceiver {

    private final Path baseDir;
    private int handshakeCount;
    private TSStatusCode loginStatus = TSStatusCode.SUCCESS_STATUS;

    private TestReceiver(final Path baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    protected TPipeTransferResp handleTransferHandshakeV2(final PipeTransferHandshakeV2Req req)
        throws IOException {
      handshakeCount++;
      return super.handleTransferHandshakeV2(req);
    }

    @Override
    protected String getClusterId() {
      return "receiver-cluster";
    }

    @Override
    protected String getReceiverFileBaseDir() {
      return baseDir.toString();
    }

    @Override
    protected String getSenderHost() {
      return "127.0.0.1";
    }

    @Override
    protected String getSenderPort() {
      return "9999";
    }

    @Override
    protected TSStatus login() {
      return RpcUtils.getStatus(loginStatus);
    }

    private File getReceiverDir() {
      return receiverFileDirWithIdSuffix.get();
    }
  }
}
