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

package org.apache.iotdb.db.pipe.receiver.protocol.airgap;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.receiver.IoTDBReceiver;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapELanguageConstant;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.commons.pipe.sink.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.IoTDBSinkRequestVersion;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiverAgent;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.BytesUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.nio.ByteBuffer;

public class IoTDBAirGapReceiverTest {

  @Test
  public void testRejectOversizedAirGapPayload() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final int originalMaxPayload = commonConfig.getPipeAirGapReceiverMaxPayloadSizeInBytes();

    try {
      commonConfig.setPipeAirGapReceiverMaxPayloadSizeInBytes(16);
      final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(new Socket(), 1L);

      final byte[] oversizedLength = BytesUtils.intToBytes(32);
      final InputStream inputStream =
          new ByteArrayInputStream(BytesUtils.concatByteArray(oversizedLength, oversizedLength));

      final IOException exception =
          Assert.assertThrows(IOException.class, () -> receiver.readData(inputStream));
      Assert.assertTrue(exception.getMessage().contains("payload length"));
    } finally {
      commonConfig.setPipeAirGapReceiverMaxPayloadSizeInBytes(originalMaxPayload);
    }
  }

  @Test
  public void testRejectNestedELanguagePrefix() throws Exception {
    final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(new Socket(), 2L);

    final InputStream inputStream =
        new ByteArrayInputStream(
            BytesUtils.concatByteArray(
                AirGapELanguageConstant.E_LANGUAGE_PREFIX,
                AirGapELanguageConstant.E_LANGUAGE_PREFIX));

    final IOException exception =
        Assert.assertThrows(IOException.class, () -> receiver.readData(inputStream));
    Assert.assertTrue(exception.getMessage().contains("nested E-Language prefix"));
  }

  @Test
  public void testTemporaryUnavailableRetryTimeoutReturnsFail() throws Exception {
    final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    final long originalRetryLocalIntervalMs = commonConfig.getPipeAirGapRetryLocalIntervalMs();
    final long originalRetryMaxMs = commonConfig.getPipeAirGapRetryMaxMs();

    try {
      commonConfig.setPipeAirGapRetryLocalIntervalMs(0);
      commonConfig.setPipeAirGapRetryMaxMs(1);

      final RecordingSocket socket = new RecordingSocket();
      final IoTDBAirGapReceiver receiver = new IoTDBAirGapReceiver(socket, 3L);
      final StubIoTDBDataNodeReceiverAgent stubAgent = new StubIoTDBDataNodeReceiverAgent();
      stubAgent.setStubReceiver(
          new StubReceiver(
              new TSStatus(
                  TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode())));
      setField(receiver, "agent", stubAgent);

      final AirGapPseudoTPipeTransferRequest req = new AirGapPseudoTPipeTransferRequest();
      req.setVersion(IoTDBSinkRequestVersion.VERSION_1.getVersion());
      req.setType((short) 0);
      req.setBody(ByteBuffer.allocate(0));

      final Method handleReq =
          IoTDBAirGapReceiver.class.getDeclaredMethod(
              "handleReq", AirGapPseudoTPipeTransferRequest.class, long.class);
      handleReq.setAccessible(true);
      handleReq.invoke(receiver, req, System.currentTimeMillis() - 10_000L);

      Assert.assertArrayEquals(AirGapOneByteResponse.FAIL, socket.getWrittenBytes());
    } finally {
      commonConfig.setPipeAirGapRetryLocalIntervalMs(originalRetryLocalIntervalMs);
      commonConfig.setPipeAirGapRetryMaxMs(originalRetryMaxMs);
    }
  }

  private static void setField(final Object target, final String fieldName, final Object value)
      throws Exception {
    final Field field = IoTDBAirGapReceiver.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static class RecordingSocket extends Socket {

    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    @Override
    public OutputStream getOutputStream() {
      return outputStream;
    }

    byte[] getWrittenBytes() {
      return outputStream.toByteArray();
    }
  }

  private static class StubIoTDBDataNodeReceiverAgent extends IoTDBDataNodeReceiverAgent {

    void setStubReceiver(final IoTDBReceiver receiver) {
      setReceiverWithSpecifiedClient(null, receiver);
    }
  }

  private static class StubReceiver implements IoTDBReceiver {

    private final TPipeTransferResp response;

    private StubReceiver(final TSStatus status) {
      response = new TPipeTransferResp(status);
    }

    @Override
    public TPipeTransferResp receive(final TPipeTransferReq req) {
      return response;
    }

    @Override
    public void handleExit() {
      // noop for unit test
    }

    @Override
    public IoTDBSinkRequestVersion getVersion() {
      return IoTDBSinkRequestVersion.VERSION_1;
    }
  }
}
