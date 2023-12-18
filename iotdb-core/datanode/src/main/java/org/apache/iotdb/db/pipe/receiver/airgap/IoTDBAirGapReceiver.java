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

package org.apache.iotdb.db.pipe.receiver.airgap;

import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.connector.payload.airgap.AirGapELanguageConstant;
import org.apache.iotdb.db.pipe.connector.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.db.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.db.pipe.receiver.thrift.IoTDBThriftReceiverAgent;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

import static org.apache.iotdb.commons.utils.BasicStructureSerDeUtil.INT_LEN;
import static org.apache.iotdb.commons.utils.BasicStructureSerDeUtil.LONG_LEN;

public class IoTDBAirGapReceiver extends WrappedRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapReceiver.class);

  private final Socket socket;
  private final long receiverId;

  private final IoTDBThriftReceiverAgent agent;
  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  private boolean isELanguagePayload;

  public IoTDBAirGapReceiver(Socket socket, long receiverId) {
    this.socket = socket;
    this.receiverId = receiverId;

    agent = PipeAgent.receiver().thrift();
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
  }

  @Override
  public void runMayThrow() throws Throwable {
    socket.setSoTimeout((int) PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs());
    socket.setKeepAlive(true);

    LOGGER.info("Pipe air gap receiver {} started. Socket: {}", receiverId, socket);

    try {
      while (!socket.isClosed()) {
        isELanguagePayload = false;
        receive();
      }
      LOGGER.info(
          "Pipe air gap receiver {} closed because socket is closed. Socket: {}",
          receiverId,
          socket);
    } catch (Exception e) {
      LOGGER.warn(
          "Pipe air gap receiver {} closed because of exception. Socket: {}",
          receiverId,
          socket,
          e);
      throw e;
    } finally {
      PipeAgent.receiver().thrift().handleClientExit();
      socket.close();
    }
  }

  private void receive() throws IOException {
    final InputStream inputStream = new BufferedInputStream(socket.getInputStream());

    try {
      final byte[] data = readData(inputStream);

      if (!checkSum(data)) {
        LOGGER.warn("Checksum failed, receiverId: {}", receiverId);
        fail();
        return;
      }

      // Removed the used checksum
      final ByteBuffer byteBuffer = ByteBuffer.wrap(data, LONG_LEN, data.length - LONG_LEN);

      // Pseudo request, to reuse logic in IoTDBThriftReceiverAgent
      final AirGapPseudoTPipeTransferRequest req =
          (AirGapPseudoTPipeTransferRequest)
              new AirGapPseudoTPipeTransferRequest()
                  .setVersion(ReadWriteIOUtils.readByte(byteBuffer))
                  .setType(ReadWriteIOUtils.readShort(byteBuffer))
                  .setBody(byteBuffer.slice());
      final TPipeTransferResp resp = agent.receive(req, partitionFetcher, schemaFetcher);

      if (resp.getStatus().code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        ok();
      } else {
        LOGGER.warn(
            "Handle data failed, receiverId: {}, status: {}, req: {}",
            receiverId,
            resp.getStatus(),
            req);
        fail();
      }
    } catch (Exception e) {
      LOGGER.warn("Exception during handling receiving, receiverId: {}", receiverId, e);
      fail();
    }
  }

  private void ok() throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    outputStream.write(AirGapOneByteResponse.OK);
    outputStream.flush();
  }

  private void fail() throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    outputStream.write(AirGapOneByteResponse.FAIL);
    outputStream.flush();
  }

  private boolean checkSum(byte[] bytes) {
    try {
      final CRC32 crc32 = new CRC32();
      crc32.update(bytes, LONG_LEN, bytes.length - LONG_LEN);
      return BytesUtils.bytesToLong(BytesUtils.subBytes(bytes, 0, LONG_LEN)) == crc32.getValue();
    } catch (Exception e) {
      // ArrayIndexOutOfBoundsException when bytes.length < LONG_LEN
      return false;
    }
  }

  private byte[] readData(InputStream inputStream) throws IOException {
    final int length = readLength(inputStream);

    if (length == 0) {
      // Will fail() after checkSum()
      return new byte[0];
    }

    final byte[] resultBuffer = new byte[length];
    readTillFull(inputStream, resultBuffer);
    if (isELanguagePayload) {
      skipTillEnough(inputStream, AirGapELanguageConstant.E_LANGUAGE_SUFFIX.length);
    }
    return resultBuffer;
  }

  /**
   * Read the length of the following data. The thread may typically block here when there is no
   * data to read.
   */
  private int readLength(InputStream inputStream) throws IOException {
    final byte[] doubleIntLengthBytes = new byte[2 * INT_LEN];
    readTillFull(inputStream, doubleIntLengthBytes);

    // Check the header of the request, if it is an E-Language request, skip the E-Language header.
    // We assert AirGapELanguageConstant.E_LANGUAGE_PREFIX.length > 2 * INT_LEN here.
    if (Arrays.equals(
        doubleIntLengthBytes,
        BytesUtils.subBytes(AirGapELanguageConstant.E_LANGUAGE_PREFIX, 0, 2 * INT_LEN))) {
      isELanguagePayload = true;
      skipTillEnough(
          inputStream, (long) AirGapELanguageConstant.E_LANGUAGE_PREFIX.length - 2 * INT_LEN);
      return readLength(inputStream);
    }

    final byte[] dataLengthBytes = BytesUtils.subBytes(doubleIntLengthBytes, 0, INT_LEN);
    // for double check
    return Arrays.equals(
            dataLengthBytes, BytesUtils.subBytes(doubleIntLengthBytes, INT_LEN, INT_LEN))
        ? BytesUtils.bytesToInt(dataLengthBytes)
        : 0;
  }

  private void readTillFull(InputStream inputStream, byte[] readBuffer) throws IOException {
    int alreadyReadBytes = 0;
    while (alreadyReadBytes < readBuffer.length) {
      alreadyReadBytes +=
          inputStream.read(readBuffer, alreadyReadBytes, readBuffer.length - alreadyReadBytes);
    }
  }

  private void skipTillEnough(InputStream inputStream, long length) throws IOException {
    int currentSkippedBytes = 0;
    while (currentSkippedBytes < length) {
      currentSkippedBytes += inputStream.skip(length - currentSkippedBytes);
    }
  }
}
