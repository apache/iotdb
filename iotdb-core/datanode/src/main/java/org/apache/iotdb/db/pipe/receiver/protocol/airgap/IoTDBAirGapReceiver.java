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
import org.apache.iotdb.commons.concurrent.WrappedRunnable;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapELanguageConstant;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapOneByteResponse;
import org.apache.iotdb.commons.pipe.connector.payload.airgap.AirGapPseudoTPipeTransferRequest;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.receiver.protocol.thrift.IoTDBDataNodeReceiverAgent;
import org.apache.iotdb.db.protocol.session.ClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
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

  private final IoTDBDataNodeReceiverAgent agent;

  private boolean isELanguagePayload;

  public IoTDBAirGapReceiver(final Socket socket, final long receiverId) {
    this.socket = socket;
    this.receiverId = receiverId;

    agent = PipeDataNodeAgent.receiver().thrift();
  }

  @Override
  public void runMayThrow() throws Throwable {
    socket.setSoTimeout(PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs());
    socket.setKeepAlive(true);

    LOGGER.info("Pipe air gap receiver {} started. Socket: {}", receiverId, socket);

    final ClientSession session = new ClientSession(socket);
    SessionManager.getInstance().registerSession(session);

    try {
      while (!socket.isClosed()) {
        isELanguagePayload = false;
        receive();
      }
      LOGGER.info(
          "Pipe air gap receiver {} closed because socket is closed. Socket: {}",
          receiverId,
          socket);
    } catch (final Exception e) {
      LOGGER.warn(
          "Pipe air gap receiver {} closed because of exception. Socket: {}",
          receiverId,
          socket,
          e);
      throw e;
    } finally {
      PipeDataNodeAgent.receiver().thrift().handleClientExit();
      SessionManager.getInstance()
          .closeSession(session, Coordinator.getInstance()::cleanupQueryExecution);
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
      final TPipeTransferResp resp = agent.receive(req);

      final TSStatus status = resp.getStatus();
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        ok();
      } else if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()
          || status.getCode()
              == TSStatusCode.PIPE_RECEIVER_IDEMPOTENT_CONFLICT_EXCEPTION.getStatusCode()) {
        LOGGER.info(
            "TSStatus:{} is encountered at the air gap receiver, will ignore.", resp.getStatus());
        ok();
      } else {
        LOGGER.warn(
            "Handle data failed, receiverId: {}, status: {}, req: {}",
            receiverId,
            resp.getStatus(),
            req);
        fail();
      }
    } catch (final PipeConnectionException e) {
      LOGGER.info("Socket closed when listening to data. Because: {}", e.getMessage());
      socket.close();
    } catch (final Exception e) {
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
    } catch (final Exception e) {
      // ArrayIndexOutOfBoundsException when bytes.length < LONG_LEN
      return false;
    }
  }

  private byte[] readData(final InputStream inputStream) throws IOException {
    final int length = readLength(inputStream);

    if (length <= 0) {
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
  private int readLength(final InputStream inputStream) throws IOException {
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
    // For double check
    return Arrays.equals(
            dataLengthBytes, BytesUtils.subBytes(doubleIntLengthBytes, INT_LEN, INT_LEN))
        ? BytesUtils.bytesToInt(dataLengthBytes)
        : 0;
  }

  /**
   * Read to the buffer until it is full.
   *
   * @param inputStream the input socket stream
   * @param readBuffer the buffer to read into
   * @throws IOException if any IOException occurs
   * @throws PipeConnectionException if the socket is closed during listening
   */
  private void readTillFull(final InputStream inputStream, final byte[] readBuffer)
      throws IOException, PipeConnectionException {
    int alreadyReadBytes = 0;
    while (alreadyReadBytes < readBuffer.length) {
      final int readBytes =
          inputStream.read(readBuffer, alreadyReadBytes, readBuffer.length - alreadyReadBytes);
      // In socket input stream readBytes == -1 indicates EOF, namely the
      // socket is closed
      if (readBytes == -1) {
        throw new PipeConnectionException("Socket closed when executing readTillFull.");
      }
      alreadyReadBytes += readBytes;
    }
  }

  /**
   * Skip given number of bytes of the buffer until enough bytes is skipped.
   *
   * @param inputStream the input socket stream
   * @param length the length to skip
   * @throws IOException if any IOException occurs
   * @throws PipeConnectionException if the socket is closed during skipping
   */
  private void skipTillEnough(final InputStream inputStream, final long length)
      throws IOException, PipeConnectionException {
    long currentSkippedBytes = 0;
    while (currentSkippedBytes < length) {
      final long skippedBytes = inputStream.skip(length - currentSkippedBytes);
      // In socket input stream skippedBytes == 0 indicates EOF, namely the
      // socket is closed
      if (skippedBytes == 0) {
        throw new PipeConnectionException("Socket closed when executing skipTillEnough.");
      }
      currentSkippedBytes += skippedBytes;
    }
  }
}
