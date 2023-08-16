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

import static org.apache.iotdb.commons.utils.BasicStructureSerDeUtil.LONG_LEN;

public class IoTDBAirGapReceiver extends WrappedRunnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapReceiver.class);

  private final Socket socket;
  private final long receiverId;

  private final IoTDBThriftReceiverAgent agent;
  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;

  public IoTDBAirGapReceiver(Socket socket, long receiverId) {
    this.socket = socket;
    this.receiverId = receiverId;

    agent = PipeAgent.receiver().thrift();
    partitionFetcher = ClusterPartitionFetcher.getInstance();
    schemaFetcher = ClusterSchemaFetcher.getInstance();
  }

  @Override
  public void runMayThrow() throws Throwable {
    socket.setSoTimeout((int) PipeConfig.getInstance().getPipeConnectorTimeoutMs());
    socket.setKeepAlive(true);

    LOGGER.info("Pipe air gap receiver {} started. Socket: {}", receiverId, socket);

    try {
      while (!socket.isClosed()) {
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

    final ByteBuffer resultBuffer = ByteBuffer.allocate(length);
    final byte[] readBuffer = new byte[length];

    int alreadyReadBytes = 0;
    int currentReadBytes;
    while (alreadyReadBytes < length) {
      currentReadBytes = inputStream.read(readBuffer, 0, length - alreadyReadBytes);
      resultBuffer.put(readBuffer, 0, currentReadBytes);
      alreadyReadBytes += currentReadBytes;
    }
    return resultBuffer.array();
  }

  /**
   * Read the length of the following data. The thread may typically block here when there is no
   * data to read.
   */
  private int readLength(InputStream inputStream) throws IOException {
    byte[] lengthBytes0 = new byte[4];
    if (inputStream.read(lengthBytes0) < 4) {
      return 0;
    }

    // for double check
    byte[] lengthBytes1 = new byte[4];
    if (inputStream.read(lengthBytes1) < 4) {
      return 0;
    }

    return Arrays.equals(lengthBytes0, lengthBytes1) ? BytesUtils.bytesToInt(lengthBytes0) : 0;
  }
}
