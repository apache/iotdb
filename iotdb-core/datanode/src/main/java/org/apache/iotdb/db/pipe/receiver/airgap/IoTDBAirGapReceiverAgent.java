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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.pipe.connector.protocol.IoTDBConnectorRequestVersion;
import org.apache.iotdb.db.pipe.receiver.thrift.IoTDBThriftReceiver;
import org.apache.iotdb.db.pipe.receiver.thrift.IoTDBThriftReceiverV1;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;

public class IoTDBAirGapReceiverAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBAirGapReceiverAgent.class);

  private final ThreadLocal<IoTDBThriftReceiver> receiverThreadLocal = new ThreadLocal<>();

  private final ServerSocket serverSocket;

  {
    try {
      serverSocket = new ServerSocket(PipeConfig.getInstance().getPipeAirGapReceiverPort());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private boolean allowSubmitListening = false;

  public IoTDBAirGapReceiverAgent() {
    // Empty constructor
  }

  public void runReceive() {
    try {
      Socket socket = serverSocket.accept();
      new Thread(
              () -> {
                try {
                  socketRunReceive(
                      socket,
                      ClusterPartitionFetcher.getInstance(),
                      ClusterSchemaFetcher.getInstance());
                } catch (IOException e) {
                  LOGGER.warn("Meet exception during pipe receiving", e);
                }
              })
          .start();
    } catch (IOException e) {
      LOGGER.warn("Unhandled exception during pipe air gap receiving", e);
    }
    if (allowSubmitListening) {
      executor.submit(this::runReceive);
    }
  }

  private void socketRunReceive(
      Socket socket, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher)
      throws IOException {
    socket.setKeepAlive(true);
    while (!socket.isClosed() && socket.isConnected()) {
      receive(socket, partitionFetcher, schemaFetcher);
    }
    handleClientExit();
  }

  private boolean checkSum(byte[] bytes) {
    try {
      CRC32 crc32 = new CRC32();
      crc32.update(bytes, 8, bytes.length - 8);
      long checksum = crc32.getValue();
      return BytesUtils.bytesToLong(BytesUtils.subBytes(bytes, 0, 8)) == checksum;
    } catch (Exception e) {
      return false;
    }
  }

  private int readLength(InputStream inputStream) throws IOException {
    int bytesRead;
    byte[] lengthBytes = new byte[4];
    // The thread may typically block here
    bytesRead = inputStream.read(lengthBytes);
    if (bytesRead < 4) {
      return 0;
    }

    byte[] lengthBytesCopy = new byte[4];
    bytesRead = inputStream.read(lengthBytesCopy);
    if (bytesRead < 4) {
      return 0;
    }

    if (Arrays.equals(lengthBytes, lengthBytesCopy)) {
      return BytesUtils.bytesToInt(lengthBytes);
    }
    return 0;
  }

  private byte[] readData(InputStream inputStream) throws IOException {
    int length = readLength(inputStream);
    if (length == 0) {
      return new byte[0];
    }

    int bytesRead;
    ByteBuffer byteBuffer = ByteBuffer.allocate(length);
    byte[] data = new byte[length];
    int sum = 0;
    while (sum < length) {
      bytesRead = inputStream.read(data, 0, length - sum);
      sum += bytesRead;
      byteBuffer.put(data, 0, bytesRead);
    }
    return byteBuffer.array();
  }

  public void receive(
      Socket socket, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher)
      throws IOException {
    InputStream inputStream = socket.getInputStream();
    OutputStream outputStream = socket.getOutputStream();
    try {
      byte[] data = readData(inputStream);
      if (!checkSum(data)) {
        // 0xff means failure
        outputStream.write(new byte[] {(byte) 0xff});
        outputStream.flush();
        return;
      }
      // Throw the used checksum
      ByteBuffer byteBuffer = ByteBuffer.wrap(data, 8, data.length - 8);
      // Pseudo request, to reuse the old logic
      TPipeTransferReq req =
          new TPipeTransferReq()
              .setVersion(ReadWriteIOUtils.readByte(byteBuffer))
              .setType(ReadWriteIOUtils.readShort(byteBuffer))
              .setBody(byteBuffer.slice());
      final byte reqVersion = req.getVersion();
      if (reqVersion == IoTDBConnectorRequestVersion.VERSION_1.getVersion()) {
        TSStatus result =
            getReceiver(reqVersion).receive(req, partitionFetcher, schemaFetcher).getStatus();
        if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn("Handle data failed, status: {}, req: {}", result, req);
          outputStream.write(new byte[] {(byte) 0xff});
        } else {
          outputStream.write(new byte[] {(byte) 0});
        }
      } else {
        LOGGER.warn(
            "Handle data failed because the request version {} is not currently supported. req: {}",
            reqVersion,
            req);
        outputStream.write(new byte[] {(byte) 0xff});
      }
    } catch (IOException e) {
      LOGGER.warn("Exception during handling receiving.", e);
      outputStream.write(new byte[] {(byte) 0xff});
    }
    outputStream.flush();
  }

  private IoTDBThriftReceiver getReceiver(byte reqVersion) {
    if (receiverThreadLocal.get() == null) {
      return setAndGetReceiver(reqVersion);
    }

    final byte receiverThreadLocalVersion = receiverThreadLocal.get().getVersion().getVersion();
    if (receiverThreadLocalVersion != reqVersion) {
      LOGGER.warn(
          "The receiver version {} is different from the sender version {},"
              + " the receiver will be reset to the sender version.",
          receiverThreadLocalVersion,
          reqVersion);
      receiverThreadLocal.get().handleExit();
      receiverThreadLocal.remove();
      return setAndGetReceiver(reqVersion);
    }

    return receiverThreadLocal.get();
  }

  private IoTDBThriftReceiver setAndGetReceiver(byte reqVersion) {
    if (reqVersion == IoTDBConnectorRequestVersion.VERSION_1.getVersion()) {
      receiverThreadLocal.set(new IoTDBThriftReceiverV1());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupported pipe version %d", reqVersion));
    }
    return receiverThreadLocal.get();
  }

  public void handleClientExit() {
    final IoTDBThriftReceiver receiver = receiverThreadLocal.get();
    if (receiver != null) {
      receiver.handleExit();
      receiverThreadLocal.remove();
    }
  }

  @Override
  public void start() throws StartupException {
    executor.submit(this::runReceive);
    allowSubmitListening = true;
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    allowSubmitListening = false;
    executor.shutdown();

    IService.super.shutdown(milliseconds);
  }

  @Override
  public void stop() {
    allowSubmitListening = false;
    executor.shutdown();
  }

  /**
   * Get the name of the the service.
   *
   * @return current service name
   */
  @Override
  public ServiceType getID() {
    return ServiceType.AIR_GAP_SERVICE;
  }
}
