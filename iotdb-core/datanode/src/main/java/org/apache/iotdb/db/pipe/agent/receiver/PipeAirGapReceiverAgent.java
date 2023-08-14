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

package org.apache.iotdb.db.pipe.agent.receiver;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.IoTDBConnectorRequestVersion;
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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.CRC32;

public class PipeAirGapReceiverAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeAirGapReceiverAgent.class);

  private final ThreadLocal<IoTDBThriftReceiver> receiverThreadLocal = new ThreadLocal<>();

  private final ServerSocket serverSocket =
      new ServerSocket(IoTDBDescriptor.getInstance().getConfig().getPipeAirGapReceivePort());
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private boolean allowSubmitListening = false;

  public PipeAirGapReceiverAgent() throws IOException {
    // Empty constructor
  }

  public void start() {
    executor.submit(this::runReceive);
    allowSubmitListening = true;
  }

  public void runReceive() {
    try {
      Socket socket = serverSocket.accept();
      new Thread(
              () -> {
                try {
                  receive(
                      socket,
                      ClusterPartitionFetcher.getInstance(),
                      ClusterSchemaFetcher.getInstance());
                } catch (IOException e) {
                  LOGGER.warn("Meet exception during pipe receiving", e);
                }
              })
          .start();
      if (allowSubmitListening) {
        executor.submit(this::runReceive);
      }
    } catch (IOException e) {
      LOGGER.warn("Unhandled exception during pipe air gap receiving", e);
    }
  }

  public void stop() {
    allowSubmitListening = false;
    executor.shutdown();
  }

  private boolean checkSum(byte[] bytes) {
    try {
      CRC32 crc32 = new CRC32();
      crc32.update(bytes, 4, bytes.length - 4);
      long checksum = crc32.getValue();
      return BytesUtils.bytesToLong(BytesUtils.subBytes(bytes, 0, 4)) == checksum;
    } catch (Exception e) {
      return false;
    }
  }

  private ByteBuffer readData(InputStream inputStream) throws IOException {
    int bufferSize = 1024;
    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    byte[] data = new byte[bufferSize];
    int bytesRead;
    while ((bytesRead = inputStream.read(data)) != -1) {
      byteBuffer.put(data, 0, bytesRead);
    }
    byteBuffer.flip();
    return byteBuffer;
  }

  public void receive(
      Socket socket, IPartitionFetcher partitionFetcher, ISchemaFetcher schemaFetcher)
      throws IOException {
    InputStream inputStream = socket.getInputStream();
    OutputStream outputStream = socket.getOutputStream();
    try {
      ByteBuffer byteBuffer = readData(inputStream);
      if (!checkSum(byteBuffer.array())) {
        // 0xff means failure
        outputStream.write((byte) 0xff);
        outputStream.flush();
        return;
      }
      // Throw the used checksum
      ReadWriteIOUtils.readLong(byteBuffer);
      // Pseudo request, to reuse the old logic
      TPipeTransferReq req =
          new TPipeTransferReq()
              .setVersion(ReadWriteIOUtils.readByte(byteBuffer))
              .setType(ReadWriteIOUtils.readShort(byteBuffer))
              .setBody(byteBuffer.slice());
      final byte reqVersion = req.getVersion();
      if (reqVersion == IoTDBConnectorRequestVersion.VERSION_1.getVersion()) {
        if (getReceiver(reqVersion)
                .receive(req, partitionFetcher, schemaFetcher)
                .getStatus()
                .getCode()
            != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          outputStream.write((byte) 0xff);
        } else {
          outputStream.write((byte) 0);
        }
      } else {
        outputStream.write((byte) 0xff);
      }
    } catch (IOException e) {
      outputStream.write((byte) 0xff);
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

  public void cleanPipeReceiverDir() {
    final File receiverFileDir =
        new File(IoTDBDescriptor.getInstance().getConfig().getPipeAirGapReceiverFileDir());

    try {
      FileUtils.deleteDirectory(receiverFileDir);
      LOGGER.info("Clean pipe receiver dir {} successfully.", receiverFileDir);
    } catch (Exception e) {
      LOGGER.warn("Clean pipe receiver dir {} failed.", receiverFileDir, e);
    }

    try {
      FileUtils.forceMkdir(receiverFileDir);
      LOGGER.info("Create pipe receiver dir {} successfully.", receiverFileDir);
    } catch (IOException e) {
      LOGGER.warn("Create pipe receiver dir {} failed.", receiverFileDir, e);
    }
  }
}
