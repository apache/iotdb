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
 *
 */
package org.apache.iotdb.db.sync.transport.client;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.sync.SyncConnectionException;
import org.apache.iotdb.commons.exception.sync.SyncHandshakeException;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.sync.pipedata.PipeData;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
import org.apache.iotdb.db.sync.sender.pipe.Pipe;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static org.apache.iotdb.commons.sync.utils.SyncConstant.DATA_CHUNK_SIZE;

public class IoTDBSyncClient implements ISyncClient {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBSyncClient.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final int TRANSFER_BUFFER_SIZE_IN_BYTES = 1 * 1024 * 1024;

  private TTransport transport = null;
  private volatile IClientRPCService.Client serviceClient = null;

  /* remote IP address*/
  private final String ipAddress;
  /* remote port */
  private final int port;
  /* database name that client belongs to*/
  private final String databaseName;

  private final Pipe pipe;

  /**
   * Create IoTDBSyncClient only for data transfer
   *
   * @param pipe sync task
   * @param remoteAddress remote ip address
   * @param port remote port
   * @param databaseName database name that client belongs to
   */
  public IoTDBSyncClient(Pipe pipe, String remoteAddress, int port, String databaseName) {
    RpcTransportFactory.setThriftMaxFrameSize(config.getThriftMaxFrameSize());
    this.pipe = pipe;
    this.ipAddress = remoteAddress;
    this.port = port;
    this.databaseName = databaseName;
  }

  /**
   * Create IoTDBSyncClient only for heartbeat
   *
   * @param pipe sync task
   * @param remoteAddress remote ip address
   * @param port remote port
   */
  public IoTDBSyncClient(Pipe pipe, String remoteAddress, int port) {
    this(pipe, remoteAddress, port, "");
  }

  /**
   * Create thrift connection to receiver. Check IoTDB version to make sure compatibility
   *
   * @return true if S; false if failed to check IoTDB version.
   * @throws SyncConnectionException cannot create connection to receiver
   * @throws SyncHandshakeException cannot handshake with receiver
   */
  @Override
  public synchronized void handshake() throws SyncConnectionException {
    if (transport != null && transport.isOpen()) {
      transport.close();
    }

    try {
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              new TSocket(
                  TConfigurationConst.defaultTConfiguration,
                  ipAddress,
                  port,
                  SyncConstant.SOCKET_TIMEOUT_MILLISECONDS,
                  SyncConstant.CONNECT_TIMEOUT_MILLISECONDS));
      TProtocol protocol;
      if (config.isRpcThriftCompressionEnable()) {
        protocol = new TCompactProtocol(transport);
      } else {
        protocol = new TBinaryProtocol(transport);
      }
      serviceClient = new IClientRPCService.Client(protocol);

      // Underlay socket open.
      if (!transport.isOpen()) {
        transport.open();
      }

      TSyncIdentityInfo identityInfo =
          new TSyncIdentityInfo(
              pipe.getName(), pipe.getCreateTime(), config.getIoTDBMajorVersion(), databaseName);
      TSStatus status = serviceClient.handshake(identityInfo);
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new SyncHandshakeException(
            String.format(
                "the receiver rejected the synchronization task because %s", status.message));
      }
    } catch (TException e) {
      throw new SyncConnectionException(
          String.format("cannot connect to the receiver because %s", e.getMessage()));
    }
  }

  /**
   * Send {@link PipeData} to receiver and load. If PipeData is TsFilePipeData, The TsFiles will be
   * transferred before the PipeData transfer.
   *
   * @return true if success; false if failed to send or load.
   * @throws SyncConnectionException cannot create connection to receiver
   */
  @Override
  public boolean send(PipeData pipeData) throws SyncConnectionException {
    if (pipeData instanceof TsFilePipeData) {
      try {
        for (File file : ((TsFilePipeData) pipeData).getTsFiles(true)) {
          if (!transportSingleFilePieceByPiece(file)) {
            return false;
          }
        }
      } catch (IOException e) {
        logger.error(String.format("Get TsFiles error, because %s.", e), e);
        return false;
      }
    }
    try {
      return transportPipeData(pipeData);
    } catch (IOException e) {
      logger.error(String.format("Transport PipeData error, because %s.", e), e);
      return false;
    }
  }

  /**
   * Transport file piece by piece.
   *
   * @return true if success; false if failed.
   * @throws SyncConnectionException Connection exception, wait for a while and try again
   * @throws IOException Serialize error.
   */
  private boolean transportSingleFilePieceByPiece(File file)
      throws SyncConnectionException, IOException {
    // Cut the file into pieces to send
    long position = 0;
    long limit = getFileSizeLimit(file);

    // Try small piece to rebase the file position.
    byte[] buffer = new byte[TRANSFER_BUFFER_SIZE_IN_BYTES];
    int dataLength;
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      while (position < limit) {
        // Normal piece.
        if (position != 0L && buffer.length != DATA_CHUNK_SIZE) {
          buffer = new byte[DATA_CHUNK_SIZE];
        }
        dataLength =
            randomAccessFile.read(buffer, 0, Math.min(buffer.length, (int) (limit - position)));
        if (dataLength == -1) {
          break;
        }
        ByteBuffer buffToSend = ByteBuffer.wrap(buffer, 0, dataLength);
        TSyncTransportMetaInfo metaInfo = new TSyncTransportMetaInfo(file.getName(), position);

        TSStatus status = serviceClient.sendFile(metaInfo, buffToSend);

        if ((status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
          // Success
          position += dataLength;
        } else if (status.code == TSStatusCode.SYNC_FILE_REDIRECTION_ERROR.getStatusCode()) {
          position = Long.parseLong(status.message);
        } else if (status.code == TSStatusCode.SYNC_FILE_ERROR.getStatusCode()) {
          logger.error(
              "Receiver failed to receive data from {} because {}, abort.",
              file.getAbsoluteFile(),
              status.message);
          return false;
        }
      }
    } catch (TException e) {
      logger.error("Cannot sync data with receiver. ", e);
      throw new SyncConnectionException(e);
    }
    return true;
  }

  private long getFileSizeLimit(File file) {
    File offset = new File(file.getPath() + SyncConstant.MODS_OFFSET_FILE_SUFFIX);
    if (offset.exists()) {
      try (BufferedReader br = new BufferedReader(new FileReader(offset))) {
        return Long.parseLong(br.readLine());
      } catch (IOException e) {
        logger.error(
            String.format("Deserialize offset of file %s error, because %s.", file.getPath(), e));
      }
    }
    return file.length();
  }

  /**
   * Transport and load PipeData
   *
   * @return true if success; false if failed.
   * @throws SyncConnectionException Connection exception, wait for a while and try again
   * @throws IOException Serialize error.
   */
  private boolean transportPipeData(PipeData pipeData) throws SyncConnectionException, IOException {
    try {
      byte[] buffer = pipeData.serialize();
      ByteBuffer buffToSend = ByteBuffer.wrap(buffer);
      TSStatus status = serviceClient.sendPipeData(buffToSend);
      if (status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        logger.info("Transport PipeData {} Successfully", pipeData);
      } else if (status.code == TSStatusCode.PIPESERVER_ERROR.getStatusCode()) {
        logger.error("Receiver failed to load PipeData {}, skip it.", pipeData);
        return false;
      }
    } catch (TException e) {
      throw new SyncConnectionException(e);
    }
    return true;
  }

  @Override
  public void close() {
    if (transport != null) {
      transport.close();
      transport = null;
    }
  }
}
