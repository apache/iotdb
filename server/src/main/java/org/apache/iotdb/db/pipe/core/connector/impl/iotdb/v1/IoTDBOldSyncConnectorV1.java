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

package org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.sync.SyncConnectionException;
import org.apache.iotdb.commons.exception.sync.SyncHandshakeException;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.pipe.config.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.connector.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.apache.commons.lang.NotImplementedException;
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
import java.util.Arrays;

import static org.apache.iotdb.commons.sync.utils.SyncConstant.DATA_CHUNK_SIZE;

public class IoTDBOldSyncConnectorV1 implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBOldSyncConnectorV1.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final int TRANSFER_BUFFER_SIZE_IN_BYTES = 1 * 1024 * 1024;

  private TTransport transport = null;
  private volatile IClientRPCService.Client serviceClient = null;

  private String ipAddress;
  private int port;

  // TODO: Get user and password
  private String user = "root";
  private String password = "root";

  // TODO: Get pipeName and createTime
  private String pipeName = "defaultPipe";
  private Long createTime = 0L;

  // TODO: Get databaseName
  private String databaseName = "newPipe";

  private static SessionPool sessionPool;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator
        .validateRequiredAttribute(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY)
        .validateRequiredAttribute(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY);
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    this.ipAddress = parameters.getString(PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY);
    this.port = parameters.getInt(PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY);
  }

  @Override
  public void handshake() throws Exception {
    // Create transport for old pipe
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
          new TSyncIdentityInfo(pipeName, createTime, config.getIoTDBMajorVersion(), databaseName);
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

    // Build session pool
    sessionPool =
        new SessionPool.Builder()
            .host(ipAddress)
            .port(port)
            .user(user)
            .password(password)
            .maxSize(3)
            .build();
  }

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // TODO: support more TabletInsertionEvent
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeTabletInsertionEvent)) {
      throw new NotImplementedException(
          "IoTDBThriftConnectorV1 only support PipeTabletInsertionEvent.");
    }

    try {
      doTransfer((PipeTabletInsertionEvent) tabletInsertionEvent);
    } catch (TException e) {
      LOGGER.error(
          "Network error when transfer tablet insertion event: {}.", tabletInsertionEvent, e);
      // the connection may be broken, try to reconnect by catching PipeConnectionException
      throw new PipeConnectionException("Network error when transfer tablet insertion event.", e);
    }
  }

  private void doTransfer(PipeTabletInsertionEvent pipeTabletInsertionEvent)
      throws PipeException, TException, WALPipeException, IoTDBConnectionException,
          StatementExecutionException {
    InsertNode node = pipeTabletInsertionEvent.getInsertNode();
    Tablet tablet =
        new Tablet(node.getDeviceID().toStringID(), Arrays.asList(node.getMeasurementSchemas()));
    sessionPool.insertTablet(tablet);
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // TODO: support more TsFileInsertionEvent
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      throw new NotImplementedException(
          "IoTDBThriftConnectorV1 only support PipeTsFileInsertionEvent.");
    }

    try {
      doTransfer((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (TException e) {
      LOGGER.error(
          "Network error when transfer tsfile insertion event: {}.", tsFileInsertionEvent, e);
      // the connection may be broken, try to reconnect by catching PipeConnectionException
      throw new PipeConnectionException("Network error when transfer tsfile insertion event.", e);
    }
  }

  private void doTransfer(PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, TException, InterruptedException, IOException, SyncConnectionException {
    pipeTsFileInsertionEvent.waitForTsFileClose();

    final File tsFile = pipeTsFileInsertionEvent.getTsFile();
    if (!transportSingleFilePieceByPiece(tsFile)) {
      throw new PipeConnectionException(
          "Network error when transfer tsfile " + tsFile + " piece by piece.");
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
          LOGGER.error(
              "Receiver failed to receive data from {} because {}, abort.",
              file.getAbsoluteFile(),
              status.message);
          return false;
        }
      }
    } catch (TException e) {
      LOGGER.error("Cannot sync data with receiver. ", e);
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
        LOGGER.error(
            String.format("Deserialize offset of file %s error, because %s.", file.getPath(), e));
      }
    }
    return file.length();
  }

  @Override
  public void transfer(Event event) throws Exception {
    LOGGER.warn("IoTDBOldSyncConnectorV1 does not support transfer generic event: {}.", event);
  }

  @Override
  public void close() throws Exception {
    if (transport != null) {
      transport.close();
      transport = null;
    }
    if (sessionPool != null) {
      sessionPool.close();
      sessionPool = null;
    }
  }
}
