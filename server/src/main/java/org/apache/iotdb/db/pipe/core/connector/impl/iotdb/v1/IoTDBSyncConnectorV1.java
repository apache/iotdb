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
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.IoTDBThriftConnectorClient;
import org.apache.iotdb.db.pipe.core.event.impl.PipeInsertNodeInsertionEvent;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.core.event.impl.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.sync.pipedata.TsFilePipeData;
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
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSyncIdentityInfo;
import org.apache.iotdb.service.rpc.thrift.TSyncTransportMetaInfo;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_KEY;

public class IoTDBSyncConnectorV1 implements PipeConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSyncConnectorV1.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private IoTDBThriftConnectorClient client = null;

  private String ipAddress;
  private int port;

  private String user;
  private String password;

  public static final String IOTDB_SYNC_CONNECTOR_VERSION = "1.1";

  // TODO: Get pipeName and createTime
  private String pipeName = "defaultPipe";
  private Long createTime = 11L;

  // TODO: Get databaseName
  private String databaseName = "root.sg";

  private static SessionPool sessionPool;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator
        .validateRequiredAttribute(CONNECTOR_IOTDB_IP_KEY)
        .validateRequiredAttribute(CONNECTOR_IOTDB_PORT_KEY);
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    this.ipAddress = parameters.getString(CONNECTOR_IOTDB_IP_KEY);
    this.port = parameters.getInt(CONNECTOR_IOTDB_PORT_KEY);
    this.user =
        parameters.getStringOrDefault(CONNECTOR_IOTDB_USER_KEY, CONNECTOR_IOTDB_USER_DEFAULT_VALUE);
    this.password =
        parameters.getStringOrDefault(
            CONNECTOR_IOTDB_PASSWORD_KEY, CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);
  }

  @Override
  public void handshake() throws Exception {
    // Create transport for old pipe
    if (client != null) {
      client.close();
    }
    if (sessionPool != null) {
      sessionPool.close();
    }

    client =
        new IoTDBThriftConnectorClient(
            new ThriftClientProperty.Builder()
                .setConnectionTimeoutMs(COMMON_CONFIG.getConnectionTimeoutInMS())
                .setRpcThriftCompressionEnabled(COMMON_CONFIG.isRpcThriftCompressionEnabled())
                .build(),
            ipAddress,
            port);

    try {
      TSyncIdentityInfo identityInfo =
          new TSyncIdentityInfo(pipeName, createTime, IOTDB_SYNC_CONNECTOR_VERSION, databaseName);
      TSStatus status = client.handshake(identityInfo);
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        String errorMsg =
            String.format(
                "The receiver %s:%s rejected the pipe task because %s",
                ipAddress, port, status.message);
        LOGGER.warn(errorMsg);
        throw new PipeRuntimeCriticalException(errorMsg);
      }
    } catch (TException e) {
      LOGGER.warn(String.format("Connect to receiver %s:%s error.", ipAddress, port), e);
      throw new PipeConnectionException(e.getMessage(), e);
    }

    // Build session pool
    sessionPool =
        new SessionPool.Builder()
            .host(ipAddress)
            .port(port)
            .user(user)
            .password(password)
            .maxSize(1)
            .build();
  }

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // TODO: support more TabletInsertionEvent
    // PipeProcessor can change the type of TabletInsertionEvent
    try {
      if (tabletInsertionEvent instanceof PipeInsertNodeInsertionEvent) {
        doTransfer((PipeInsertNodeInsertionEvent) tabletInsertionEvent);
      } else if (tabletInsertionEvent instanceof PipeTabletInsertionEvent) {
        doTransfer((PipeTabletInsertionEvent) tabletInsertionEvent);
      } else {
        throw new NotImplementedException(
            "IoTDBSyncConnectorV1 only support PipeInsertNodeInsertionEvent and PipeTabletInsertionEvent.");
      }
    } catch (TException e) {
      LOGGER.error(
          "Network error when transfer tablet insertion event: {}.", tabletInsertionEvent, e);
      // the connection may be broken, try to reconnect by catching PipeConnectionException
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tablet insertion event, because %s.", e.getMessage()),
          e);
    }
  }

  private void doTransfer(PipeInsertNodeInsertionEvent pipeInsertNodeInsertionEvent)
      throws IoTDBConnectionException, StatementExecutionException {
    sessionPool.insertTablet(pipeInsertNodeInsertionEvent.convertToTablet());
  }

  private void doTransfer(PipeTabletInsertionEvent pipeTabletInsertionEvent)
      throws PipeException, TException, IoTDBConnectionException, StatementExecutionException {
    sessionPool.insertTablet(pipeTabletInsertionEvent.convertToTablet());
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // TODO: support more TsFileInsertionEvent
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      throw new NotImplementedException(
          "IoTDBSyncConnectorV1 only support PipeTsFileInsertionEvent.");
    }

    try {
      doTransfer((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (TException e) {
      LOGGER.error(
          "Network error when transfer tsFile insertion event: {}.", tsFileInsertionEvent, e);
      // The connection may be broken, try to reconnect by catching PipeConnectionException
      throw new PipeConnectionException("Network error when transfer tsFile insertion event.", e);
    }
  }

  private void doTransfer(PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, TException, InterruptedException, IOException {
    pipeTsFileInsertionEvent.waitForTsFileClose();

    final File tsFile = pipeTsFileInsertionEvent.getTsFile();
    transportSingleFilePieceByPiece(tsFile);
    client.sendPipeData(ByteBuffer.wrap(new TsFilePipeData("", tsFile.getName(), -1).serialize()));
  }

  private void transportSingleFilePieceByPiece(File file) throws IOException {
    // Cut the file into pieces to send
    long position = 0;

    // Try small piece to rebase the file position.
    byte[] buffer = new byte[PipeConfig.getInstance().getPipeConnectorReadFileBufferSize()];
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      while (true) {
        final int dataLength = randomAccessFile.read(buffer);
        if (dataLength == -1) {
          break;
        }

        ByteBuffer buffToSend = ByteBuffer.wrap(buffer, 0, dataLength);
        TSyncTransportMetaInfo metaInfo = new TSyncTransportMetaInfo(file.getName(), position);

        TSStatus status = client.sendFile(metaInfo, buffToSend);

        if ((status.code == TSStatusCode.SUCCESS_STATUS.getStatusCode())) {
          // Success
          position += dataLength;
        } else if (status.code == TSStatusCode.SYNC_FILE_REDIRECTION_ERROR.getStatusCode()) {
          position = Long.parseLong(status.message);
          randomAccessFile.seek(position);
          LOGGER.info(
              String.format("Redirect to position %s in transferring tsFile %s.", position, file));
        } else if (status.code == TSStatusCode.SYNC_FILE_ERROR.getStatusCode()) {
          String errorMsg =
              String.format("Network failed to receive tsFile %s, status: %s", file, status);
          LOGGER.warn(errorMsg);
          throw new PipeConnectionException(errorMsg);
        }
      }
    } catch (TException e) {
      LOGGER.error(String.format("Cannot send pipe data to receiver %s:%s.", ipAddress, port), e);
      throw new PipeConnectionException(e.getMessage(), e);
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    LOGGER.warn("IoTDBSyncConnectorV1 does not support transfer generic event: {}.", event);
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      client.close();
      client = null;
    }
    if (sessionPool != null) {
      sessionPool.close();
      sessionPool = null;
    }
  }
}
