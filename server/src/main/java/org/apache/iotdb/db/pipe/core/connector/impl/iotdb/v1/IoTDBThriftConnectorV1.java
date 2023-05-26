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

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.config.PipeConnectorConstant;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.IoTDBThriftConnectorClient;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.core.connector.impl.iotdb.v1.request.PipeTransferInsertNodeReq;
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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class IoTDBThriftConnectorV1 implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftConnectorV1.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private String ipAddress;
  private int port;

  private IoTDBThriftConnectorClient client;

  public IoTDBThriftConnectorV1() {}

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
    if (client != null) {
      client.close();
    }

    client =
        new IoTDBThriftConnectorClient(
            new ThriftClientProperty.Builder()
                .setConnectionTimeoutMs(COMMON_CONFIG.getConnectionTimeoutInMS())
                .setRpcThriftCompressionEnabled(COMMON_CONFIG.isRpcThriftCompressionEnabled())
                .build(),
            ipAddress,
            port);

    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferHandshakeReq.toTPipeTransferReq(IOTDB_CONFIG.getTimestampPrecision()));
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(String.format("Handshake error, result status %s.", resp.status));
    }
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
      throws PipeException, TException, WALPipeException {
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferInsertNodeReq.toTPipeTransferReq(pipeTabletInsertionEvent.getInsertNode()));

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer tablet insertion event %s error, result status %s",
              pipeTabletInsertionEvent, resp.status));
    }
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
      throws PipeException, TException, InterruptedException, IOException {
    pipeTsFileInsertionEvent.waitForTsFileClose();

    final File tsFile = pipeTsFileInsertionEvent.getTsFile();

    // 1. transfer file piece by piece
    final int readFileBufferSize = PipeConfig.getInstance().getReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(tsFile, "r")) {
      while (true) {
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final PipeTransferFilePieceResp resp =
            PipeTransferFilePieceResp.fromTPipeTransferResp(
                client.pipeTransfer(
                    PipeTransferFilePieceReq.toTPipeTransferReq(
                        tsFile.getName(),
                        position,
                        readLength == readFileBufferSize
                            ? readBuffer
                            : Arrays.copyOfRange(readBuffer, 0, readLength))));
        position += readLength;

        // this case only happens when the connection is broken, and the connector is reconnected
        // to the receiver, then the receiver will redirect the file position to the last position
        if (resp.getStatus().getCode()
            == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
          position = resp.getEndWritingOffset();
          reader.seek(position);
          LOGGER.info(String.format("Redirect file position to %s.", position));
          continue;
        }

        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new PipeException(
              String.format("Transfer file %s error, result status %s.", tsFile, resp.getStatus()));
        }
      }
    }

    // 2. transfer file seal signal, which means the file is transferred completely
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferFileSealReq.toTPipeTransferReq(tsFile.getName(), tsFile.length()));
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format("Seal file %s error, result status %s.", tsFile, resp.getStatus()));
    }
  }

  @Override
  public void transfer(Event event) {
    LOGGER.warn("IoTDBThriftConnectorV1 does not support transfer generic event: {}.", event);
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }
}
