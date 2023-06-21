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

package org.apache.iotdb.db.pipe.connector.v1;

import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.v1.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferTabletReq;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
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

import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY;

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
        .validateRequiredAttribute(CONNECTOR_IOTDB_IP_KEY)
        .validateRequiredAttribute(CONNECTOR_IOTDB_PORT_KEY);
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    this.ipAddress = parameters.getString(CONNECTOR_IOTDB_IP_KEY);
    this.port = parameters.getInt(CONNECTOR_IOTDB_PORT_KEY);
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

    try {
      final TPipeTransferResp resp =
          client.pipeTransfer(
              PipeTransferHandshakeReq.toTPipeTransferReq(
                  CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(String.format("Handshake error, result status %s.", resp.status));
      }
    } catch (TException e) {
      LOGGER.warn(String.format("Connect to receiver %s:%s error.", ipAddress, port), e);
      throw new PipeConnectionException(e.getMessage(), e);
    }
  }

  @Override
  public void heartbeat() throws Exception {}

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    try {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        doTransfer((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
        doTransfer((PipeRawTabletInsertionEvent) tabletInsertionEvent);
      } else {
        throw new NotImplementedException(
            "IoTDBThriftConnectorV1 only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent.");
      }
    } catch (TException e) {
      LOGGER.warn(
          "Network error when transfer tablet insertion event: {}.", tabletInsertionEvent, e);
      // the connection may be broken, try to reconnect by catching PipeConnectionException
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tablet insertion event, because %s.", e.getMessage()),
          e);
    }
  }

  private void doTransfer(PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, TException, WALPipeException {
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferInsertNodeReq.toTPipeTransferReq(
                pipeInsertNodeTabletInsertionEvent.getInsertNode()));

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeInsertNodeTabletInsertionEvent %s error, result status %s",
              pipeInsertNodeTabletInsertionEvent, resp.status));
    }
  }

  private void doTransfer(PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, TException, IOException {
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferTabletReq.toTPipeTransferReq(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.isAligned()));

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent, resp.status));
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      throw new NotImplementedException(
          "IoTDBThriftConnectorV1 only support PipeTsFileInsertionEvent.");
    }

    try {
      doTransfer((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (TException e) {
      LOGGER.warn(
          "Network error when transfer tsfile insertion event: {}.", tsFileInsertionEvent, e);
      // the connection may be broken, try to reconnect by catching PipeConnectionException
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile insertion event, because %s.", e.getMessage()),
          e);
    }
  }

  private void doTransfer(PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, TException, InterruptedException, IOException {
    pipeTsFileInsertionEvent.waitForTsFileClose();

    final File tsFile = pipeTsFileInsertionEvent.getTsFile();

    // 1. transfer file piece by piece
    final int readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
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
