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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.connector.v1.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferHandshakeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.v1.request.PipeTransferTabletReq;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
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
import org.apache.iotdb.session.util.SessionUtils;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_NODE_URLS_KEY;
import static org.apache.iotdb.db.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PORT_KEY;

public class IoTDBThriftConnectorV1 implements PipeConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftConnectorV1.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private List<TEndPoint> nodeUrls;
  private List<Boolean> isNodeUrlAvailable;
  private final AtomicLong commitIdGenerator = new AtomicLong(0);

  public IoTDBThriftConnectorV1() {
    // Do nothing
  }

  public IoTDBThriftConnectorV1(String ipAddress, int port) {
    this.nodeUrls = new ArrayList<>();
    nodeUrls.add(new TEndPoint(ipAddress, port));
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validate(
        args -> (boolean) args[0] || ((boolean) args[1] && (boolean) args[2]),
        "Should use node urls or (ip and port) for connector-v1.",
        validator.getParameters().hasAttribute(CONNECTOR_IOTDB_NODE_URLS_KEY),
        validator.getParameters().hasAttribute(CONNECTOR_IOTDB_IP_KEY),
        validator.getParameters().hasAttribute(CONNECTOR_IOTDB_PORT_KEY));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    List<String> nodeUrlStrings = new ArrayList<>();
    if (parameters.hasAttribute(CONNECTOR_IOTDB_NODE_URLS_KEY)) {
      nodeUrlStrings.addAll(
          Arrays.asList(parameters.getString(CONNECTOR_IOTDB_NODE_URLS_KEY).split(",")));
    }
    if (parameters.hasAttribute(CONNECTOR_IOTDB_IP_KEY)
        && parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY)) {
      nodeUrlStrings.add(
          parameters.getString(CONNECTOR_IOTDB_IP_KEY)
              + ":"
              + parameters.getString(CONNECTOR_IOTDB_PORT_KEY));
    }
    this.nodeUrls =
        SessionUtils.parseSeedNodeUrls(
            nodeUrlStrings.stream().distinct().collect(Collectors.toList()));
    if (this.nodeUrls.isEmpty()) {
      throw new PipeException("Node urls is empty.");
    }
    this.isNodeUrlAvailable = new ArrayList<>();
    for (int i = 0; i < this.nodeUrls.size(); ++i) {
      this.isNodeUrlAvailable.add(false);
    }
  }

  @Override
  public void handshake() throws Exception {
    boolean atLeastOneAvailable = false;
    for (int i = 0; i < nodeUrls.size(); ++i) {
      final TEndPoint targetNodeUrl = nodeUrls.get(i);
      try (final IoTDBThriftConnectorClient client =
          new IoTDBThriftConnectorClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(COMMON_CONFIG.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(COMMON_CONFIG.isRpcThriftCompressionEnabled())
                  .build(),
              targetNodeUrl.getIp(),
              targetNodeUrl.getPort())) {
        final TPipeTransferResp resp =
            client.pipeTransfer(
                PipeTransferHandshakeReq.toTPipeTransferReq(
                    CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn(
              "Handshake error, target server ip: {}, port: {}, result status {}.",
              targetNodeUrl.getIp(),
              targetNodeUrl.getPort(),
              resp.status);
          isNodeUrlAvailable.set(i, false);
        } else {
          LOGGER.info(
              "Handshake success. Target server ip: {}, port: {}",
              targetNodeUrl.getIp(),
              targetNodeUrl.getPort());
          atLeastOneAvailable = true;
          isNodeUrlAvailable.set(i, true);
        }
      } catch (TException e) {
        LOGGER.warn(
            "Connect to receiver {}:{} error, because: {}",
            targetNodeUrl.getIp(),
            targetNodeUrl.getPort(),
            e.getMessage());
        isNodeUrlAvailable.set(i, false);
      }
    }
    if (!atLeastOneAvailable) {
      throw new PipeConnectionException("Failed to connect to any receiver node.");
    }
  }

  @Override
  public void heartbeat() throws Exception {
    handshake();
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    try {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        doTransfer((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
        doTransfer((PipeRawTabletInsertionEvent) tabletInsertionEvent);
      } else {
        LOGGER.warn(
            "IoTDBThriftConnectorV1 only support "
                + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
                + "Ignore {}.",
            tabletInsertionEvent);
      }
    } catch (TException e) {
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tablet insertion event %s, because %s.",
              tabletInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftConnectorV1 only support PipeTsFileInsertionEvent. Ignore {}.",
          tsFileInsertionEvent);
      return;
    }

    try {
      doTransfer((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (TException e) {
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile insertion event %s, because %s.",
              tsFileInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(Event event) {
    LOGGER.warn("IoTDBThriftConnectorV1 does not support transfer generic event: {}.", event);
  }

  private void doTransfer(PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, TException, WALPipeException {
    final long requestCommitId = commitIdGenerator.incrementAndGet();
    final IoTDBThriftConnectorClient client = getClientByCommitId(requestCommitId);
    try {
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
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Close client error, because: {}", e.getMessage(), e);
      }
    }
  }

  private void doTransfer(PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, TException, IOException {
    final long requestCommitId = commitIdGenerator.incrementAndGet();
    final IoTDBThriftConnectorClient client = getClientByCommitId(requestCommitId);
    try {
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
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Close client error, because: {}", e.getMessage(), e);
      }
    }
  }

  private void doTransfer(PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, TException, InterruptedException, IOException {
    pipeTsFileInsertionEvent.waitForTsFileClose();
    final long requestCommitId = commitIdGenerator.incrementAndGet();
    final IoTDBThriftConnectorClient client = getClientByCommitId(requestCommitId);
    try {
      final File tsFile = pipeTsFileInsertionEvent.getTsFile();

      // 1. Transfer file piece by piece
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

          // This case only happens when the connection is broken, and the connector is reconnected
          // to the receiver, then the receiver will redirect the file position to the last position
          if (resp.getStatus().getCode()
              == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
            position = resp.getEndWritingOffset();
            reader.seek(position);
            LOGGER.info("Redirect file position to {}.", position);
            continue;
          }

          if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new PipeException(
                String.format(
                    "Transfer file %s error, result status %s.", tsFile, resp.getStatus()));
          }
        }
      }

      // 2. Transfer file seal signal, which means the file is transferred completely
      final TPipeTransferResp resp =
          client.pipeTransfer(
              PipeTransferFileSealReq.toTPipeTransferReq(tsFile.getName(), tsFile.length()));
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new PipeException(
            String.format("Seal file %s error, result status %s.", tsFile, resp.getStatus()));
      } else {
        LOGGER.info("Successfully transferred file {}.", tsFile);
      }
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Close client error, because: {}", e.getMessage(), e);
      }
    }
  }

  @Override
  public void close() throws Exception {}

  private IoTDBThriftConnectorClient getClientByCommitId(long requestCommitId)
      throws TTransportException {
    for (int shift = 0; shift < nodeUrls.size(); ++shift) {
      int nodeUrlIndex = (int) ((requestCommitId + shift) % nodeUrls.size());
      if (Boolean.TRUE.equals(isNodeUrlAvailable.get(nodeUrlIndex))) {
        final TEndPoint targetNodeUrl = nodeUrls.get(nodeUrlIndex);
        return new IoTDBThriftConnectorClient(
            new ThriftClientProperty.Builder()
                .setConnectionTimeoutMs(COMMON_CONFIG.getConnectionTimeoutInMS())
                .setRpcThriftCompressionEnabled(COMMON_CONFIG.isRpcThriftCompressionEnabled())
                .build(),
            targetNodeUrl.getIp(),
            targetNodeUrl.getPort());
      }
    }
    throw new PipeException("All receiver nodes can't be connected.");
  }
}
