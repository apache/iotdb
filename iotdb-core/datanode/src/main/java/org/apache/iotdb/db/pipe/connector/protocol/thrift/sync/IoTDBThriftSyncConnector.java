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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.iotdb.IoTDBConnector;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.builder.IoTDBThriftSyncPipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.reponse.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
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
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK;

public class IoTDBThriftSyncConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftSyncConnector.class);

  private IoTDBThriftSyncPipeTransferBatchReqBuilder tabletBatchBuilder;

  private IoTDBThriftSyncClientManager clientManager;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
    final PipeParameters parameters = validator.getParameters();

    final String userSpecifiedConnectorName =
        parameters
            .getStringOrDefault(
                ImmutableList.of(CONNECTOR_KEY, SINK_KEY),
                IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            .toLowerCase();
    final Set<TEndPoint> givenNodeUrls = parseNodeUrls(parameters);

    validator
        .validate(
            empty -> {
              try {
                // Ensure the sink doesn't point to the thrift receiver on DataNode itself
                return !NodeUrlUtils.containsLocalAddress(
                    givenNodeUrls.stream()
                        .filter(tEndPoint -> tEndPoint.getPort() == ioTDBConfig.getRpcPort())
                        .map(TEndPoint::getIp)
                        .collect(Collectors.toList()));
              } catch (UnknownHostException e) {
                LOGGER.warn("Unknown host when checking pipe sink IP.", e);
                return false;
              }
            },
            String.format(
                "One of the endpoints %s of the receivers is pointing back to the thrift receiver %s on sender itself, or unknown host when checking pipe sink IP.",
                givenNodeUrls,
                new TEndPoint(ioTDBConfig.getRpcAddress(), ioTDBConfig.getRpcPort())))
        .validate(
            args -> !((boolean) args[0]) || ((boolean) args[1] && (boolean) args[2]),
            String.format(
                "When ssl transport is enabled, %s and %s must be specified",
                SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY, SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY),
            IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().equals(userSpecifiedConnectorName)
                || IOTDB_THRIFT_SSL_SINK.getPipePluginName().equals(userSpecifiedConnectorName)
                || parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false),
            parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY),
            parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    // tablet batch mode configuration
    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new IoTDBThriftSyncPipeTransferBatchReqBuilder(parameters);
    }

    // ssl transport configuration
    final String userSpecifiedConnectorName =
        parameters
            .getStringOrDefault(
                ImmutableList.of(CONNECTOR_KEY, SINK_KEY),
                IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            .toLowerCase();
    final boolean useSSL =
        IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().equals(userSpecifiedConnectorName)
            || IOTDB_THRIFT_SSL_SINK.getPipePluginName().equals(userSpecifiedConnectorName)
            || parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false);
    final String trustStorePath = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY);
    final String trustStorePwd = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY);

    // leader cache configuration
    final boolean useLeaderCache =
        parameters.getBooleanOrDefault(
            Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
            CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE);

    clientManager =
        new IoTDBThriftSyncClientManager(
            nodeUrls, useSSL, trustStorePath, trustStorePwd, useLeaderCache);
  }

  @Override
  public void handshake() throws Exception {
    clientManager.checkClientStatusAndTryReconstructIfNecessary();
  }

  @Override
  public void heartbeat() {
    try {
      handshake();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to reconnect to target server, because: {}. Try to reconnect later.",
          e.getMessage(),
          e);
    }
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (((EnrichedEvent) tabletInsertionEvent).shouldParsePatternOrTime()) {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        transfer(
            ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent)
                .parseEventWithPatternOrTime());
      } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
        transfer(
            ((PipeRawTabletInsertionEvent) tabletInsertionEvent).parseEventWithPatternOrTime());
      }
      return;
    } else {
      // ignore raw tablet event with zero rows
      if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
        if (((PipeRawTabletInsertionEvent) tabletInsertionEvent).hasNoNeedParsingAndIsEmpty()) {
          return;
        }
      }
    }

    try {
      if (isTabletBatchModeEnabled) {
        if (tabletBatchBuilder.onEvent(tabletInsertionEvent)) {
          doTransfer();
        }
      } else {
        if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
          doTransfer((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
        } else {
          doTransfer((PipeRawTabletInsertionEvent) tabletInsertionEvent);
        }
      }
    } catch (Exception e) {
      throw new PipeConnectionException(
          String.format(
              "Failed to transfer tablet insertion event %s, because %s.",
              tabletInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of tsFileInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector only support PipeTsFileInsertionEvent. Ignore {}.",
          tsFileInsertionEvent);
      return;
    }

    if (!((PipeTsFileInsertionEvent) tsFileInsertionEvent).waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          ((PipeTsFileInsertionEvent) tsFileInsertionEvent).getTsFile());
      return;
    }

    if (((EnrichedEvent) tsFileInsertionEvent).shouldParsePatternOrTime()) {
      for (final TabletInsertionEvent event : tsFileInsertionEvent.toTabletInsertionEvents()) {
        transfer(event);
      }
      return;
    }

    try {
      // in order to commit in order
      if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
        doTransfer();
      }

      doTransfer((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (Exception e) {
      throw new PipeConnectionException(
          String.format(
              "Failed to transfer tsfile insertion event %s, because %s.",
              tsFileInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(Event event) throws TException, IOException {
    // in order to commit in order
    if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
      doTransfer();
    }

    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransfer() {
    Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus = clientManager.getClient();
    final TPipeTransferResp resp;
    try {
      resp = clientAndStatus.getLeft().pipeTransfer(tabletBatchBuilder.toTPipeTransferReq());
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format("Network error when transfer tablet batch, because %s.", e.getMessage()),
          e);
    }

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeTransferTabletBatchReq error, result status %s", resp.status));
    }

    tabletBatchBuilder.onSuccess();
  }

  private void doTransfer(PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException {
    InsertNode insertNode;
    Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus = null;
    final TPipeTransferResp resp;

    try {
      insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();

      if (insertNode != null) {
        clientAndStatus = clientManager.getClient(insertNode.getDevicePath().getFullPath());
        resp =
            clientAndStatus
                .getLeft()
                .pipeTransfer(PipeTransferTabletInsertNodeReq.toTPipeTransferReq(insertNode));
      } else {
        clientAndStatus = clientManager.getClient();
        resp =
            clientAndStatus
                .getLeft()
                .pipeTransfer(
                    PipeTransferTabletBinaryReq.toTPipeTransferReq(
                        pipeInsertNodeTabletInsertionEvent.getByteBuffer()));
      }
    } catch (Exception e) {
      if (clientAndStatus != null) {
        clientAndStatus.setRight(false);
      }
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer insert node tablet insertion event, because %s.",
              e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeInsertNodeTabletInsertionEvent %s error, result status %s",
              pipeInsertNodeTabletInsertionEvent, status));
    }
    if (insertNode != null && status.isSetRedirectNode()) {
      clientManager.updateLeaderCache(
          insertNode.getDevicePath().getFullPath(), status.getRedirectNode());
    }
  }

  private void doTransfer(PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    final Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus =
        clientManager.getClient(pipeRawTabletInsertionEvent.getDeviceId());
    final TPipeTransferResp resp;

    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferTabletRawReq.toTPipeTransferReq(
                      pipeRawTabletInsertionEvent.convertToTablet(),
                      pipeRawTabletInsertionEvent.isAligned()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer raw tablet insertion event, because %s.",
              e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent, status));
    }
    if (status.isSetRedirectNode()) {
      clientManager.updateLeaderCache(
          pipeRawTabletInsertionEvent.getDeviceId(), status.getRedirectNode());
    }
  }

  private void doTransfer(PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    final File tsFile = pipeTsFileInsertionEvent.getTsFile();
    final Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus = clientManager.getClient();

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

        final PipeTransferFilePieceResp resp;
        try {
          resp =
              PipeTransferFilePieceResp.fromTPipeTransferResp(
                  clientAndStatus
                      .getLeft()
                      .pipeTransfer(
                          PipeTransferFilePieceReq.toTPipeTransferReq(
                              tsFile.getName(),
                              position,
                              readLength == readFileBufferSize
                                  ? readBuffer
                                  : Arrays.copyOfRange(readBuffer, 0, readLength))));
        } catch (Exception e) {
          clientAndStatus.setRight(false);
          throw new PipeConnectionException(
              String.format(
                  "Network error when transfer file %s, because %s.", tsFile, e.getMessage()),
              e);
        }

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
              String.format("Transfer file %s error, result status %s.", tsFile, resp.getStatus()));
        }
      }
    }

    // 2. Transfer file seal signal, which means the file is transferred completely
    final TPipeTransferResp resp;
    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferFileSealReq.toTPipeTransferReq(tsFile.getName(), tsFile.length()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format("Network error when seal file %s, because %s.", tsFile, e.getMessage()), e);
    }

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format("Seal file %s error, result status %s.", tsFile, resp.getStatus()));
    } else {
      LOGGER.info("Successfully transferred file {}.", tsFile);
    }
  }

  @Override
  public void close() {
    if (clientManager != null) {
      clientManager.close();
    }

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }
  }
}
