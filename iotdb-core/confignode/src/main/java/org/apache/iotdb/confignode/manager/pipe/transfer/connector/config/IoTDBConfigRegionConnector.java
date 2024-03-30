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

package org.apache.iotdb.confignode.manager.pipe.transfer.connector.config;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBSslSyncConnector;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.client.IoTDBConfigNodeSyncClientManager;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class IoTDBConfigRegionConnector extends IoTDBSslSyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigRegionConnector.class);

  @Override
  protected IoTDBSyncClientManager constructClient(
      List<TEndPoint> nodeUrls,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      boolean useLeaderCache) {
    return new IoTDBConfigNodeSyncClientManager(nodeUrls, useSSL, trustStorePath, trustStorePwd);
  }

  @Override
  protected PipeTransferFilePieceReq getTransferSingleFilePieceReq(
      String fileName, long position, byte[] payLoad) {
    throw new UnsupportedOperationException(
        "The config region connector does not support transferring single file piece req.");
  }

  @Override
  protected PipeTransferFilePieceReq getTransferMultiFilePieceReq(
      String fileName, long position, byte[] payLoad) throws IOException {
    return PipeTransferConfigSnapshotPieceReq.toTPipeTransferReq(fileName, position, payLoad);
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(Event event) throws Exception {
    if (event instanceof PipeConfigRegionWritePlanEvent) {
      doTransfer((PipeConfigRegionWritePlanEvent) event);
    } else if (event instanceof PipeConfigRegionSnapshotEvent) {
      doTransfer((PipeConfigRegionSnapshotEvent) event);
    } else if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBConfigRegionConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransfer(PipeConfigRegionWritePlanEvent writePlanEvent) throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    final TPipeTransferResp resp;
    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferConfigPlanReq.toTPipeTransferReq(
                      writePlanEvent.getConfigPhysicalPlan()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer config region write plan %s, because %s.",
              writePlanEvent.getConfigPhysicalPlan().getType(), e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Send handshake req and then re-transfer the event
    if (status.getCode() == TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode()) {
      clientManager.sendHandshakeReq(clientAndStatus);
    }
    // Only handle the failed statuses to avoid string format performance overhead
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "Transfer config region write plan %s error, result status %s.",
              writePlanEvent.getConfigPhysicalPlan().getType(), status),
          writePlanEvent.getConfigPhysicalPlan().toString());
    }
  }

  private void doTransfer(PipeConfigRegionSnapshotEvent snapshotEvent)
      throws PipeException, IOException {
    final File snapshotFile = snapshotEvent.getSnapshotFile();
    final File templateFile = snapshotEvent.getTemplateFile();
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    // 1. Transfer snapshotFile, and template File if exists
    transferFilePieces(snapshotFile, clientAndStatus, true);
    if (Objects.nonNull(templateFile)) {
      transferFilePieces(templateFile, clientAndStatus, true);
    }
    // 2. Transfer file seal signal, which means the snapshots are transferred completely
    final TPipeTransferResp resp;
    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferConfigSnapshotSealReq.toTPipeTransferReq(
                      snapshotFile.getName(),
                      snapshotFile.length(),
                      Objects.nonNull(templateFile) ? templateFile.getName() : null,
                      Objects.nonNull(templateFile) ? templateFile.length() : 0,
                      snapshotEvent.getFileType(),
                      snapshotEvent.toSealTypeString()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when seal config region snapshot %s, because %s.",
              snapshotFile, e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Send handshake req and then re-transfer the event
    if (status.getCode() == TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode()) {
      clientManager.sendHandshakeReq(clientAndStatus);
    }
    // Only handle the failed statuses to avoid string format performance overhead
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "Seal config region snapshot file %s error, result status %s.",
              snapshotFile, resp.getStatus()),
          snapshotFile.toString());
    }
    LOGGER.info("Successfully transferred config region snapshot {}.", snapshotFile);
  }
}
