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

package org.apache.iotdb.confignode.manager.pipe.connector.protocol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBSslSyncConnector;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.manager.pipe.connector.client.IoTDBConfigNodeSyncClientManager;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.connector.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class IoTDBConfigRegionConnector extends IoTDBSslSyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigRegionConnector.class);

  @Override
  protected IoTDBSyncClientManager constructClient(
      final List<TEndPoint> nodeUrls,
      final boolean useSSL,
      final String trustStorePath,
      final String trustStorePwd,
      /* The following parameters are used locally. */
      final boolean useLeaderCache,
      final String loadBalanceStrategy,
      /* The following parameters are used to handshake with the receiver. */
      final String username,
      final String password,
      final boolean shouldReceiverConvertOnTypeMismatch,
      final String loadTsFileStrategy) {
    return new IoTDBConfigNodeSyncClientManager(
        nodeUrls,
        useSSL,
        Objects.nonNull(trustStorePath) ? ConfigNodeConfig.addHomeDir(trustStorePath) : null,
        trustStorePwd,
        loadBalanceStrategy,
        username,
        password,
        shouldReceiverConvertOnTypeMismatch,
        loadTsFileStrategy);
  }

  @Override
  protected PipeTransferFilePieceReq getTransferSingleFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) {
    throw new UnsupportedOperationException(
        "The config region connector does not support transferring single file piece req.");
  }

  @Override
  protected PipeTransferFilePieceReq getTransferMultiFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferConfigSnapshotPieceReq.toTPipeTransferReq(fileName, position, payLoad);
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof PipeConfigRegionWritePlanEvent) {
      doTransferWrapper((PipeConfigRegionWritePlanEvent) event);
    } else if (event instanceof PipeConfigRegionSnapshotEvent) {
      doTransferWrapper((PipeConfigRegionSnapshotEvent) event);
    } else if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBConfigRegionConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransferWrapper(
      final PipeConfigRegionWritePlanEvent pipeConfigRegionWritePlanEvent) throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeConfigRegionWritePlanEvent.increaseReferenceCount(
        IoTDBConfigRegionConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeConfigRegionWritePlanEvent);
    } finally {
      pipeConfigRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBConfigRegionConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeConfigRegionWritePlanEvent pipeConfigRegionWritePlanEvent)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    final TPipeTransferResp resp;
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferConfigPlanReq.toTPipeTransferReq(
                  pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan()));
      rateLimitIfNeeded(
          pipeConfigRegionWritePlanEvent.getPipeName(),
          pipeConfigRegionWritePlanEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer config region write plan %s, because %s.",
              pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan().getType(), e.getMessage()),
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
              pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan().getType(), status),
          pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan().toString());
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Successfully transferred config event {}.", pipeConfigRegionWritePlanEvent);
    }
  }

  private void doTransferWrapper(final PipeConfigRegionSnapshotEvent pipeConfigRegionSnapshotEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeConfigRegionSnapshotEvent.increaseReferenceCount(
        IoTDBConfigRegionConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeConfigRegionSnapshotEvent);
    } finally {
      pipeConfigRegionSnapshotEvent.decreaseReferenceCount(
          IoTDBConfigRegionConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeConfigRegionSnapshotEvent snapshotEvent)
      throws PipeException, IOException {
    final String pipeName = snapshotEvent.getPipeName();
    final long creationTime = snapshotEvent.getCreationTime();
    final File snapshotFile = snapshotEvent.getSnapshotFile();
    final File templateFile = snapshotEvent.getTemplateFile();
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    // 1. Transfer snapshotFile, and template File if exists
    transferFilePieces(
        Collections.singletonMap(new Pair<>(pipeName, creationTime), 1.0),
        snapshotFile,
        clientAndStatus,
        true);
    if (Objects.nonNull(templateFile)) {
      transferFilePieces(
          Collections.singletonMap(new Pair<>(pipeName, creationTime), 1.0),
          templateFile,
          clientAndStatus,
          true);
    }
    // 2. Transfer file seal signal, which means the snapshots are transferred completely
    final TPipeTransferResp resp;
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferConfigSnapshotSealReq.toTPipeTransferReq(
                  // The pattern is surely Non-null
                  snapshotEvent.getTreePatternString(),
                  snapshotFile.getName(),
                  snapshotFile.length(),
                  Objects.nonNull(templateFile) ? templateFile.getName() : null,
                  Objects.nonNull(templateFile) ? templateFile.length() : 0,
                  snapshotEvent.getFileType(),
                  snapshotEvent.toSealTypeString()));
      rateLimitIfNeeded(
          snapshotEvent.getPipeName(),
          snapshotEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
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
