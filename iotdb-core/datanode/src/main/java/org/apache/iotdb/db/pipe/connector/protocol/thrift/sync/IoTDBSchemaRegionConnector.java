/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.connector.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
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
import java.util.Objects;

public class IoTDBSchemaRegionConnector extends IoTDBDataNodeSyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSchemaRegionConnector.class);

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof PipeSchemaRegionWritePlanEvent) {
      doTransferWrapper((PipeSchemaRegionWritePlanEvent) event);
    } else if (event instanceof PipeSchemaRegionSnapshotEvent) {
      doTransferWrapper((PipeSchemaRegionSnapshotEvent) event);
    } else if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBSchemaRegionConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransferWrapper(
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent) throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionWritePlanEvent.increaseReferenceCount(
        IoTDBDataNodeSyncConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeSchemaRegionWritePlanEvent);
    } finally {
      pipeSchemaRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBDataNodeSyncConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    final TPipeTransferResp resp;
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferPlanNodeReq.toTPipeTransferReq(
                  pipeSchemaRegionWritePlanEvent.getPlanNode()));
      rateLimitIfNeeded(
          pipeSchemaRegionWritePlanEvent.getPipeName(),
          pipeSchemaRegionWritePlanEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer schema region write plan %s, because %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Only handle the failed statuses to avoid string format performance overhead
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && resp.getStatus().getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "Transfer data node write plan %s error, result status %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), status),
          pipeSchemaRegionWritePlanEvent.getPlanNode().toString());
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Successfully transferred schema event {}.", pipeSchemaRegionWritePlanEvent);
    }
  }

  private void doTransferWrapper(final PipeSchemaRegionSnapshotEvent pipeSchemaRegionSnapshotEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionSnapshotEvent.increaseReferenceCount(
        IoTDBSchemaRegionConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeSchemaRegionSnapshotEvent);
    } finally {
      pipeSchemaRegionSnapshotEvent.decreaseReferenceCount(
          IoTDBSchemaRegionConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeSchemaRegionSnapshotEvent snapshotEvent)
      throws PipeException, IOException {
    final String pipeName = snapshotEvent.getPipeName();
    final long creationTime = snapshotEvent.getCreationTime();
    final File mTreeSnapshotFile = snapshotEvent.getMTreeSnapshotFile();
    final File tagLogSnapshotFile = snapshotEvent.getTagLogSnapshotFile();
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();
    final TPipeTransferResp resp;

    // 1. Transfer mTreeSnapshotFile, and tLog file if exists
    transferFilePieces(
        Collections.singletonMap(new Pair<>(pipeName, creationTime), 1.0),
        mTreeSnapshotFile,
        clientAndStatus,
        true);
    if (Objects.nonNull(tagLogSnapshotFile)) {
      transferFilePieces(
          Collections.singletonMap(new Pair<>(pipeName, creationTime), 1.0),
          tagLogSnapshotFile,
          clientAndStatus,
          true);
    }
    // 2. Transfer file seal signal, which means the snapshots are transferred completely
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(
                  // The pattern is surely Non-null
                  snapshotEvent.getTreePatternString(),
                  mTreeSnapshotFile.getName(),
                  mTreeSnapshotFile.length(),
                  Objects.nonNull(tagLogSnapshotFile) ? tagLogSnapshotFile.getName() : null,
                  Objects.nonNull(tagLogSnapshotFile) ? tagLogSnapshotFile.length() : 0,
                  snapshotEvent.getDatabaseName(),
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
              "Network error when seal snapshot file %s and %s, because %s.",
              mTreeSnapshotFile, tagLogSnapshotFile, e.getMessage()),
          e);
    }

    // Only handle the failed statuses to avoid string format performance overhead
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && resp.getStatus().getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          resp.getStatus(),
          String.format(
              "Seal file %s and %s error, result status %s.",
              mTreeSnapshotFile, tagLogSnapshotFile, resp.getStatus()),
          snapshotEvent.toString());
    }

    LOGGER.info("Successfully transferred file {} and {}.", mTreeSnapshotFile, tagLogSnapshotFile);
  }

  @Override
  protected PipeTransferFilePieceReq getTransferSingleFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) {
    throw new UnsupportedOperationException(
        "The schema region connector does not support transferring single file piece req.");
  }

  @Override
  protected PipeTransferFilePieceReq getTransferMultiFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq(fileName, position, payLoad);
  }
}
