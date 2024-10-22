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

package org.apache.iotdb.db.pipe.connector.protocol.airgap;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

public class IoTDBSchemaRegionAirGapConnector extends IoTDBDataNodeAirGapConnector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSchemaRegionAirGapConnector.class);

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionAirGapConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionAirGapConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(final Event event) throws Exception {
    final int socketIndex = nextSocketIndex();
    final AirGapSocket socket = sockets.get(socketIndex);

    try {
      if (event instanceof PipeSchemaRegionWritePlanEvent) {
        doTransferWrapper(socket, (PipeSchemaRegionWritePlanEvent) event);
      } else if (event instanceof PipeSchemaRegionSnapshotEvent) {
        doTransferWrapper(socket, (PipeSchemaRegionSnapshotEvent) event);
      } else if (!(event instanceof PipeHeartbeatEvent)) {
        LOGGER.warn(
            "IoTDBSchemaRegionAirGapConnector does not support transferring generic event: {}.",
            event);
      }
    } catch (final IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer event %s, because %s.",
              ((EnrichedEvent) event).coreReportMessage(), e.getMessage()),
          e);
    }
  }

  private void doTransferWrapper(
      final AirGapSocket socket,
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionWritePlanEvent.increaseReferenceCount(
        IoTDBDataNodeAirGapConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeSchemaRegionWritePlanEvent);
    } finally {
      pipeSchemaRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBDataNodeAirGapConnector.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket,
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    if (!send(
        pipeSchemaRegionWritePlanEvent.getPipeName(),
        pipeSchemaRegionWritePlanEvent.getCreationTime(),
        socket,
        PipeTransferPlanNodeReq.toTPipeTransferBytes(
            pipeSchemaRegionWritePlanEvent.getPlanNode()))) {
      final String errorMessage =
          String.format(
              "Transfer data node write plan %s error. Socket: %s.",
              pipeSchemaRegionWritePlanEvent.getPlanNode().getType(), socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          pipeSchemaRegionWritePlanEvent.toString());
    }
  }

  private void doTransferWrapper(
      final AirGapSocket socket, final PipeSchemaRegionSnapshotEvent pipeSchemaRegionSnapshotEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionSnapshotEvent.increaseReferenceCount(
        IoTDBSchemaRegionAirGapConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeSchemaRegionSnapshotEvent);
    } finally {
      pipeSchemaRegionSnapshotEvent.decreaseReferenceCount(
          IoTDBSchemaRegionAirGapConnector.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeSchemaRegionSnapshotEvent pipeSchemaRegionSnapshotEvent)
      throws PipeException, IOException {
    final String pipeName = pipeSchemaRegionSnapshotEvent.getPipeName();
    final long creationTime = pipeSchemaRegionSnapshotEvent.getCreationTime();
    final File mtreeSnapshotFile = pipeSchemaRegionSnapshotEvent.getMTreeSnapshotFile();
    final File tagLogSnapshotFile = pipeSchemaRegionSnapshotEvent.getTagLogSnapshotFile();

    // 1. Transfer mTreeSnapshotFile, and tLog file if exists
    transferFilePieces(pipeName, creationTime, mtreeSnapshotFile, socket, true);
    if (Objects.nonNull(tagLogSnapshotFile)) {
      transferFilePieces(pipeName, creationTime, tagLogSnapshotFile, socket, true);
    }
    // 2. Transfer file seal signal, which means the snapshots is transferred completely
    if (!send(
        pipeName,
        creationTime,
        socket,
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferBytes(
            // The pattern is surely Non-null
            pipeSchemaRegionSnapshotEvent.getTreePatternString(),
            mtreeSnapshotFile.getName(),
            mtreeSnapshotFile.length(),
            Objects.nonNull(tagLogSnapshotFile) ? tagLogSnapshotFile.getName() : null,
            Objects.nonNull(tagLogSnapshotFile) ? tagLogSnapshotFile.length() : 0,
            pipeSchemaRegionSnapshotEvent.getDatabaseName(),
            pipeSchemaRegionSnapshotEvent.toSealTypeString()))) {
      final String errorMessage =
          String.format(
              "Seal schema region snapshot file %s and %s error. Socket %s.",
              mtreeSnapshotFile, tagLogSnapshotFile, socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          pipeSchemaRegionSnapshotEvent.toString());
    } else {
      LOGGER.info(
          "Successfully transferred schema region snapshot {} and {}.",
          mtreeSnapshotFile,
          tagLogSnapshotFile);
    }
  }

  @Override
  protected byte[] getTransferSingleFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) {
    throw new UnsupportedOperationException(
        "The schema region air gap connector does not support transferring single file piece bytes.");
  }

  @Override
  protected byte[] getTransferMultiFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferSchemaSnapshotPieceReq.toTPipeTransferBytes(fileName, position, payLoad);
  }
}
