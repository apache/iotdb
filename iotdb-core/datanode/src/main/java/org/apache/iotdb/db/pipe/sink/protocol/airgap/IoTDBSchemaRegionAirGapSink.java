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

package org.apache.iotdb.db.pipe.sink.protocol.airgap;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeSchemaRegionWritePlanEventBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
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
import java.nio.ByteBuffer;
import java.util.Objects;

@TreeModel
@TableModel
public class IoTDBSchemaRegionAirGapSink extends IoTDBDataNodeAirGapSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSchemaRegionAirGapSink.class);

  private PipeSchemaRegionWritePlanEventBatch schemaRegionWritePlanEventBatch;

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    if (isTabletBatchModeEnabled) {
      schemaRegionWritePlanEventBatch = new PipeSchemaRegionWritePlanEventBatch(parameters);
    }
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TABLETINSERTIONEVENT);
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.IOTDBSCHEMAREGIONAIRGAPSINK_CAN_T_TRANSFER_TSFILEINSERTIONEVENT);
  }

  @Override
  public void transfer(final Event event) throws Exception {
    final int socketIndex = nextSocketIndex();
    final AirGapSocket socket = sockets.get(socketIndex);

    try {
      if (event instanceof PipeSchemaRegionWritePlanEvent) {
        if (isTabletBatchModeEnabled && Objects.nonNull(schemaRegionWritePlanEventBatch)) {
          doTransferWithBatch(socket, (PipeSchemaRegionWritePlanEvent) event);
        } else {
          doTransferWrapper(socket, (PipeSchemaRegionWritePlanEvent) event);
        }
      } else if (event instanceof PipeSchemaRegionSnapshotEvent) {
        flushBatchedEventsIfNecessary(socket);
        doTransferWrapper(socket, (PipeSchemaRegionSnapshotEvent) event);
      } else {
        flushBatchedEventsIfNecessary(socket);
        if (!(event instanceof PipeHeartbeatEvent)) {
          LOGGER.warn(
              DataNodePipeMessages
                  .IOTDBSCHEMAREGIONAIRGAPSINK_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT,
              event);
        }
      }
    } catch (final IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_EVENT_S_BECAUSE_S_60A63AD7,
              ((EnrichedEvent) event).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  private void doTransferWithBatch(
      final AirGapSocket socket, final PipeSchemaRegionWritePlanEvent event)
      throws PipeException, IOException {
    if (tryTransferInBatch(socket, event)) {
      return;
    }

    doTransferWrapper(socket, event);
  }

  private boolean tryTransferInBatch(
      final AirGapSocket socket, final PipeSchemaRegionWritePlanEvent event)
      throws PipeException, IOException {
    if (tryAppendToBatchAndFlushIfNecessary(socket, event)) {
      return true;
    }

    if (schemaRegionWritePlanEventBatch.isEmpty()) {
      return false;
    }

    flushBatchedEventsIfNecessary(socket);
    return tryAppendToBatchAndFlushIfNecessary(socket, event);
  }

  private boolean tryAppendToBatchAndFlushIfNecessary(
      final AirGapSocket socket, final PipeSchemaRegionWritePlanEvent event)
      throws PipeException, IOException {
    if (!schemaRegionWritePlanEventBatch.onEvent(event)) {
      return false;
    }

    if (schemaRegionWritePlanEventBatch.shouldEmit()) {
      flushBatchedEventsIfNecessary(socket);
    }
    return true;
  }

  private void flushBatchedEventsIfNecessary(final AirGapSocket socket)
      throws PipeException, IOException {
    if (Objects.isNull(schemaRegionWritePlanEventBatch)
        || schemaRegionWritePlanEventBatch.isEmpty()) {
      return;
    }

    schemaRegionWritePlanEventBatch.recordBatchMetrics();
    doTransfer(socket, schemaRegionWritePlanEventBatch);
    schemaRegionWritePlanEventBatch.decreaseEventsReferenceCount(
        IoTDBSchemaRegionAirGapSink.class.getName(), true);
    schemaRegionWritePlanEventBatch.onSuccess();
  }

  private void doTransferWrapper(
      final AirGapSocket socket,
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionWritePlanEvent.increaseReferenceCount(
        IoTDBDataNodeAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeSchemaRegionWritePlanEvent);
    } finally {
      pipeSchemaRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBDataNodeAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket,
      final PipeSchemaRegionWritePlanEvent pipeSchemaRegionWritePlanEvent)
      throws PipeException, IOException {
    doTransfer(
        socket,
        pipeSchemaRegionWritePlanEvent.getPlanNode(),
        pipeSchemaRegionWritePlanEvent.getPipeName(),
        pipeSchemaRegionWritePlanEvent.getCreationTime(),
        pipeSchemaRegionWritePlanEvent.toString());
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeSchemaRegionWritePlanEventBatch batch)
      throws PipeException, IOException {
    final PlanNode planNode = batch.toPlanNode();
    doTransfer(
        socket,
        planNode,
        batch.toPlanNodeByteBuffer(),
        batch.getPipeName(),
        batch.getCreationTime(),
        planNode.toString());
  }

  private void doTransfer(
      final AirGapSocket socket,
      final PlanNode planNode,
      final String pipeName,
      final long creationTime,
      final String eventDescription)
      throws PipeException, IOException {
    doTransfer(socket, planNode, null, pipeName, creationTime, eventDescription);
  }

  private void doTransfer(
      final AirGapSocket socket,
      final PlanNode planNode,
      final ByteBuffer serializedPlanNode,
      final String pipeName,
      final long creationTime,
      final String eventDescription)
      throws PipeException, IOException {
    if (!send(
        pipeName,
        creationTime,
        socket,
        Objects.nonNull(serializedPlanNode)
            ? PipeTransferPlanNodeReq.toTPipeTransferBytes(serializedPlanNode)
            : PipeTransferPlanNodeReq.toTPipeTransferBytes(planNode))) {
      final String errorMessage =
          String.format(
              "Transfer data node write plan %s error. Socket: %s.", planNode.getType(), socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          eventDescription,
          true);
    }
  }

  private void doTransferWrapper(
      final AirGapSocket socket, final PipeSchemaRegionSnapshotEvent pipeSchemaRegionSnapshotEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionSnapshotEvent.increaseReferenceCount(
        IoTDBSchemaRegionAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeSchemaRegionSnapshotEvent);
    } finally {
      pipeSchemaRegionSnapshotEvent.decreaseReferenceCount(
          IoTDBSchemaRegionAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeSchemaRegionSnapshotEvent pipeSchemaRegionSnapshotEvent)
      throws PipeException, IOException {
    final String pipeName = pipeSchemaRegionSnapshotEvent.getPipeName();
    final long creationTime = pipeSchemaRegionSnapshotEvent.getCreationTime();
    final File mtreeSnapshotFile = pipeSchemaRegionSnapshotEvent.getMTreeSnapshotFile();
    final File tagLogSnapshotFile = pipeSchemaRegionSnapshotEvent.getTagLogSnapshotFile();
    final File attributeSnapshotFile = pipeSchemaRegionSnapshotEvent.getAttributeSnapshotFile();

    // 1. Transfer mTreeSnapshotFile, and tLog file if exists
    transferFilePieces(pipeName, creationTime, mtreeSnapshotFile, socket, true);
    if (Objects.nonNull(tagLogSnapshotFile)) {
      transferFilePieces(pipeName, creationTime, tagLogSnapshotFile, socket, true);
    }
    if (Objects.nonNull(attributeSnapshotFile)) {
      transferFilePieces(pipeName, creationTime, attributeSnapshotFile, socket, true);
    }
    // 2. Transfer file seal signal, which means the snapshots is transferred completely
    if (!send(
        pipeName,
        creationTime,
        socket,
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferBytes(
            // The pattern is surely Non-null
            pipeSchemaRegionSnapshotEvent.getTreePatternString(),
            pipeSchemaRegionSnapshotEvent.getTablePattern().getDatabasePattern(),
            pipeSchemaRegionSnapshotEvent.getTablePattern().getTablePattern(),
            pipeSchemaRegionSnapshotEvent.getTreePattern().isTreeModelDataAllowedToBeCaptured(),
            pipeSchemaRegionSnapshotEvent.getTablePattern().isTableModelDataAllowedToBeCaptured(),
            mtreeSnapshotFile.getName(),
            mtreeSnapshotFile.length(),
            Objects.nonNull(tagLogSnapshotFile) ? tagLogSnapshotFile.getName() : null,
            Objects.nonNull(tagLogSnapshotFile) ? tagLogSnapshotFile.length() : 0,
            Objects.nonNull(attributeSnapshotFile) ? attributeSnapshotFile.getName() : null,
            Objects.nonNull(attributeSnapshotFile) ? attributeSnapshotFile.length() : 0,
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
          pipeSchemaRegionSnapshotEvent.toString(),
          true);
    } else {
      LOGGER.info(
          DataNodePipeMessages.SUCCESSFULLY_TRANSFERRED_SCHEMA_REGION_SNAPSHOT_AND,
          mtreeSnapshotFile,
          tagLogSnapshotFile,
          attributeSnapshotFile);
    }
  }

  @Override
  protected void mayLimitRateAndRecordIO(final long requiredBytes) {
    // Do nothing
  }

  @Override
  protected byte[] getTransferSingleFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.THE_SCHEMA_REGION_AIR_GAP_CONNECTOR_DOES);
  }

  @Override
  protected byte[] getTransferMultiFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferSchemaSnapshotPieceReq.toTPipeTransferBytes(fileName, position, payLoad);
  }

  @Override
  public synchronized void discardEventsOfPipe(
      final String pipeNameToDrop, final long creationTimeToDrop, final int regionId) {
    if (Objects.nonNull(schemaRegionWritePlanEventBatch)) {
      schemaRegionWritePlanEventBatch.discardEventsOfPipe(
          pipeNameToDrop, creationTimeToDrop, regionId);
    }
  }

  @Override
  public void close() {
    if (Objects.nonNull(schemaRegionWritePlanEventBatch)) {
      schemaRegionWritePlanEventBatch.close();
    }
    super.close();
  }

  @Override
  public void setSchemaBatchSizeHistogram(final Histogram schemaBatchSizeHistogram) {
    if (Objects.nonNull(schemaRegionWritePlanEventBatch)) {
      schemaRegionWritePlanEventBatch.setBatchSizeHistogram(schemaBatchSizeHistogram);
    }
  }

  @Override
  public void setSchemaBatchTimeIntervalHistogram(
      final Histogram schemaBatchTimeIntervalHistogram) {
    if (Objects.nonNull(schemaRegionWritePlanEventBatch)) {
      schemaRegionWritePlanEventBatch.setBatchTimeIntervalHistogram(
          schemaBatchTimeIntervalHistogram);
    }
  }

  @Override
  public void setBatchEventSizeHistogram(final Histogram eventSizeHistogram) {
    if (Objects.nonNull(schemaRegionWritePlanEventBatch)) {
      schemaRegionWritePlanEventBatch.setEventSizeHistogram(eventSizeHistogram);
    }
  }
}
