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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeSchemaRegionWritePlanEventBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

public class IoTDBSchemaRegionSink extends IoTDBDataNodeSyncSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSchemaRegionSink.class);

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
      if (isTabletBatchModeEnabled && Objects.nonNull(schemaRegionWritePlanEventBatch)) {
        doTransferWithBatch((PipeSchemaRegionWritePlanEvent) event);
      } else {
        super.doTransferWrapper((PipeSchemaRegionWritePlanEvent) event);
      }
    } else if (event instanceof PipeSchemaRegionSnapshotEvent) {
      flushBatchedEventsIfNecessary();
      doTransferWrapper((PipeSchemaRegionSnapshotEvent) event);
    } else {
      flushBatchedEventsIfNecessary();
      if (!(event instanceof PipeHeartbeatEvent)) {
        LOGGER.warn(
            "IoTDBSchemaRegionConnector does not support transferring generic event: {}.", event);
      }
    }
  }

  private void doTransferWithBatch(final PipeSchemaRegionWritePlanEvent event)
      throws PipeException {
    if (tryTransferInBatch(event)) {
      return;
    }

    super.doTransferWrapper(event);
  }

  private boolean tryTransferInBatch(final PipeSchemaRegionWritePlanEvent event)
      throws PipeException {
    if (tryAppendToBatchAndFlushIfNecessary(event)) {
      return true;
    }

    if (schemaRegionWritePlanEventBatch.isEmpty()) {
      return false;
    }

    flushBatchedEventsIfNecessary();
    return tryAppendToBatchAndFlushIfNecessary(event);
  }

  private boolean tryAppendToBatchAndFlushIfNecessary(final PipeSchemaRegionWritePlanEvent event)
      throws PipeException {
    if (!schemaRegionWritePlanEventBatch.onEvent(event)) {
      return false;
    }

    if (schemaRegionWritePlanEventBatch.shouldEmit()) {
      flushBatchedEventsIfNecessary();
    }
    return true;
  }

  private void flushBatchedEventsIfNecessary() throws PipeException {
    if (Objects.isNull(schemaRegionWritePlanEventBatch)
        || schemaRegionWritePlanEventBatch.isEmpty()) {
      return;
    }

    schemaRegionWritePlanEventBatch.recordBatchMetrics();
    doTransfer(schemaRegionWritePlanEventBatch);
    schemaRegionWritePlanEventBatch.decreaseEventsReferenceCount(
        IoTDBSchemaRegionSink.class.getName(), true);
    schemaRegionWritePlanEventBatch.onSuccess();
  }

  private void doTransfer(final PipeSchemaRegionWritePlanEventBatch batch) throws PipeException {
    final PlanNode planNode = batch.toPlanNode();
    doTransfer(
        planNode,
        batch.toPlanNodeByteBuffer(),
        batch.getPipeName(),
        batch.getCreationTime(),
        planNode.toString());
    LOGGER.info("Successfully transferred batched schema events, batch size {}.", batch.size());
  }

  private void doTransfer(
      final PlanNode planNode,
      final ByteBuffer serializedPlanNode,
      final String pipeName,
      final long creationTime,
      final String eventDescription)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    final TPipeTransferResp resp;
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              Objects.nonNull(serializedPlanNode)
                  ? PipeTransferPlanNodeReq.toTPipeTransferReq(planNode, serializedPlanNode)
                  : PipeTransferPlanNodeReq.toTPipeTransferReq(planNode));
      rateLimitIfNeeded(
          pipeName, creationTime, clientAndStatus.getLeft().getEndPoint(), req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer schema region write plan %s, because %s.",
              planNode.getType(), e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "Transfer data node write plan %s error, result status %s.",
              planNode.getType(), status),
          eventDescription,
          true);
    }
  }

  private void doTransferWrapper(final PipeSchemaRegionSnapshotEvent pipeSchemaRegionSnapshotEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeSchemaRegionSnapshotEvent.increaseReferenceCount(
        IoTDBSchemaRegionSink.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeSchemaRegionSnapshotEvent);
    } finally {
      pipeSchemaRegionSnapshotEvent.decreaseReferenceCount(
          IoTDBSchemaRegionSink.class.getName(), false);
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
                  snapshotEvent.getPatternString(),
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
          snapshotEvent.toString(),
          true);
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

  @Override
  protected void mayLimitRateAndRecordIO(final long requiredBytes) {
    // Do nothing
  }
}
