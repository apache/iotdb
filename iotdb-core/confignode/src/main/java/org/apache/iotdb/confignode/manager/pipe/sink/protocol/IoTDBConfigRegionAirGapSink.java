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

package org.apache.iotdb.confignode.manager.pipe.sink.protocol;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBAirGapSink;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
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
import java.util.HashMap;
import java.util.Objects;

@TreeModel
@TableModel
public class IoTDBConfigRegionAirGapSink extends IoTDBAirGapSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigRegionAirGapSink.class);

  @Override
  protected byte[] generateHandShakeV1Payload() throws IOException {
    return PipeTransferConfigNodeHandshakeV1Req.toTPipeTransferBytes(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  @Override
  protected byte[] generateHandShakeV2Payload() throws IOException {
    final HashMap<String, String> params = new HashMap<>();
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLUSTER_ID,
        ConfigNode.getInstance().getConfigManager().getClusterManager().getClusterId());
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_CONVERT_ON_TYPE_MISMATCH,
        Boolean.toString(shouldReceiverConvertOnTypeMismatch));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_LOAD_TSFILE_STRATEGY, loadTsFileStrategy);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USER_ID, userId);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_USERNAME, username);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_CLI_HOSTNAME, cliHostname);
    params.put(PipeTransferHandshakeConstant.HANDSHAKE_KEY_PASSWORD, password);
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_VALIDATE_TSFILE,
        Boolean.toString(loadTsFileValidation));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_MARK_AS_PIPE_REQUEST,
        Boolean.toString(shouldMarkAsPipeRequest));
    params.put(
        PipeTransferHandshakeConstant.HANDSHAKE_KEY_SKIP_IF, Boolean.toString(skipIfNoPrivileges));

    return PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferBytes(params);
  }

  @Override
  protected void mayLimitRateAndRecordIO(final long requiredBytes) {
    // Do nothing
  }

  @Override
  protected boolean mayNeedHandshakeWhenFail() {
    return true;
  }

  @Override
  protected byte[] getTransferSingleFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) {
    throw new UnsupportedOperationException(
        "The config region air gap connector does not support transferring single file piece bytes.");
  }

  @Override
  protected byte[] getTransferMultiFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferConfigSnapshotPieceReq.toTPipeTransferBytes(fileName, position, payLoad);
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionAirGapConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionAirGapConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(final Event event) throws Exception {
    final int socketIndex = nextSocketIndex();
    final AirGapSocket socket = sockets.get(socketIndex);

    try {
      if (event instanceof PipeConfigRegionWritePlanEvent) {
        doTransferWrapper(socket, (PipeConfigRegionWritePlanEvent) event);
      } else if (event instanceof PipeConfigRegionSnapshotEvent) {
        doTransferWrapper(socket, (PipeConfigRegionSnapshotEvent) event);
      } else if (!(event instanceof PipeHeartbeatEvent)) {
        LOGGER.warn(
            "IoTDBConfigRegionAirGapConnector does not support transferring generic event: {}.",
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
      final PipeConfigRegionWritePlanEvent pipeConfigRegionWritePlanEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeConfigRegionWritePlanEvent.increaseReferenceCount(
        IoTDBConfigRegionAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeConfigRegionWritePlanEvent);
    } finally {
      pipeConfigRegionWritePlanEvent.decreaseReferenceCount(
          IoTDBConfigRegionAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket,
      final PipeConfigRegionWritePlanEvent pipeConfigRegionWritePlanEvent)
      throws PipeException, IOException {
    if (!send(
        pipeConfigRegionWritePlanEvent.getPipeName(),
        pipeConfigRegionWritePlanEvent.getCreationTime(),
        socket,
        PipeTransferConfigPlanReq.toTPipeTransferBytes(
            pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan()))) {
      final String errorMessage =
          String.format(
              "Transfer config region write plan %s error. Socket: %s.",
              pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan().getType(), socket);
      // Send handshake because we don't know whether the receiver side configNode
      // has set up a new one
      sendHandshakeReq(socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          pipeConfigRegionWritePlanEvent.toString(),
          true);
    }
  }

  private void doTransferWrapper(
      final AirGapSocket socket, final PipeConfigRegionSnapshotEvent pipeConfigRegionSnapshotEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeConfigRegionSnapshotEvent.increaseReferenceCount(
        IoTDBConfigRegionAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeConfigRegionSnapshotEvent);
    } finally {
      pipeConfigRegionSnapshotEvent.decreaseReferenceCount(
          IoTDBConfigRegionAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeConfigRegionSnapshotEvent pipeConfigRegionSnapshotEvent)
      throws PipeException, IOException {
    final String pipeName = pipeConfigRegionSnapshotEvent.getPipeName();
    final long creationTime = pipeConfigRegionSnapshotEvent.getCreationTime();
    final File snapshot = pipeConfigRegionSnapshotEvent.getSnapshotFile();
    final File templateFile = pipeConfigRegionSnapshotEvent.getTemplateFile();

    // 1. Transfer snapshotFile, and template file if exists
    transferFilePieces(pipeName, creationTime, snapshot, socket, true);
    if (Objects.nonNull(templateFile)) {
      transferFilePieces(pipeName, creationTime, templateFile, socket, true);
    }
    // 2. Transfer file seal signal, which means the snapshots are transferred completely
    if (!send(
        pipeName,
        creationTime,
        socket,
        PipeTransferConfigSnapshotSealReq.toTPipeTransferBytes(
            // The pattern is surely Non-null
            pipeConfigRegionSnapshotEvent.getTreePatternString(),
            pipeConfigRegionSnapshotEvent.getTablePattern().getDatabasePattern(),
            pipeConfigRegionSnapshotEvent.getTablePattern().getTablePattern(),
            pipeConfigRegionSnapshotEvent.getTreePattern().isTreeModelDataAllowedToBeCaptured(),
            pipeConfigRegionSnapshotEvent.getTablePattern().isTableModelDataAllowedToBeCaptured(),
            snapshot.getName(),
            snapshot.length(),
            Objects.nonNull(templateFile) ? templateFile.getName() : null,
            Objects.nonNull(templateFile) ? templateFile.length() : 0,
            pipeConfigRegionSnapshotEvent.getFileType(),
            pipeConfigRegionSnapshotEvent.toSealTypeString(),
            pipeConfigRegionSnapshotEvent.getAuthUserName()))) {
      final String errorMessage =
          String.format("Seal config region snapshot %s error. Socket %s.", snapshot, socket);
      // Send handshake because we don't know whether the receiver side configNode
      // has set up a new one
      sendHandshakeReq(socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          pipeConfigRegionSnapshotEvent.toString(),
          true);
    } else {
      LOGGER.info("Successfully transferred config region snapshot {}.", snapshot);
    }
  }
}
