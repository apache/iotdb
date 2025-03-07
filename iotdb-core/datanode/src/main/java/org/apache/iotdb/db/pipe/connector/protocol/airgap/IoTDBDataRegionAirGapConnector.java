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
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@TreeModel
@TableModel
public class IoTDBDataRegionAirGapConnector extends IoTDBDataNodeAirGapConnector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBDataRegionAirGapConnector.class);

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBDataRegionAirGapConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    try {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        doTransferWrapper((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      } else {
        doTransferWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent);
      }
    } catch (final IOException e) {
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tablet insertion event %s, because %s.",
              ((EnrichedEvent) tabletInsertionEvent).coreReportMessage(), e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of tsFileInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBDataRegionAirGapConnector only support PipeTsFileInsertionEvent. Ignore {}.",
          tsFileInsertionEvent);
      return;
    }

    if (!((PipeTsFileInsertionEvent) tsFileInsertionEvent).waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          ((PipeTsFileInsertionEvent) tsFileInsertionEvent).getTsFile());
      return;
    }

    try {
      doTransferWrapper((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (final IOException e) {
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile insertion event %s, because %s.",
              ((PipeTsFileInsertionEvent) tsFileInsertionEvent).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    try {
      if (event instanceof PipeDeleteDataNodeEvent) {
        doTransferWrapper((PipeDeleteDataNodeEvent) event);
      } else if (!(event instanceof PipeHeartbeatEvent || event instanceof PipeTerminateEvent)) {
        LOGGER.warn(
            "IoTDBDataRegionAirGapConnector does not support transferring generic event: {}.",
            event);
      }
    } catch (final IOException e) {
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile event %s, because %s.",
              ((PipeDeleteDataNodeEvent) event).coreReportMessage(), e.getMessage()),
          e);
    }
  }

  private void doTransferWrapper(final PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeDeleteDataNodeEvent.increaseReferenceCount(
        IoTDBDataNodeAirGapConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeDeleteDataNodeEvent);
    } finally {
      pipeDeleteDataNodeEvent.decreaseReferenceCount(
          IoTDBDataNodeAirGapConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent)
      throws PipeException, IOException {
    final List<Integer> socketIndexes =
        shouldSendToAllClients
            ? allAliveSocketsIndex()
            : Collections.singletonList(nextSocketIndex());
    final byte[] bytes =
        PipeTransferPlanNodeReq.toTPipeTransferBytes(pipeDeleteDataNodeEvent.getDeleteDataNode());
    for (final int socketIndex : socketIndexes) {
      final AirGapSocket socket = sockets.get(socketIndex);
      try {
        if (!send(
            pipeDeleteDataNodeEvent.getPipeName(),
            pipeDeleteDataNodeEvent.getCreationTime(),
            socket,
            bytes)) {
          final String errorMessage =
              String.format(
                  "Transfer deletion %s error. Socket: %s.",
                  pipeDeleteDataNodeEvent.getDeleteDataNode().getType(), socket);
          receiverStatusHandler.handle(
              new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                  .setMessage(errorMessage),
              errorMessage,
              pipeDeleteDataNodeEvent.toString());
        }
      } catch (final IOException e) {
        isSocketAlive.set(socketIndex, false);
        throw e;
      }
    }
  }

  private void doTransferWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAirGapConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeInsertNodeTabletInsertionEvent);
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAirGapConnector.class.getName(), false);
    }
  }

  private void doTransfer(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException, IOException {
    final List<Integer> socketIndexes =
        shouldSendToAllClients
            ? allAliveSocketsIndex()
            : Collections.singletonList(nextSocketIndex());
    final InsertNode insertNode =
        pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
    final byte[] bytes =
        Objects.isNull(insertNode)
            ? PipeTransferTabletBinaryReqV2.toTPipeTransferBytes(
                pipeInsertNodeTabletInsertionEvent.getByteBuffer(),
                pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
                    ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
                    : null)
            : PipeTransferTabletInsertNodeReqV2.toTPipeTransferBytes(
                insertNode,
                pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
                    ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
                    : null);
    for (final int socketIndex : socketIndexes) {
      final AirGapSocket socket = sockets.get(socketIndex);
      try {
        if (!send(
            pipeInsertNodeTabletInsertionEvent.getPipeName(),
            pipeInsertNodeTabletInsertionEvent.getCreationTime(),
            socket,
            bytes)) {
          final String errorMessage =
              String.format(
                  "Transfer PipeInsertNodeTabletInsertionEvent %s error. Socket: %s",
                  pipeInsertNodeTabletInsertionEvent, socket);
          receiverStatusHandler.handle(
              new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                  .setMessage(errorMessage),
              errorMessage,
              pipeInsertNodeTabletInsertionEvent.toString());
        }
      } catch (final IOException e) {
        isSocketAlive.set(socketIndex, false);
        throw e;
      }
    }
  }

  private void doTransferWrapper(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAirGapConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeRawTabletInsertionEvent);
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAirGapConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, IOException {
    final List<Integer> socketIndexes =
        shouldSendToAllClients
            ? allAliveSocketsIndex()
            : Collections.singletonList(nextSocketIndex());
    final byte[] bytes =
        PipeTransferTabletRawReqV2.toTPipeTransferBytes(
            pipeRawTabletInsertionEvent.convertToTablet(),
            pipeRawTabletInsertionEvent.isAligned(),
            pipeRawTabletInsertionEvent.isTableModelEvent()
                ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                : null);
    for (final int socketIndex : socketIndexes) {
      final AirGapSocket socket = sockets.get(socketIndex);
      try {
        if (!send(
            pipeRawTabletInsertionEvent.getPipeName(),
            pipeRawTabletInsertionEvent.getCreationTime(),
            socket,
            bytes)) {
          final String errorMessage =
              String.format(
                  "Transfer PipeRawTabletInsertionEvent %s error. Socket: %s.",
                  pipeRawTabletInsertionEvent, socket);
          receiverStatusHandler.handle(
              new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                  .setMessage(errorMessage),
              errorMessage,
              pipeRawTabletInsertionEvent.toString());
        }
      } catch (final IOException e) {
        isSocketAlive.set(socketIndex, false);
        throw e;
      }
    }
  }

  private void doTransferWrapper(final PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeTsFileInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAirGapConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeTsFileInsertionEvent);
    } finally {
      pipeTsFileInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAirGapConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    final String pipeName = pipeTsFileInsertionEvent.getPipeName();
    final long creationTime = pipeTsFileInsertionEvent.getCreationTime();
    final File tsFile = pipeTsFileInsertionEvent.getTsFile();

    final List<Integer> socketIndexes =
        shouldSendToAllClients
            ? allAliveSocketsIndex()
            : Collections.singletonList(nextSocketIndex());

    // 1. Transfer file piece by piece, and mod if needed
    final byte[] bytes;
    if (pipeTsFileInsertionEvent.isWithMod() && supportModsIfIsDataNodeReceiver) {
      final File modFile = pipeTsFileInsertionEvent.getModFile();
      transferFilePieces(pipeName, creationTime, modFile, socketIndexes, true);
      transferFilePieces(pipeName, creationTime, tsFile, socketIndexes, true);
      bytes =
          PipeTransferTsFileSealWithModReq.toTPipeTransferBytes(
              modFile.getName(),
              modFile.length(),
              tsFile.getName(),
              tsFile.length(),
              pipeTsFileInsertionEvent.isTableModelEvent()
                  ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
                  : null);
    } else {
      transferFilePieces(pipeName, creationTime, tsFile, socketIndexes, false);
      bytes =
          PipeTransferTsFileSealWithModReq.toTPipeTransferBytes(
              tsFile.getName(),
              tsFile.length(),
              pipeTsFileInsertionEvent.isTableModelEvent()
                  ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
                  : null);
    }
    // 2. Transfer file seal signal , which means the file is transferred completely
    for (final int socketIndex : socketIndexes) {
      final AirGapSocket socket = sockets.get(socketIndex);
      final String errorMessage = String.format("Seal file %s error. Socket %s.", tsFile, socket);
      try {
        if (!send(pipeName, creationTime, socket, bytes)) {
          receiverStatusHandler.handle(
              new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                  .setMessage(errorMessage),
              errorMessage,
              pipeTsFileInsertionEvent.toString());
        }
      } catch (final IOException e) {
        isSocketAlive.set(socketIndex, false);
        throw e;
      }
    }

    LOGGER.info("Successfully transferred file {}.", tsFile);
  }

  @Override
  protected byte[] getTransferSingleFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferTsFilePieceReq.toTPipeTransferBytes(fileName, position, payLoad);
  }

  @Override
  protected byte[] getTransferMultiFilePieceBytes(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferTsFilePieceWithModReq.toTPipeTransferBytes(fileName, position, payLoad);
  }
}
