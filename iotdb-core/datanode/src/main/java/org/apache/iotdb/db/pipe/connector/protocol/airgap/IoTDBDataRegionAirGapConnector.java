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
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
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
import java.net.Socket;
import java.util.Objects;

public class IoTDBDataRegionAirGapConnector extends IoTDBDataNodeAirGapConnector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBDataRegionAirGapConnector.class);

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
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

    final int socketIndex = nextSocketIndex();
    final Socket socket = sockets.get(socketIndex);

    try {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        doTransfer(socket, (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      } else {
        doTransfer(socket, (PipeRawTabletInsertionEvent) tabletInsertionEvent);
      }
    } catch (IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tablet insertion event %s, because %s.",
              tabletInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
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

    final int socketIndex = nextSocketIndex();
    final Socket socket = sockets.get(socketIndex);

    try {
      doTransfer(socket, (PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile insertion event %s, because %s.",
              tsFileInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    final int socketIndex = nextSocketIndex();
    final Socket socket = sockets.get(socketIndex);

    try {
      if (event instanceof PipeSchemaRegionWritePlanEvent) {
        doTransfer(socket, (PipeSchemaRegionWritePlanEvent) event);
      } else if (!(event instanceof PipeHeartbeatEvent)) {
        LOGGER.warn(
            "IoTDBDataRegionAirGapConnector does not support transferring generic event: {}.",
            event);
      }
    } catch (IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile event %s, because %s.", event, e.getMessage()),
          e);
    }
  }

  private void doTransfer(
      Socket socket, PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException, IOException {
    final InsertNode insertNode =
        pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
    final byte[] bytes =
        Objects.isNull(insertNode)
            ? PipeTransferTabletBinaryReq.toTPipeTransferBytes(
                pipeInsertNodeTabletInsertionEvent.getByteBuffer())
            : PipeTransferTabletInsertNodeReq.toTPipeTransferBytes(insertNode);

    if (!send(socket, bytes)) {
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
  }

  private void doTransfer(Socket socket, PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, IOException {
    if (!send(
        socket,
        PipeTransferTabletRawReq.toTPipeTransferBytes(
            pipeRawTabletInsertionEvent.convertToTablet(),
            pipeRawTabletInsertionEvent.isAligned()))) {
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
  }

  private void doTransfer(Socket socket, PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    final File tsFile = pipeTsFileInsertionEvent.getTsFile();
    final String errorMessage = String.format("Seal file %s error. Socket %s.", tsFile, socket);

    // 1. Transfer file piece by piece, and mod if needed
    if (pipeTsFileInsertionEvent.isWithMod() && supportModsIfIsDataNodeReceiver) {
      final File modFile = pipeTsFileInsertionEvent.getModFile();
      transferFilePieces(modFile, socket, true);
      transferFilePieces(tsFile, socket, true);
      // 2. Transfer file seal signal with mod, which means the file is transferred completely
      if (!send(
          socket,
          PipeTransferTsFileSealWithModReq.toTPipeTransferBytes(
              modFile.getName(), modFile.length(), tsFile.getName(), tsFile.length()))) {
        receiverStatusHandler.handle(
            new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                .setMessage(errorMessage),
            errorMessage,
            pipeTsFileInsertionEvent.toString());
      } else {
        LOGGER.info("Successfully transferred file {}.", tsFile);
      }
    } else {
      transferFilePieces(tsFile, socket, false);
      // 2. Transfer file seal signal without mod, which means the file is transferred completely
      if (!send(
          socket,
          PipeTransferTsFileSealReq.toTPipeTransferBytes(tsFile.getName(), tsFile.length()))) {
        receiverStatusHandler.handle(
            new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                .setMessage(errorMessage),
            errorMessage,
            pipeTsFileInsertionEvent.toString());
      } else {
        LOGGER.info("Successfully transferred file {}.", tsFile);
      }
    }
  }

  @Override
  protected byte[] getTransferSingleFilePieceBytes(String fileName, long position, byte[] payLoad)
      throws IOException {
    return PipeTransferTsFilePieceReq.toTPipeTransferBytes(fileName, position, payLoad);
  }

  @Override
  protected byte[] getTransferMultiFilePieceBytes(String fileName, long position, byte[] payLoad)
      throws IOException {
    return PipeTransferTsFilePieceWithModReq.toTPipeTransferBytes(fileName, position, payLoad);
  }
}
