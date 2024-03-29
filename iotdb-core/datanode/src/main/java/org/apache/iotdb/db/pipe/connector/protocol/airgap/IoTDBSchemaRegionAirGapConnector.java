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
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Objects;

public class IoTDBSchemaRegionAirGapConnector extends IoTDBDataNodeAirGapConnector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBSchemaRegionAirGapConnector.class);

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionAirGapConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionAirGapConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(Event event) throws Exception {
    final int socketIndex = nextSocketIndex();
    final Socket socket = sockets.get(socketIndex);

    if (event instanceof PipeSchemaRegionWritePlanEvent) {
      doTransfer(socket, (PipeSchemaRegionWritePlanEvent) event);
    } else if (event instanceof PipeSchemaRegionSnapshotEvent) {
      doTransfer(socket, (PipeSchemaRegionSnapshotEvent) event);
    } else if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBSchemaRegionAirGapConnector does not support transferring generic event: {}.",
          event);
    }
  }

  private void doTransfer(
      Socket socket, PipeSchemaRegionSnapshotEvent pipeSchemaRegionSnapshotEvent)
      throws PipeException, IOException {
    final File mtreeSnapshotFile = pipeSchemaRegionSnapshotEvent.getMTreeSnapshotFile();
    final File tagLogSnapshotFile = pipeSchemaRegionSnapshotEvent.getTagLogSnapshotFile();

    // 1. Transfer mTreeSnapshotFile, and tLog file if exists
    transferFilePieces(mtreeSnapshotFile, socket, true);
    if (Objects.nonNull(tagLogSnapshotFile)) {
      transferFilePieces(tagLogSnapshotFile, socket, true);
    }
    // 2. Transfer file seal signal, which means the snapshots is transferred completely
    if (!send(
        socket,
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferBytes(
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
  protected byte[] getTransferSingleFilePieceBytes(String fileName, long position, byte[] payLoad) {
    throw new UnsupportedOperationException(
        "The schema region air gap connector does not support transferring single file piece bytes.");
  }

  @Override
  protected byte[] getTransferMultiFilePieceBytes(String fileName, long position, byte[] payLoad)
      throws IOException {
    return PipeTransferSchemaSnapshotPieceReq.toTPipeTransferBytes(fileName, position, payLoad);
  }
}
