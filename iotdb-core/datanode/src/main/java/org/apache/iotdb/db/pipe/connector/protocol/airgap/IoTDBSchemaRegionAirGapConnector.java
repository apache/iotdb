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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.Arrays;

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
    final File snapshot = pipeSchemaRegionSnapshotEvent.getSnapshot();

    // 1. Transfer file piece by piece
    final int readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(snapshot, "r")) {
      while (true) {
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        if (!send(
            socket,
            PipeTransferSchemaSnapshotPieceReq.toTPipeTransferBytes(
                snapshot.getName(),
                position,
                readLength == readFileBufferSize
                    ? readBuffer
                    : Arrays.copyOfRange(readBuffer, 0, readLength)))) {
          throw new PipeException(
              String.format(
                  "Transfer schema region snapshot %s error. Socket %s.", snapshot, socket));
        } else {
          position += readLength;
        }
      }
    }

    // 2. Transfer file seal signal, which means the file is transferred completely
    if (!send(
        socket,
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferBytes(
            snapshot.getName(), snapshot.length()))) {
      throw new PipeException(
          String.format("Seal schema region snapshot %s error. Socket %s.", snapshot, socket));
    } else {
      LOGGER.info("Successfully transferred schema region snapshot {}.", snapshot);
    }
  }
}
