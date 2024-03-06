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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.response.PipeTransferFilePieceResp;
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
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class IoTDBSchemaRegionConnector extends IoTDBDataNodeSyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSchemaRegionConnector.class);

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBSchemaRegionConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(Event event) throws Exception {
    if (event instanceof PipeSchemaRegionWritePlanEvent) {
      doTransfer((PipeSchemaRegionWritePlanEvent) event);
    } else if (event instanceof PipeSchemaRegionSnapshotEvent) {
      doTransfer((PipeSchemaRegionSnapshotEvent) event);
    } else if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBSchemaRegionConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransfer(PipeSchemaRegionSnapshotEvent snapshotEvent)
      throws PipeException, IOException {
    final File snapshot = snapshotEvent.getSnapshot();
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

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

        final PipeTransferFilePieceResp resp;
        try {
          resp =
              PipeTransferFilePieceResp.fromTPipeTransferResp(
                  clientAndStatus
                      .getLeft()
                      .pipeTransfer(
                          PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq(
                              snapshot.getName(),
                              position,
                              readLength == readFileBufferSize
                                  ? readBuffer
                                  : Arrays.copyOfRange(readBuffer, 0, readLength))));
        } catch (Exception e) {
          clientAndStatus.setRight(false);
          throw new PipeConnectionException(
              String.format(
                  "Network error when transfer schema region snapshot %s, because %s.",
                  snapshot, e.getMessage()),
              e);
        }

        position += readLength;

        // This case only happens when the connection is broken, and the connector is reconnected
        // to the receiver, then the receiver will redirect the file position to the last position
        if (resp.getStatus().getCode()
            == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
          position = resp.getEndWritingOffset();
          reader.seek(position);
          LOGGER.info("Redirect schema region file position to {}.", position);
          continue;
        }

        receiverStatusHandler.handle(
            resp.getStatus(),
            String.format(
                "Transfer schema region snapshot %s error, result status %s.",
                snapshot, resp.getStatus()),
            snapshot.toString());
      }
    }

    // 2. Transfer file seal signal, which means the file is transferred completely
    final TPipeTransferResp resp;
    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(
                      snapshot.getName(), snapshot.length()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when seal schema region snapshot file %s, because %s.",
              snapshot, e.getMessage()),
          e);
    }

    receiverStatusHandler.handle(
        resp.getStatus(),
        String.format(
            "Seal schema region snapshot snapshot %s file error, result status %s.",
            snapshot, resp.getStatus()),
        snapshot.toString());
    LOGGER.info("Successfully transferred schema region snapshot {}.", snapshot);
  }
}
