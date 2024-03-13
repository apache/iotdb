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

package org.apache.iotdb.confignode.manager.pipe.transfer.connector.config;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBAirGapConnector;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigNodeHandshakeV1Req;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigNodeHandshakeV2Req;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
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
import java.util.HashMap;

public class IoTDBConfigRegionAirGapConnector extends IoTDBAirGapConnector {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConfigRegionAirGapConnector.class);

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

    return PipeTransferConfigNodeHandshakeV2Req.toTPipeTransferBytes(params);
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionAirGapConnector can't transfer TabletInsertionEvent.");
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    throw new UnsupportedOperationException(
        "IoTDBConfigRegionAirGapConnector can't transfer TsFileInsertionEvent.");
  }

  @Override
  public void transfer(Event event) throws Exception {
    final int socketIndex = nextSocketIndex();
    final Socket socket = sockets.get(socketIndex);

    if (event instanceof PipeConfigRegionWritePlanEvent) {
      doTransfer(socket, (PipeConfigRegionWritePlanEvent) event);
    } else if (event instanceof PipeConfigRegionSnapshotEvent) {
      doTransfer(socket, (PipeConfigRegionSnapshotEvent) event);
    } else if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBConfigRegionAirGapConnector does not support transferring generic event: {}.",
          event);
    }
  }

  private void doTransfer(
      Socket socket, PipeConfigRegionWritePlanEvent pipeConfigRegionWritePlanEvent)
      throws PipeException, IOException {
    if (!send(
        socket,
        PipeTransferConfigPlanReq.toTPipeTransferBytes(
            pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan()))) {
      throw new PipeException(
          String.format(
              "Transfer config region write plan %s error. Socket: %s.",
              pipeConfigRegionWritePlanEvent.getConfigPhysicalPlan().getType(), socket));
    }
  }

  private void doTransfer(
      Socket socket, PipeConfigRegionSnapshotEvent pipeConfigRegionSnapshotEvent)
      throws PipeException, IOException {
    final File snapshot = pipeConfigRegionSnapshotEvent.getSnapshot();

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
            PipeTransferConfigSnapshotPieceReq.toTPipeTransferBytes(
                snapshot.getName(),
                position,
                readLength == readFileBufferSize
                    ? readBuffer
                    : Arrays.copyOfRange(readBuffer, 0, readLength)))) {
          throw new PipeException(
              String.format(
                  "Transfer config region snapshot %s error. Socket %s.", snapshot, socket));
        } else {
          position += readLength;
        }
      }
    }

    // 2. Transfer file seal signal, which means the file is transferred completely
    if (!send(
        socket,
        PipeTransferConfigSnapshotSealReq.toTPipeTransferBytes(
            snapshot.getName(), snapshot.length()))) {
      throw new PipeException(
          String.format("Seal config region snapshot %s error. Socket %s.", snapshot, socket));
    } else {
      LOGGER.info("Successfully transferred config region snapshot {}.", snapshot);
    }
  }
}
