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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncClientManager;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.commons.pipe.connector.payload.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBMetaConnector;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeWriteConfigPlanEvent;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.client.IoTDBThriftSyncClientConfigNodeManager;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.pipe.api.event.Event;
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
import java.util.List;

public class IoTDBConfigRegionConnector extends IoTDBMetaConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConfigRegionConnector.class);

  @Override
  protected IoTDBThriftSyncClientManager constructClient(
      List<TEndPoint> nodeUrls,
      boolean useSSL,
      String trustStorePath,
      String trustStorePwd,
      boolean useLeaderCache) {
    return new IoTDBThriftSyncClientConfigNodeManager(
        nodeUrls, useSSL, trustStorePath, trustStorePwd);
  }

  @Override
  public void transfer(Event event) throws Exception {
    if (event instanceof PipeWriteConfigPlanEvent) {
      doTransfer((PipeWriteConfigPlanEvent) event);
    } else if (event instanceof PipeConfigRegionSnapshotEvent) {
      doTransfer((PipeConfigRegionSnapshotEvent) event);
    }
    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBConfigRegionConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransfer(PipeWriteConfigPlanEvent pipeWriteConfigPlanEvent) throws PipeException {
    Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus = clientManager.getClient();
    final TPipeTransferResp resp;

    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferConfigPlanReq.toTPipeTransferReq(
                      pipeWriteConfigPlanEvent.getPhysicalPlan()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer pipe write schema plan event, because %s.",
              e.getMessage()),
          e);
    }
    final TSStatus status = resp.getStatus();
    // TODO: judge return code
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeWriteConfigPlanEvent %s error, result status %s",
              pipeWriteConfigPlanEvent, status));
    }
  }

  private void doTransfer(PipeConfigRegionSnapshotEvent pipeConfigRegionSnapshotEvent)
      throws PipeException, IOException {
    final File snapshot = pipeConfigRegionSnapshotEvent.getSnapshot();
    final Pair<IoTDBThriftSyncConnectorClient, Boolean> clientAndStatus = clientManager.getClient();

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
                          PipeTransferConfigSnapshotPieceReq.toTPipeTransferReq(
                              snapshot.getName(),
                              position,
                              readLength == readFileBufferSize
                                  ? readBuffer
                                  : Arrays.copyOfRange(readBuffer, 0, readLength))));
        } catch (Exception e) {
          clientAndStatus.setRight(false);
          throw new PipeConnectionException(
              String.format(
                  "Network error when transfer file %s, because %s.", snapshot, e.getMessage()),
              e);
        }

        position += readLength;

        // This case only happens when the connection is broken, and the connector is reconnected
        // to the receiver, then the receiver will redirect the file position to the last position
        if (resp.getStatus().getCode()
            == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
          position = resp.getEndWritingOffset();
          reader.seek(position);
          LOGGER.info("Redirect file position to {}.", position);
          continue;
        }

        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new PipeException(
              String.format(
                  "Transfer file %s error, result status %s.", snapshot, resp.getStatus()));
        }
      }
    }

    // 2. Transfer file seal signal, which means the file is transferred completely
    final TPipeTransferResp resp;
    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeTransfer(
                  PipeTransferConfigSnapshotSealReq.toTPipeTransferReq(
                      snapshot.getName(), snapshot.length()));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format("Network error when seal file %s, because %s.", snapshot, e.getMessage()),
          e);
    }

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format("Seal file %s error, result status %s.", snapshot, resp.getStatus()));
    } else {
      LOGGER.info("Successfully transferred file {}.", snapshot);
    }
  }
}
