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

package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.response.PipeConsensusTransferFilePieceResp;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBConnector;
import org.apache.iotdb.consensus.pipe.client.SyncPipeConsensusServiceClient;
import org.apache.iotdb.consensus.pipe.client.manager.PipeConsensusSyncClientManager;
import org.apache.iotdb.consensus.pipe.thrift.TCommitId;
import org.apache.iotdb.consensus.pipe.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.builder.PipeConsensusSyncBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTabletRawReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.payload.request.PipeConsensusTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;

/** This connector is used for PipeConsensus to transfer queued event. */
public class PipeConsensusSyncConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusSyncConnector.class);

  private final List<TEndPoint> peers;

  private final PipeConsensusSyncClientManager syncRetryAndHandshakeClientManager;

  private PipeConsensusSyncBatchReqBuilder tabletBatchBuilder;

  public PipeConsensusSyncConnector(List<TEndPoint> peers) {
    // In PipeConsensus, one pipeConsensusTask corresponds to a pipeConsensusConnector. Thus,
    // `peers` here actually is a singletonList that contains one peer's TEndPoint. But here we
    // retain the implementation of list to cope with possible future expansion
    this.peers = peers;
    this.syncRetryAndHandshakeClientManager = PipeConsensusSyncClientManager.onPeers(peers);
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new PipeConsensusSyncBatchReqBuilder(parameters);
    }
  }

  @Override
  public void handshake() throws Exception {
    syncRetryAndHandshakeClientManager.checkClientStatusAndTryReconstructIfNecessary(nodeUrls);
  }

  @Override
  public void heartbeat() throws Exception {
    try {
      handshake();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to reconnect to target server, because: {}. Try to reconnect later.",
          e.getMessage(),
          e);
      throw e;
    }
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // Note: here we don't need to do type judgment here, because PipeConsensus uses DO_NOTHING
    // processor and will not change the event type like
    // org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBDataRegionSyncConnector
    try {
      if (isTabletBatchModeEnabled) {
        if (tabletBatchBuilder.onEvent(tabletInsertionEvent)) {
          doTransfer();
        }
      } else {
        if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
          doTransfer((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
        } else {
          doTransfer((PipeRawTabletInsertionEvent) tabletInsertionEvent);
        }
      }
    } catch (Exception e) {
      throw new PipeConnectionException(
          String.format(
              "Failed to transfer tablet insertion event %s, because %s.",
              tabletInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // Note: here we don't need to do type judgment here, because PipeConsensus uses DO_NOTHING
    // processor and will not change the event type like
    // org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBDataRegionSyncConnector
    try {
      // In order to commit in order
      if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
        doTransfer();
      }

      doTransfer((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (Exception e) {
      throw new PipeConnectionException(
          String.format(
              "Failed to transfer tsfile insertion event %s, because %s.",
              tsFileInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    // in order to commit in order
    if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
      doTransfer();
    }

    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "PipeConsensusSyncConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransfer() {
    final MutablePair<SyncPipeConsensusServiceClient, Boolean> clientAndStatus =
        syncRetryAndHandshakeClientManager.borrowClient(getFollowerUrl());
    final TPipeConsensusTransferResp resp;
    try {
      resp = null;
      clientAndStatus
          .getLeft()
          .pipeConsensusBatchTransfer(tabletBatchBuilder.toTPipeConsensusBatchTransferReq());
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format("Network error when transfer tablet batch, because %s.", e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Only handle the failed statuses to avoid string format performance overhead
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          resp.getStatus(),
          String.format(
              "Transfer PipeConsensusTransferTabletBatchReq error, result status %s", resp.status),
          tabletBatchBuilder.deepCopyEvents().toString());
    }

    tabletBatchBuilder.onSuccess();
  }

  private void doTransfer(PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException {
    final InsertNode insertNode;
    MutablePair<SyncPipeConsensusServiceClient, Boolean> clientAndStatus = null;
    final TPipeConsensusTransferResp resp;
    TCommitId commitId =
        new TCommitId(
            pipeInsertNodeTabletInsertionEvent.getCommitId(),
            pipeInsertNodeTabletInsertionEvent.getRebootTimes());

    try {
      insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();

      clientAndStatus = syncRetryAndHandshakeClientManager.borrowClient(getFollowerUrl());
      if (insertNode != null) {
        resp =
            clientAndStatus
                .getLeft()
                .pipeConsensusTransfer(
                    PipeConsensusTabletInsertNodeReq.toTPipeConsensusTransferReq(
                        insertNode, commitId));
      } else {
        resp =
            clientAndStatus
                .getLeft()
                .pipeConsensusTransfer(
                    PipeConsensusTabletBinaryReq.toTPipeConsensusTransferReq(
                        pipeInsertNodeTabletInsertionEvent.getByteBuffer(), commitId));
      }
    } catch (Exception e) {
      if (clientAndStatus != null) {
        clientAndStatus.setRight(false);
      }
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer insert node tablet insertion event, because %s.",
              e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Only handle the failed statuses to avoid string format performance overhead
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "PipeConsensus transfer PipeInsertNodeTabletInsertionEvent %s error, result status %s",
              pipeInsertNodeTabletInsertionEvent, status),
          pipeInsertNodeTabletInsertionEvent.toString());
    }
  }

  private void doTransfer(PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    final MutablePair<SyncPipeConsensusServiceClient, Boolean> clientAndStatus =
        syncRetryAndHandshakeClientManager.borrowClient(getFollowerUrl());
    final TPipeConsensusTransferResp resp;
    TCommitId commitId =
        new TCommitId(
            pipeRawTabletInsertionEvent.getCommitId(),
            pipeRawTabletInsertionEvent.getRebootTimes());

    try {
      resp =
          clientAndStatus
              .getLeft()
              .pipeConsensusTransfer(
                  PipeConsensusTabletRawReq.toTPipeConsensusTransferReq(
                      pipeRawTabletInsertionEvent.convertToTablet(),
                      pipeRawTabletInsertionEvent.isAligned(),
                      commitId));
    } catch (Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer raw tablet insertion event, because %s.",
              e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Only handle the failed statuses to avoid string format performance overhead
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "PipeConsensus transfer PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent, status),
          pipeRawTabletInsertionEvent.toString());
    }
  }

  // TODO: 一个 event 分成了多个，共享 commitId，需要改造
  private void doTransfer(PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    final File tsFile = pipeTsFileInsertionEvent.getTsFile();
    final File modFile = pipeTsFileInsertionEvent.getModFile();
    final MutablePair<SyncPipeConsensusServiceClient, Boolean> clientAndStatus =
        syncRetryAndHandshakeClientManager.borrowClient(getFollowerUrl());
    final TPipeConsensusTransferResp resp;
    final TCommitId commitId =
        new TCommitId(
            pipeTsFileInsertionEvent.getCommitId(), pipeTsFileInsertionEvent.getRebootTimes());

    // 1. Transfer tsFile, and mod file if exists and receiver's version >= 2
    if (pipeTsFileInsertionEvent.isWithMod()) {
      transferFilePieces(modFile, clientAndStatus, true, commitId);
      transferFilePieces(tsFile, clientAndStatus, true, commitId);
      // 2. Transfer filre seal signal with mod, which means the file is tansferred completely
      try {
        resp =
            clientAndStatus
                .getLeft()
                .pipeConsensusTransfer(
                    PipeConsensusTsFileSealWithModReq.toTPipeConsensusTransferReq(
                        modFile.getName(),
                        modFile.length(),
                        tsFile.getName(),
                        tsFile.length(),
                        commitId));
      } catch (Exception e) {
        clientAndStatus.setRight(false);
        throw new PipeConnectionException(
            String.format("Network error when seal file %s, because %s.", tsFile, e.getMessage()),
            e);
      }
    } else {
      transferFilePieces(tsFile, clientAndStatus, false, commitId);
      // 2. Transfer file seal signal without mod, which means the file is transferred completely
      try {
        resp =
            clientAndStatus
                .getLeft()
                .pipeConsensusTransfer(
                    PipeConsensusTsFileSealReq.toTPipeConsensusTransferReq(
                        tsFile.getName(), tsFile.length(), commitId));
      } catch (Exception e) {
        clientAndStatus.setRight(false);
        throw new PipeConnectionException(
            String.format("Network error when seal file %s, because %s.", tsFile, e.getMessage()),
            e);
      }
    }

    final TSStatus status = resp.getStatus();
    // Only handle the failed statuses to avoid string format performance overhead
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          resp.getStatus(),
          String.format("Seal file %s error, result status %s.", tsFile, resp.getStatus()),
          tsFile.getName());
    }

    LOGGER.info("Successfully transferred file {}.", tsFile);
  }

  protected void transferFilePieces(
      File file,
      MutablePair<SyncPipeConsensusServiceClient, Boolean> clientAndStatus,
      boolean isMultiFile,
      TCommitId commitId)
      throws PipeException, IOException {
    final int readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
      while (true) {
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] payLoad =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);
        final PipeConsensusTransferFilePieceResp resp;
        try {
          resp =
              PipeConsensusTransferFilePieceResp.fromTPipeConsensusTransferResp(
                  clientAndStatus
                      .getLeft()
                      .pipeConsensusTransfer(
                          isMultiFile
                              ? PipeConsensusTsFilePieceWithModReq.toTPipeConsensusTransferReq(
                                  file.getName(), position, payLoad, commitId)
                              : PipeConsensusTsFilePieceReq.toTPipeConsensusTransferReq(
                                  file.getName(), position, payLoad, commitId)));
        } catch (Exception e) {
          clientAndStatus.setRight(false);
          throw new PipeConnectionException(
              String.format(
                  "Network error when transfer file %s, because %s.", file, e.getMessage()),
              e);
        }

        position += readLength;

        final TSStatus status = resp.getStatus();
        // This case only happens when the connection is broken, and the connector is reconnected
        // to the receiver, then the receiver will redirect the file position to the last position
        if (status.getCode() == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
          position = resp.getEndWritingOffset();
          reader.seek(position);
          LOGGER.info("Redirect file position to {}.", position);
          continue;
        }

        // Send handshake req and then re-transfer the event
        if (status.getCode()
            == TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode()) {
          syncRetryAndHandshakeClientManager.sendHandshakeReq(clientAndStatus);
        }
        // Only handle the failed statuses to avoid string format performance overhead
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
            && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
          receiverStatusHandler.handle(
              resp.getStatus(),
              String.format("Transfer file %s error, result status %s.", file, resp.getStatus()),
              file.getName());
        }
      }
    }
  }

  private TEndPoint getFollowerUrl() {
    // In current pipeConsensus design, one connector corresponds to one follower, so the peers is
    // actually a singleton list
    return peers.get(0);
  }

  // synchronized to avoid close connector when transfer event
  @Override
  public synchronized void close() throws Exception {
    if (syncRetryAndHandshakeClientManager != null) {
      syncRetryAndHandshakeClientManager.close();
    }

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }
  }

  //////////////////////////// TODO: APIs provided for metric framework ////////////////////////////

}
