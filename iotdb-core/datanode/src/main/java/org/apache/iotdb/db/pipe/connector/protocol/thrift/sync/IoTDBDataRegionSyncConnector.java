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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventBatch;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventPlainBatch;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.connector.util.LeaderCacheUtils;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
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
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class IoTDBDataRegionSyncConnector extends IoTDBDataNodeSyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionSyncConnector.class);

  private PipeTransferBatchReqBuilder tabletBatchBuilder;

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    // tablet batch mode configuration
    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new PipeTransferBatchReqBuilder(parameters);
    }
  }

  @Override
  protected PipeTransferFilePieceReq getTransferSingleFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferTsFilePieceReq.toTPipeTransferReq(fileName, position, payLoad);
  }

  @Override
  protected PipeTransferFilePieceReq getTransferMultiFilePieceReq(
      final String fileName, final long position, final byte[] payLoad) throws IOException {
    return PipeTransferTsFilePieceWithModReq.toTPipeTransferReq(fileName, position, payLoad);
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    try {
      if (isTabletBatchModeEnabled) {
        final Pair<TEndPoint, PipeTabletEventBatch> endPointAndBatch =
            tabletBatchBuilder.onEvent(tabletInsertionEvent);
        if (Objects.nonNull(endPointAndBatch)) {
          doTransferWrapper(endPointAndBatch);
        }
      } else {
        if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
          doTransferWrapper((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
        } else {
          doTransferWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent);
        }
      }
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              "Failed to transfer tablet insertion event %s, because %s.",
              ((EnrichedEvent) tabletInsertionEvent).coreReportMessage(), e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of tsFileInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector only support PipeTsFileInsertionEvent. Ignore {}.",
          tsFileInsertionEvent);
      return;
    }

    try {
      // In order to commit in order
      if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
        doTransferWrapper();
      }

      doTransferWrapper((PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              "Failed to transfer tsfile insertion event %s, because %s.",
              ((PipeTsFileInsertionEvent) tsFileInsertionEvent).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof PipeDeleteDataNodeEvent) {
      doTransferWrapper((PipeDeleteDataNodeEvent) event);
      return;
    }

    // in order to commit in order
    if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
      doTransferWrapper();
    }

    if (!(event instanceof PipeHeartbeatEvent || event instanceof PipeTerminateEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransferWrapper(final PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeDeleteDataNodeEvent.increaseReferenceCount(
        IoTDBDataRegionSyncConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeDeleteDataNodeEvent);
    } finally {
      pipeDeleteDataNodeEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();

    final TPipeTransferResp resp;
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferPlanNodeReq.toTPipeTransferReq(
                  pipeDeleteDataNodeEvent.getDeleteDataNode()));
      rateLimitIfNeeded(
          pipeDeleteDataNodeEvent.getPipeName(),
          pipeDeleteDataNodeEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer deletion %s, because %s.",
              pipeDeleteDataNodeEvent.getDeleteDataNode().getType(), e.getMessage()),
          e);
    }

    final TSStatus status = resp.getStatus();
    // Only handle the failed statuses to avoid string format performance overhead
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && resp.getStatus().getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          status,
          String.format(
              "Transfer deletion %s error, result status %s.",
              pipeDeleteDataNodeEvent.getDeleteDataNode().getType(), status),
          pipeDeleteDataNodeEvent.getDeletionResource().toString());
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Successfully transferred deletion event {}.", pipeDeleteDataNodeEvent);
    }
  }

  private void doTransferWrapper() throws IOException, WriteProcessException {
    for (final Pair<TEndPoint, PipeTabletEventBatch> nonEmptyBatch :
        tabletBatchBuilder.getAllNonEmptyBatches()) {
      doTransferWrapper(nonEmptyBatch);
    }
  }

  private void doTransferWrapper(final Pair<TEndPoint, PipeTabletEventBatch> endPointAndBatch)
      throws IOException, WriteProcessException {
    final PipeTabletEventBatch batch = endPointAndBatch.getRight();
    if (batch instanceof PipeTabletEventPlainBatch) {
      doTransfer(endPointAndBatch.getLeft(), (PipeTabletEventPlainBatch) batch);
    } else if (batch instanceof PipeTabletEventTsFileBatch) {
      doTransfer((PipeTabletEventTsFileBatch) batch);
    } else {
      LOGGER.warn("Unsupported batch type {}.", batch.getClass());
    }
    batch.decreaseEventsReferenceCount(IoTDBDataRegionSyncConnector.class.getName(), true);
    batch.onSuccess();
  }

  private void doTransfer(
      final TEndPoint endPoint, final PipeTabletEventPlainBatch batchToTransfer) {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient(endPoint);

    final TPipeTransferResp resp;
    try {
      final TPipeTransferReq uncompressedReq = batchToTransfer.toTPipeTransferReq();
      final long uncompressedSize = uncompressedReq.getBody().length;

      final TPipeTransferReq req = compressIfNeeded(uncompressedReq);
      final long compressedSize = req.getBody().length;

      final double compressionRatio = (double) compressedSize / uncompressedSize;

      for (final Map.Entry<Pair<String, Long>, Long> entry :
          batchToTransfer.getPipe2BytesAccumulated().entrySet()) {
        rateLimitIfNeeded(
            entry.getKey().getLeft(),
            entry.getKey().getRight(),
            clientAndStatus.getLeft().getEndPoint(),
            (long) (entry.getValue() * compressionRatio));
      }

      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
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
          String.format("Transfer PipeTransferTabletBatchReq error, result status %s", resp.status),
          batchToTransfer.deepCopyEvents().toString());
    }

    for (final Pair<String, TEndPoint> redirectPair :
        LeaderCacheUtils.parseRecommendedRedirections(status)) {
      clientManager.updateLeaderCache(redirectPair.getLeft(), redirectPair.getRight());
    }
  }

  private void doTransfer(final PipeTabletEventTsFileBatch batchToTransfer)
      throws IOException, WriteProcessException {
    final List<File> sealedFiles = batchToTransfer.sealTsFiles();
    final Map<Pair<String, Long>, Double> pipe2WeightMap = batchToTransfer.deepCopyPipe2WeightMap();

    for (final File tsFile : sealedFiles) {
      doTransfer(pipe2WeightMap, tsFile, null, null);
      try {
        FileUtils.delete(tsFile);
      } catch (final NoSuchFileException e) {
        LOGGER.info("The file {} is not found, may already be deleted.", tsFile);
      } catch (final Exception e) {
        LOGGER.warn(
            "Failed to delete batch file {}, this file should be deleted manually later", tsFile);
      }
    }
  }

  private void doTransferWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionSyncConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeInsertNodeTabletInsertionEvent);
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncConnector.class.getName(), false);
    }
  }

  private void doTransfer(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException {
    final TPipeTransferResp resp;

    Pair<IoTDBSyncClient, Boolean> clientAndStatus = null;
    try {
      // getDeviceId() may return null for InsertRowsNode, will be equal to getClient(null)
      clientAndStatus = clientManager.getClient(pipeInsertNodeTabletInsertionEvent.getDeviceId());

      final InsertNode insertNode =
          pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
      final TPipeTransferReq req =
          compressIfNeeded(
              insertNode != null
                  ? PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(
                      insertNode,
                      pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
                          ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
                          : null)
                  : PipeTransferTabletBinaryReqV2.toTPipeTransferReq(
                      pipeInsertNodeTabletInsertionEvent.getByteBuffer(),
                      pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
                          ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
                          : null));
      rateLimitIfNeeded(
          pipeInsertNodeTabletInsertionEvent.getPipeName(),
          pipeInsertNodeTabletInsertionEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
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
              "Transfer PipeInsertNodeTabletInsertionEvent %s error, result status %s",
              pipeInsertNodeTabletInsertionEvent.coreReportMessage(), status),
          pipeInsertNodeTabletInsertionEvent.toString());
    }
    if (status.isSetRedirectNode()) {
      clientManager.updateLeaderCache(
          // pipeInsertNodeTabletInsertionEvent.getDeviceId() is null for InsertRowsNode
          pipeInsertNodeTabletInsertionEvent.getDeviceId(), status.getRedirectNode());
    }
  }

  private void doTransferWrapper(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionSyncConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeRawTabletInsertionEvent);
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncConnector.class.getName(), false);
    }
  }

  private void doTransfer(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus =
        clientManager.getClient(pipeRawTabletInsertionEvent.getDeviceId());
    final TPipeTransferResp resp;

    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferTabletRawReqV2.toTPipeTransferReq(
                  pipeRawTabletInsertionEvent.convertToTablet(),
                  pipeRawTabletInsertionEvent.isAligned(),
                  pipeRawTabletInsertionEvent.isTableModelEvent()
                      ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                      : null));
      rateLimitIfNeeded(
          pipeRawTabletInsertionEvent.getPipeName(),
          pipeRawTabletInsertionEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
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
              "Transfer PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent.coreReportMessage(), status),
          pipeRawTabletInsertionEvent.toString());
    }
    if (status.isSetRedirectNode()) {
      clientManager.updateLeaderCache(
          pipeRawTabletInsertionEvent.getDeviceId(), status.getRedirectNode());
    }
  }

  private void doTransferWrapper(final PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeTsFileInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionSyncConnector.class.getName())) {
      return;
    }
    try {
      doTransfer(
          Collections.singletonMap(
              new Pair<>(
                  pipeTsFileInsertionEvent.getPipeName(),
                  pipeTsFileInsertionEvent.getCreationTime()),
              1.0),
          pipeTsFileInsertionEvent.getTsFile(),
          pipeTsFileInsertionEvent.isWithMod() ? pipeTsFileInsertionEvent.getModFile() : null,
          pipeTsFileInsertionEvent.isTableModelEvent()
              ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
              : null);
    } finally {
      pipeTsFileInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncConnector.class.getName(), false);
    }
  }

  private void doTransfer(
      final Map<Pair<String, Long>, Double> pipeName2WeightMap,
      final File tsFile,
      final File modFile,
      final String dataBaseName)
      throws PipeException, IOException {

    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();
    final TPipeTransferResp resp;

    // 1. Transfer tsFile, and mod file if exists and receiver's version >= 2
    if (Objects.nonNull(modFile) && clientManager.supportModsIfIsDataNodeReceiver()) {
      transferFilePieces(pipeName2WeightMap, modFile, clientAndStatus, true);
      transferFilePieces(pipeName2WeightMap, tsFile, clientAndStatus, true);

      // 2. Transfer file seal signal with mod, which means the file is transferred completely
      try {
        final TPipeTransferReq req =
            compressIfNeeded(
                PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                    modFile.getName(),
                    modFile.length(),
                    tsFile.getName(),
                    tsFile.length(),
                    dataBaseName));

        pipeName2WeightMap.forEach(
            (pipePair, weight) ->
                rateLimitIfNeeded(
                    pipePair.getLeft(),
                    pipePair.getRight(),
                    clientAndStatus.getLeft().getEndPoint(),
                    (long) (req.getBody().length * weight)));

        resp = clientAndStatus.getLeft().pipeTransfer(req);
      } catch (final Exception e) {
        clientAndStatus.setRight(false);
        clientManager.adjustTimeoutIfNecessary(e);
        throw new PipeConnectionException(
            String.format("Network error when seal file %s, because %s.", tsFile, e.getMessage()),
            e);
      }
    } else {
      transferFilePieces(pipeName2WeightMap, tsFile, clientAndStatus, false);

      // 2. Transfer file seal signal without mod, which means the file is transferred completely
      try {
        final TPipeTransferReq req =
            compressIfNeeded(
                PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                    tsFile.getName(), tsFile.length(), dataBaseName));

        pipeName2WeightMap.forEach(
            (pipePair, weight) ->
                rateLimitIfNeeded(
                    pipePair.getLeft(),
                    pipePair.getRight(),
                    clientAndStatus.getLeft().getEndPoint(),
                    (long) (req.getBody().length * weight)));

        resp = clientAndStatus.getLeft().pipeTransfer(req);
      } catch (final Exception e) {
        clientAndStatus.setRight(false);
        clientManager.adjustTimeoutIfNecessary(e);
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

  @Override
  public synchronized void discardEventsOfPipe(final String pipeNameToDrop, final int regionId) {
    tabletBatchBuilder.discardEventsOfPipe(pipeNameToDrop, regionId);
  }

  @Override
  public void close() {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }

    super.close();
  }
}
