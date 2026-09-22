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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.client.IoTDBSyncClient;
import org.apache.iotdb.commons.pipe.sink.limiter.TsFileSendRateLimiter;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFilePieceReq;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.util.PipeObjectPathUtil;
import org.apache.iotdb.db.pipe.metric.overview.PipeResourceMetrics;
import org.apache.iotdb.db.pipe.metric.sink.PipeDataRegionSinkMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeTsFileMemoryBlock;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeSyncClientManager;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventPlainBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFilePieceWithModReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTsFileSealWithModReq;
import org.apache.iotdb.db.pipe.sink.util.PipeTsFileObjectBatchTransfer;
import org.apache.iotdb.db.pipe.sink.util.cacher.LeaderCacheUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.load.converter.TabletObjectSplitIterator;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
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

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_ENABLE_SEND_TSFILE_LIMIT;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_ENABLE_SEND_TSFILE_LIMIT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_ENABLE_SEND_TSFILE_LIMIT;

@TreeModel
@TableModel
public class IoTDBDataRegionSyncSink extends IoTDBDataNodeSyncSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionSyncSink.class);

  private PipeTransferBatchReqBuilder tabletBatchBuilder;
  private boolean enableSendTsFileLimit;

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    // tablet batch mode configuration
    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new PipeTransferBatchReqBuilder(parameters);
    }

    enableSendTsFileLimit =
        parameters.getBooleanOrDefault(
            Arrays.asList(SINK_ENABLE_SEND_TSFILE_LIMIT, CONNECTOR_ENABLE_SEND_TSFILE_LIMIT),
            CONNECTOR_ENABLE_SEND_TSFILE_LIMIT_DEFAULT_VALUE);
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
  protected void mayLimitRateAndRecordIO(final long requiredBytes) {
    PipeResourceMetrics.getInstance().recordDiskIO(requiredBytes);
    if (enableSendTsFileLimit) {
      TsFileSendRateLimiter.getInstance().acquire(requiredBytes);
    }
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIP,
          tabletInsertionEvent);
      return;
    }

    try {
      if (tryTransferObjectTablet(tabletInsertionEvent)) {
        return;
      }

      transferNonObjectTablet(tabletInsertionEvent);
    } catch (final PipeException e) {
      throw e;
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FAILED_TO_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_S_9710318F,
              ((EnrichedEvent) tabletInsertionEvent).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  private void transferNonObjectTablet(final TabletInsertionEvent tabletInsertionEvent)
      throws Exception {
    if (isTabletBatchModeEnabled) {
      try {
        tabletBatchBuilder.onEvent(tabletInsertionEvent);
      } catch (final PipeRuntimeOutOfMemoryCriticalException memoryException) {
        try {
          doTransferWrapper();
        } catch (final Exception transferException) {
          transferException.addSuppressed(memoryException);
          throw transferException;
        }
        tabletBatchBuilder.onEvent(tabletInsertionEvent);
      }
      doTransferWrapper();
    } else {
      transferTabletInsertionEventDirectly(tabletInsertionEvent);
    }
  }

  private void transferTabletInsertionEventDirectly(final TabletInsertionEvent tabletInsertionEvent)
      throws Exception {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      doTransferWrapper((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    } else {
      doTransferWrapper((PipeRawTabletInsertionEvent) tabletInsertionEvent);
    }
  }

  public void transferTabletInsertionEventSynchronously(
      final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIP,
          tabletInsertionEvent);
      return;
    }

    transferAllBatchedEventsIfNecessary();

    try {
      if (isObjectTabletEvent(tabletInsertionEvent)) {
        transferObjectTabletSynchronously(tabletInsertionEvent);
        return;
      }

      transferTabletInsertionEventDirectly(tabletInsertionEvent);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages.FAILED_TO_TRANSFER_TABLET_INSERTION_EVENT_SYNCHRONOUSLY,
              ((EnrichedEvent) tabletInsertionEvent).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  private boolean shouldTransferAsObjectTablet(final TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      return shouldTransferAsObjectTablets(
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    }
    if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawEvent =
          (PipeRawTabletInsertionEvent) tabletInsertionEvent;
      return rawEvent.isTableModelEvent() && rawEvent.hasObjectData();
    }
    return false;
  }

  private boolean tryTransferObjectTablet(final TabletInsertionEvent tabletInsertionEvent)
      throws Exception {
    if (!isObjectTabletEvent(tabletInsertionEvent)) {
      return false;
    }

    if (isTabletBatchModeEnabled && tabletBatchBuilder.isTsFileBatchMode()) {
      transferObjectTabletToBatch(tabletInsertionEvent);
      doTransferWrapper();
      return true;
    }

    transferAllBatchedEventsIfNecessary();
    if (isTabletBatchModeEnabled) {
      transferObjectTabletToBatch(tabletInsertionEvent);
      transferAllBatchedEventsIfNecessary();
    } else {
      transferObjectTabletSynchronously(tabletInsertionEvent);
    }
    return true;
  }

  private boolean isObjectTabletEvent(final TabletInsertionEvent tabletInsertionEvent) {
    return shouldTransferAsObjectTablet(tabletInsertionEvent)
        || (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
            && ((PipeRawTabletInsertionEvent) tabletInsertionEvent).isObjectValueContentEvent());
  }

  private void transferObjectTabletSynchronously(final TabletInsertionEvent tabletInsertionEvent)
      throws Exception {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      transferTabletInsertionEventDirectly(tabletInsertionEvent);
      return;
    }

    final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) tabletInsertionEvent;
    if (rawEvent.isObjectValueContentEvent()) {
      doTransferWrapper(rawEvent);
    } else {
      splitRawObjectTabletAndTransferSynchronously(rawEvent);
    }
  }

  private void transferObjectTabletToBatch(final TabletInsertionEvent tabletInsertionEvent)
      throws Exception {
    if (tabletBatchBuilder.isTsFileBatchMode()) {
      tabletBatchBuilder.onEvent(tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      splitInsertNodeObjectTabletToBatch((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      return;
    }
    final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) tabletInsertionEvent;
    if (rawEvent.isObjectValueContentEvent()) {
      offerSplitTabletEventToBatch(rawEvent);
      return;
    }
    splitRawObjectTabletToBatch(rawEvent);
  }

  private void splitInsertNodeObjectTabletToBatch(final PipeInsertNodeTabletInsertionEvent event)
      throws Exception {
    if (tabletBatchBuilder.isTsFileBatchMode()) {
      tabletBatchBuilder.onEvent(event);
      doTransferWrapper();
      return;
    }

    final List<Tablet> tablets = event.convertToTablets();
    for (int i = 0; i < tablets.size(); i++) {
      final Tablet tablet = tablets.get(i);
      try (final TabletObjectSplitIterator splitIterator =
          new TabletObjectSplitIterator(
              tablet,
              event.getTsFileResource() == null ? null : event.getTsFileResource().getTsFile(),
              PipeObjectPathUtil.resolveLinkedObjectDirectory(
                  event.getTsFileResource(), event.getPipeName()),
              true)) {
        while (splitIterator.hasNext()) {
          offerSplitTabletEventToBatch(
              createSplitRawTabletEvent(event, splitIterator.next(), event.isAligned(i)));
        }
      }
    }
  }

  private void splitRawObjectTabletToBatch(final PipeRawTabletInsertionEvent event)
      throws Exception {
    if (tabletBatchBuilder.isTsFileBatchMode()) {
      tabletBatchBuilder.onEvent(event);
      doTransferWrapper();
      return;
    }

    try (final TabletObjectSplitIterator splitIterator =
        new TabletObjectSplitIterator(
            event.convertToTablet(),
            event.getTsFileResource() == null ? null : event.getTsFileResource().getTsFile(),
            PipeObjectPathUtil.resolveLinkedObjectDirectory(
                event.getTsFileResource(), event.getPipeName()),
            true)) {
      while (splitIterator.hasNext()) {
        offerSplitTabletEventToBatch(
            createSplitRawTabletEvent(event, splitIterator.next(), event.isAligned()));
      }
    }
  }

  private void splitRawObjectTabletAndTransferSynchronously(final PipeRawTabletInsertionEvent event)
      throws Exception {
    try (final TabletObjectSplitIterator splitIterator =
        new TabletObjectSplitIterator(
            event.convertToTablet(),
            event.getTsFileResource() == null ? null : event.getTsFileResource().getTsFile(),
            PipeObjectPathUtil.resolveLinkedObjectDirectory(
                event.getTsFileResource(), event.getPipeName()),
            true)) {
      while (splitIterator.hasNext()) {
        doTransferWrapper(
            createSplitRawTabletEvent(event, splitIterator.next(), event.isAligned()));
      }
    }
  }

  private void offerSplitTabletEventToBatch(final PipeRawTabletInsertionEvent splitEvent)
      throws Exception {
    tabletBatchBuilder.onEvent(splitEvent);
    doTransferWrapper();
  }

  private PipeRawTabletInsertionEvent createSplitRawTabletEvent(
      final PipeInsertionEvent sourceEvent, final Tablet splitTablet, final boolean aligned) {
    final PipeRawTabletInsertionEvent splitEvent =
        new PipeRawTabletInsertionEvent(
            sourceEvent.getRawIsTableModelEvent(),
            sourceEvent.getSourceDatabaseNameFromDataRegion(),
            sourceEvent.getRawTableModelDataBase(),
            sourceEvent.getRawTreeModelDataBase(),
            splitTablet,
            aligned,
            sourceEvent.getPipeName(),
            sourceEvent.getCreationTime(),
            sourceEvent.getPipeTaskMeta(),
            sourceEvent,
            true,
            sourceEvent.getUserId(),
            sourceEvent.getUserName(),
            sourceEvent.getCliHostname());
    if (sourceEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      splitEvent.setTsFileResource(
          ((PipeInsertNodeTabletInsertionEvent) sourceEvent).getTsFileResource());
    } else if (sourceEvent instanceof PipeRawTabletInsertionEvent) {
      splitEvent.setTsFileResource(((PipeRawTabletInsertionEvent) sourceEvent).getTsFileResource());
    }
    splitEvent.setObjectValueContentEvent(true);
    return splitEvent;
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of tsFileInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE,
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
              DataNodePipeMessages
                  .PIPE_EXCEPTION_FAILED_TO_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_S_21AD3263,
              ((PipeTsFileInsertionEvent) tsFileInsertionEvent).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  public void transferTsFileInsertionEventSynchronously(final TsFileInsertionEvent tsFileEvent)
      throws Exception {
    if (!(tsFileEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBTHRIFTSYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE,
          tsFileEvent);
      return;
    }

    try {
      transferAllBatchedEventsIfNecessary();
      doTransferWrapper((PipeTsFileInsertionEvent) tsFileEvent);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages.FAILED_TO_TRANSFER_TSFILE_INSERTION_EVENT_SYNCHRONOUSLY,
              ((PipeTsFileInsertionEvent) tsFileEvent).coreReportMessage(),
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
          DataNodePipeMessages.IOTDBTHRIFTSYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT,
          event);
    }
  }

  private void doTransferWrapper(final PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeDeleteDataNodeEvent.increaseReferenceCount(IoTDBDataRegionSyncSink.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeDeleteDataNodeEvent);
    } finally {
      pipeDeleteDataNodeEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncSink.class.getName(), false);
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
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_DELETION_S_BECAUSE_S_3B250B4B,
              pipeDeleteDataNodeEvent.getDeleteDataNode().getType(),
              e.getMessage()),
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
          pipeDeleteDataNodeEvent.getDeleteDataNode().toString(),
          true);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          DataNodePipeMessages.SUCCESSFULLY_TRANSFERRED_DELETION_EVENT, pipeDeleteDataNodeEvent);
    }
  }

  private void doTransferWrapper() throws IOException, WriteProcessException {
    for (final Pair<TEndPoint, PipeTabletEventBatch> nonEmptyAndShouldEmitBatch :
        tabletBatchBuilder.getAllNonEmptyAndShouldEmitBatches()) {
      doTransferWrapper(nonEmptyAndShouldEmitBatch);
    }
  }

  public void transferAllBatchedEventsIfNecessary() throws IOException, WriteProcessException {
    if (!isTabletBatchModeEnabled || tabletBatchBuilder == null || tabletBatchBuilder.isEmpty()) {
      return;
    }

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
      LOGGER.warn(DataNodePipeMessages.UNSUPPORTED_BATCH_TYPE, batch.getClass());
    }
    batch.decreaseEventsReferenceCount(IoTDBDataRegionSyncSink.class.getName(), true);
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
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_BATCH_BECAUSE_S_6BEC52E7,
              e.getMessage()),
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
    final List<Pair<String, Pair<File, File>>> dbTsFilePairs = batchToTransfer.sealTsFiles();
    final Map<Pair<String, Long>, Double> pipe2WeightMap = batchToTransfer.deepCopyPipe2WeightMap();
    final List<EnrichedEvent> events = batchToTransfer.deepCopyEvents();

    try {
      for (int outputIndex = 0; outputIndex < dbTsFilePairs.size(); outputIndex++) {
        final Pair<String, Pair<File, File>> dbTsFile = dbTsFilePairs.get(outputIndex);
        final File tsFile = dbTsFile.right.left;
        final File objectDir = dbTsFile.right.right;
        doTransfer(pipe2WeightMap, tsFile, null, objectDir, dbTsFile.left, events, outputIndex);
      }
    } finally {
      for (final Pair<String, Pair<File, File>> dbTsFile : dbTsFilePairs) {
        final File tsFile = dbTsFile.right.left;
        final File objectDir = dbTsFile.right.right;
        try {
          RetryUtils.retryOnException(
              () -> {
                if (tsFile != null && tsFile.exists()) {
                  FileUtils.delete(tsFile);
                }
                if (objectDir != null && objectDir.exists()) {
                  FileUtils.deleteDirectory(objectDir);
                }
                return null;
              });
        } catch (final NoSuchFileException e) {
          LOGGER.info(DataNodePipeMessages.THE_FILE_IS_NOT_FOUND_MAY_ALREADY, dbTsFile);
        } catch (final Exception e) {
          LOGGER.warn(DataNodePipeMessages.FAILED_TO_DELETE_BATCH_FILE_THIS_FILE, dbTsFile);
        }
      }
    }
  }

  private void doTransferWrapper(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionSyncSink.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeInsertNodeTabletInsertionEvent);
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException {
    Pair<IoTDBSyncClient, Boolean> clientAndStatus = null;
    try {
      // getDeviceId() may return null for InsertRowsNode, will be equal to getClient(null)
      clientAndStatus = clientManager.getClient(pipeInsertNodeTabletInsertionEvent.getDeviceId());

      if (shouldTransferAsObjectTablets(pipeInsertNodeTabletInsertionEvent)) {
        transferInsertNodeEventAsObjectTablets(pipeInsertNodeTabletInsertionEvent, clientAndStatus);
      } else {
        final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
        final TPipeTransferReq req =
            compressIfNeeded(
                PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(
                    insertNode,
                    pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
                        ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
                        : pipeInsertNodeTabletInsertionEvent.getTreeModelDatabaseName()));
        transferReqWithStatusCheck(pipeInsertNodeTabletInsertionEvent, clientAndStatus, req);
      }
    } catch (final PipeException e) {
      throw e;
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
  }

  private boolean shouldTransferAsObjectTablets(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent) {
    if (!pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
        || pipeInsertNodeTabletInsertionEvent.getTsFileResource() == null
        || pipeInsertNodeTabletInsertionEvent.getTsFileResource().getTsFile() == null) {
      return false;
    }
    return pipeInsertNodeTabletInsertionEvent.hasObjectData();
  }

  private void transferInsertNodeEventAsObjectTablets(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus)
      throws Exception {
    final List<Tablet> tablets = pipeInsertNodeTabletInsertionEvent.convertToTablets();
    for (int i = 0; i < tablets.size(); i++) {
      final Tablet tablet = tablets.get(i);
      try (final TabletObjectSplitIterator splitIterator =
          new TabletObjectSplitIterator(
              tablet,
              pipeInsertNodeTabletInsertionEvent.getTsFileResource() == null
                  ? null
                  : pipeInsertNodeTabletInsertionEvent.getTsFileResource().getTsFile(),
              PipeObjectPathUtil.resolveLinkedObjectDirectory(
                  pipeInsertNodeTabletInsertionEvent.getTsFileResource(),
                  pipeInsertNodeTabletInsertionEvent.getPipeName()),
              true)) {
        while (splitIterator.hasNext()) {
          final TPipeTransferReq req =
              compressIfNeeded(
                  PipeTransferTabletRawReqV2.toTPipeTransferReq(
                      splitIterator.next(),
                      pipeInsertNodeTabletInsertionEvent.isAligned(i),
                      pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()));
          transferReqWithStatusCheck(pipeInsertNodeTabletInsertionEvent, clientAndStatus, req);
        }
      }
    }
  }

  private void transferReqWithStatusCheck(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus,
      final TPipeTransferReq req)
      throws PipeException {
    final TPipeTransferResp resp;
    try {
      rateLimitIfNeeded(
          pipeInsertNodeTabletInsertionEvent.getPipeName(),
          pipeInsertNodeTabletInsertionEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_INSERT_NODE_TABLET_INSERTION_D993C7AB,
              e.getMessage()),
          e);
    }

    handleInsertNodeTransferStatus(pipeInsertNodeTabletInsertionEvent, resp.getStatus());
  }

  private void handleInsertNodeTransferStatus(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent,
      final TSStatus status)
      throws PipeException {
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
    for (final Pair<String, TEndPoint> redirectPair :
        LeaderCacheUtils.parseRecommendedRedirections(status)) {
      clientManager.updateLeaderCache(redirectPair.getLeft(), redirectPair.getRight());
    }
  }

  private void doTransferWrapper(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionSyncSink.class.getName())) {
      return;
    }
    try {
      doTransfer(pipeRawTabletInsertionEvent);
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncSink.class.getName(), false);
    }
  }

  private void doTransfer(final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException {
    final Pair<IoTDBSyncClient, Boolean> clientAndStatus =
        clientManager.getClient(pipeRawTabletInsertionEvent.getDeviceId());

    try {
      final Tablet tablet = pipeRawTabletInsertionEvent.convertToTablet();
      final TPipeTransferReq req =
          compressIfNeeded(
              PipeTransferTabletRawReqV2.toTPipeTransferReq(
                  tablet,
                  pipeRawTabletInsertionEvent.isAligned(),
                  pipeRawTabletInsertionEvent.isTableModelEvent()
                      ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                      : pipeRawTabletInsertionEvent.getTreeModelDatabaseName()));
      transferRawReqWithStatusCheck(pipeRawTabletInsertionEvent, clientAndStatus, req);
    } catch (final PipeException e) {
      throw e;
    } catch (final Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              "Network error when transfer raw tablet insertion event, because %s.",
              e.getMessage()),
          e);
    }
  }

  private void transferRawReqWithStatusCheck(
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus,
      final TPipeTransferReq req)
      throws PipeException {
    final TPipeTransferResp resp;
    try {
      rateLimitIfNeeded(
          pipeRawTabletInsertionEvent.getPipeName(),
          pipeRawTabletInsertionEvent.getCreationTime(),
          clientAndStatus.getLeft().getEndPoint(),
          req.getBody().length);
      resp = clientAndStatus.getLeft().pipeTransfer(req);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_RAW_TABLET_INSERTION_EVENT_BECAUSE_D8ACEC3C,
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
    if (!pipeTsFileInsertionEvent.increaseReferenceCount(IoTDBDataRegionSyncSink.class.getName())) {
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
          PipeObjectPathUtil.resolveLinkedObjectDirectory(
              pipeTsFileInsertionEvent.getTsFileResource(), pipeTsFileInsertionEvent.getPipeName()),
          pipeTsFileInsertionEvent.isTableModelEvent()
              ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
              : pipeTsFileInsertionEvent.getTreeModelDatabaseName(),
          Collections.singletonList(pipeTsFileInsertionEvent),
          0);
    } finally {
      pipeTsFileInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionSyncSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final Map<Pair<String, Long>, Double> pipeName2WeightMap,
      final File tsFile,
      final File modFile,
      final File objectDir,
      final String dataBaseName,
      final Iterable<? extends EnrichedEvent> events,
      final int outputIndex)
      throws PipeException, IOException {

    final Pair<IoTDBSyncClient, Boolean> clientAndStatus = clientManager.getClient();
    final TPipeTransferResp resp;
    final String tsFileNameWithoutSuffix =
        PipeObjectPathUtil.tsFileBaseNameWithoutSuffix(tsFile.getName());
    final String conversionTaskId =
        shouldAsyncLoadTsFileOnTypeMismatch
            ? PipeTransferTsFileSealWithModReq.generateConversionTaskId(
                sinkTaskId,
                events,
                dataBaseName,
                outputIndex,
                Objects.nonNull(modFile) && clientManager.supportModsIfIsDataNodeReceiver())
            : null;

    // 1. Transfer object files (batched RPC to reduce ops).
    try (final Stream<Pair<Path, File>> objectFileStream =
        PipeObjectPathUtil.getObjectFileStream(objectDir == null ? null : objectDir.toPath())) {
      transferObjectBatches(
          pipeName2WeightMap, tsFileNameWithoutSuffix, objectFileStream, clientAndStatus);
    }

    // 2. Transfer tsFile, and mod file if exists and receiver's version >= 2
    if (Objects.nonNull(modFile) && clientManager.supportModsIfIsDataNodeReceiver()) {
      transferFilePieces(pipeName2WeightMap, modFile, clientAndStatus, true);
      transferFilePieces(pipeName2WeightMap, tsFile, clientAndStatus, true);

      // 3. Transfer file seal signal with mod.
      try {
        final TPipeTransferReq req =
            compressIfNeeded(
                PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                        modFile.getName(),
                        modFile.length(),
                        tsFile.getName(),
                        tsFile.length(),
                        dataBaseName,
                        shouldWaitForSchemaBeforeLoad)
                    .setConversionTaskInfo(conversionTaskId, shouldAsyncLoadTsFileOnTypeMismatch));

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
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_FILE_S_BECAUSE_S_DC87F263,
                tsFile,
                e.getMessage()),
            e);
      }
    } else {
      transferFilePieces(pipeName2WeightMap, tsFile, clientAndStatus, false);

      // 3. Transfer file seal signal without mod, which means the file is transferred completely
      try {
        final TPipeTransferReq req =
            compressIfNeeded(
                PipeTransferTsFileSealWithModReq.toTPipeTransferReq(
                        tsFile.getName(),
                        tsFile.length(),
                        dataBaseName,
                        shouldWaitForSchemaBeforeLoad)
                    .setConversionTaskInfo(conversionTaskId, shouldAsyncLoadTsFileOnTypeMismatch));

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
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_SEAL_FILE_S_BECAUSE_S_DC87F263,
                tsFile,
                e.getMessage()),
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

    LOGGER.info(DataNodePipeMessages.SUCCESSFULLY_TRANSFERRED_FILE, tsFile);
  }

  private void transferObjectBatches(
      final Map<Pair<String, Long>, Double> weightMap,
      final String tsFileBaseName,
      final Stream<Pair<Path, File>> objectFileStream,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus)
      throws PipeException, IOException {
    final int maxPieceBytes = PipeConfig.getInstance().getPipeSinkReadFileBufferSize();
    final int maxBatchBytes = PipeTsFileObjectBatchTransfer.defaultMaxBatchSerializedSumBytes();
    final Iterator<PipeTsFileObjectBatchTransfer.ObjectBatch> it =
        PipeTsFileObjectBatchTransfer.batchIterator(
            tsFileBaseName, objectFileStream, maxPieceBytes, maxBatchBytes);
    while (it.hasNext()) {
      final PipeTsFileObjectBatchTransfer.ObjectBatch batch = it.next();
      final PipeTransferFilePieceResp resp;
      try {
        final TPipeTransferReq req = compressIfNeeded(batch.toThrift(tsFileBaseName));
        weightMap.forEach(
            (namePair, weight) ->
                rateLimitIfNeeded(
                    namePair.getLeft(),
                    namePair.getRight(),
                    clientAndStatus.getLeft().getEndPoint(),
                    (long) (req.getBody().length * weight)));
        resp =
            PipeTransferFilePieceResp.fromTPipeTransferResp(
                clientAndStatus.getLeft().pipeTransfer(req));
      } catch (final Exception e) {
        clientAndStatus.setRight(false);
        throw new PipeConnectionException(
            String.format(
                DataNodePipeMessages.TRANSFER_OBJECT_BATCH_NETWORK_ERROR,
                tsFileBaseName,
                e.getMessage()),
            e);
      }

      final TSStatus status = resp.getStatus();
      if (status.getCode() == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
        receiverStatusHandler.handle(
            resp.getStatus(),
            String.format(
                DataNodePipeMessages.TRANSFER_OBJECT_BATCH_NEEDS_OFFSET_RESET, tsFileBaseName),
            tsFileBaseName);
        return;
      }
      if (status.getCode() == TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode()) {
        clientManager.sendHandshakeReq(clientAndStatus);
      }
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        receiverStatusHandler.handle(
            resp.getStatus(),
            String.format(
                DataNodePipeMessages.TRANSFER_OBJECT_BATCH_ERROR_STATUS,
                tsFileBaseName,
                resp.getStatus()),
            tsFileBaseName);
      }
    }
  }

  @Override
  protected void transferFilePieces(
      final Map<Pair<String, Long>, Double> pipe2WeightMap,
      final File file,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus,
      final boolean isMultiFile)
      throws PipeException, IOException {
    final int readFileBufferSize = getReadFileBufferSize(file);
    try (final PipeTsFileMemoryBlock ignored =
            PipeDataNodeResourceManager.memory()
                .forceAllocateForTsFileWithRetry(readFileBufferSize);
        final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
      final byte[] readBuffer = new byte[readFileBufferSize];
      long position = 0;
      int readLength;
      while ((readLength = readNextFilePiece(reader, readBuffer)) != -1) {
        position =
            transferFilePiece(
                pipe2WeightMap,
                file,
                clientAndStatus,
                isMultiFile,
                readBuffer,
                position,
                readLength,
                reader);
      }
    }
  }

  private int readNextFilePiece(final RandomAccessFile reader, final byte[] readBuffer)
      throws IOException {
    final int readLength = reader.read(readBuffer);
    if (readLength != -1) {
      mayLimitRateAndRecordIO(readLength);
    }
    return readLength;
  }

  private long transferFilePiece(
      final Map<Pair<String, Long>, Double> pipe2WeightMap,
      final File file,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus,
      final boolean isMultiFile,
      final byte[] readBuffer,
      final long position,
      final int readLength,
      final RandomAccessFile reader)
      throws PipeException, IOException {
    final byte[] payLoad = buildFilePiecePayload(readBuffer, readLength);
    final PipeTransferFilePieceResp resp =
        doTransferFilePiece(pipe2WeightMap, file, clientAndStatus, isMultiFile, payLoad, position);
    return handleTransferFilePieceResp(file, clientAndStatus, reader, position + readLength, resp);
  }

  private byte[] buildFilePiecePayload(final byte[] readBuffer, final int readLength) {
    return readLength == readBuffer.length
        ? readBuffer
        : Arrays.copyOfRange(readBuffer, 0, readLength);
  }

  private PipeTransferFilePieceResp doTransferFilePiece(
      final Map<Pair<String, Long>, Double> pipe2WeightMap,
      final File file,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus,
      final boolean isMultiFile,
      final byte[] payLoad,
      final long position)
      throws PipeException, IOException {
    try {
      final TPipeTransferReq req =
          compressIfNeeded(
              isMultiFile
                  ? getTransferMultiFilePieceReq(file.getName(), position, payLoad)
                  : getTransferSingleFilePieceReq(file.getName(), position, payLoad));
      pipe2WeightMap.forEach(
          (namePair, weight) ->
              rateLimitIfNeeded(
                  namePair.getLeft(),
                  namePair.getRight(),
                  clientAndStatus.getLeft().getEndPoint(),
                  (long) (req.getBody().length * weight)));
      return PipeTransferFilePieceResp.fromTPipeTransferResp(
          clientAndStatus.getLeft().pipeTransfer(req));
    } catch (final Exception e) {
      clientAndStatus.setRight(false);
      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_FILE_S_BECAUSE_S_3C673B7A,
              file,
              e.getMessage()),
          e);
    }
  }

  private long handleTransferFilePieceResp(
      final File file,
      final Pair<IoTDBSyncClient, Boolean> clientAndStatus,
      final RandomAccessFile reader,
      final long nextPosition,
      final PipeTransferFilePieceResp resp)
      throws PipeException, IOException {
    final TSStatus status = resp.getStatus();
    if (status.getCode() == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
      reader.seek(resp.getEndWritingOffset());
      LOGGER.info(DataNodePipeMessages.REDIRECT_FILE_POSITION_TO, resp.getEndWritingOffset());
      return resp.getEndWritingOffset();
    }

    if (status.getCode() == TSStatusCode.PIPE_CONFIG_RECEIVER_HANDSHAKE_NEEDED.getStatusCode()) {
      getClientManager().sendHandshakeReq(clientAndStatus);
    }

    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        && status.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      receiverStatusHandler.handle(
          resp.getStatus(),
          String.format(
              DataNodePipeMessages.MESSAGE_TRANSFER_FILE_ARG_ERROR_RESULT_STATUS_ARG_E565D9FD,
              file,
              resp.getStatus()),
          file.getName());
    }
    return nextPosition;
  }

  private int getReadFileBufferSize(final File file) {
    return (int)
        Math.min(
            PipeConfig.getInstance().getPipeSinkReadFileBufferSize(), Math.max(file.length(), 1L));
  }

  @Override
  public TPipeTransferReq compressIfNeeded(final TPipeTransferReq req) throws IOException {
    if (Objects.isNull(compressionTimer) && Objects.nonNull(sinkTaskId)) {
      compressionTimer = PipeDataRegionSinkMetrics.getInstance().getCompressionTimer(sinkTaskId);
    }
    return super.compressIfNeeded(req);
  }

  @Override
  public synchronized void discardEventsOfPipe(
      final String pipeNameToDrop, final long creationTimeToDrop, final int regionId) {
    discardEventsOfPipe(new CommitterKey(pipeNameToDrop, creationTimeToDrop, regionId, -1));
  }

  @Override
  public synchronized void discardEventsOfPipe(final CommitterKey committerKey) {
    if (Objects.nonNull(tabletBatchBuilder)) {
      tabletBatchBuilder.discardEventsOfPipe(committerKey);
    }
  }

  public int getBatchSize() {
    return Objects.nonNull(tabletBatchBuilder) ? tabletBatchBuilder.size() : 0;
  }

  @Override
  public void close() {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }

    super.close();
  }

  public IoTDBDataNodeSyncClientManager getClientManager() {
    return clientManager;
  }

  @Override
  public void setTabletBatchSizeHistogram(Histogram tabletBatchSizeHistogram) {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.setTabletBatchSizeHistogram(tabletBatchSizeHistogram);
    }
  }

  @Override
  public void setTsFileBatchSizeHistogram(Histogram tsFileBatchSizeHistogram) {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.setTsFileBatchSizeHistogram(tsFileBatchSizeHistogram);
    }
  }

  @Override
  public void setTabletBatchTimeIntervalHistogram(Histogram tabletBatchTimeIntervalHistogram) {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.setTabletBatchTimeIntervalHistogram(tabletBatchTimeIntervalHistogram);
    }
  }

  @Override
  public void setTsFileBatchTimeIntervalHistogram(Histogram tsFileBatchTimeIntervalHistogram) {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.setTsFileBatchTimeIntervalHistogram(tsFileBatchTimeIntervalHistogram);
    }
  }

  @Override
  public void setBatchEventSizeHistogram(Histogram eventSizeHistogram) {
    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.setEventSizeHistogram(eventSizeHistogram);
    }
  }
}
