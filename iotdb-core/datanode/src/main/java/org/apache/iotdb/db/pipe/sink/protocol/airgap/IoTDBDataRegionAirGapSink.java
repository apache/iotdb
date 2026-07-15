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

package org.apache.iotdb.db.pipe.sink.protocol.airgap;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.limiter.TsFileSendRateLimiter;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
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

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
public class IoTDBDataRegionAirGapSink extends IoTDBDataNodeAirGapSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionAirGapSink.class);

  private PipeTransferBatchReqBuilder tabletBatchBuilder;
  private boolean enableSendTsFileLimit;

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new PipeTransferBatchReqBuilder(parameters);
    }

    enableSendTsFileLimit =
        parameters.getBooleanOrDefault(
            Arrays.asList(SINK_ENABLE_SEND_TSFILE_LIMIT, CONNECTOR_ENABLE_SEND_TSFILE_LIMIT),
            CONNECTOR_ENABLE_SEND_TSFILE_LIMIT_DEFAULT_VALUE);
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_A,
          tabletInsertionEvent);
      return;
    }

    final int socketIndex = nextSocketIndex();
    final AirGapSocket socket = sockets.get(socketIndex);

    try {
      // When receiver encountered packet loss, the transfer will time out
      // We need to restore the transfer quickly by retry under this circumstance
      socket.setSoTimeout(PIPE_CONFIG.getPipeAirGapSinkTabletTimeoutMs());

      if (tryTransferObjectTablet(socket, tabletInsertionEvent)) {
        return;
      }

      transferNonObjectTablet(socket, tabletInsertionEvent);
    } catch (final IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TABLET_INSERTION_EVENT_S_BECAUSE_A6F87EF5,
              ((EnrichedEvent) tabletInsertionEvent).coreReportMessage(),
              e.getMessage()),
          e);
    } finally {
      socket.setSoTimeout(PIPE_CONFIG.getPipeSinkTransferTimeoutMs());
    }
  }

  private void transferNonObjectTablet(
      final AirGapSocket socket, final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder.onEvent(tabletInsertionEvent);
      doTransferWrapper(socket);
    } else {
      transferTabletInsertionEventDirectly(socket, tabletInsertionEvent);
    }
  }

  private void transferTabletInsertionEventDirectly(
      final AirGapSocket socket, final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      doTransferWrapper(socket, (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    } else {
      doTransferWrapper(socket, (PipeRawTabletInsertionEvent) tabletInsertionEvent);
    }
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of tsFileInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBDATAREGIONAIRGAPCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_IGNORE,
          tsFileInsertionEvent);
      return;
    }

    if (!((PipeTsFileInsertionEvent) tsFileInsertionEvent).waitForTsFileClose()) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_SKIPPING_TEMPORARY_TSFILE_WHICH_SHOULDN_T,
          ((PipeTsFileInsertionEvent) tsFileInsertionEvent).getTsFile());
      return;
    }

    final int socketIndex = nextSocketIndex();
    final AirGapSocket socket = sockets.get(socketIndex);

    try {
      if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
        doTransferWrapper(socket);
      }
      doTransferWrapper(socket, (PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (final IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_INSERTION_EVENT_S_BECAUSE_BDE61690,
              ((PipeTsFileInsertionEvent) tsFileInsertionEvent).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof PipeDeleteDataNodeEvent) {
      final int socketIndex = nextSocketIndex();
      final AirGapSocket socket = sockets.get(socketIndex);

      try {
        doTransferWrapper(socket, (PipeDeleteDataNodeEvent) event);
      } catch (final IOException e) {
        isSocketAlive.set(socketIndex, false);

        throw new PipeConnectionException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_EVENT_S_BECAUSE_S_F36D2A6B,
                ((EnrichedEvent) event).coreReportMessage(),
                e.getMessage()),
            e);
      }
      return;
    }

    final int socketIndex = nextSocketIndex();
    final AirGapSocket socket = sockets.get(socketIndex);

    try {
      if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
        doTransferWrapper(socket);
      }

      if (!(event instanceof PipeHeartbeatEvent || event instanceof PipeTerminateEvent)) {
        LOGGER.warn(
            DataNodePipeMessages
                .IOTDBDATAREGIONAIRGAPCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT,
            event);
      }
    } catch (final IOException e) {
      isSocketAlive.set(socketIndex, false);

      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_NETWORK_ERROR_WHEN_TRANSFER_TSFILE_EVENT_S_BECAUSE_S_F36D2A6B,
              ((EnrichedEvent) event).coreReportMessage(),
              e.getMessage()),
          e);
    }
  }

  private void doTransferWrapper(final AirGapSocket socket)
      throws IOException, WriteProcessException {
    for (final Pair<?, PipeTabletEventBatch> nonEmptyAndShouldEmitBatch :
        tabletBatchBuilder.getAllNonEmptyAndShouldEmitBatches()) {
      doTransferWrapper(socket, nonEmptyAndShouldEmitBatch.getRight());
    }
  }

  private void transferAllBatchedEventsIfNecessary(final AirGapSocket socket)
      throws IOException, WriteProcessException {
    if (!isTabletBatchModeEnabled || tabletBatchBuilder == null || tabletBatchBuilder.isEmpty()) {
      return;
    }

    for (final Pair<?, PipeTabletEventBatch> nonEmptyBatch :
        tabletBatchBuilder.getAllNonEmptyBatches()) {
      doTransferWrapper(socket, nonEmptyBatch.getRight());
    }
  }

  private void doTransferWrapper(final AirGapSocket socket, final PipeTabletEventBatch batch)
      throws IOException, WriteProcessException {
    if (batch instanceof PipeTabletEventPlainBatch) {
      doTransfer(socket, (PipeTabletEventPlainBatch) batch);
    } else if (batch instanceof PipeTabletEventTsFileBatch) {
      doTransfer(socket, (PipeTabletEventTsFileBatch) batch);
    } else {
      throw new IllegalArgumentException(
          String.format(DataNodePipeMessages.UNSUPPORTED_BATCH_TYPE, batch.getClass()));
    }
    batch.decreaseEventsReferenceCount(IoTDBDataRegionAirGapSink.class.getName(), true);
    batch.onSuccess();
  }

  private boolean shouldTransferAsObjectTablet(final TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      return shouldTransferInsertNodeAsObjectTablets(
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
    }
    if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent rawEvent =
          (PipeRawTabletInsertionEvent) tabletInsertionEvent;
      return rawEvent.isTableModelEvent() && rawEvent.hasObjectData();
    }
    return false;
  }

  private boolean tryTransferObjectTablet(
      final AirGapSocket socket, final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (!isObjectTabletEvent(tabletInsertionEvent)) {
      return false;
    }

    if (isTabletBatchModeEnabled && tabletBatchBuilder.isTsFileBatchMode()) {
      transferObjectTabletToBatch(socket, tabletInsertionEvent);
      doTransferWrapper(socket);
      return true;
    }

    transferAllBatchedEventsIfNecessary(socket);
    if (isTabletBatchModeEnabled) {
      transferObjectTabletToBatch(socket, tabletInsertionEvent);
      transferAllBatchedEventsIfNecessary(socket);
    } else {
      transferObjectTabletSynchronously(socket, tabletInsertionEvent);
    }
    return true;
  }

  private boolean isObjectTabletEvent(final TabletInsertionEvent tabletInsertionEvent) {
    return shouldTransferAsObjectTablet(tabletInsertionEvent)
        || (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
            && ((PipeRawTabletInsertionEvent) tabletInsertionEvent).isObjectValueContentEvent());
  }

  private void transferObjectTabletSynchronously(
      final AirGapSocket socket, final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      transferTabletInsertionEventDirectly(socket, tabletInsertionEvent);
      return;
    }

    final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) tabletInsertionEvent;
    if (rawEvent.isObjectValueContentEvent()) {
      doTransferWrapper(socket, rawEvent);
    } else {
      splitRawObjectTabletAndTransferSynchronously(socket, rawEvent);
    }
  }

  private void transferObjectTabletToBatch(
      final AirGapSocket socket, final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    if (tabletBatchBuilder.isTsFileBatchMode()) {
      tabletBatchBuilder.onEvent(tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      splitInsertNodeObjectTabletToBatch(
          socket, (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      return;
    }
    final PipeRawTabletInsertionEvent rawEvent = (PipeRawTabletInsertionEvent) tabletInsertionEvent;
    if (rawEvent.isObjectValueContentEvent()) {
      offerSplitTabletEventToBatch(socket, rawEvent);
      return;
    }
    splitRawObjectTabletToBatch(socket, rawEvent);
  }

  private void splitInsertNodeObjectTabletToBatch(
      final AirGapSocket socket, final PipeInsertNodeTabletInsertionEvent event) throws Exception {
    if (tabletBatchBuilder.isTsFileBatchMode()) {
      tabletBatchBuilder.onEvent(event);
      doTransferWrapper(socket);
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
              socket, createSplitRawTabletEvent(event, splitIterator.next(), event.isAligned(i)));
        }
      }
    }
  }

  private void splitRawObjectTabletToBatch(
      final AirGapSocket socket, final PipeRawTabletInsertionEvent event) throws Exception {
    if (tabletBatchBuilder.isTsFileBatchMode()) {
      tabletBatchBuilder.onEvent(event);
      doTransferWrapper(socket);
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
            socket, createSplitRawTabletEvent(event, splitIterator.next(), event.isAligned()));
      }
    }
  }

  private void splitRawObjectTabletAndTransferSynchronously(
      final AirGapSocket socket, final PipeRawTabletInsertionEvent event) throws Exception {
    try (final TabletObjectSplitIterator splitIterator =
        new TabletObjectSplitIterator(
            event.convertToTablet(),
            event.getTsFileResource() == null ? null : event.getTsFileResource().getTsFile(),
            PipeObjectPathUtil.resolveLinkedObjectDirectory(
                event.getTsFileResource(), event.getPipeName()),
            true)) {
      while (splitIterator.hasNext()) {
        doTransferWrapper(
            socket, createSplitRawTabletEvent(event, splitIterator.next(), event.isAligned()));
      }
    }
  }

  private void offerSplitTabletEventToBatch(
      final AirGapSocket socket, final PipeRawTabletInsertionEvent splitEvent) throws Exception {
    tabletBatchBuilder.onEvent(splitEvent);
    doTransferWrapper(socket);
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

  private void doTransfer(
      final AirGapSocket socket, final PipeTabletEventPlainBatch batchToTransfer)
      throws IOException {
    if (!sendBatch(
        socket,
        toTPipeTransferBytes(batchToTransfer.toTPipeTransferReq()),
        batchToTransfer.getPipe2BytesAccumulated())) {
      final String errorMessage =
          String.format("Transfer PipeTransferTabletBatchReq error. Socket: %s.", socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          batchToTransfer.deepCopyEvents().toString());
    }
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeTabletEventTsFileBatch batchToTransfer)
      throws IOException, WriteProcessException {
    final List<Pair<String, Pair<File, File>>> dbTsFilePairs = batchToTransfer.sealTsFiles();
    final Map<Pair<String, Long>, Double> pipe2WeightMap = batchToTransfer.deepCopyPipe2WeightMap();

    try {
      for (final Pair<String, Pair<File, File>> dbTsFile : dbTsFilePairs) {
        final File tsFile = dbTsFile.right.left;
        final File objectDir = dbTsFile.right.right;
        final String tsFileNameWithoutSuffix =
            PipeObjectPathUtil.tsFileBaseNameWithoutSuffix(tsFile.getName());
        try (final Stream<Pair<Path, File>> objectFileStream =
            PipeObjectPathUtil.getObjectFileStream(objectDir == null ? null : objectDir.toPath())) {
          transferObjectBatches(pipe2WeightMap, tsFileNameWithoutSuffix, objectFileStream, socket);
        }
        doTransfer(pipe2WeightMap, socket, tsFile, null, dbTsFile.left, tsFile.getName());
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
      final AirGapSocket socket, final PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeDeleteDataNodeEvent.increaseReferenceCount(IoTDBDataNodeAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeDeleteDataNodeEvent);
    } finally {
      pipeDeleteDataNodeEvent.decreaseReferenceCount(
          IoTDBDataNodeAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent)
      throws PipeException, IOException {
    if (!send(
        pipeDeleteDataNodeEvent.getPipeName(),
        pipeDeleteDataNodeEvent.getCreationTime(),
        socket,
        PipeTransferPlanNodeReq.toTPipeTransferBytes(
            pipeDeleteDataNodeEvent.getDeleteDataNode()))) {
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
  }

  private void doTransferWrapper(
      final AirGapSocket socket,
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, WALPipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeInsertNodeTabletInsertionEvent);
    } finally {
      pipeInsertNodeTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket,
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, IOException {
    if (shouldTransferInsertNodeAsObjectTablets(pipeInsertNodeTabletInsertionEvent)) {
      transferInsertNodeAsObjectTablets(socket, pipeInsertNodeTabletInsertionEvent);
      return;
    }
    final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
    final byte[] bytes =
        PipeTransferTabletInsertNodeReqV2.toTPipeTransferBytes(
            insertNode,
            pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
                ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
                : pipeInsertNodeTabletInsertionEvent.getTreeModelDatabaseName());

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
  }

  private boolean shouldTransferInsertNodeAsObjectTablets(
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent) {
    if (!pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
        || pipeInsertNodeTabletInsertionEvent.getTsFileResource() == null
        || pipeInsertNodeTabletInsertionEvent.getTsFileResource().getTsFile() == null) {
      return false;
    }
    return pipeInsertNodeTabletInsertionEvent.hasObjectData();
  }

  private void transferInsertNodeAsObjectTablets(
      final AirGapSocket socket,
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, IOException {
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
          transferRawTabletBytesWithCheck(
              socket,
              pipeInsertNodeTabletInsertionEvent.getPipeName(),
              pipeInsertNodeTabletInsertionEvent.getCreationTime(),
              PipeTransferTabletRawReqV2.toTPipeTransferBytes(
                  splitIterator.next(),
                  pipeInsertNodeTabletInsertionEvent.isAligned(i),
                  pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()),
              pipeInsertNodeTabletInsertionEvent.toString());
        }
      }
    }
  }

  private void doTransferWrapper(
      final AirGapSocket socket, final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeRawTabletInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeRawTabletInsertionEvent);
    } finally {
      pipeRawTabletInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, IOException {
    if (shouldTransferRawTabletAsObjectTablets(pipeRawTabletInsertionEvent)) {
      try (final TabletObjectSplitIterator splitIterator =
          new TabletObjectSplitIterator(
              pipeRawTabletInsertionEvent.convertToTablet(),
              pipeRawTabletInsertionEvent.getTsFileResource() == null
                  ? null
                  : pipeRawTabletInsertionEvent.getTsFileResource().getTsFile(),
              PipeObjectPathUtil.resolveLinkedObjectDirectory(
                  pipeRawTabletInsertionEvent.getTsFileResource(),
                  pipeRawTabletInsertionEvent.getPipeName()),
              true)) {
        while (splitIterator.hasNext()) {
          transferRawTabletBytesWithCheck(
              socket,
              pipeRawTabletInsertionEvent.getPipeName(),
              pipeRawTabletInsertionEvent.getCreationTime(),
              PipeTransferTabletRawReqV2.toTPipeTransferBytes(
                  splitIterator.next(),
                  pipeRawTabletInsertionEvent.isAligned(),
                  pipeRawTabletInsertionEvent.getTableModelDatabaseName()),
              pipeRawTabletInsertionEvent.toString());
        }
      }
      return;
    }

    transferRawTabletBytesWithCheck(
        socket,
        pipeRawTabletInsertionEvent.getPipeName(),
        pipeRawTabletInsertionEvent.getCreationTime(),
        PipeTransferTabletRawReqV2.toTPipeTransferBytes(
            pipeRawTabletInsertionEvent.convertToTablet(),
            pipeRawTabletInsertionEvent.isAligned(),
            pipeRawTabletInsertionEvent.isTableModelEvent()
                ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                : pipeRawTabletInsertionEvent.getTreeModelDatabaseName()),
        pipeRawTabletInsertionEvent.toString());
  }

  private boolean shouldTransferRawTabletAsObjectTablets(
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent) {
    return pipeRawTabletInsertionEvent.isTableModelEvent()
        && !pipeRawTabletInsertionEvent.isObjectValueContentEvent()
        && pipeRawTabletInsertionEvent.hasObjectData()
        && pipeRawTabletInsertionEvent.getTsFileResource() != null
        && pipeRawTabletInsertionEvent.getTsFileResource().getTsFile() != null;
  }

  private void transferRawTabletBytesWithCheck(
      final AirGapSocket socket,
      final String pipeName,
      final long creationTime,
      final byte[] bytes,
      final String eventDescription)
      throws PipeException, IOException {
    if (!send(pipeName, creationTime, socket, bytes)) {
      final String errorMessage =
          String.format(
              DataNodePipeMessages.TRANSFER_TABLET_EVENT_ERROR_SOCKET, eventDescription, socket);
      receiverStatusHandler.handle(
          new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
              .setMessage(errorMessage),
          errorMessage,
          eventDescription);
    }
  }

  private void doTransferWrapper(
      final AirGapSocket socket, final PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeTsFileInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAirGapSink.class.getName())) {
      return;
    }
    try {
      doTransfer(socket, pipeTsFileInsertionEvent);
    } finally {
      pipeTsFileInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAirGapSink.class.getName(), false);
    }
  }

  private void doTransfer(
      final AirGapSocket socket, final PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, IOException {
    final String pipeName = pipeTsFileInsertionEvent.getPipeName();
    final long creationTime = pipeTsFileInsertionEvent.getCreationTime();
    final File tsFile = pipeTsFileInsertionEvent.getTsFile();
    final File objectDir =
        PipeObjectPathUtil.resolveLinkedObjectDirectory(
            pipeTsFileInsertionEvent.getTsFileResource(), pipeTsFileInsertionEvent.getPipeName());
    final String tsFileNameWithoutSuffix =
        PipeObjectPathUtil.tsFileBaseNameWithoutSuffix(tsFile.getName());
    try (final Stream<Pair<Path, File>> objectFileStream =
        PipeObjectPathUtil.getObjectFileStream(objectDir == null ? null : objectDir.toPath())) {
      transferObjectBatches(
          pipeName, creationTime, tsFileNameWithoutSuffix, objectFileStream, socket);
    }

    doTransfer(
        Collections.singletonMap(new Pair<>(pipeName, creationTime), 1.0),
        socket,
        tsFile,
        pipeTsFileInsertionEvent.isWithMod() && supportModsIfIsDataNodeReceiver
            ? pipeTsFileInsertionEvent.getModFile()
            : null,
        pipeTsFileInsertionEvent.isTableModelEvent()
            ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
            : pipeTsFileInsertionEvent.getTreeModelDatabaseName(),
        pipeTsFileInsertionEvent.toString());
  }

  private void doTransfer(
      final Map<Pair<String, Long>, Double> pipe2WeightMap,
      final AirGapSocket socket,
      final File tsFile,
      final File modFile,
      final String dataBaseName,
      final String receiverStatusContext)
      throws PipeException, IOException {
    final String errorMessage = String.format("Seal file %s error. Socket %s.", tsFile, socket);

    if (Objects.nonNull(modFile)) {
      transferFilePieces(pipe2WeightMap, modFile, socket, true);
      transferFilePieces(pipe2WeightMap, tsFile, socket, true);
      if (!sendWeighted(
          socket,
          PipeTransferTsFileSealWithModReq.toTPipeTransferBytes(
              modFile.getName(), modFile.length(), tsFile.getName(), tsFile.length(), dataBaseName),
          pipe2WeightMap)) {
        receiverStatusHandler.handle(
            new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                .setMessage(errorMessage),
            errorMessage,
            receiverStatusContext);
      } else {
        LOGGER.info(DataNodePipeMessages.SUCCESSFULLY_TRANSFERRED_FILE, tsFile);
      }
    } else {
      transferFilePieces(pipe2WeightMap, tsFile, socket, false);
      if (!sendWeighted(
          socket,
          PipeTransferTsFileSealWithModReq.toTPipeTransferBytes(
              tsFile.getName(), tsFile.length(), dataBaseName),
          pipe2WeightMap)) {
        receiverStatusHandler.handle(
            new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                .setMessage(errorMessage),
            errorMessage,
            receiverStatusContext);
      } else {
        LOGGER.info(DataNodePipeMessages.SUCCESSFULLY_TRANSFERRED_FILE, tsFile);
      }
    }
  }

  private void transferFilePieces(
      final Map<Pair<String, Long>, Double> pipe2WeightMap,
      final File file,
      final AirGapSocket socket,
      final boolean isMultiFile)
      throws PipeException, IOException {
    final int readFileBufferSize = getReadFileBufferSize(file);
    try (final PipeTsFileMemoryBlock ignored =
            PipeDataNodeResourceManager.memory()
                .forceAllocateForTsFileWithRetry(readFileBufferSize);
        final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
      final byte[] readBuffer = new byte[readFileBufferSize];
      long position = 0;
      while (true) {
        mayLimitRateAndRecordIO(readFileBufferSize);
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final byte[] payload =
            readLength == readFileBufferSize
                ? readBuffer
                : Arrays.copyOfRange(readBuffer, 0, readLength);
        if (!sendWeighted(
            socket,
            isMultiFile
                ? getTransferMultiFilePieceBytes(file.getName(), position, payload)
                : getTransferSingleFilePieceBytes(file.getName(), position, payload),
            pipe2WeightMap)) {
          final String errorMessage =
              String.format("Transfer file %s error. Socket %s.", file, socket);
          receiverStatusHandler.handle(
              new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                  .setMessage(errorMessage),
              errorMessage,
              file.toString());
        } else {
          position += readLength;
        }
      }
    }
  }

  private int getReadFileBufferSize(final File file) {
    return (int)
        Math.min((long) PIPE_CONFIG.getPipeSinkReadFileBufferSize(), Math.max(file.length(), 1L));
  }

  private boolean sendBatch(
      final AirGapSocket socket,
      byte[] bytes,
      final Map<Pair<String, Long>, Long> pipe2BytesAccumulated)
      throws IOException {
    final long uncompressedSize = bytes.length;
    bytes = compressIfNeeded(bytes);

    final double compressionRatio =
        uncompressedSize == 0 ? 1 : (double) bytes.length / uncompressedSize;
    for (final Map.Entry<Pair<String, Long>, Long> entry : pipe2BytesAccumulated.entrySet()) {
      rateLimitIfNeeded(
          entry.getKey().getLeft(),
          entry.getKey().getRight(),
          socket.getEndPoint(),
          (long) (entry.getValue() * compressionRatio));
    }
    return sendBytes(socket, bytes);
  }

  private boolean sendWeighted(
      final AirGapSocket socket, byte[] bytes, final Map<Pair<String, Long>, Double> pipe2WeightMap)
      throws IOException {
    bytes = compressIfNeeded(bytes);

    for (final Map.Entry<Pair<String, Long>, Double> entry : pipe2WeightMap.entrySet()) {
      rateLimitIfNeeded(
          entry.getKey().getLeft(),
          entry.getKey().getRight(),
          socket.getEndPoint(),
          (long) (bytes.length * entry.getValue()));
    }
    return sendBytes(socket, bytes);
  }

  private byte[] toTPipeTransferBytes(final TPipeTransferReq req) throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(req.version, outputStream);
      ReadWriteIOUtils.write(req.type, outputStream);

      final ByteBuffer bodyBuffer = req.body.duplicate();
      final byte[] body = new byte[bodyBuffer.remaining()];
      bodyBuffer.get(body);
      outputStream.write(body);

      return byteArrayOutputStream.toByteArray();
    }
  }

  @Override
  protected void mayLimitRateAndRecordIO(final long requiredBytes) {
    PipeResourceMetrics.getInstance().recordDiskIO(requiredBytes);
    if (enableSendTsFileLimit) {
      TsFileSendRateLimiter.getInstance().acquire(requiredBytes);
    }
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

  private void transferObjectBatches(
      final Map<Pair<String, Long>, Double> pipe2WeightMap,
      final String tsFileNameWithoutSuffix,
      final Stream<Pair<Path, File>> objectFileStream,
      final AirGapSocket socket)
      throws PipeException, IOException {
    final int maxPieceBytes = PipeConfig.getInstance().getPipeSinkReadFileBufferSize();
    final int maxBatchBytes = PipeTsFileObjectBatchTransfer.defaultMaxBatchSerializedSumBytes();
    final Iterator<PipeTsFileObjectBatchTransfer.ObjectBatch> it =
        PipeTsFileObjectBatchTransfer.batchIterator(
            tsFileNameWithoutSuffix, objectFileStream, maxPieceBytes, maxBatchBytes);
    while (it.hasNext()) {
      final PipeTsFileObjectBatchTransfer.ObjectBatch batch = it.next();
      if (!sendWeighted(socket, batch.toAirGapBytes(tsFileNameWithoutSuffix), pipe2WeightMap)) {
        final String errorMessage =
            String.format(
                DataNodePipeMessages.TRANSFER_OBJECT_BATCH_ERROR_SOCKET,
                tsFileNameWithoutSuffix,
                socket);
        receiverStatusHandler.handle(
            new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                .setMessage(errorMessage),
            errorMessage,
            tsFileNameWithoutSuffix);
        return;
      }
    }
  }

  private void transferObjectBatches(
      final String pipeName,
      final long creationTime,
      final String tsFileNameWithoutSuffix,
      final Stream<Pair<Path, File>> objectFileStream,
      final AirGapSocket socket)
      throws PipeException, IOException {
    final int maxPieceBytes = PipeConfig.getInstance().getPipeSinkReadFileBufferSize();
    final int maxBatchBytes = PipeTsFileObjectBatchTransfer.defaultMaxBatchSerializedSumBytes();
    final Iterator<PipeTsFileObjectBatchTransfer.ObjectBatch> it =
        PipeTsFileObjectBatchTransfer.batchIterator(
            tsFileNameWithoutSuffix, objectFileStream, maxPieceBytes, maxBatchBytes);
    while (it.hasNext()) {
      final PipeTsFileObjectBatchTransfer.ObjectBatch batch = it.next();
      if (!send(
          pipeName,
          creationTime,
          socket,
          compressIfNeeded(batch.toAirGapBytes(tsFileNameWithoutSuffix)))) {
        final String errorMessage =
            String.format(
                DataNodePipeMessages.TRANSFER_OBJECT_BATCH_ERROR_SOCKET,
                tsFileNameWithoutSuffix,
                socket);
        receiverStatusHandler.handle(
            new TSStatus(TSStatusCode.PIPE_RECEIVER_USER_CONFLICT_EXCEPTION.getStatusCode())
                .setMessage(errorMessage),
            errorMessage,
            tsFileNameWithoutSuffix);
        return;
      }
    }
  }

  @Override
  protected byte[] compressIfNeeded(final byte[] reqInBytes) throws IOException {
    if (Objects.isNull(compressionTimer) && Objects.nonNull(sinkTaskId)) {
      compressionTimer = PipeDataRegionSinkMetrics.getInstance().getCompressionTimer(sinkTaskId);
    }
    return super.compressIfNeeded(reqInBytes);
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
