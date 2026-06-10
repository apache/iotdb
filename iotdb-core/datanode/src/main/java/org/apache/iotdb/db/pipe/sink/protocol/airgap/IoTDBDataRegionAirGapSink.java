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
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.limiter.TsFileSendRateLimiter;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeResourceMetrics;
import org.apache.iotdb.db.pipe.metric.sink.PipeDataRegionSinkMetrics;
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
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
      if (isTabletBatchModeEnabled) {
        tabletBatchBuilder.onEvent(tabletInsertionEvent);
        doTransferWrapper(socket);
      } else if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        doTransferWrapper(socket, (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
      } else {
        doTransferWrapper(socket, (PipeRawTabletInsertionEvent) tabletInsertionEvent);
      }
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
      final PipeTabletEventBatch batch = nonEmptyAndShouldEmitBatch.getRight();
      if (batch instanceof PipeTabletEventPlainBatch) {
        doTransfer(socket, (PipeTabletEventPlainBatch) batch);
      } else if (batch instanceof PipeTabletEventTsFileBatch) {
        doTransfer(socket, (PipeTabletEventTsFileBatch) batch);
      } else {
        LOGGER.warn(DataNodePipeMessages.UNSUPPORTED_BATCH_TYPE, batch.getClass());
      }
      batch.decreaseEventsReferenceCount(IoTDBDataRegionAirGapSink.class.getName(), true);
      batch.onSuccess();
    }
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
    final List<Pair<String, File>> dbTsFilePairs = batchToTransfer.sealTsFiles();
    final Map<Pair<String, Long>, Double> pipe2WeightMap = batchToTransfer.deepCopyPipe2WeightMap();

    for (final Pair<String, File> dbTsFile : dbTsFilePairs) {
      doTransfer(
          pipe2WeightMap, socket, dbTsFile.right, null, dbTsFile.left, dbTsFile.right.getName());
      try {
        RetryUtils.retryOnException(
            () -> {
              FileUtils.delete(dbTsFile.right);
              return null;
            });
      } catch (final NoSuchFileException e) {
        LOGGER.info(DataNodePipeMessages.THE_FILE_IS_NOT_FOUND_MAY_ALREADY, dbTsFile);
      } catch (final Exception e) {
        LOGGER.warn(DataNodePipeMessages.FAILED_TO_DELETE_BATCH_FILE_THIS_FILE, dbTsFile);
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
    if (!send(
        pipeRawTabletInsertionEvent.getPipeName(),
        pipeRawTabletInsertionEvent.getCreationTime(),
        socket,
        PipeTransferTabletRawReqV2.toTPipeTransferBytes(
            pipeRawTabletInsertionEvent.convertToTablet(),
            pipeRawTabletInsertionEvent.isAligned(),
            pipeRawTabletInsertionEvent.isTableModelEvent()
                ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                : pipeRawTabletInsertionEvent.getTreeModelDatabaseName()))) {
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
    doTransfer(
        Collections.singletonMap(
            new Pair<>(
                pipeTsFileInsertionEvent.getPipeName(), pipeTsFileInsertionEvent.getCreationTime()),
            1.0),
        socket,
        pipeTsFileInsertionEvent.getTsFile(),
        pipeTsFileInsertionEvent.isWithMod() && supportModsIfIsDataNodeReceiver
            ? pipeTsFileInsertionEvent.getModFile()
            : null,
        pipeTsFileInsertionEvent.isTableModelEvent()
            ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
            : null,
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
    final int readFileBufferSize = PIPE_CONFIG.getPipeSinkReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(file, "r")) {
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
