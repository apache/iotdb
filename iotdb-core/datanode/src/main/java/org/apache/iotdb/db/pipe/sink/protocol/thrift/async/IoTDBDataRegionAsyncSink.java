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

package org.apache.iotdb.db.pipe.sink.protocol.thrift.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSink;
import org.apache.iotdb.commons.pipe.sink.protocol.PipeSinkWithSchedulingDelay;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.sink.PipeDataRegionSinkMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeAsyncClientManager;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventPlainBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler.PipeTransferTabletBatchEventHandler;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler.PipeTransferTabletInsertNodeEventHandler;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler.PipeTransferTabletRawEventHandler;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler.PipeTransferTrackableHandler;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.handler.PipeTransferTsFileHandler;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.UrlUtils;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_ENABLE_SEND_TSFILE_LIMIT;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_ENABLE_SEND_TSFILE_LIMIT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_SSL_KEY_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_ENABLE_SEND_TSFILE_LIMIT;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_KEY_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_LEADER_CACHE_ENABLE_KEY;

@TreeModel
@TableModel
public class IoTDBDataRegionAsyncSink extends IoTDBSink implements PipeSinkWithSchedulingDelay {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionAsyncSink.class);

  private static final String THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT =
      "Failed to borrow client from client pool when sending to receiver.";
  private static final String THRIFT_ERROR_FORMATTER_WITH_ENDPOINT =
      "Exception occurred while sending to receiver %s:%s.";

  private static final boolean isSplitTSFileBatchModeEnabled = true;

  private final IoTDBDataRegionSyncSink syncSink = new IoTDBDataRegionSyncSink();

  private final BlockingQueue<Event> retryEventQueue = new LinkedBlockingQueue<>();
  private final BlockingQueue<TsFileInsertionEvent> retryTsFileQueue = new LinkedBlockingQueue<>();
  private final PipeDataRegionEventCounter retryEventQueueEventCounter =
      new PipeDataRegionEventCounter();

  private IoTDBDataNodeAsyncClientManager clientManager;
  private IoTDBDataNodeAsyncClientManager transferTsFileClientManager;

  // It is necessary to ensure that other classes that inherit Async Connector will not have NPE
  public AtomicInteger transferTsFileCounter = new AtomicInteger(0);

  private PipeTransferBatchReqBuilder tabletBatchBuilder;

  // use these variables to prevent reference count leaks under some corner cases when closing
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private int consecutiveHandshakeFailureCount = 0;
  private final AtomicLong schedulingDelayMs = new AtomicLong(0);
  private final Map<PipeTransferTrackableHandler, PipeTransferTrackableHandler> pendingHandlers =
      new ConcurrentHashMap<>();

  private final Set<CommitterKey> droppedPipeTaskKeys = ConcurrentHashMap.newKeySet();
  private final Map<String, ReceiverTemporaryUnavailableBackoff> receiverBackoffMap =
      new ConcurrentHashMap<>();

  private boolean enableSendTsFileLimit;
  private volatile boolean isConnectionException;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);
    syncSink.validate(validator);

    final PipeParameters parameters = validator.getParameters();

    validator.validate(
        args ->
            !((boolean) args[0]
                || (boolean) args[1]
                || (boolean) args[2]
                || (boolean) args[3]
                || (boolean) args[4]),
        "Only 'iotdb-thrift-ssl-sink' supports SSL transmission currently.",
        parameters.getBooleanOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_SSL_ENABLE_KEY, SINK_IOTDB_SSL_ENABLE_KEY), false),
        parameters.hasAnyAttributes(
            CONNECTOR_IOTDB_SSL_TRUST_STORE_PATH_KEY, SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY),
        parameters.hasAnyAttributes(
            CONNECTOR_IOTDB_SSL_TRUST_STORE_PWD_KEY, SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY),
        parameters.hasAnyAttributes(
            CONNECTOR_IOTDB_SSL_KEY_STORE_PATH_KEY, SINK_IOTDB_SSL_KEY_STORE_PATH_KEY),
        parameters.hasAnyAttributes(
            CONNECTOR_IOTDB_SSL_KEY_STORE_PWD_KEY, SINK_IOTDB_SSL_KEY_STORE_PWD_KEY));
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
    syncSink.customize(parameters, configuration);

    clientManager =
        new IoTDBDataNodeAsyncClientManager(
            nodeUrls,
            parameters.getBooleanOrDefault(
                Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
                CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE),
            loadBalanceStrategy,
            new UserEntity(Long.parseLong(userId), username, cliHostname),
            password,
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy,
            loadTsFileValidation,
            shouldMarkAsPipeRequest,
            false,
            skipIfNoPrivileges);

    transferTsFileClientManager =
        new IoTDBDataNodeAsyncClientManager(
            nodeUrls,
            parameters.getBooleanOrDefault(
                Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
                CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE),
            loadBalanceStrategy,
            new UserEntity(Long.parseLong(userId), username, cliHostname),
            password,
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy,
            loadTsFileValidation,
            shouldMarkAsPipeRequest,
            isSplitTSFileBatchModeEnabled,
            skipIfNoPrivileges);

    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new PipeTransferBatchReqBuilder(parameters);
    }

    enableSendTsFileLimit =
        parameters.getBooleanOrDefault(
            Arrays.asList(SINK_ENABLE_SEND_TSFILE_LIMIT, CONNECTOR_ENABLE_SEND_TSFILE_LIMIT),
            CONNECTOR_ENABLE_SEND_TSFILE_LIMIT_DEFAULT_VALUE);
  }

  @Override
  // Synchronized to avoid close connector when transfer event
  public synchronized void handshake() throws Exception {
    syncSink.handshake();
  }

  @Override
  public void heartbeat() throws Exception {
    if (!isClosed()) {
      syncSink.heartbeat();
    }
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary(false);

    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PI,
          tabletInsertionEvent);
      return;
    }

    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder.onEvent(tabletInsertionEvent);
      transferBatchedEventsIfNecessary();
    } else {
      transferInEventWithoutCheck(tabletInsertionEvent);
    }
  }

  private void transferInBatchWithoutCheck(
      final Pair<TEndPoint, PipeTabletEventBatch> endPointAndBatch)
      throws IOException, WriteProcessException {
    if (Objects.isNull(endPointAndBatch)) {
      return;
    }

    final PipeTabletEventBatch batch = endPointAndBatch.getRight();

    if (batch instanceof PipeTabletEventPlainBatch) {
      transfer(
          endPointAndBatch.getLeft(),
          new PipeTransferTabletBatchEventHandler((PipeTabletEventPlainBatch) batch, this));
    } else if (batch instanceof PipeTabletEventTsFileBatch) {
      final PipeTabletEventTsFileBatch tsFileBatch = (PipeTabletEventTsFileBatch) batch;
      final List<Pair<String, File>> dbTsFilePairs = tsFileBatch.sealTsFiles();
      final Map<Pair<String, Long>, Double> pipe2WeightMap = tsFileBatch.deepCopyPipe2WeightMap();
      final List<EnrichedEvent> events = tsFileBatch.deepCopyEvents();
      final AtomicInteger eventsReferenceCount = new AtomicInteger(dbTsFilePairs.size());
      final AtomicBoolean eventsHadBeenAddedToRetryQueue = new AtomicBoolean(false);

      int transferredFileCount = 0;
      try {
        for (final Pair<String, File> sealedFile : dbTsFilePairs) {
          transfer(
              new PipeTransferTsFileHandler(
                  this,
                  pipe2WeightMap,
                  events,
                  eventsReferenceCount,
                  eventsHadBeenAddedToRetryQueue,
                  sealedFile.right,
                  null,
                  false,
                  sealedFile.left));
          transferredFileCount++;
        }
      } catch (final Exception e) {
        for (int i = transferredFileCount; i < dbTsFilePairs.size(); i++) {
          final Pair<String, File> untransferredFile = dbTsFilePairs.get(i);
          if (untransferredFile.right.exists()
              && !FileUtils.deleteQuietly(untransferredFile.right)) {
            LOGGER.warn(
                DataNodePipeMessages.FAILED_TO_DELETE_BATCH_FILE_THIS_FILE, untransferredFile);
          }
        }
        PipeLogger.log(
            ignored ->
                LOGGER.warn(DataNodePipeMessages.FAILED_TO_TRANSFER_TSFILE_BATCH, dbTsFilePairs, e),
            e,
            DataNodePipeMessages.FAILED_TO_TRANSFER_TSFILE_BATCH,
            dbTsFilePairs);
        if (eventsHadBeenAddedToRetryQueue.compareAndSet(false, true)) {
          addFailureEventsToRetryQueue(events, e);
        }
      }
    } else {
      LOGGER.warn(
          DataNodePipeMessages.UNSUPPORTED_BATCH_TYPE_WHEN_TRANSFERRING_TABLET_INSERTION,
          batch.getClass());
    }

    endPointAndBatch.getRight().onSuccess();
  }

  private boolean transferInEventWithoutCheck(final TabletInsertionEvent tabletInsertionEvent)
      throws Exception {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
      // We increase the reference count for this event to determine if the event may be released.
      if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
          IoTDBDataRegionAsyncSink.class.getName())) {
        return false;
      }

      final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
      final String databaseName =
          pipeInsertNodeTabletInsertionEvent.isTableModelEvent()
              ? pipeInsertNodeTabletInsertionEvent.getTableModelDatabaseName()
              : pipeInsertNodeTabletInsertionEvent.getTreeModelDatabaseName();
      final TPipeTransferReq pipeTransferReq =
          compressIfNeeded(
              PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(insertNode, databaseName));
      final PipeTransferTabletInsertNodeEventHandler pipeTransferInsertNodeReqHandler =
          new PipeTransferTabletInsertNodeEventHandler(
              pipeInsertNodeTabletInsertionEvent, pipeTransferReq, this);

      transfer(
          // getDeviceId() may return null for InsertRowsNode
          pipeInsertNodeTabletInsertionEvent.getDeviceId(), pipeTransferInsertNodeReqHandler);
    } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) tabletInsertionEvent;
      // We increase the reference count for this event to determine if the event may be released.
      if (!pipeRawTabletInsertionEvent.increaseReferenceCount(
          IoTDBDataRegionAsyncSink.class.getName())) {
        return false;
      }

      final TPipeTransferReq pipeTransferTabletRawReq =
          compressIfNeeded(
              PipeTransferTabletRawReqV2.toTPipeTransferReq(
                  pipeRawTabletInsertionEvent.convertToTablet(),
                  pipeRawTabletInsertionEvent.isAligned(),
                  pipeRawTabletInsertionEvent.isTableModelEvent()
                      ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                      : pipeRawTabletInsertionEvent.getTreeModelDatabaseName()));
      final PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler =
          new PipeTransferTabletRawEventHandler(
              pipeRawTabletInsertionEvent, pipeTransferTabletRawReq, this);

      transfer(pipeRawTabletInsertionEvent.getDeviceId(), pipeTransferTabletReqHandler);
    }

    return true;
  }

  private void transfer(
      final TEndPoint endPoint,
      final PipeTransferTabletBatchEventHandler pipeTransferTabletBatchEventHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient(endPoint);
      markHandshakeSucceeded();
      pipeTransferTabletBatchEventHandler.transfer(client);
    } catch (final Exception ex) {
      markSchedulingDelayIfHandshakeFailed(client);
      logOnClientException(client, ex);
      pipeTransferTabletBatchEventHandler.onError(ex);
    }
  }

  private void transfer(
      final String deviceId,
      final PipeTransferTabletInsertNodeEventHandler pipeTransferInsertNodeReqHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient(deviceId);
      markHandshakeSucceeded();
      pipeTransferInsertNodeReqHandler.transfer(client);
    } catch (final Exception ex) {
      markSchedulingDelayIfHandshakeFailed(client);
      logOnClientException(client, ex);
      pipeTransferInsertNodeReqHandler.onError(ex);
    }
  }

  private void transfer(
      final String deviceId, final PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient(deviceId);
      markHandshakeSucceeded();
      pipeTransferTabletReqHandler.transfer(client);
    } catch (final Exception ex) {
      markSchedulingDelayIfHandshakeFailed(client);
      logOnClientException(client, ex);
      pipeTransferTabletReqHandler.onError(ex);
    }
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary(false);
    transferBatchedEventsIfNecessary();

    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBTHRIFTASYNCCONNECTOR_ONLY_SUPPORT_PIPETSFILEINSERTIONEVENT_CURRENT_EVENT,
          tsFileInsertionEvent);
      return;
    }

    transferWithoutCheck(tsFileInsertionEvent);
  }

  private boolean transferWithoutCheck(final TsFileInsertionEvent tsFileInsertionEvent)
      throws Exception {
    final PipeTsFileInsertionEvent pipeTsFileInsertionEvent =
        (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeTsFileInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAsyncSink.class.getName())) {
      return false;
    }

    // We assume that no exceptions will be thrown after reference count is increased.
    try {
      // Just in case. To avoid the case that exception occurred when constructing the handler.
      if (!pipeTsFileInsertionEvent.getTsFile().exists()) {
        throw new FileNotFoundException(pipeTsFileInsertionEvent.getTsFile().getAbsolutePath());
      }

      final PipeTransferTsFileHandler pipeTransferTsFileHandler =
          new PipeTransferTsFileHandler(
              this,
              Collections.singletonMap(
                  new Pair<>(
                      pipeTsFileInsertionEvent.getPipeName(),
                      pipeTsFileInsertionEvent.getCreationTime()),
                  1.0),
              Collections.singletonList(pipeTsFileInsertionEvent),
              new AtomicInteger(1),
              new AtomicBoolean(false),
              pipeTsFileInsertionEvent.getTsFile(),
              pipeTsFileInsertionEvent.getModFile(),
              pipeTsFileInsertionEvent.isWithMod()
                  && clientManager.supportModsIfIsDataNodeReceiver(),
              pipeTsFileInsertionEvent.isTableModelEvent()
                  ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
                  : pipeTsFileInsertionEvent.getTreeModelDatabaseName());

      transfer(pipeTransferTsFileHandler);
      return true;
    } catch (final Exception e) {
      // Just in case. To avoid the case that exception occurred when constructing the handler.
      pipeTsFileInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAsyncSink.class.getName(), false);
      throw e;
    }
  }

  private void transfer(final PipeTransferTsFileHandler pipeTransferTsFileHandler) {
    transferTsFileCounter.incrementAndGet();
    final CompletableFuture<Void> completableFuture;
    try {
      completableFuture =
          CompletableFuture.supplyAsync(
              () -> {
                AsyncPipeDataTransferServiceClient client = null;
                try {
                  client = transferTsFileClientManager.borrowClient();
                  markHandshakeSucceeded();
                  pipeTransferTsFileHandler.transfer(transferTsFileClientManager, client);
                } catch (final Exception ex) {
                  markSchedulingDelayIfHandshakeFailed(client);
                  logOnClientException(client, ex);
                  pipeTransferTsFileHandler.onError(ex);
                } finally {
                  transferTsFileCounter.decrementAndGet();
                }
                return null;
              },
              transferTsFileClientManager.getExecutor());
    } catch (final RuntimeException e) {
      transferTsFileCounter.decrementAndGet();
      throw e;
    }

    if (PipeConfig.getInstance().isTransferTsFileSync()) {
      try {
        completableFuture.get();
      } catch (final Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          PipeLogger.log(
              ignored ->
                  LOGGER.warn(
                      DataNodePipeMessages.TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY_WAS_INTERRUPTED,
                      pipeTransferTsFileHandler.getTsFile(),
                      e),
              e,
              DataNodePipeMessages.TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY_WAS_INTERRUPTED,
              pipeTransferTsFileHandler.getTsFile());
        }

        pipeTransferTsFileHandler.onError(e);
        PipeLogger.log(
            ignored ->
                LOGGER.warn(
                    DataNodePipeMessages.FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY,
                    pipeTransferTsFileHandler.getTsFile(),
                    e),
            e,
            DataNodePipeMessages.FAILED_TO_TRANSFER_TSFILE_EVENT_ASYNCHRONOUSLY,
            pipeTransferTsFileHandler.getTsFile());
      }
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    transferQueuedEventsIfNecessary(true);
    transferBatchedEventsIfNecessary();

    if (!(event instanceof PipeHeartbeatEvent
        || event instanceof PipeDeleteDataNodeEvent
        || event instanceof PipeTerminateEvent)) {
      LOGGER.warn(
          DataNodePipeMessages
              .IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFERRING_GENERIC_EVENT,
          event);
      return;
    }

    syncSink.transfer(event);
  }

  /** Try its best to commit data in order. Flush can also be a trigger to transfer batched data. */
  private void transferBatchedEventsIfNecessary() throws IOException, WriteProcessException {
    if (!isTabletBatchModeEnabled || tabletBatchBuilder.isEmpty()) {
      return;
    }

    for (final Pair<TEndPoint, PipeTabletEventBatch> endPointAndBatch :
        tabletBatchBuilder.getAllNonEmptyAndShouldEmitBatches()) {
      transferInBatchWithoutCheck(endPointAndBatch);
    }
  }

  @Override
  public TPipeTransferReq compressIfNeeded(final TPipeTransferReq req) throws IOException {
    if (Objects.isNull(compressionTimer) && Objects.nonNull(sinkTaskId)) {
      compressionTimer = PipeDataRegionSinkMetrics.getInstance().getCompressionTimer(sinkTaskId);
    }
    return super.compressIfNeeded(req);
  }

  //////////////////////////// Leader cache update ////////////////////////////

  public void updateLeaderCache(final String deviceId, final TEndPoint endPoint) {
    clientManager.updateLeaderCache(deviceId, endPoint);
  }

  //////////////////////////// Exception handlers ////////////////////////////

  private void logOnClientException(
      final AsyncPipeDataTransferServiceClient client, final Exception e) {
    if (client == null) {
      PipeLogger.log(LOGGER::warn, THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT);
    } else {
      client.resetMethodStateIfStopped();
      PipeLogger.log(
          LOGGER::warn,
          e,
          String.format(THRIFT_ERROR_FORMATTER_WITH_ENDPOINT, client.getIp(), client.getPort()));
    }
  }

  private void markHandshakeSucceeded() {
    consecutiveHandshakeFailureCount = 0;
  }

  private void markSchedulingDelayIfHandshakeFailed(
      final AsyncPipeDataTransferServiceClient client) {
    if (client != null) {
      return;
    }

    if (++consecutiveHandshakeFailureCount < getSchedulingDelayFailureThreshold()) {
      return;
    }

    schedulingDelayMs.accumulateAndGet(
        PipeConfig.getInstance().getPipeSinkRetryIntervalMs(), Math::max);
  }

  private int getSchedulingDelayFailureThreshold() {
    return Math.max(1, nodeUrls.size() << 1);
  }

  @Override
  public long peekSchedulingDelayMs() {
    return schedulingDelayMs.get();
  }

  @Override
  public long consumeSchedulingDelayMs() {
    return schedulingDelayMs.getAndSet(0);
  }

  /**
   * Transfer queued {@link Event}s which are waiting for retry.
   *
   * @see PipeConnector#transfer(Event) for more details.
   * @see PipeConnector#transfer(TabletInsertionEvent) for more details.
   * @see PipeConnector#transfer(TsFileInsertionEvent) for more details.
   */
  private void transferQueuedEventsIfNecessary(final boolean forced) {
    throwIfReceiverProbeIsDelayed();

    if ((retryEventQueue.isEmpty() && retryTsFileQueue.isEmpty())
        || (!forced
            && retryEventQueueEventCounter.getTabletInsertionEventCount()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTabletEventQueueSize()
            && retryEventQueueEventCounter.getTsFileInsertionEventCount()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTsFileEventQueueSize()
            && retryEventQueue.size() + retryTsFileQueue.size()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTotalEventQueueSize())) {
      return;
    }

    final long retryStartTime = System.currentTimeMillis();
    final int remainingEvents = retryEventQueue.size() + retryTsFileQueue.size();
    while (!retryEventQueue.isEmpty() || !retryTsFileQueue.isEmpty()) {
      synchronized (this) {
        if (isClosed.get()) {
          return;
        }
        if (retryEventQueue.isEmpty() && retryTsFileQueue.isEmpty()) {
          break;
        }

        final Event peekedEvent;
        final Event polledEvent;
        if (!retryEventQueue.isEmpty()) {
          peekedEvent = retryEventQueue.peek();

          if (peekedEvent instanceof PipeInsertNodeTabletInsertionEvent) {
            retryTransfer((PipeInsertNodeTabletInsertionEvent) peekedEvent);
          } else if (peekedEvent instanceof PipeRawTabletInsertionEvent) {
            retryTransfer((PipeRawTabletInsertionEvent) peekedEvent);
          } else {
            LOGGER.warn(
                DataNodePipeMessages
                    .IOTDBTHRIFTASYNCCONNECTOR_DOES_NOT_SUPPORT_TRANSFER_GENERIC_EVENT,
                peekedEvent);
          }

          polledEvent = retryEventQueue.poll();
        } else {
          if (transferTsFileCounter.get()
              >= PipeConfig.getInstance().getPipeRealTimeQueueMaxWaitingTsFileSize()) {
            return;
          }
          peekedEvent = retryTsFileQueue.peek();
          retryTransfer((PipeTsFileInsertionEvent) peekedEvent);
          polledEvent = retryTsFileQueue.poll();
        }

        retryEventQueueEventCounter.decreaseEventCount(polledEvent);
        if (polledEvent != peekedEvent) {
          LOGGER.error(
              DataNodePipeMessages.THE_EVENT_POLLED_FROM_THE_QUEUE_IS, peekedEvent, polledEvent);
        }
        if (polledEvent != null && LOGGER.isDebugEnabled()) {
          LOGGER.debug(DataNodePipeMessages.POLLED_EVENT_FROM_RETRY_QUEUE, polledEvent);
        }
      }

      throwIfReceiverProbeIsDelayed();

      // Stop retrying if the execution time exceeds the threshold for better realtime performance
      if (System.currentTimeMillis() - retryStartTime
          > PipeConfig.getInstance().getPipeAsyncSinkMaxRetryExecutionTimeMsPerCall()) {
        if (retryEventQueueEventCounter.getTabletInsertionEventCount()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTabletEventQueueSize()
            && retryEventQueueEventCounter.getTsFileInsertionEventCount()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTsFileEventQueueSize()
            && retryEventQueue.size() + retryTsFileQueue.size()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTotalEventQueueSize()) {
          return;
        }

        if (remainingEvents <= retryEventQueue.size() + retryTsFileQueue.size()) {
          final String message =
              "Failed to retry transferring events in the retry queue. Remaining events: "
                  + (retryEventQueue.size() + retryTsFileQueue.size())
                  + " (tablet events: "
                  + retryEventQueueEventCounter.getTabletInsertionEventCount()
                  + ", tsfile events: "
                  + retryEventQueueEventCounter.getTsFileInsertionEventCount()
                  + ").";
          throw isConnectionException
              ? new PipeConnectionException(message)
              : new PipeException(message);
        }
      }
    }
  }

  private void retryTransfer(final TabletInsertionEvent tabletInsertionEvent) {
    if (isTabletBatchModeEnabled) {
      try {
        tabletBatchBuilder.onEvent(tabletInsertionEvent);
        transferBatchedEventsIfNecessary();
        if (tabletInsertionEvent instanceof EnrichedEvent) {
          ((EnrichedEvent) tabletInsertionEvent)
              .decreaseReferenceCount(IoTDBDataRegionAsyncSink.class.getName(), false);
        }
      } catch (final Exception e) {
        addFailureEventToRetryQueue(tabletInsertionEvent, e);
      }
      return;
    }

    // Tablet batch mode is not enabled, so we need to transfer the event directly.
    try {
      if (transferInEventWithoutCheck(tabletInsertionEvent)) {
        if (tabletInsertionEvent instanceof EnrichedEvent) {
          ((EnrichedEvent) tabletInsertionEvent)
              .decreaseReferenceCount(IoTDBDataRegionAsyncSink.class.getName(), false);
        }
      } else {
        addFailureEventToRetryQueue(tabletInsertionEvent, null);
      }
    } catch (final Exception e) {
      if (tabletInsertionEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) tabletInsertionEvent)
            .decreaseReferenceCount(IoTDBDataRegionAsyncSink.class.getName(), false);
      }
      addFailureEventToRetryQueue(tabletInsertionEvent, e);
    }
  }

  private void retryTransfer(final PipeTsFileInsertionEvent tsFileInsertionEvent) {
    try {
      if (transferWithoutCheck(tsFileInsertionEvent)) {
        tsFileInsertionEvent.decreaseReferenceCount(
            IoTDBDataRegionAsyncSink.class.getName(), false);
      } else {
        addFailureEventToRetryQueue(tsFileInsertionEvent, null);
      }
    } catch (final Exception e) {
      addFailureEventToRetryQueue(tsFileInsertionEvent, e);
    }
  }

  /**
   * Add failure {@link Event} to retry queue.
   *
   * @param event {@link Event} to retry
   */
  @SuppressWarnings("java:S899")
  public void addFailureEventToRetryQueue(final Event event, final Exception e) {
    isConnectionException =
        e instanceof PipeConnectionException || ThriftClient.isConnectionBroken(e);
    if (event instanceof EnrichedEvent) {
      final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
      if (enrichedEvent.isReleased()) {
        return;
      }
      if (isDroppedPipe(enrichedEvent)) {
        enrichedEvent.clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
        return;
      }
    }

    if (isClosed.get()) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
      }
      return;
    }

    if (event instanceof PipeTsFileInsertionEvent) {
      retryTsFileQueue.offer((PipeTsFileInsertionEvent) event);
      retryEventQueueEventCounter.increaseEventCount(event);
    } else {
      retryEventQueue.offer(event);
      retryEventQueueEventCounter.increaseEventCount(event);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(DataNodePipeMessages.ADDED_EVENT_TO_RETRY_QUEUE, event);
    }

    if (isClosed.get()) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
      }
    }
  }

  /**
   * Add failure {@link EnrichedEvent}s to retry queue.
   *
   * @param events {@link EnrichedEvent}s to retry
   */
  public void addFailureEventsToRetryQueue(
      final Iterable<EnrichedEvent> events, final Exception e) {
    events.forEach(event -> addFailureEventToRetryQueue(event, e));
  }

  public boolean isEnableSendTsFileLimit() {
    return enableSendTsFileLimit;
  }

  public void waitIfReceiverTemporarilyUnavailable(final TEndPoint endPoint) {
    final String endPointKey = format(endPoint);
    if (Objects.isNull(endPointKey)) {
      return;
    }

    final ReceiverTemporaryUnavailableBackoff backoff = receiverBackoffMap.get(endPointKey);
    if (Objects.isNull(backoff)) {
      return;
    }

    while (!isClosed.get() && backoff.isActive()) {
      if (backoff.isRetryMaxDurationExceeded()) {
        final long probeDelayInMs = backoff.tryAcquireProbeAndGetDelayInMs();
        if (probeDelayInMs <= 0) {
          return;
        }
        schedulingDelayMs.accumulateAndGet(probeDelayInMs, Math::max);
        throw createReceiverProbeDelayException(endPointKey, backoff);
      }

      final long retryTimeInMs = backoff.reserveNextRetryTimeInMs();
      while (!isClosed.get() && backoff.isActive()) {
        if (backoff.isRetryMaxDurationExceeded()) {
          break;
        }

        final long waitTimeInMs = retryTimeInMs - System.currentTimeMillis();
        if (waitTimeInMs <= 0) {
          return;
        }

        try {
          Thread.sleep(Math.min(waitTimeInMs, 1000L));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private void throwIfReceiverProbeIsDelayed() {
    for (final Map.Entry<String, ReceiverTemporaryUnavailableBackoff> entry :
        receiverBackoffMap.entrySet()) {
      final long probeDelayInMs = entry.getValue().getRemainingProbeDelayInMs();
      if (probeDelayInMs <= 0) {
        continue;
      }

      schedulingDelayMs.accumulateAndGet(probeDelayInMs, Math::max);
      throw createReceiverProbeDelayException(entry.getKey(), entry.getValue());
    }
  }

  private static PipeRuntimeSinkNonReportTimeConfigurableException
      createReceiverProbeDelayException(
          final String endPointKey, final ReceiverTemporaryUnavailableBackoff backoff) {
    return new PipeRuntimeSinkNonReportTimeConfigurableException(
        String.format(
            DataNodePipeMessages
                .EXCEPTION_RECEIVER_ARG_REMAINED_TEMPORARILY_UNAVAILABLE_FOR_MORE_THAN_ARG_MS_PAUSE_REGULAR_RETRIES_AND_PROBE_EVERY_ARG_MS_C515DD97,
            endPointKey,
            backoff.getRetryMaxDurationInMs(),
            backoff.getRetryProbeIntervalInMs()),
        Long.MAX_VALUE);
  }

  public void recordReceiverStatus(final TEndPoint endPoint, final TSStatus status) {
    final String endPointKey = format(endPoint);
    if (Objects.isNull(endPointKey) || Objects.isNull(status)) {
      return;
    }

    if (isReceiverTemporarilyUnavailable(status)) {
      final long backoffTimeInMs =
          receiverBackoffMap
              .computeIfAbsent(endPointKey, key -> new ReceiverTemporaryUnavailableBackoff())
              .markTemporarilyUnavailable();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            DataNodePipeMessages
                .MESSAGE_RECEIVER_ARG_IS_TEMPORARILY_UNAVAILABLE_THROTTLE_REQUESTS_FOR_ARG_MS_STATUS_ARG_F37192D9,
            endPointKey,
            backoffTimeInMs,
            status);
      }
    } else if (isSuccess(status)) {
      receiverBackoffMap.computeIfPresent(
          endPointKey,
          (key, backoff) -> {
            if (!backoff.shouldResetOnSuccess()) {
              return backoff;
            }
            backoff.markAvailable();
            return null;
          });
      if (receiverBackoffMap.isEmpty()) {
        schedulingDelayMs.set(0);
      }
    }
  }

  private static boolean isReceiverTemporarilyUnavailable(final TSStatus status) {
    if (Objects.isNull(status)) {
      return false;
    }

    final int statusCode = status.getCode();
    if (statusCode == TSStatusCode.PIPE_RECEIVER_TEMPORARY_UNAVAILABLE_EXCEPTION.getStatusCode()
        || statusCode == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode()) {
      return true;
    }

    return status.isSetSubStatus()
        && status.getSubStatus().stream()
            .anyMatch(IoTDBDataRegionAsyncSink::isReceiverTemporarilyUnavailable);
  }

  private static boolean isSuccess(final TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode();
  }

  private static String format(final TEndPoint endPoint) {
    return Objects.isNull(endPoint) ? null : UrlUtils.convertTEndPointIpv4AndIpv6Url(endPoint);
  }

  //////////////////////////// Operations for close ////////////////////////////

  @Override
  public synchronized void discardEventsOfPipe(
      final String pipeNameToDrop, final long creationTimeToDrop, final int regionId) {
    discardEventsOfPipe(new CommitterKey(pipeNameToDrop, creationTimeToDrop, regionId, -1));
  }

  @Override
  public synchronized void discardEventsOfPipe(final CommitterKey committerKey) {
    droppedPipeTaskKeys.add(committerKey);

    if (isTabletBatchModeEnabled && Objects.nonNull(tabletBatchBuilder)) {
      tabletBatchBuilder.discardEventsOfPipe(committerKey);
    }
    retryEventQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent
              && isDroppedPipe((EnrichedEvent) event, committerKey)) {
            ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
            retryEventQueueEventCounter.decreaseEventCount(event);
            return true;
          }
          return false;
        });

    retryTsFileQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent
              && isDroppedPipe((EnrichedEvent) event, committerKey)) {
            ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
            retryEventQueueEventCounter.decreaseEventCount(event);
            return true;
          }
          return false;
        });
  }

  @Override
  // synchronized to avoid close connector when transfer event
  public synchronized void close() {
    isClosed.set(true);

    syncSink.close();

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }

    // ensure all on-the-fly handlers have been cleared
    if (hasPendingHandlers()) {
      ImmutableSet.copyOf(pendingHandlers.keySet())
          .forEach(
              handler -> {
                handler.clearEventsReferenceCount();
                eliminateHandler(handler, true);
              });
    }

    try {
      if (clientManager != null) {
        clientManager.close();
      }

      if (transferTsFileClientManager != null) {
        transferTsFileClientManager.close();
      }
    } catch (final Exception e) {
      LOGGER.warn(DataNodePipeMessages.FAILED_TO_CLOSE_CLIENT_MANAGER, e);
    }

    // clear reference count of events in retry queue after closing async client
    clearRetryEventsReferenceCount();
    droppedPipeTaskKeys.clear();
    receiverBackoffMap.clear();

    super.close();
  }

  public synchronized void clearRetryEventsReferenceCount() {
    while (!retryEventQueue.isEmpty() || !retryTsFileQueue.isEmpty()) {
      final Event event =
          retryTsFileQueue.isEmpty() ? retryEventQueue.poll() : retryTsFileQueue.poll();
      retryEventQueueEventCounter.decreaseEventCount(event);
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
      }
    }
  }

  //////////////////////// APIs provided for metric framework ////////////////////////

  public int getRetryEventQueueSize() {
    return retryEventQueue.size() + retryTsFileQueue.size();
  }

  public int getBatchSize() {
    return Objects.nonNull(tabletBatchBuilder) ? tabletBatchBuilder.size() : 0;
  }

  public int getPendingHandlersSize() {
    return pendingHandlers.size();
  }

  //////////////////////// APIs provided for PipeTransferTrackableHandler ////////////////////////

  public boolean isClosed() {
    return isClosed.get();
  }

  public void trackHandler(final PipeTransferTrackableHandler handler) {
    pendingHandlers.put(handler, handler);
  }

  public void eliminateHandler(
      final PipeTransferTrackableHandler handler, final boolean closeClient) {
    if (closeClient) {
      handler.closeClient();
    }
    handler.close();
    pendingHandlers.remove(handler);
  }

  public boolean hasPendingHandlers() {
    return !pendingHandlers.isEmpty();
  }

  public void setTransferTsFileCounter(AtomicInteger transferTsFileCounter) {
    this.transferTsFileCounter = transferTsFileCounter;
  }

  private boolean isDroppedPipe(final EnrichedEvent event) {
    return droppedPipeTaskKeys.stream().anyMatch(key -> isDroppedPipe(event, key));
  }

  private static boolean isDroppedPipe(final EnrichedEvent event, final CommitterKey committerKey) {
    return committerKey.getPipeName().equals(event.getPipeName())
        && committerKey.getCreationTime() == event.getCreationTime()
        && committerKey.getRegionId() == event.getRegionId()
        && (committerKey.getRestartTimes() < 0 || committerKey.equals(event.getCommitterKey()));
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

  private static class ReceiverTemporaryUnavailableBackoff {

    private final long maxBackoffTimeInMs =
        Math.max(0, PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalMaxMs());
    private final long initialBackoffTimeInMs =
        Math.min(
            Math.max(1, PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalInitMs()),
            maxBackoffTimeInMs);
    private final long retryMaxDurationInMs =
        PipeConfig.getInstance().getPipeAsyncSinkRetryMaxDurationMs();
    private final long retryProbeIntervalInMs =
        Math.max(1, PipeConfig.getInstance().getPipeAsyncSinkRetryProbeIntervalMs());

    private boolean active = false;
    private long firstUnavailableTimeInMs = 0;
    private long currentBackoffTimeInMs = initialBackoffTimeInMs;
    private long failureBackoffUntilInMs = 0;
    private long nextReservedRetryTimeInMs = 0;
    private long nextProbeTimeInMs = 0;

    private synchronized long markTemporarilyUnavailable() {
      final long currentTimeInMs = System.currentTimeMillis();
      if (!active) {
        active = true;
        firstUnavailableTimeInMs = currentTimeInMs;
        currentBackoffTimeInMs = initialBackoffTimeInMs;
        failureBackoffUntilInMs = 0;
        nextReservedRetryTimeInMs = 0;
        nextProbeTimeInMs = 0;
      }

      final long backoffTimeInMs = currentBackoffTimeInMs;
      failureBackoffUntilInMs =
          Math.max(failureBackoffUntilInMs, safeAdd(currentTimeInMs, backoffTimeInMs));
      nextReservedRetryTimeInMs = Math.max(nextReservedRetryTimeInMs, failureBackoffUntilInMs);
      currentBackoffTimeInMs = getNextBackoffTimeInMs(currentBackoffTimeInMs);
      return backoffTimeInMs;
    }

    private synchronized boolean isActive() {
      return active;
    }

    private synchronized boolean isRetryMaxDurationExceeded() {
      return active
          && retryMaxDurationInMs >= 0
          && System.currentTimeMillis() - firstUnavailableTimeInMs >= retryMaxDurationInMs;
    }

    private synchronized long reserveNextRetryTimeInMs() {
      final long currentTimeInMs = System.currentTimeMillis();
      final long retryTimeInMs =
          Math.max(currentTimeInMs, Math.max(failureBackoffUntilInMs, nextReservedRetryTimeInMs));
      nextReservedRetryTimeInMs = safeAdd(retryTimeInMs, currentBackoffTimeInMs);
      return retryTimeInMs;
    }

    private synchronized long tryAcquireProbeAndGetDelayInMs() {
      final long currentTimeInMs = System.currentTimeMillis();
      if (currentTimeInMs >= nextProbeTimeInMs) {
        nextProbeTimeInMs = safeAdd(currentTimeInMs, retryProbeIntervalInMs);
        return 0;
      }
      return nextProbeTimeInMs - currentTimeInMs;
    }

    private synchronized long getRemainingProbeDelayInMs() {
      return isRetryMaxDurationExceeded()
          ? Math.max(0, nextProbeTimeInMs - System.currentTimeMillis())
          : 0;
    }

    private synchronized boolean shouldResetOnSuccess() {
      return active
          && (isRetryMaxDurationExceeded()
              || failureBackoffUntilInMs - System.currentTimeMillis() <= 0);
    }

    private synchronized void markAvailable() {
      active = false;
    }

    private long getRetryMaxDurationInMs() {
      return retryMaxDurationInMs;
    }

    private long getRetryProbeIntervalInMs() {
      return retryProbeIntervalInMs;
    }

    private long getNextBackoffTimeInMs(final long currentBackoffTimeInMs) {
      if (currentBackoffTimeInMs <= 0 || currentBackoffTimeInMs >= maxBackoffTimeInMs) {
        return maxBackoffTimeInMs;
      }
      return currentBackoffTimeInMs >= maxBackoffTimeInMs - currentBackoffTimeInMs
          ? maxBackoffTimeInMs
          : currentBackoffTimeInMs << 1;
    }

    private static long safeAdd(final long left, final long right) {
      return left >= Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
  }
}
