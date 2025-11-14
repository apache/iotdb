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
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSink;
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
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
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
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.exception.write.WriteProcessException;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_ENABLE_SEND_TSFILE_LIMIT;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_ENABLE_SEND_TSFILE_LIMIT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_ENABLE_SEND_TSFILE_LIMIT;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_LEADER_CACHE_ENABLE_KEY;

@TreeModel
@TableModel
public class IoTDBDataRegionAsyncSink extends IoTDBSink {

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
  private final Map<PipeTransferTrackableHandler, PipeTransferTrackableHandler> pendingHandlers =
      new ConcurrentHashMap<>();

  private boolean enableSendTsFileLimit;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);
    syncSink.validate(validator);

    final PipeParameters parameters = validator.getParameters();

    validator.validate(
        args -> !((boolean) args[0] || (boolean) args[1] || (boolean) args[2]),
        "Only 'iotdb-thrift-ssl-sink' supports SSL transmission currently.",
        parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false),
        parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY),
        parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY));
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
          "IoTDBThriftAsyncConnector only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
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
        }
      } catch (final Throwable t) {
        LOGGER.warn("Failed to transfer tsfile batch ({}).", dbTsFilePairs, t);
        if (eventsHadBeenAddedToRetryQueue.compareAndSet(false, true)) {
          addFailureEventsToRetryQueue(events);
        }
      }
    } else {
      LOGGER.warn(
          "Unsupported batch type {} when transferring tablet insertion event.", batch.getClass());
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
              : null;
      final TPipeTransferReq pipeTransferReq =
          compressIfNeeded(
              Objects.isNull(insertNode)
                  ? PipeTransferTabletBinaryReqV2.toTPipeTransferReq(
                      pipeInsertNodeTabletInsertionEvent.getByteBuffer(), databaseName)
                  : PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(insertNode, databaseName));
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
                      : null));
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
      pipeTransferTabletBatchEventHandler.transfer(client);
    } catch (final Exception ex) {
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
      pipeTransferInsertNodeReqHandler.transfer(client);
    } catch (final Exception ex) {
      logOnClientException(client, ex);
      pipeTransferInsertNodeReqHandler.onError(ex);
    }
  }

  private void transfer(
      final String deviceId, final PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient(deviceId);
      pipeTransferTabletReqHandler.transfer(client);
    } catch (final Exception ex) {
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
          "IoTDBThriftAsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.",
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
                  : null);

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
    CompletableFuture<Void> completableFuture =
        CompletableFuture.supplyAsync(
            () -> {
              AsyncPipeDataTransferServiceClient client = null;
              try {
                client = transferTsFileClientManager.borrowClient();
                pipeTransferTsFileHandler.transfer(transferTsFileClientManager, client);
              } catch (final Exception ex) {
                logOnClientException(client, ex);
                pipeTransferTsFileHandler.onError(ex);
              } finally {
                transferTsFileCounter.decrementAndGet();
              }
              return null;
            },
            transferTsFileClientManager.getExecutor());

    if (PipeConfig.getInstance().isTransferTsFileSync() || !isRealtimeFirst) {
      try {
        completableFuture.get();
      } catch (final Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          LOGGER.warn(
              "Transfer tsfile event {} asynchronously was interrupted.",
              pipeTransferTsFileHandler.getTsFile(),
              e);
        }

        pipeTransferTsFileHandler.onError(e);
        LOGGER.warn(
            "Failed to transfer tsfile event {} asynchronously.",
            pipeTransferTsFileHandler.getTsFile(),
            e);
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
          "IoTDBThriftAsyncConnector does not support transferring generic event: {}.", event);
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
    if (Objects.isNull(compressionTimer) && Objects.nonNull(attributeSortedString)) {
      compressionTimer =
          PipeDataRegionSinkMetrics.getInstance().getCompressionTimer(attributeSortedString);
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

  /**
   * Transfer queued {@link Event}s which are waiting for retry.
   *
   * @see PipeConnector#transfer(Event) for more details.
   * @see PipeConnector#transfer(TabletInsertionEvent) for more details.
   * @see PipeConnector#transfer(TsFileInsertionEvent) for more details.
   */
  private void transferQueuedEventsIfNecessary(final boolean forced) {
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
                "IoTDBThriftAsyncConnector does not support transfer generic event: {}.",
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
              "The event polled from the queue is not the same as the event peeked from the queue. "
                  + "Peeked event: {}, polled event: {}.",
              peekedEvent,
              polledEvent);
        }
        if (polledEvent != null && LOGGER.isDebugEnabled()) {
          LOGGER.debug("Polled event {} from retry queue.", polledEvent);
        }
      }

      // Stop retrying if the execution time exceeds the threshold for better realtime performance
      if (System.currentTimeMillis() - retryStartTime
          > PipeConfig.getInstance().getPipeAsyncConnectorMaxRetryExecutionTimeMsPerCall()) {
        if (retryEventQueueEventCounter.getTabletInsertionEventCount()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTabletEventQueueSize()
            && retryEventQueueEventCounter.getTsFileInsertionEventCount()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTsFileEventQueueSize()
            && retryEventQueue.size() + retryTsFileQueue.size()
                < PipeConfig.getInstance().getPipeAsyncSinkForcedRetryTotalEventQueueSize()) {
          return;
        }

        if (remainingEvents <= retryEventQueue.size() + retryTsFileQueue.size()) {
          throw new PipeException(
              "Failed to retry transferring events in the retry queue. Remaining events: "
                  + (retryEventQueue.size() + retryTsFileQueue.size())
                  + " (tablet events: "
                  + retryEventQueueEventCounter.getTabletInsertionEventCount()
                  + ", tsfile events: "
                  + retryEventQueueEventCounter.getTsFileInsertionEventCount()
                  + ").");
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
        addFailureEventToRetryQueue(tabletInsertionEvent);
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
        addFailureEventToRetryQueue(tabletInsertionEvent);
      }
    } catch (final Exception e) {
      if (tabletInsertionEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) tabletInsertionEvent)
            .decreaseReferenceCount(IoTDBDataRegionAsyncSink.class.getName(), false);
      }
      addFailureEventToRetryQueue(tabletInsertionEvent);
    }
  }

  private void retryTransfer(final PipeTsFileInsertionEvent tsFileInsertionEvent) {
    try {
      if (transferWithoutCheck(tsFileInsertionEvent)) {
        tsFileInsertionEvent.decreaseReferenceCount(
            IoTDBDataRegionAsyncSink.class.getName(), false);
      } else {
        addFailureEventToRetryQueue(tsFileInsertionEvent);
      }
    } catch (final Exception e) {
      tsFileInsertionEvent.decreaseReferenceCount(IoTDBDataRegionAsyncSink.class.getName(), false);
      addFailureEventToRetryQueue(tsFileInsertionEvent);
    }
  }

  /**
   * Add failure {@link Event} to retry queue.
   *
   * @param event {@link Event} to retry
   */
  @SuppressWarnings("java:S899")
  public void addFailureEventToRetryQueue(final Event event) {
    if (event instanceof EnrichedEvent && ((EnrichedEvent) event).isReleased()) {
      return;
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
      LOGGER.debug("Added event {} to retry queue.", event);
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
  public void addFailureEventsToRetryQueue(final Iterable<EnrichedEvent> events) {
    events.forEach(this::addFailureEventToRetryQueue);
  }

  public boolean isEnableSendTsFileLimit() {
    return enableSendTsFileLimit;
  }

  //////////////////////////// Operations for close ////////////////////////////

  @Override
  public synchronized void discardEventsOfPipe(final String pipeNameToDrop, final int regionId) {
    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder.discardEventsOfPipe(pipeNameToDrop, regionId);
    }
    retryEventQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent
              && pipeNameToDrop.equals(((EnrichedEvent) event).getPipeName())
              && regionId == ((EnrichedEvent) event).getRegionId()) {
            ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncSink.class.getName());
            retryEventQueueEventCounter.decreaseEventCount(event);
            return true;
          }
          return false;
        });

    retryTsFileQueue.removeIf(
        event -> {
          if (event instanceof EnrichedEvent
              && pipeNameToDrop.equals(((EnrichedEvent) event).getPipeName())
              && regionId == ((EnrichedEvent) event).getRegionId()) {
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
      LOGGER.warn("Failed to close client manager.", e);
    }

    // clear reference count of events in retry queue after closing async client
    clearRetryEventsReferenceCount();

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
