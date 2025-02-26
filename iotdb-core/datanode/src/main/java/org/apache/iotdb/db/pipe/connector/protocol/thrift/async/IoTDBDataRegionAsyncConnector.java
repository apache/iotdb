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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.async;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.pipe.connector.protocol.IoTDBConnector;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.agent.task.subtask.connector.PipeConnectorSubtask;
import org.apache.iotdb.db.pipe.connector.client.IoTDBDataNodeAsyncClientManager;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventBatch;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventPlainBatch;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.batch.PipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletBatchEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletInsertNodeEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletRawEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTrackableHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTsFileHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBDataRegionSyncConnector;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import com.google.common.collect.ImmutableSet;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_LEADER_CACHE_ENABLE_KEY;

@TreeModel
@TableModel
public class IoTDBDataRegionAsyncConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionAsyncConnector.class);

  private static final String THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT =
      "Failed to borrow client from client pool when sending to receiver.";
  private static final String THRIFT_ERROR_FORMATTER_WITH_ENDPOINT =
      "Exception occurred while sending to receiver %s:%s.";

  private final IoTDBDataRegionSyncConnector retryConnector = new IoTDBDataRegionSyncConnector();
  private final BlockingQueue<Event> retryEventQueue = new LinkedBlockingQueue<>();

  private IoTDBDataNodeAsyncClientManager clientManager;

  private PipeTransferBatchReqBuilder tabletBatchBuilder;

  // use these variables to prevent reference count leaks under some corner cases when closing
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final Map<PipeTransferTrackableHandler, PipeTransferTrackableHandler> pendingHandlers =
      new ConcurrentHashMap<>();

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);
    retryConnector.validate(validator);

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
    retryConnector.customize(parameters, configuration);

    final boolean useLeaderCache =
        !shouldSendToAllClients
            && parameters.getBooleanOrDefault(
                Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
                CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE);

    clientManager =
        new IoTDBDataNodeAsyncClientManager(
            nodeUrls,
            useLeaderCache,
            loadBalanceStrategy,
            username,
            password,
            shouldReceiverConvertOnTypeMismatch,
            loadTsFileStrategy,
            loadTsFileValidation,
            shouldMarkAsPipeRequest);

    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new PipeTransferBatchReqBuilder(parameters);
    }
  }

  @Override
  // Synchronized to avoid close connector when transfer event
  public synchronized void handshake() throws Exception {
    retryConnector.handshake();
  }

  @Override
  public void heartbeat() {
    retryConnector.heartbeat();
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary();

    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftAsyncConnector only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }

    if (isTabletBatchModeEnabled) {
      final Pair<TEndPoint, PipeTabletEventBatch> endPointAndBatch =
          tabletBatchBuilder.onEvent(tabletInsertionEvent);
      if (Objects.isNull(endPointAndBatch)) {
        return;
      }
      transferInBatchWithoutCheck(endPointAndBatch);
    } else {
      transferInEventWithoutCheck(tabletInsertionEvent);
    }
  }

  private void transferInBatchWithoutCheck(
      final Pair<TEndPoint, PipeTabletEventBatch> endPointAndBatch) throws Exception {
    final PipeTabletEventBatch batch = endPointAndBatch.getRight();

    if (batch instanceof PipeTabletEventPlainBatch) {
      if (shouldSendToAllClients) {
        final List<AsyncPipeDataTransferServiceClient> clients = clientManager.borrowAllClients();
        transferAllClients(
            clients,
            new PipeTransferTabletBatchEventHandler(
                (PipeTabletEventPlainBatch) batch, this, new AtomicInteger(clients.size())));
      } else {
        transfer(
            endPointAndBatch.getLeft(),
            new PipeTransferTabletBatchEventHandler(
                (PipeTabletEventPlainBatch) batch, this, new AtomicInteger(1)));
      }
    } else if (batch instanceof PipeTabletEventTsFileBatch) {
      if (shouldSendToAllClients) {
        transferAllClients((PipeTabletEventTsFileBatch) batch);
      } else {

        final PipeTabletEventTsFileBatch tsFileBatch = (PipeTabletEventTsFileBatch) batch;
        final List<Pair<String, File>> dbTsFilePairs = tsFileBatch.sealTsFiles();
        final Map<Pair<String, Long>, Double> pipe2WeightMap = tsFileBatch.deepCopyPipe2WeightMap();
        final List<EnrichedEvent> events = tsFileBatch.deepCopyEvents();
        final AtomicInteger eventsReferenceCount = new AtomicInteger(dbTsFilePairs.size());
        final AtomicBoolean eventsHadBeenAddedToRetryQueue = new AtomicBoolean(false);

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
      }
    } else {
      LOGGER.warn(
          "Unsupported batch type {} when transferring tablet insertion event.", batch.getClass());
    }

    endPointAndBatch.getRight().onSuccess();
  }

  private void transferInEventWithoutCheck(final TabletInsertionEvent tabletInsertionEvent)
      throws Exception {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
          (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
      // We increase the reference count for this event to determine if the event may be released.
      if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
          IoTDBDataRegionAsyncConnector.class.getName())) {
        return;
      }

      final InsertNode insertNode =
          pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
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

      if (shouldSendToAllClients) {
        final List<AsyncPipeDataTransferServiceClient> clients = clientManager.borrowAllClients();
        final PipeTransferTabletInsertNodeEventHandler pipeTransferInsertNodeReqHandler =
            new PipeTransferTabletInsertNodeEventHandler(
                pipeInsertNodeTabletInsertionEvent,
                pipeTransferReq,
                this,
                new AtomicInteger(clients.size()));
        transferAllClients(clients, pipeTransferInsertNodeReqHandler);
      } else {
        final PipeTransferTabletInsertNodeEventHandler pipeTransferInsertNodeReqHandler =
            new PipeTransferTabletInsertNodeEventHandler(
                pipeInsertNodeTabletInsertionEvent, pipeTransferReq, this, new AtomicInteger(1));
        transfer(
            // getDeviceId() may return null for InsertRowsNode
            pipeInsertNodeTabletInsertionEvent.getDeviceId(), pipeTransferInsertNodeReqHandler);
      }
    } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
      final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
          (PipeRawTabletInsertionEvent) tabletInsertionEvent;
      // We increase the reference count for this event to determine if the event may be released.
      if (!pipeRawTabletInsertionEvent.increaseReferenceCount(
          IoTDBDataRegionAsyncConnector.class.getName())) {
        return;
      }

      final TPipeTransferReq pipeTransferTabletRawReq =
          compressIfNeeded(
              PipeTransferTabletRawReqV2.toTPipeTransferReq(
                  pipeRawTabletInsertionEvent.convertToTablet(),
                  pipeRawTabletInsertionEvent.isAligned(),
                  pipeRawTabletInsertionEvent.isTableModelEvent()
                      ? pipeRawTabletInsertionEvent.getTableModelDatabaseName()
                      : null));
      if (shouldSendToAllClients) {
        final List<AsyncPipeDataTransferServiceClient> clients = clientManager.borrowAllClients();
        final PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler =
            new PipeTransferTabletRawEventHandler(
                pipeRawTabletInsertionEvent,
                pipeTransferTabletRawReq,
                this,
                new AtomicInteger(clients.size()));
        transferAllClients(clients, pipeTransferTabletReqHandler);
      } else {
        final PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler =
            new PipeTransferTabletRawEventHandler(
                pipeRawTabletInsertionEvent, pipeTransferTabletRawReq, this, new AtomicInteger(1));
        transfer(pipeRawTabletInsertionEvent.getDeviceId(), pipeTransferTabletReqHandler);
      }
    }
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

  private void transferAllClients(
      final List<AsyncPipeDataTransferServiceClient> clients,
      final PipeTransferTabletBatchEventHandler pipeTransferTabletBatchEventHandler) {

    for (final AsyncPipeDataTransferServiceClient client : clients) {
      try {
        pipeTransferTabletBatchEventHandler.transfer(client);
      } catch (final Exception ex) {
        logOnClientException(client, ex);
        pipeTransferTabletBatchEventHandler.onError(ex);
      }
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

  private void transferAllClients(
      final List<AsyncPipeDataTransferServiceClient> clients,
      final PipeTransferTabletInsertNodeEventHandler pipeTransferInsertNodeReqHandler) {
    for (final AsyncPipeDataTransferServiceClient client : clients) {
      try {
        pipeTransferInsertNodeReqHandler.transfer(client);
      } catch (final Exception ex) {
        logOnClientException(client, ex);
        pipeTransferInsertNodeReqHandler.onError(ex);
      }
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

  private void transferAllClients(
      final List<AsyncPipeDataTransferServiceClient> clients,
      final PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler) {
    for (final AsyncPipeDataTransferServiceClient client : clients) {
      try {
        pipeTransferTabletReqHandler.transfer(client);
      } catch (final Exception ex) {
        logOnClientException(client, ex);
        pipeTransferTabletReqHandler.onError(ex);
      }
    }
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary();
    transferBatchedEventsIfNecessary();

    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftAsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.",
          tsFileInsertionEvent);
      return;
    }

    transferWithoutCheck(tsFileInsertionEvent);
  }

  private void transferWithoutCheck(final TsFileInsertionEvent tsFileInsertionEvent)
      throws Exception {
    final PipeTsFileInsertionEvent pipeTsFileInsertionEvent =
        (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeTsFileInsertionEvent.increaseReferenceCount(
        IoTDBDataRegionAsyncConnector.class.getName())) {
      return;
    }

    // We assume that no exceptions will be thrown after reference count is increased.
    try {
      // Just in case. To avoid the case that exception occurred when constructing the handler.
      if (!pipeTsFileInsertionEvent.getTsFile().exists()) {
        throw new FileNotFoundException(pipeTsFileInsertionEvent.getTsFile().getAbsolutePath());
      }

      if (shouldSendToAllClients) {
        transferAllClients(pipeTsFileInsertionEvent);
      } else {
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
      }
    } catch (final Exception e) {
      // Just in case. To avoid the case that exception occurred when constructing the handler.
      pipeTsFileInsertionEvent.decreaseReferenceCount(
          IoTDBDataRegionAsyncConnector.class.getName(), false);
      throw e;
    }
  }

  private void transfer(final PipeTransferTsFileHandler pipeTransferTsFileHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient();
      pipeTransferTsFileHandler.transfer(clientManager, client);
    } catch (final Exception ex) {
      logOnClientException(client, ex);
      pipeTransferTsFileHandler.onError(ex);
    }
  }

  private void transferAllClients(final PipeTabletEventTsFileBatch tsFileBatch) throws Exception {
    final List<AsyncPipeDataTransferServiceClient> clients = clientManager.borrowAllClients();
    final List<Pair<String, File>> dbTsFilePairs = tsFileBatch.sealTsFiles();
    final Map<Pair<String, Long>, Double> pipe2WeightMap = tsFileBatch.deepCopyPipe2WeightMap();
    final List<EnrichedEvent> events = tsFileBatch.deepCopyEvents();
    final AtomicInteger eventsReferenceCount =
        new AtomicInteger(dbTsFilePairs.size() * clients.size());
    final AtomicBoolean eventsHadBeenAddedToRetryQueue = new AtomicBoolean(false);
    for (final Pair<String, File> sealedFile : dbTsFilePairs) {
      final PipeTransferTsFileHandler pipeTransferTsFileHandler =
          new PipeTransferTsFileHandler(
              this,
              pipe2WeightMap,
              events,
              eventsReferenceCount,
              eventsHadBeenAddedToRetryQueue,
              sealedFile.right,
              null,
              false,
              sealedFile.left);
      pipeTransferTsFileHandler.transfer(clientManager, clients);
    }
  }

  private void transferAllClients(final PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws Exception {
    final List<AsyncPipeDataTransferServiceClient> clients = clientManager.borrowAllClients();
    final AtomicInteger eventsReferenceCount = new AtomicInteger(clients.size());
    final PipeTransferTsFileHandler pipeTransferTsFileHandler =
        new PipeTransferTsFileHandler(
            this,
            Collections.singletonMap(
                new Pair<>(
                    pipeTsFileInsertionEvent.getPipeName(),
                    pipeTsFileInsertionEvent.getCreationTime()),
                1.0),
            Collections.singletonList(pipeTsFileInsertionEvent),
            eventsReferenceCount,
            new AtomicBoolean(false),
            pipeTsFileInsertionEvent.getTsFile(),
            pipeTsFileInsertionEvent.getModFile(),
            pipeTsFileInsertionEvent.isWithMod() && clientManager.supportModsIfIsDataNodeReceiver(),
            pipeTsFileInsertionEvent.isTableModelEvent()
                ? pipeTsFileInsertionEvent.getTableModelDatabaseName()
                : null);

    pipeTransferTsFileHandler.transfer(clientManager, clients);
  }

  @Override
  public void transfer(final Event event) throws Exception {
    transferQueuedEventsIfNecessary();
    transferBatchedEventsIfNecessary();

    if (!(event instanceof PipeHeartbeatEvent
        || event instanceof PipeDeleteDataNodeEvent
        || event instanceof PipeTerminateEvent)) {
      LOGGER.warn(
          "IoTDBThriftAsyncConnector does not support transferring generic event: {}.", event);
      return;
    }

    retryConnector.transfer(event);
  }

  //////////////////////////// Leader cache update ////////////////////////////

  public void updateLeaderCache(final String deviceId, final TEndPoint endPoint) {
    clientManager.updateLeaderCache(deviceId, endPoint);
  }

  //////////////////////////// Exception handlers ////////////////////////////

  private void logOnClientException(
      final AsyncPipeDataTransferServiceClient client, final Exception e) {
    if (client == null) {
      LOGGER.warn(THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT);
    } else {
      client.resetMethodStateIfStopped();
      LOGGER.warn(
          String.format(THRIFT_ERROR_FORMATTER_WITH_ENDPOINT, client.getIp(), client.getPort()), e);
    }
  }

  /**
   * Transfer queued {@link Event}s which are waiting for retry.
   *
   * @throws Exception if an error occurs. The error will be handled by pipe framework, which will
   *     retry the {@link Event} and mark the {@link Event} as failure and stop the pipe if the
   *     retry times exceeds the threshold.
   * @see PipeConnector#transfer(Event) for more details.
   * @see PipeConnector#transfer(TabletInsertionEvent) for more details.
   * @see PipeConnector#transfer(TsFileInsertionEvent) for more details.
   */
  private void transferQueuedEventsIfNecessary() throws Exception {
    while (!retryEventQueue.isEmpty()) {
      synchronized (this) {
        if (isClosed.get() || retryEventQueue.isEmpty()) {
          return;
        }

        final Event peekedEvent = retryEventQueue.peek();

        if (peekedEvent instanceof PipeInsertNodeTabletInsertionEvent) {
          retryConnector.transfer((PipeInsertNodeTabletInsertionEvent) peekedEvent);
        } else if (peekedEvent instanceof PipeRawTabletInsertionEvent) {
          retryConnector.transfer((PipeRawTabletInsertionEvent) peekedEvent);
        } else if (peekedEvent instanceof PipeTsFileInsertionEvent) {
          retryConnector.transfer((PipeTsFileInsertionEvent) peekedEvent);
        } else {
          LOGGER.warn(
              "IoTDBThriftAsyncConnector does not support transfer generic event: {}.",
              peekedEvent);
        }

        if (peekedEvent instanceof EnrichedEvent) {
          ((EnrichedEvent) peekedEvent)
              .decreaseReferenceCount(IoTDBDataRegionAsyncConnector.class.getName(), true);
        }

        final Event polledEvent = retryEventQueue.poll();
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
    }

    // Trigger cron heartbeat event in retry connector to send batch in time
    retryConnector.transfer(PipeConnectorSubtask.CRON_HEARTBEAT_EVENT);
  }

  /** Try its best to commit data in order. Flush can also be a trigger to transfer batched data. */
  private void transferBatchedEventsIfNecessary() throws Exception {
    if (!isTabletBatchModeEnabled || tabletBatchBuilder.isEmpty()) {
      return;
    }

    for (final Pair<TEndPoint, PipeTabletEventBatch> endPointAndBatch :
        tabletBatchBuilder.getAllNonEmptyBatches()) {
      transferInBatchWithoutCheck(endPointAndBatch);
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
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncConnector.class.getName());
      }
      return;
    }

    retryEventQueue.offer(event);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Added event {} to retry queue.", event);
    }

    if (isClosed.get()) {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncConnector.class.getName());
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

  public synchronized void clearRetryEventsReferenceCount() {
    while (!retryEventQueue.isEmpty()) {
      final Event event = retryEventQueue.poll();
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).clearReferenceCount(IoTDBDataRegionAsyncConnector.class.getName());
      }
    }
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
            ((EnrichedEvent) event)
                .clearReferenceCount(IoTDBDataRegionAsyncConnector.class.getName());
            return true;
          }
          return false;
        });
  }

  @Override
  // synchronized to avoid close connector when transfer event
  public synchronized void close() {
    isClosed.set(true);

    retryConnector.close();

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }

    // ensure all on-the-fly handlers have been cleared
    if (hasPendingHandlers()) {
      ImmutableSet.copyOf(pendingHandlers.keySet())
          .forEach(
              handler -> {
                handler.clearEventsReferenceCount();
                eliminateHandler(handler);
              });
    }

    try {
      if (clientManager != null) {
        clientManager.close();
      }
    } catch (final Exception e) {
      LOGGER.warn("Failed to close client manager.", e);
    }

    // clear reference count of events in retry queue after closing async client
    clearRetryEventsReferenceCount();

    super.close();
  }

  //////////////////////// APIs provided for metric framework ////////////////////////

  public int getRetryEventQueueSize() {
    return retryEventQueue.size();
  }

  // For performance, this will not acquire lock and does not guarantee the correct
  // result. However, this shall not cause any exceptions when concurrently read & written.
  public int getRetryEventCount(final String pipeName) {
    final AtomicInteger count = new AtomicInteger(0);
    try {
      retryEventQueue.forEach(
          event -> {
            if (event instanceof EnrichedEvent
                && pipeName.equals(((EnrichedEvent) event).getPipeName())) {
              count.incrementAndGet();
            }
          });
      return count.get();
    } catch (final Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Failed to get retry event count for pipe {}.", pipeName, e);
      }
      return count.get();
    }
  }

  //////////////////////// APIs provided for PipeTransferTrackableHandler ////////////////////////

  public boolean isClosed() {
    return isClosed.get();
  }

  public void trackHandler(final PipeTransferTrackableHandler handler) {
    pendingHandlers.put(handler, handler);
  }

  public void eliminateHandler(final PipeTransferTrackableHandler handler) {
    handler.close();
    pendingHandlers.remove(handler);
  }

  public boolean hasPendingHandlers() {
    return !pendingHandlers.isEmpty();
  }
}
