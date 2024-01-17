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
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.iotdb.IoTDBConnector;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.builder.IoTDBThriftAsyncPipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletBatchEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletInsertNodeEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTabletRawEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.handler.PipeTransferTsFileInsertionEventHandler;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.sync.IoTDBThriftSyncConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.PriorityBlockingQueue;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_LEADER_CACHE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_BATCH_MODE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_LEADER_CACHE_ENABLE_KEY;

public class IoTDBThriftAsyncConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftAsyncConnector.class);

  private static final String THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT =
      "Failed to borrow client from client pool or exception occurred "
          + "when sending to receiver.";
  private static final String THRIFT_ERROR_FORMATTER_WITH_ENDPOINT =
      "Failed to borrow client from client pool or exception occurred "
          + "when sending to receiver %s:%s.";

  private IoTDBThriftAsyncClientManager clientManager;

  private final IoTDBThriftSyncConnector retryConnector = new IoTDBThriftSyncConnector();
  private final PriorityBlockingQueue<Event> retryEventQueue =
      new PriorityBlockingQueue<>(
          11,
          Comparator.comparing(
              e ->
                  // Non-enriched events will be put at the front of the queue,
                  // because they are more likely to be lost and need to be retried first.
                  e instanceof EnrichedEvent ? ((EnrichedEvent) e).getCommitId() : 0));

  private IoTDBThriftAsyncPipeTransferBatchReqBuilder tabletBatchBuilder;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
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
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    // Disable batch mode for retry connector, in case retry events are never sent again
    PipeParameters retryParameters = new PipeParameters(new HashMap<>(parameters.getAttribute()));
    retryParameters.getAttribute().put(SINK_IOTDB_BATCH_MODE_ENABLE_KEY, "false");
    retryParameters.getAttribute().put(CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY, "false");
    retryConnector.customize(retryParameters, configuration);

    clientManager =
        new IoTDBThriftAsyncClientManager(
            nodeUrls,
            parameters.getBooleanOrDefault(
                Arrays.asList(SINK_LEADER_CACHE_ENABLE_KEY, CONNECTOR_LEADER_CACHE_ENABLE_KEY),
                CONNECTOR_LEADER_CACHE_ENABLE_DEFAULT_VALUE));

    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new IoTDBThriftAsyncPipeTransferBatchReqBuilder(parameters);
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
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary();

    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftAsyncConnector only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Current event: {}.",
          tabletInsertionEvent);
      return;
    }

    if (((EnrichedEvent) tabletInsertionEvent).shouldParsePatternOrTime()) {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        transfer(
            ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent)
                .parseEventWithPatternOrTime());
      } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
        transfer(
            ((PipeRawTabletInsertionEvent) tabletInsertionEvent).parseEventWithPatternOrTime());
      }
      return;
    } else {
      // ignore raw tablet event with zero rows
      if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
        if (((PipeRawTabletInsertionEvent) tabletInsertionEvent).hasNoNeedParsingAndIsEmpty()) {
          return;
        }
      }
    }

    if (isTabletBatchModeEnabled) {
      if (tabletBatchBuilder.onEvent(tabletInsertionEvent)) {
        final PipeTransferTabletBatchEventHandler pipeTransferTabletBatchEventHandler =
            new PipeTransferTabletBatchEventHandler(tabletBatchBuilder, this);

        transfer(pipeTransferTabletBatchEventHandler);

        tabletBatchBuilder.onSuccess();
      }
    } else {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
            (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
        final InsertNode insertNode =
            pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible();
        final TPipeTransferReq pipeTransferReq =
            Objects.isNull(insertNode)
                ? PipeTransferTabletBinaryReq.toTPipeTransferReq(
                    pipeInsertNodeTabletInsertionEvent.getByteBuffer())
                : PipeTransferTabletInsertNodeReq.toTPipeTransferReq(insertNode);
        final PipeTransferTabletInsertNodeEventHandler pipeTransferInsertNodeReqHandler =
            new PipeTransferTabletInsertNodeEventHandler(
                pipeInsertNodeTabletInsertionEvent, pipeTransferReq, this);

        transfer(pipeTransferInsertNodeReqHandler);
      } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
        final PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent =
            (PipeRawTabletInsertionEvent) tabletInsertionEvent;
        final PipeTransferTabletRawReq pipeTransferTabletRawReq =
            PipeTransferTabletRawReq.toTPipeTransferReq(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.isAligned());
        final PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler =
            new PipeTransferTabletRawEventHandler(
                pipeRawTabletInsertionEvent, pipeTransferTabletRawReq, this);

        transfer(pipeTransferTabletReqHandler);
      }
    }
  }

  private void transfer(PipeTransferTabletBatchEventHandler pipeTransferTabletBatchEventHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient();
      pipeTransferTabletBatchEventHandler.transfer(client);
    } catch (Exception ex) {
      logOnClientException(client, ex);
      pipeTransferTabletBatchEventHandler.onError(ex);
    }
  }

  private void transfer(PipeTransferTabletInsertNodeEventHandler pipeTransferInsertNodeReqHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient();
      pipeTransferInsertNodeReqHandler.transfer(client);
    } catch (Exception ex) {
      logOnClientException(client, ex);
      pipeTransferInsertNodeReqHandler.onError(ex);
    }
  }

  private void transfer(PipeTransferTabletRawEventHandler pipeTransferTabletReqHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient();
      pipeTransferTabletReqHandler.transfer(client);
    } catch (Exception ex) {
      logOnClientException(client, ex);
      pipeTransferTabletReqHandler.onError(ex);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    transferQueuedEventsIfNecessary();
    transferBatchedEventsIfNecessary();

    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftAsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.",
          tsFileInsertionEvent);
      return;
    }

    final PipeTsFileInsertionEvent pipeTsFileInsertionEvent =
        (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    if (!pipeTsFileInsertionEvent.waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          pipeTsFileInsertionEvent.getTsFile());
      return;
    }

    if ((pipeTsFileInsertionEvent).shouldParsePatternOrTime()) {
      try {
        for (final TabletInsertionEvent event :
            pipeTsFileInsertionEvent.toTabletInsertionEvents()) {
          transfer(event);
        }
      } finally {
        pipeTsFileInsertionEvent.close();
      }
      return;
    }

    // Just in case. To avoid the case that exception occurred when constructing the handler.
    if (!pipeTsFileInsertionEvent.getTsFile().exists()) {
      throw new FileNotFoundException(pipeTsFileInsertionEvent.getTsFile().getAbsolutePath());
    }

    final PipeTransferTsFileInsertionEventHandler pipeTransferTsFileInsertionEventHandler =
        new PipeTransferTsFileInsertionEventHandler(pipeTsFileInsertionEvent, this);

    transfer(pipeTransferTsFileInsertionEventHandler);
  }

  private void transfer(
      PipeTransferTsFileInsertionEventHandler pipeTransferTsFileInsertionEventHandler) {
    AsyncPipeDataTransferServiceClient client = null;
    try {
      client = clientManager.borrowClient();
      pipeTransferTsFileInsertionEventHandler.transfer(client);
    } catch (Exception ex) {
      logOnClientException(client, ex);
      pipeTransferTsFileInsertionEventHandler.onError(ex);
    }
  }

  @Override
  public void transfer(Event event) throws Exception {
    transferQueuedEventsIfNecessary();
    transferBatchedEventsIfNecessary();

    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBThriftAsyncConnector does not support transferring generic event: {}.", event);
    }
  }

  //////////////////////////// Leader cache update ////////////////////////////

  public void updateLeaderCache(String deviceId, TEndPoint endPoint) {
    clientManager.updateLeaderCache(deviceId, endPoint);
  }

  //////////////////////////// Exception handlers ////////////////////////////

  private void logOnClientException(AsyncPipeDataTransferServiceClient client, Exception e) {
    if (client == null) {
      LOGGER.warn(THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT, e);
    } else {
      LOGGER.warn(
          String.format(THRIFT_ERROR_FORMATTER_WITH_ENDPOINT, client.getIp(), client.getPort()), e);
    }
  }

  /**
   * Transfer queued events which are waiting for retry.
   *
   * @throws Exception if an error occurs. The error will be handled by pipe framework, which will
   *     retry the event and mark the event as failure and stop the pipe if the retry times exceeds
   *     the threshold.
   * @see PipeConnector#transfer(Event) for more details.
   * @see PipeConnector#transfer(TabletInsertionEvent) for more details.
   * @see PipeConnector#transfer(TsFileInsertionEvent) for more details.
   */
  private synchronized void transferQueuedEventsIfNecessary() throws Exception {
    while (!retryEventQueue.isEmpty()) {
      final Event peekedEvent = retryEventQueue.peek();

      if (peekedEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        retryConnector.transfer((PipeInsertNodeTabletInsertionEvent) peekedEvent);
      } else if (peekedEvent instanceof PipeRawTabletInsertionEvent) {
        retryConnector.transfer((PipeRawTabletInsertionEvent) peekedEvent);
      } else if (peekedEvent instanceof PipeTsFileInsertionEvent) {
        retryConnector.transfer((PipeTsFileInsertionEvent) peekedEvent);
      } else {
        LOGGER.warn(
            "IoTDBThriftAsyncConnector does not support transfer generic event: {}.", peekedEvent);
      }

      if (peekedEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) peekedEvent)
            .decreaseReferenceCount(IoTDBThriftAsyncConnector.class.getName(), true);
      }

      final Event polledEvent = retryEventQueue.poll();
      if (polledEvent != peekedEvent) {
        LOGGER.error(
            "The event polled from the queue is not the same as the event peeked from the queue. "
                + "Peeked event: {}, polled event: {}.",
            peekedEvent,
            polledEvent);
      }
      if (polledEvent != null) {
        LOGGER.info("Polled event {} from retry queue.", polledEvent);
      }
    }
  }

  /** Try its best to commit data in order. Flush can also be a trigger to transfer batched data. */
  private void transferBatchedEventsIfNecessary() throws IOException {
    if (!isTabletBatchModeEnabled || tabletBatchBuilder.isEmpty()) {
      return;
    }

    transfer(new PipeTransferTabletBatchEventHandler(tabletBatchBuilder, this));

    tabletBatchBuilder.onSuccess();
  }

  /**
   * Add failure event to retry queue.
   *
   * @param event event to retry
   */
  public synchronized void addFailureEventToRetryQueue(Event event) {
    retryEventQueue.offer(event);

    LOGGER.info("Added event {} to retry queue.", event);
  }

  //////////////////////////// Operations for close ////////////////////////////

  /**
   * When a pipe is dropped, the connector maybe reused and will not be closed. So we just discard
   * its queued events in the output pipe connector.
   */
  public synchronized void discardEventsOfPipe(String pipeNameToDrop) {
    retryEventQueue.removeIf(
        event ->
            event instanceof EnrichedEvent
                && pipeNameToDrop.equals(((EnrichedEvent) event).getPipeName()));
  }

  @Override
  // synchronized to avoid close connector when transfer event
  public synchronized void close() throws Exception {
    retryConnector.close();

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getRetryEventQueueSize() {
    return retryEventQueue.size();
  }
}
