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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncIoTConsensusV2ServiceClient;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.iotv2.container.IoTV2GlobalComponentContainer;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkRetryTimesConfigurableException;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSink;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TCommitId;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferReq;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeSink;
import org.apache.iotdb.consensus.pipe.metric.IoTConsensusV2SyncLagManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.ReplicateProgressDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2SinkMetrics;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler.IoTConsensusV2DeleteEventHandler;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler.IoTConsensusV2TabletBatchEventHandler;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler.IoTConsensusV2TabletInsertNodeEventHandler;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler.IoTConsensusV2TsFileInsertionEventHandler;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.builder.IoTConsensusV2AsyncBatchReqBuilder;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2DeleteNodeReq;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request.IoTConsensusV2TabletInsertNodeReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_CONSENSUS_GROUP_ID_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_CONSENSUS_PIPE_NAME;

// TODO: Optimize the network and disk io for TsFile onComplete
// TODO: support Tablet Batch
@TreeModel
@TableModel
public class IoTConsensusV2AsyncSink extends IoTDBSink implements ConsensusPipeSink {
  private static final Logger LOGGER = LoggerFactory.getLogger(IoTConsensusV2AsyncSink.class);
  private static final String ENQUEUE_EXCEPTION_MSG =
      "Timeout: IoTConsensusV2Connector offers an event into transferBuffer failed, because transferBuffer is full.";
  private static final String THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT =
      "Failed to borrow client from client pool or exception occurred "
          + "when sending to receiver.";
  private static final String THRIFT_ERROR_FORMATTER_WITH_ENDPOINT =
      "Failed to borrow client from client pool or exception occurred "
          + "when sending to receiver %s:%s.";
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static final long IOT_CONSENSUS_V2_EVENT_ENQUEUE_TIMEOUT_IN_MS =
      IOTDB_CONFIG.getConnectionTimeoutInMS() / 6;
  private final Queue<EnrichedEvent> retryEventQueue =
      new PriorityBlockingQueue<>(
          IOTDB_CONFIG.getIotConsensusV2PipelineSize(),
          Comparator.comparingLong(EnrichedEvent::getReplicateIndexForIoTV2));
  // We use enrichedEvent here to make use of EnrichedEvent.equalsInIoTConsensusV2
  private final BlockingQueue<EnrichedEvent> transferBuffer =
      new LinkedBlockingDeque<>(IOTDB_CONFIG.getIotConsensusV2PipelineSize());
  private ScheduledExecutorService backgroundTaskService;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final int thisDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  private IoTConsensusV2SinkMetrics iotConsensusV2SinkMetrics;
  private String consensusPipeName;
  private int consensusGroupId;
  private IoTConsensusV2SyncSink retryConnector;
  private IClientManager<TEndPoint, AsyncIoTConsensusV2ServiceClient> asyncTransferClientManager;
  private IoTConsensusV2AsyncBatchReqBuilder tabletBatchBuilder;
  private volatile long currentReplicateProgress = 0;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);
    // validate consensus pipe's parameters
    final PipeParameters parameters = validator.getParameters();
    validator.validate(
        args -> (boolean) args[0] || (boolean) args[1],
        String.format(
            "One of %s, %s must be specified in consensus pipe",
            CONNECTOR_CONSENSUS_GROUP_ID_KEY, CONNECTOR_CONSENSUS_PIPE_NAME),
        parameters.hasAttribute(CONNECTOR_CONSENSUS_GROUP_ID_KEY),
        parameters.hasAttribute(CONNECTOR_CONSENSUS_PIPE_NAME));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    // Get consensusGroupId from parameters passed by IoTConsensusV2Impl
    consensusGroupId = parameters.getInt(CONNECTOR_CONSENSUS_GROUP_ID_KEY);
    // Get consensusPipeName from parameters passed by IoTConsensusV2Impl
    consensusPipeName = parameters.getString(CONNECTOR_CONSENSUS_PIPE_NAME);

    // initialize metric components
    iotConsensusV2SinkMetrics = new IoTConsensusV2SinkMetrics(this);
    IoTConsensusV2SyncLagManager.getInstance(getConsensusGroupIdStr())
        .addConsensusPipeConnector(new ConsensusPipeName(consensusPipeName), this);
    MetricService.getInstance().addMetricSet(this.iotConsensusV2SinkMetrics);

    // In IoTConsensusV2, one iotConsensusV2Task corresponds to a iotConsensusV2Connector. Thus,
    // `nodeUrls` here actually is a singletonList that contains one peer's TEndPoint. But here we
    // retain the implementation of list to cope with possible future expansion
    retryConnector =
        new IoTConsensusV2SyncSink(
            nodeUrls, consensusGroupId, thisDataNodeId, iotConsensusV2SinkMetrics);
    retryConnector.customize(parameters, configuration);
    asyncTransferClientManager =
        IoTV2GlobalComponentContainer.getInstance().getGlobalAsyncClientManager();

    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder =
          new IoTConsensusV2AsyncBatchReqBuilder(
              parameters,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, consensusGroupId),
              thisDataNodeId);
    }

    // currently, tablet batch is false by default in IoTConsensusV2;
    isTabletBatchModeEnabled = false;
    this.backgroundTaskService =
        IoTV2GlobalComponentContainer.getInstance().getBackgroundTaskService();
  }

  /**
   * Add an event to transferBuffer, whose events will be asynchronously transferred to receiver.
   */
  private boolean addEvent2Buffer(EnrichedEvent event) {
    try {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "IoTConsensusV2-ConsensusGroup-{}: no.{} event-{} added to connector buffer",
            consensusGroupId,
            event.getReplicateIndexForIoTV2(),
            event);
      }
      // Special judge to avoid transfer stuck when re-transfer events that will not be put in
      // retryQueue.
      if (transferBuffer.contains(event)) {
        return true;
      }
      long currentTime = System.nanoTime();
      boolean result =
          transferBuffer.offer(
              event, IOT_CONSENSUS_V2_EVENT_ENQUEUE_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
      long duration = System.nanoTime() - currentTime;
      iotConsensusV2SinkMetrics.recordConnectorEnqueueTimer(duration);
      // add reference
      if (result) {
        event.increaseReferenceCount(IoTConsensusV2AsyncSink.class.getName());
      }
      // if connector is closed when executing this method, need to clear this event's reference
      // count to avoid unnecessarily pinning some resource such as WAL.
      if (isClosed.get()) {
        event.clearReferenceCount(IoTConsensusV2AsyncSink.class.getName());
      }
      return result;
    } catch (InterruptedException e) {
      LOGGER.info("IoTConsensusV2Connector transferBuffer queue offer is interrupted.", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * if one event is successfully processed by receiver in IoTConsensusV2, we will remove this event
   * from transferBuffer in order to transfer other event.
   */
  public synchronized void removeEventFromBuffer(EnrichedEvent event) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "IoTConsensusV2-ConsensusGroup-{}: one event-{} successfully received by the follower, will be removed from queue, queue size = {}, limit size = {}",
          consensusGroupId,
          event,
          transferBuffer.size(),
          IOTDB_CONFIG.getIotConsensusV2PipelineSize());
    }
    if (transferBuffer.isEmpty()) {
      LOGGER.info(
          "IoTConsensusV2-ConsensusGroup-{}: try to remove event-{} after iotConsensusV2AsyncConnector being closed. Ignore it.",
          consensusGroupId,
          event);
      return;
    }
    Iterator<EnrichedEvent> iterator = transferBuffer.iterator();
    EnrichedEvent current = iterator.next();
    while (!current.equalsInIoTConsensusV2(event) && iterator.hasNext()) {
      current = iterator.next();
    }
    if (current.equalsInIoTConsensusV2(event)) {
      iterator.remove();
    } else {
      LOGGER.warn(
          "IoTConsensusV2-ConsensusGroup-{}: event-{} not found in transferBuffer, skip removing. queue size = {}",
          consensusGroupId,
          event,
          transferBuffer.size());
    }
    // update replicate progress
    currentReplicateProgress =
        Math.max(currentReplicateProgress, event.getReplicateIndexForIoTV2());
    // decrease reference count
    event.decreaseReferenceCount(IoTConsensusV2AsyncSink.class.getName(), true);
  }

  @Override
  public void handshake() throws Exception {
    // do nothing
    // IoTConsensusV2 doesn't need to do handshake, since nodes in same consensusGroup/cluster
    // usually have same configuration.
  }

  @Override
  public void heartbeat() throws Exception {
    // do nothing
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    asyncTransferQueuedEventsIfNecessary();

    boolean enqueueResult = addEvent2Buffer((EnrichedEvent) tabletInsertionEvent);
    if (!enqueueResult) {
      throw new PipeRuntimeSinkRetryTimesConfigurableException(
          ENQUEUE_EXCEPTION_MSG, Integer.MAX_VALUE);
    }
    // batch transfer tablets.
    if (isTabletBatchModeEnabled) {
      if (tabletBatchBuilder.onEvent(tabletInsertionEvent)) {
        final IoTConsensusV2TabletBatchEventHandler iotConsensusV2TabletBatchEventHandler =
            new IoTConsensusV2TabletBatchEventHandler(
                tabletBatchBuilder, this, iotConsensusV2SinkMetrics);

        transfer(iotConsensusV2TabletBatchEventHandler);

        tabletBatchBuilder.onSuccess();
      }
    } else {
      transferInEventWithoutCheck((PipeInsertionEvent) tabletInsertionEvent);
    }
  }

  private void transfer(
      final IoTConsensusV2TabletBatchEventHandler iotConsensusV2TabletBatchEventHandler) {
    AsyncIoTConsensusV2ServiceClient client = null;
    try {
      client = asyncTransferClientManager.borrowClient(getFollowerUrl());
      iotConsensusV2TabletBatchEventHandler.transfer(client);
    } catch (final Exception ex) {
      logOnClientException(client, ex);
      iotConsensusV2TabletBatchEventHandler.onError(ex);
    }
  }

  private boolean transferInEventWithoutCheck(PipeInsertionEvent tabletInsertionEvent)
      throws Exception {
    // tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
    final PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent =
        (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent;
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeInsertNodeTabletInsertionEvent.increaseReferenceCount(
        IoTConsensusV2AsyncSink.class.getName())) {
      return false;
    }

    TCommitId tCommitId;
    TConsensusGroupId tConsensusGroupId =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, consensusGroupId);
    tCommitId =
        new TCommitId(
            pipeInsertNodeTabletInsertionEvent.getReplicateIndexForIoTV2(),
            pipeInsertNodeTabletInsertionEvent.getCommitterKey().getRestartTimes(),
            pipeInsertNodeTabletInsertionEvent.getRebootTimes());

    final InsertNode insertNode = pipeInsertNodeTabletInsertionEvent.getInsertNode();
    final ProgressIndex progressIndex = pipeInsertNodeTabletInsertionEvent.getProgressIndex();
    final TIoTConsensusV2TransferReq iotConsensusV2TransferReq =
        IoTConsensusV2TabletInsertNodeReq.toTIoTConsensusV2TransferReq(
            insertNode, tCommitId, tConsensusGroupId, progressIndex, thisDataNodeId);
    final IoTConsensusV2TabletInsertNodeEventHandler iotConsensusV2InsertNodeReqHandler =
        new IoTConsensusV2TabletInsertNodeEventHandler(
            pipeInsertNodeTabletInsertionEvent,
            iotConsensusV2TransferReq,
            this,
            iotConsensusV2SinkMetrics);

    transfer(iotConsensusV2InsertNodeReqHandler);
    return true;
  }

  private void transfer(
      final IoTConsensusV2TabletInsertNodeEventHandler iotConsensusV2InsertNodeReqHandler) {
    AsyncIoTConsensusV2ServiceClient client = null;
    try {
      client = asyncTransferClientManager.borrowClient(getFollowerUrl());
      iotConsensusV2InsertNodeReqHandler.transfer(client);
    } catch (final Exception ex) {
      logOnClientException(client, ex);
      iotConsensusV2InsertNodeReqHandler.onError(ex);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    asyncTransferQueuedEventsIfNecessary();
    transferBatchedEventsIfNecessary();

    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTConsensusV2AsyncConnector only support PipeTsFileInsertionEvent. Current event: {}.",
          tsFileInsertionEvent);
      return;
    }

    boolean enqueueResult = addEvent2Buffer((EnrichedEvent) tsFileInsertionEvent);
    if (!enqueueResult) {
      throw new PipeRuntimeSinkRetryTimesConfigurableException(
          ENQUEUE_EXCEPTION_MSG, Integer.MAX_VALUE);
    }

    transferWithoutCheck(tsFileInsertionEvent);
  }

  private boolean transferWithoutCheck(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    final PipeTsFileInsertionEvent pipeTsFileInsertionEvent =
        (PipeTsFileInsertionEvent) tsFileInsertionEvent;
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeTsFileInsertionEvent.increaseReferenceCount(IoTConsensusV2AsyncSink.class.getName())) {
      return false;
    }

    TCommitId tCommitId =
        new TCommitId(
            pipeTsFileInsertionEvent.getReplicateIndexForIoTV2(),
            pipeTsFileInsertionEvent.getCommitterKey().getRestartTimes(),
            pipeTsFileInsertionEvent.getRebootTimes());
    TConsensusGroupId tConsensusGroupId =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, consensusGroupId);

    try {
      // Just in case. To avoid the case that exception occurred when constructing the handler.
      if (!pipeTsFileInsertionEvent.getTsFile().exists()) {
        throw new FileNotFoundException(pipeTsFileInsertionEvent.getTsFile().getAbsolutePath());
      }

      final IoTConsensusV2TsFileInsertionEventHandler iotConsensusV2TsFileInsertionEventHandler =
          new IoTConsensusV2TsFileInsertionEventHandler(
              pipeTsFileInsertionEvent,
              this,
              tCommitId,
              tConsensusGroupId,
              consensusPipeName,
              thisDataNodeId,
              iotConsensusV2SinkMetrics);

      transfer(iotConsensusV2TsFileInsertionEventHandler);
      return true;
    } catch (Exception e) {
      // Just in case. To avoid the case that exception occurred when constructing the handler.
      pipeTsFileInsertionEvent.decreaseReferenceCount(
          IoTConsensusV2AsyncSink.class.getName(), false);
      throw e;
    }
  }

  private void transfer(
      final IoTConsensusV2TsFileInsertionEventHandler iotConsensusV2TsFileInsertionEventHandler) {
    AsyncIoTConsensusV2ServiceClient client = null;
    try {
      client = asyncTransferClientManager.borrowClient(getFollowerUrl());
      iotConsensusV2TsFileInsertionEventHandler.transfer(client);
    } catch (final Exception ex) {
      logOnClientException(client, ex);
      iotConsensusV2TsFileInsertionEventHandler.onError(ex);
    }
  }

  /**
   * IoTConsensusV2 only need transfer heartbeat event here. And heartbeat event doesn't need to be
   * added to transferBuffer.
   */
  @Override
  public void transfer(Event event) throws Exception {
    asyncTransferQueuedEventsIfNecessary();
    transferBatchedEventsIfNecessary();

    // Transfer deletion
    if (event instanceof PipeDeleteDataNodeEvent) {
      final PipeDeleteDataNodeEvent deleteDataNodeEvent = (PipeDeleteDataNodeEvent) event;
      final boolean enqueueResult = addEvent2Buffer(deleteDataNodeEvent);
      if (!enqueueResult) {
        throw new PipeRuntimeSinkRetryTimesConfigurableException(
            ENQUEUE_EXCEPTION_MSG, Integer.MAX_VALUE);
      }

      transferDeletion(deleteDataNodeEvent);
      return;
    }

    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTConsensusV2AsyncConnector does not support transferring generic event: {}.", event);
    }
  }

  private boolean transferDeletion(PipeDeleteDataNodeEvent pipeDeleteDataNodeEvent) {
    // We increase the reference count for this event to determine if the event may be released.
    if (!pipeDeleteDataNodeEvent.increaseReferenceCount(IoTConsensusV2SyncSink.class.getName())) {
      return false;
    }

    final ProgressIndex progressIndex = pipeDeleteDataNodeEvent.getProgressIndex();
    final TCommitId tCommitId =
        new TCommitId(
            pipeDeleteDataNodeEvent.getReplicateIndexForIoTV2(),
            pipeDeleteDataNodeEvent.getCommitterKey().getRestartTimes(),
            pipeDeleteDataNodeEvent.getRebootTimes());
    final TConsensusGroupId tConsensusGroupId =
        new TConsensusGroupId(TConsensusGroupType.DataRegion, consensusGroupId);

    final TIoTConsensusV2TransferReq iotConsensusV2TransferReq =
        IoTConsensusV2DeleteNodeReq.toTIoTConsensusV2TransferReq(
            pipeDeleteDataNodeEvent.getDeleteDataNode(),
            tCommitId,
            tConsensusGroupId,
            progressIndex,
            thisDataNodeId);
    final IoTConsensusV2DeleteEventHandler iotConsensusV2DeleteEventHandler =
        new IoTConsensusV2DeleteEventHandler(
            pipeDeleteDataNodeEvent, iotConsensusV2TransferReq, this, iotConsensusV2SinkMetrics);

    transfer(iotConsensusV2DeleteEventHandler);
    return true;
  }

  private void transfer(final IoTConsensusV2DeleteEventHandler iotConsensusV2DeleteEventHandler) {
    AsyncIoTConsensusV2ServiceClient client = null;
    try {
      client = asyncTransferClientManager.borrowClient(getFollowerUrl());
      iotConsensusV2DeleteEventHandler.transfer(client);
    } catch (final Exception ex) {
      logOnClientException(client, ex);
      iotConsensusV2DeleteEventHandler.onError(ex);
    }
  }

  /** Try its best to commit data in order. Flush can also be a trigger to transfer batched data. */
  private void transferBatchedEventsIfNecessary() throws IOException {
    if (!isTabletBatchModeEnabled || tabletBatchBuilder.isEmpty()) {
      return;
    }

    transfer(
        new IoTConsensusV2TabletBatchEventHandler(
            tabletBatchBuilder, this, iotConsensusV2SinkMetrics));
    tabletBatchBuilder.onSuccess();
  }

  /** Transfer queued {@link Event}s which are waiting for retry. */
  private void asyncTransferQueuedEventsIfNecessary() {
    long retryStartTime = System.currentTimeMillis();
    while (!retryEventQueue.isEmpty()) {
      synchronized (this) {
        if (isClosed.get() || retryEventQueue.isEmpty()) {
          return;
        }
        if (System.currentTimeMillis() - retryStartTime > TimeUnit.SECONDS.toMillis(20)) {
          // just in case that some events are polled and re-added into queue again and again,
          // causing this loop to run forever.
          LOGGER.warn(
              "IoTConsensusV2-ConsensusGroup-{}: retryEventQueue is not empty after 20 seconds. retryQueue size: {}",
              consensusGroupId,
              retryEventQueue.size());
          return;
        }

        // remove this event from queue. If retry fail as well, event will be re-added into
        // retryQueue.
        final EnrichedEvent peekedEvent = retryEventQueue.poll();
        // retry with interval when necessarily
        long retryInterval =
            peekedEvent.getRetryInterval() > EnrichedEvent.INITIAL_RETRY_INTERVAL_FOR_IOTV2
                ? peekedEvent.getRetryInterval()
                : 0L;
        LOGGER.info(
            "IoTConsensusV2-ConsensusGroup-{}: retry with interval {} for index {} {}",
            consensusGroupId,
            retryInterval,
            peekedEvent.getReplicateIndexForIoTV2(),
            peekedEvent);
        // need to retry in background service, otherwise the retryInterval will block the sender
        // procedure.
        backgroundTaskService.schedule(
            () -> {
              // do transfer
              if (peekedEvent instanceof PipeInsertNodeTabletInsertionEvent) {
                retryTransfer((PipeInsertNodeTabletInsertionEvent) peekedEvent);
              } else if (peekedEvent instanceof PipeTsFileInsertionEvent) {
                retryTransfer((PipeTsFileInsertionEvent) peekedEvent);
              } else if (peekedEvent instanceof PipeDeleteDataNodeEvent) {
                retryTransfer((PipeDeleteDataNodeEvent) peekedEvent);
              } else {
                if (LOGGER.isWarnEnabled()) {
                  LOGGER.warn(
                      "IoTConsensusV2AsyncConnector does not support transfer generic event: {}.",
                      peekedEvent);
                }
              }
            },
            retryInterval,
            TimeUnit.MILLISECONDS);
      }
    }
  }

  private void retryTransfer(final PipeInsertionEvent tabletInsertionEvent) {
    // TODO: batch transfer
    try {
      if (transferInEventWithoutCheck(tabletInsertionEvent)) {
        tabletInsertionEvent.decreaseReferenceCount(IoTConsensusV2AsyncSink.class.getName(), false);
      } else {
        addFailureEventToRetryQueue(tabletInsertionEvent);
      }
    } catch (final Exception e) {
      tabletInsertionEvent.decreaseReferenceCount(IoTConsensusV2AsyncSink.class.getName(), false);
      addFailureEventToRetryQueue(tabletInsertionEvent);
    }
  }

  private void retryTransfer(final PipeTsFileInsertionEvent tsFileInsertionEvent) {
    try {
      if (transferWithoutCheck(tsFileInsertionEvent)) {
        tsFileInsertionEvent.decreaseReferenceCount(IoTConsensusV2AsyncSink.class.getName(), false);
      } else {
        addFailureEventToRetryQueue(tsFileInsertionEvent);
      }
    } catch (final Exception e) {
      tsFileInsertionEvent.decreaseReferenceCount(IoTConsensusV2AsyncSink.class.getName(), false);
      addFailureEventToRetryQueue(tsFileInsertionEvent);
    }
  }

  private void retryTransfer(final PipeDeleteDataNodeEvent deleteDataNodeEvent) {
    try {
      if (transferDeletion(deleteDataNodeEvent)) {
        deleteDataNodeEvent.decreaseReferenceCount(IoTConsensusV2AsyncSink.class.getName(), false);
      } else {
        addFailureEventToRetryQueue(deleteDataNodeEvent);
      }
    } catch (final Exception e) {
      deleteDataNodeEvent.decreaseReferenceCount(IoTConsensusV2AsyncSink.class.getName(), false);
      addFailureEventToRetryQueue(deleteDataNodeEvent);
    }
  }

  /**
   * Add failure event to retry queue.
   *
   * @param event event to retry
   */
  @SuppressWarnings("java:S899")
  public synchronized void addFailureEventToRetryQueue(final EnrichedEvent event) {
    if (event.isReleased()) {
      return;
    }

    if (isClosed.get()) {
      event.clearReferenceCount(IoTConsensusV2AsyncSink.class.getName());
      return;
    }
    // just in case
    if (retryEventQueue.contains(event)) {
      return;
    }

    boolean res = retryEventQueue.offer(event);
    if (res) {
      LOGGER.info(
          "IoTConsensusV2-ConsensusGroup-{}: Event {} replicate index {} transfer failed, will be added to retry queue.",
          consensusGroupId,
          event,
          event.getReplicateIndexForIoTV2());
    } else {
      LOGGER.warn(
          "IoTConsensusV2-ConsensusGroup-{}: Event {} replicate index {} transfer failed, added to retry queue failed, this event will be ignored.",
          consensusGroupId,
          event,
          event.getReplicateIndexForIoTV2());
    }

    if (isClosed.get()) {
      event.clearReferenceCount(IoTConsensusV2AsyncSink.class.getName());
    }
  }

  /**
   * Add failure events to retry queue.
   *
   * @param events events to retry
   */
  public void addFailureEventsToRetryQueue(final Iterable<EnrichedEvent> events) {
    for (final EnrichedEvent event : events) {
      addFailureEventToRetryQueue(event);
    }
  }

  public synchronized void clearRetryEventsReferenceCount() {
    while (!retryEventQueue.isEmpty()) {
      final EnrichedEvent event = retryEventQueue.poll();
      event.clearReferenceCount(IoTConsensusV2AsyncSink.class.getName());
    }
  }

  public synchronized void clearTransferBufferReferenceCount() {
    while (!transferBuffer.isEmpty()) {
      final EnrichedEvent event = transferBuffer.poll();
      event.clearReferenceCount(IoTConsensusV2AsyncSink.class.getName());
    }
  }

  private void logOnClientException(
      final AsyncIoTConsensusV2ServiceClient client, final Exception e) {
    if (client == null) {
      LOGGER.warn(THRIFT_ERROR_FORMATTER_WITHOUT_ENDPOINT, e);
    } else {
      LOGGER.warn(
          String.format(
              THRIFT_ERROR_FORMATTER_WITH_ENDPOINT,
              client.getTEndpoint().getIp(),
              client.getTEndpoint().getPort()),
          e);
    }
  }

  private TEndPoint getFollowerUrl() {
    // In current iotConsensusV2 design, one connector corresponds to one follower, so the peers is
    // actually a singleton list
    return nodeUrls.get(0);
  }

  // synchronized to avoid close connector when transfer event
  @Override
  public synchronized void close() {
    super.close();
    isClosed.set(true);

    retryConnector.close();
    clearRetryEventsReferenceCount();
    clearTransferBufferReferenceCount();

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }

    IoTConsensusV2SyncLagManager.getInstance(getConsensusGroupIdStr())
        .removeConsensusPipeConnector(new ConsensusPipeName(consensusPipeName));
    MetricService.getInstance().removeMetricSet(this.iotConsensusV2SinkMetrics);
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getTransferBufferSize() {
    return transferBuffer.size();
  }

  public int getRetryBufferSize() {
    return retryEventQueue.size();
  }

  @Override
  public long getLeaderReplicateProgress() {
    return ReplicateProgressDataNodeManager.getReplicateIndexForIoTV2(consensusPipeName);
  }

  @Override
  public long getFollowerApplyProgress() {
    return currentReplicateProgress;
  }

  public String getConsensusGroupIdStr() {
    ConsensusGroupId groupId =
        ConsensusGroupId.Factory.create(
            TConsensusGroupType.DataRegion.getValue(), consensusGroupId);
    return groupId.toString();
  }
}
