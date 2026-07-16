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

package org.apache.iotdb.db.pipe.agent.task.subtask.sink;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeAbstractSinkSubtask;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSink;
import org.apache.iotdb.commons.pipe.sink.protocol.PipeConnectorWithEventDiscard;
import org.apache.iotdb.commons.pipe.sink.protocol.PipeSinkWithSchedulingDelay;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.utils.ErrorHandlingCommonUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionSinkMetrics;
import org.apache.iotdb.db.pipe.metric.sink.PipeDataRegionSinkMetrics;
import org.apache.iotdb.db.pipe.sink.protocol.airgap.IoTDBDataRegionAirGapSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class PipeSinkSubtask extends PipeAbstractSinkSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSinkSubtask.class);

  // For input
  protected final UnboundedBlockingPendingQueue<Event> inputPendingQueue;

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String attributeSortedString;
  private final String attributeDisplayString;
  private final int sinkIndex;

  // Now parallel connectors run the same time, thus the heartbeat events are not sure
  // to trigger the general event transfer function, causing potentially such as
  // the random delay of the batch transmission. Therefore, here we inject cron events
  // when no event can be pulled.
  public static final PipeHeartbeatEvent CRON_HEARTBEAT_EVENT = new PipeHeartbeatEvent(-1, false);
  private final ReentrantLock outputPipeSinkOperationLock = new ReentrantLock();
  private final Queue<CommitterKey> pendingDiscardCommitterKeys = new ConcurrentLinkedQueue<>();

  public PipeSinkSubtask(
      final String taskID,
      final long creationTime,
      final String attributeSortedString,
      final int sinkIndex,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue,
      final PipeConnector outputPipeConnector) {
    this(
        taskID,
        creationTime,
        attributeSortedString,
        attributeSortedString,
        sinkIndex,
        inputPendingQueue,
        outputPipeConnector);
  }

  public PipeSinkSubtask(
      final String taskID,
      final long creationTime,
      final String attributeSortedString,
      final String attributeDisplayString,
      final int sinkIndex,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue,
      final PipeConnector outputPipeConnector) {
    super(taskID, creationTime, outputPipeConnector);
    this.attributeSortedString = attributeSortedString;
    this.attributeDisplayString = attributeDisplayString;
    this.sinkIndex = sinkIndex;
    this.inputPendingQueue = inputPendingQueue;

    if (!attributeSortedString.startsWith("schema_")) {
      PipeDataRegionSinkMetrics.getInstance().register(this);
    } else {
      PipeSchemaRegionSinkMetrics.getInstance().register(this);
    }
  }

  @Override
  protected boolean executeOnce() {
    if (isClosed.get()) {
      return false;
    }

    final Event event =
        lastEvent != null
            ? lastEvent
            : UserDefinedEnrichedEvent.maybeOf(inputPendingQueue.waitedPoll());
    // Record this event for retrying on connection failure or other exceptions
    setLastEvent(event);
    if (event instanceof EnrichedEvent && ((EnrichedEvent) event).isReleased()) {
      lastEvent = null;
      return true;
    }

    try {
      if (Objects.isNull(event)) {
        transferHeartbeatEvent(CRON_HEARTBEAT_EVENT);
        return false;
      }

      if (event instanceof TabletInsertionEvent) {
        if (executeOutputPipeSinkOperation(
            () -> outputPipeSink.transfer((TabletInsertionEvent) event))) {
          PipeDataRegionSinkMetrics.getInstance().markTabletEvent(taskID);
        }
      } else if (event instanceof TsFileInsertionEvent) {
        if (executeOutputPipeSinkOperation(
            () -> outputPipeSink.transfer((TsFileInsertionEvent) event))) {
          PipeDataRegionSinkMetrics.getInstance().markTsFileEvent(taskID);
        }
      } else if (event instanceof PipeSchemaRegionWritePlanEvent) {
        if (executeOutputPipeSinkOperation(() -> outputPipeSink.transfer(event))
            && ((PipeSchemaRegionWritePlanEvent) event).getPlanNode().getType()
                != PlanNodeType.DELETE_DATA) {
          // Only plan nodes in schema region will be marked, delete data node is currently not
          // taken into account
          PipeSchemaRegionSinkMetrics.getInstance().markSchemaEvent(taskID);
        }
      } else if (event instanceof PipeHeartbeatEvent) {
        transferHeartbeatEvent((PipeHeartbeatEvent) event);
      } else {
        executeOutputPipeSinkOperation(
            () ->
                outputPipeSink.transfer(
                    event instanceof UserDefinedEnrichedEvent
                        ? ((UserDefinedEnrichedEvent) event).getUserDefinedEvent()
                        : event));
      }

      decreaseReferenceCountAndReleaseLastEvent(event, true);
      sleepInterval = PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalInitMs();
    } catch (final Exception e) {
      handleException(event, e);
    }

    return true;
  }

  @Override
  protected long peekSchedulingDelayInMs() {
    if (!(outputPipeSink instanceof PipeSinkWithSchedulingDelay)) {
      return 0;
    }

    outputPipeSinkOperationLock.lock();
    try {
      discardPendingEventsOfPipeUnderLock();
      return isClosed.get()
          ? 0
          : ((PipeSinkWithSchedulingDelay) outputPipeSink).peekSchedulingDelayMs();
    } finally {
      outputPipeSinkOperationLock.unlock();
    }
  }

  @Override
  protected long consumeSchedulingDelayInMs() {
    if (!(outputPipeSink instanceof PipeSinkWithSchedulingDelay)) {
      return 0;
    }

    final long remainingSchedulingDelayMs;
    outputPipeSinkOperationLock.lock();
    try {
      discardPendingEventsOfPipeUnderLock();
      remainingSchedulingDelayMs =
          isClosed.get()
              ? 0
              : ((PipeSinkWithSchedulingDelay) outputPipeSink).consumeSchedulingDelayMs();
    } finally {
      outputPipeSinkOperationLock.unlock();
    }
    if (remainingSchedulingDelayMs <= 0) {
      return 0;
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          DataNodePipeMessages.PIPE_SINK_SUBTASK_DELAYED_TO_AVOID_FREQUENT_HANDSHAKES,
          getDisplayTaskID(),
          remainingSchedulingDelayMs);
    }

    return remainingSchedulingDelayMs;
  }

  private void transferHeartbeatEvent(final PipeHeartbeatEvent event) {
    // DO NOT call heartbeat or transfer after closed, or will cause connection leak
    if (isClosed.get()) {
      return;
    }

    try {
      if (!executeOutputPipeSinkOperation(
          () -> {
            outputPipeSink.heartbeat();
            outputPipeSink.transfer(event);
          })) {
        return;
      }
    } catch (final Exception e) {
      throw new PipeConnectionException(
          String.format(
              DataNodePipeMessages
                  .EXCEPTION_PIPECONNECTOR_ARG_ID_ARG_HEARTBEAT_FAILED_OR_ENCOUNTERED_FAILURE_WHEN_TRANSFERRING_GENERIC_EVENT_FAILURE_ARG_679A4A49,
              outputPipeSink.getClass().getName(),
              getDisplayTaskID(),
              e.getMessage()),
          e);
    }

    event.onTransferred();
    PipeDataRegionSinkMetrics.getInstance().markPipeHeartbeatEvent(taskID);
  }

  @Override
  protected boolean handshakeOutputPipeSink() throws Exception {
    return executeOutputPipeSinkOperation(() -> outputPipeSink.handshake());
  }

  private boolean executeOutputPipeSinkOperation(final OutputPipeSinkOperation operation)
      throws Exception {
    outputPipeSinkOperationLock.lock();
    try {
      discardPendingEventsOfPipeUnderLock();
      if (isClosed.get()) {
        return false;
      }

      operation.execute();
      discardPendingEventsOfPipeUnderLock();
      return true;
    } finally {
      outputPipeSinkOperationLock.unlock();
    }
  }

  private void discardPendingEventsOfPipeUnderLock() {
    if (!(outputPipeSink instanceof PipeConnectorWithEventDiscard)) {
      pendingDiscardCommitterKeys.clear();
      return;
    }

    CommitterKey committerKey;
    while ((committerKey = pendingDiscardCommitterKeys.poll()) != null) {
      try {
        ((PipeConnectorWithEventDiscard) outputPipeSink).discardEventsOfPipe(committerKey);
      } catch (final Exception e) {
        LOGGER.warn(
            DataNodePipeMessages.FAILED_TO_DISCARD_EVENTS_OF_PIPE_IN_CONNECTOR_SUBTASK,
            committerKey.getPipeName(),
            getDisplayTaskID(),
            e);
      }
    }
  }

  @FunctionalInterface
  private interface OutputPipeSinkOperation {

    void execute() throws Exception;
  }

  @Override
  public void close() {
    if (!attributeSortedString.startsWith("schema_")) {
      PipeDataRegionSinkMetrics.getInstance().deregister(taskID);
    } else {
      PipeSchemaRegionSinkMetrics.getInstance().deregister(taskID);
    }

    isClosed.set(true);
    try {
      final long startTime = System.currentTimeMillis();
      if (closeOutputPipeSink()) {
        LOGGER.info(
            DataNodePipeMessages.PIPE_CONNECTOR_SUBTASK_WAS_CLOSED_WITHIN_MS,
            getDisplayTaskID(),
            outputPipeSink,
            System.currentTimeMillis() - startTime);
      }
    } catch (final Exception e) {
      LOGGER.info(
          DataNodePipeMessages.EXCEPTION_OCCURRED_WHEN_CLOSING_PIPE_CONNECTOR_SUBTASK,
          getDisplayTaskID(),
          ErrorHandlingCommonUtils.getRootCause(e).getMessage(),
          e);
    } finally {
      inputPendingQueue.discardAllEvents();

      // Should be called after outputPipeConnector.close()
      super.close();
    }
  }

  private boolean closeOutputPipeSink() throws Exception {
    final AtomicReference<Exception> exception = new AtomicReference<>();
    final AtomicBoolean closeStarted = new AtomicBoolean(false);
    final Thread closeThread =
        new Thread(
            () -> {
              outputPipeSinkOperationLock.lock();
              try {
                discardPendingEventsOfPipeUnderLock();
                closeStarted.set(true);
                outputPipeSink.close();
              } catch (final Exception e) {
                exception.set(e);
              } finally {
                outputPipeSinkOperationLock.unlock();
              }
            },
            "PipeSinkSubtaskClose-" + getDisplayTaskID());
    closeThread.setDaemon(true);
    closeThread.start();

    final long timeoutInMs =
        Math.max(
            1L, CommonDescriptor.getInstance().getConfig().getDnConnectionTimeoutInMS() * 2L / 3);
    try {
      closeThread.join(timeoutInMs);
    } catch (final InterruptedException e) {
      closeThread.interrupt();
      Thread.currentThread().interrupt();
      throw e;
    }
    if (closeThread.isAlive()) {
      if (closeStarted.get()) {
        closeThread.interrupt();
      }
      LOGGER.warn(
          DataNodePipeMessages.PIPE_SINK_SUBTASK_CLOSE_TIMED_OUT,
          timeoutInMs,
          getDisplayTaskID(),
          closeStarted.get()
              ? DataNodePipeMessages.PIPE_SINK_SUBTASK_CLOSE_OPERATION_STILL_RUNNING
              : DataNodePipeMessages
                  .PIPE_SINK_SUBTASK_CLOSE_OPERATION_WILL_RUN_AFTER_CURRENT_CONNECTOR_OPERATION);
      return false;
    }

    if (exception.get() != null) {
      throw exception.get();
    }
    return true;
  }

  /**
   * When a pipe is dropped, the connector maybe reused and will not be closed. So we just discard
   * its queued events in the output pipe connector.
   */
  public void discardEventsOfPipe(final CommitterKey committerKey) {
    // Try to remove the events as much as possible
    inputPendingQueue.discardEventsOfPipe(committerKey);

    try {
      increaseHighPriorityTaskCount();

      // synchronized to use the lastEvent & lastExceptionEvent
      synchronized (this) {
        // Here we discard the last event, and re-submit the pipe task to avoid that the pipe task
        // has stopped submission but will not be stopped by critical exceptions, because when it
        // acquires lock, the pipe is already dropped, thus it will do nothing. Note that since we
        // use a new thread to stop all the pipes, we will not encounter deadlock here. Or else we
        // will.
        if (lastEvent instanceof EnrichedEvent
            && isEventFromPipe((EnrichedEvent) lastEvent, committerKey)) {
          // Do not clear the last event's reference counts because it may be on transferring
          lastEvent = null;
          // Submit self to avoid that the lastEvent has been retried "max times" times and has
          // stopped executing.
          // 1. If the last event is still on execution, or submitted by the previous "onSuccess" or
          //    "onFailure", the "submitSelf" causes nothing.
          // 2. If the last event is waiting the instance lock to call "onSuccess", then the
          //    callback method will skip this turn of submission.
          // 3. If the last event is waiting to call "onFailure", then it will be ignored because
          //    the last event has been set to null.
          // 4. If the last event has called "onFailure" and caused the subtask to stop submission,
          //    it's submitted here and the "report" will wait for the "drop pipe" lock to stop all
          //    the pipes with critical exceptions. As illustrated above, the "report" will do
          //    nothing.
          submitSelf();
        }

        // We only clear the lastEvent's reference counts when it's already on failure. Namely, we
        // clear the lastExceptionEvent. It's safe to potentially clear it twice because we have the
        // "nonnull" detection.
        if (lastExceptionEvent instanceof EnrichedEvent
            && isEventFromPipe((EnrichedEvent) lastExceptionEvent, committerKey)) {
          clearReferenceCountAndReleaseLastExceptionEvent();
        }
      }
    } finally {
      decreaseHighPriorityTaskCount();
    }

    discardOutputPipeSinkEventsOfPipe(committerKey);
  }

  private void discardOutputPipeSinkEventsOfPipe(final CommitterKey committerKey) {
    if (!(outputPipeSink instanceof PipeConnectorWithEventDiscard)) {
      return;
    }

    pendingDiscardCommitterKeys.offer(committerKey);
    if (outputPipeSinkOperationLock.tryLock()) {
      try {
        discardPendingEventsOfPipeUnderLock();
      } finally {
        outputPipeSinkOperationLock.unlock();
      }
    }
  }

  private static boolean isEventFromPipe(
      final EnrichedEvent event, final CommitterKey committerKey) {
    return committerKey.getPipeName().equals(event.getPipeName())
        && committerKey.getCreationTime() == event.getCreationTime()
        && committerKey.getRegionId() == event.getRegionId()
        && (committerKey.getRestartTimes() < 0 || committerKey.equals(event.getCommitterKey()));
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getAttributeSortedString() {
    return attributeSortedString;
  }

  public int getSinkIndex() {
    return sinkIndex;
  }

  public int getTsFileInsertionEventCount() {
    return inputPendingQueue.getTsFileInsertionEventCount()
        + (lastEvent instanceof TsFileInsertionEvent ? 1 : 0);
  }

  public int getTabletInsertionEventCount() {
    return inputPendingQueue.getTabletInsertionEventCount()
        + (lastEvent instanceof TabletInsertionEvent ? 1 : 0);
  }

  public int getPipeHeartbeatEventCount() {
    return inputPendingQueue.getPipeHeartbeatEventCount()
        + (lastEvent instanceof PipeHeartbeatEvent ? 1 : 0);
  }

  public int getAsyncSinkRetryEventQueueSize() {
    return outputPipeSink instanceof IoTDBDataRegionAsyncSink
        ? ((IoTDBDataRegionAsyncSink) outputPipeSink).getRetryEventQueueSize()
        : 0;
  }

  public int getPendingHandlersSize() {
    return outputPipeSink instanceof IoTDBDataRegionAsyncSink
        ? ((IoTDBDataRegionAsyncSink) outputPipeSink).getPendingHandlersSize()
        : 0;
  }

  public int getBatchSize() {
    if (outputPipeSink instanceof IoTDBDataRegionAsyncSink) {
      return ((IoTDBDataRegionAsyncSink) outputPipeSink).getBatchSize();
    }
    if (outputPipeSink instanceof IoTDBDataRegionSyncSink) {
      return ((IoTDBDataRegionSyncSink) outputPipeSink).getBatchSize();
    }
    if (outputPipeSink instanceof IoTDBDataRegionAirGapSink) {
      return ((IoTDBDataRegionAirGapSink) outputPipeSink).getBatchSize();
    }
    return 0;
  }

  public double getTotalUncompressedSize() {
    return outputPipeSink instanceof IoTDBSink
        ? ((IoTDBSink) outputPipeSink).getTotalUncompressedSize()
        : 0;
  }

  public double getTotalCompressedSize() {
    return outputPipeSink instanceof IoTDBSink
        ? ((IoTDBSink) outputPipeSink).getTotalCompressedSize()
        : 0;
  }

  public void setTabletBatchSizeHistogram(Histogram tabletBatchSizeHistogram) {
    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink).setTabletBatchSizeHistogram(tabletBatchSizeHistogram);
    }
  }

  public void setSchemaBatchSizeHistogram(Histogram schemaBatchSizeHistogram) {
    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink).setSchemaBatchSizeHistogram(schemaBatchSizeHistogram);
    }
  }

  public void setTsFileBatchSizeHistogram(Histogram tsFileBatchSizeHistogram) {
    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink).setTsFileBatchSizeHistogram(tsFileBatchSizeHistogram);
    }
  }

  public void setTabletBatchTimeIntervalHistogram(Histogram tabletBatchTimeIntervalHistogram) {
    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink)
          .setTabletBatchTimeIntervalHistogram(tabletBatchTimeIntervalHistogram);
    }
  }

  public void setSchemaBatchTimeIntervalHistogram(Histogram schemaBatchTimeIntervalHistogram) {
    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink)
          .setSchemaBatchTimeIntervalHistogram(schemaBatchTimeIntervalHistogram);
    }
  }

  public void setTsFileBatchTimeIntervalHistogram(Histogram tsFileBatchTimeIntervalHistogram) {
    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink)
          .setTsFileBatchTimeIntervalHistogram(tsFileBatchTimeIntervalHistogram);
    }
  }

  public void setEventSizeHistogram(Histogram eventSizeHistogram) {
    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink).setBatchEventSizeHistogram(eventSizeHistogram);
    }
  }

  //////////////////////////// Error report ////////////////////////////

  @Override
  protected String getRootCause(final Throwable throwable) {
    return ErrorHandlingCommonUtils.getRootCause(throwable).getMessage();
  }

  @Override
  protected void report(final EnrichedEvent event, final PipeRuntimeException exception) {
    lastExceptionTime = Long.MAX_VALUE;
    PipeDataNodeAgent.runtime().report(event, exception);
  }

  @Override
  public String getDisplayTaskID() {
    return generateDisplayTaskID(attributeDisplayString, creationTime, sinkIndex);
  }

  static String generateDisplayTaskID(
      final String attributeDisplayString, final long creationTime, final int sinkIndex) {
    return String.format("%s_%s_%s", attributeDisplayString, creationTime, sinkIndex);
  }
}
