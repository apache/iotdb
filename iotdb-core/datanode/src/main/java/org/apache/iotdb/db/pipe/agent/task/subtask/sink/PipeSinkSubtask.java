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

import org.apache.iotdb.commons.exception.pipe.PipeNonReportException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeAbstractSinkSubtask;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.protocol.IoTDBSink;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.metric.schema.PipeSchemaRegionSinkMetrics;
import org.apache.iotdb.db.pipe.metric.sink.PipeDataRegionSinkMetrics;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.async.IoTDBDataRegionAsyncSink;
import org.apache.iotdb.db.pipe.sink.protocol.thrift.sync.IoTDBDataRegionSyncSink;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class PipeSinkSubtask extends PipeAbstractSinkSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSinkSubtask.class);

  // For input
  protected final UnboundedBlockingPendingQueue<Event> inputPendingQueue;

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String attributeSortedString;
  private final int connectorIndex;

  // Now parallel connectors run the same time, thus the heartbeat events are not sure
  // to trigger the general event transfer function, causing potentially such as
  // the random delay of the batch transmission. Therefore, here we inject cron events
  // when no event can be pulled.
  public static final PipeHeartbeatEvent CRON_HEARTBEAT_EVENT =
      new PipeHeartbeatEvent("cron", false);

  public PipeSinkSubtask(
      final String taskID,
      final long creationTime,
      final String attributeSortedString,
      final int connectorIndex,
      final UnboundedBlockingPendingQueue<Event> inputPendingQueue,
      final PipeConnector outputPipeConnector) {
    super(taskID, creationTime, outputPipeConnector);
    this.attributeSortedString = attributeSortedString;
    this.connectorIndex = connectorIndex;
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
        outputPipeSink.transfer((TabletInsertionEvent) event);
        PipeDataRegionSinkMetrics.getInstance().markTabletEvent(taskID);
      } else if (event instanceof TsFileInsertionEvent) {
        outputPipeSink.transfer((TsFileInsertionEvent) event);
        PipeDataRegionSinkMetrics.getInstance().markTsFileEvent(taskID);
      } else if (event instanceof PipeSchemaRegionWritePlanEvent) {
        outputPipeSink.transfer(event);
        if (((PipeSchemaRegionWritePlanEvent) event).getPlanNode().getType()
            != PlanNodeType.DELETE_DATA) {
          // Only plan nodes in schema region will be marked, delete data node is currently not
          // taken into account
          PipeSchemaRegionSinkMetrics.getInstance().markSchemaEvent(taskID);
        }
      } else if (event instanceof PipeHeartbeatEvent) {
        transferHeartbeatEvent((PipeHeartbeatEvent) event);
      } else {
        outputPipeSink.transfer(
            event instanceof UserDefinedEnrichedEvent
                ? ((UserDefinedEnrichedEvent) event).getUserDefinedEvent()
                : event);
      }

      decreaseReferenceCountAndReleaseLastEvent(event, true);
      sleepInterval = PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalInitMs();
    } catch (final PipeNonReportException e) {
      sleep4NonReportException();
    } catch (final PipeException e) {
      if (!isClosed.get()) {
        setLastExceptionEvent(event);
        throw e;
      } else {
        LOGGER.info(
            "{} in pipe transfer, ignored because the connector subtask is dropped.{}",
            e.getClass().getSimpleName(),
            e.getMessage() != null ? " Message: " + e.getMessage() : "");
        clearReferenceCountAndReleaseLastEvent(event);
      }
    } catch (final Exception e) {
      if (!isClosed.get()) {
        setLastExceptionEvent(event);
        throw new PipeException(
            String.format(
                "Exception in pipe transfer, subtask: %s, last event: %s, root cause: %s",
                taskID,
                event instanceof EnrichedEvent
                    ? ((EnrichedEvent) event).coreReportMessage()
                    : event,
                ErrorHandlingUtils.getRootCause(e).getMessage()),
            e);
      } else {
        LOGGER.info(
            "Exception in pipe transfer, ignored because the sink subtask is dropped.{}",
            e.getMessage() != null ? " Message: " + e.getMessage() : "");
        clearReferenceCountAndReleaseLastEvent(event);
      }
    }

    return true;
  }

  private void transferHeartbeatEvent(final PipeHeartbeatEvent event) {
    // DO NOT call heartbeat or transfer after closed, or will cause connection leak
    if (isClosed.get()) {
      return;
    }

    try {
      outputPipeSink.heartbeat();
      outputPipeSink.transfer(event);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          "PipeConnector: "
              + outputPipeSink.getClass().getName()
              + "(id: "
              + taskID
              + ")"
              + " heartbeat failed, or encountered failure when transferring generic event. Failure: "
              + e.getMessage(),
          e);
    }

    event.onTransferred();
    PipeDataRegionSinkMetrics.getInstance().markPipeHeartbeatEvent(taskID);
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
      outputPipeSink.close();
      LOGGER.info(
          "Pipe: connector subtask {} ({}) was closed within {} ms",
          taskID,
          outputPipeSink,
          System.currentTimeMillis() - startTime);
    } catch (final Exception e) {
      LOGGER.info(
          "Exception occurred when closing pipe connector subtask {}, root cause: {}",
          taskID,
          ErrorHandlingUtils.getRootCause(e).getMessage(),
          e);
    } finally {
      inputPendingQueue.discardAllEvents();

      // Should be called after outputPipeConnector.close()
      super.close();
    }
  }

  /**
   * When a pipe is dropped, the connector maybe reused and will not be closed. So we just discard
   * its queued events in the output pipe connector.
   */
  public void discardEventsOfPipe(final String pipeNameToDrop, int regionId) {
    // Try to remove the events as much as possible
    inputPendingQueue.discardEventsOfPipe(pipeNameToDrop, regionId);

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
            && pipeNameToDrop.equals(((EnrichedEvent) lastEvent).getPipeName())
            && regionId == ((EnrichedEvent) lastEvent).getRegionId()) {
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
            && pipeNameToDrop.equals(((EnrichedEvent) lastExceptionEvent).getPipeName())
            && regionId == ((EnrichedEvent) lastExceptionEvent).getRegionId()) {
          clearReferenceCountAndReleaseLastExceptionEvent();
        }
      }
    } finally {
      decreaseHighPriorityTaskCount();
    }

    if (outputPipeSink instanceof IoTDBSink) {
      ((IoTDBSink) outputPipeSink).discardEventsOfPipe(pipeNameToDrop, regionId);
    }
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getAttributeSortedString() {
    return attributeSortedString;
  }

  public int getConnectorIndex() {
    return connectorIndex;
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

  public int getAsyncConnectorRetryEventQueueSize() {
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
    return ErrorHandlingUtils.getRootCause(throwable).getMessage();
  }

  @Override
  protected void report(final EnrichedEvent event, final PipeRuntimeException exception) {
    PipeDataNodeAgent.runtime().report(event, exception);
  }
}
