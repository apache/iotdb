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

package org.apache.iotdb.db.pipe.task.subtask.connector;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.task.subtask.PipeAbstractConnectorSubtask;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBDataRegionAsyncConnector;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionConnectorMetrics;
import org.apache.iotdb.db.pipe.metric.PipeSchemaRegionConnectorMetrics;
import org.apache.iotdb.db.pipe.task.connection.PipeEventCollector;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class PipeConnectorSubtask extends PipeAbstractConnectorSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtask.class);

  // For input
  protected final UnboundedBlockingPendingQueue<Event> inputPendingQueue;

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String attributeSortedString;
  private final int connectorIndex;

  // Now parallel connectors run the same time, thus the heartbeat events are not sure
  // to trigger the general event transfer function, causing potentially such as
  // the random delay of the batch transmission. Therefore, here we inject cron events
  // when no event can be pulled.
  private static final PipeHeartbeatEvent CRON_HEARTBEAT_EVENT =
      new PipeHeartbeatEvent("cron", false);
  private static final long CRON_HEARTBEAT_EVENT_INJECT_INTERVAL_MILLISECONDS =
      PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds() * 1000;
  private long lastHeartbeatEventInjectTime = System.currentTimeMillis();

  public PipeConnectorSubtask(
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
      PipeDataRegionConnectorMetrics.getInstance().register(this);
    } else {
      PipeSchemaRegionConnectorMetrics.getInstance().register(this);
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

    try {
      if (event == null) {
        if (System.currentTimeMillis() - lastHeartbeatEventInjectTime
            > CRON_HEARTBEAT_EVENT_INJECT_INTERVAL_MILLISECONDS) {
          transferHeartbeatEvent(CRON_HEARTBEAT_EVENT);
        }
        return false;
      }

      if (event instanceof TabletInsertionEvent) {
        outputPipeConnector.transfer((TabletInsertionEvent) event);
        PipeDataRegionConnectorMetrics.getInstance().markTabletEvent(taskID);
      } else if (event instanceof TsFileInsertionEvent) {
        outputPipeConnector.transfer((TsFileInsertionEvent) event);
        PipeDataRegionConnectorMetrics.getInstance().markTsFileEvent(taskID);
      } else if (event instanceof PipeSchemaRegionWritePlanEvent) {
        outputPipeConnector.transfer(event);
        PipeSchemaRegionConnectorMetrics.getInstance().markSchemaEvent(taskID);
      } else if (event instanceof PipeHeartbeatEvent) {
        transferHeartbeatEvent((PipeHeartbeatEvent) event);
      } else {
        outputPipeConnector.transfer(
            event instanceof UserDefinedEnrichedEvent
                ? ((UserDefinedEnrichedEvent) event).getUserDefinedEvent()
                : event);
      }

      decreaseReferenceCountAndReleaseLastEvent(true);
    } catch (final PipeException e) {
      if (!isClosed.get()) {
        throw e;
      } else {
        LOGGER.info(
            "{} in pipe transfer, ignored because pipe is dropped.",
            e.getClass().getSimpleName(),
            e);
        clearReferenceCountAndReleaseLastEvent();
      }
    } catch (final Exception e) {
      if (!isClosed.get()) {
        throw new PipeException(
            String.format(
                "Exception in pipe transfer, subtask: %s, last event: %s, root cause: %s",
                taskID,
                lastEvent instanceof EnrichedEvent
                    ? ((EnrichedEvent) lastEvent).coreReportMessage()
                    : lastEvent,
                ErrorHandlingUtils.getRootCause(e).getMessage()),
            e);
      } else {
        LOGGER.info("Exception in pipe transfer, ignored because pipe is dropped.", e);
        clearReferenceCountAndReleaseLastEvent();
      }
    }

    return true;
  }

  private void transferHeartbeatEvent(final PipeHeartbeatEvent event) {
    try {
      outputPipeConnector.heartbeat();
      outputPipeConnector.transfer(event);
    } catch (final Exception e) {
      throw new PipeConnectionException(
          "PipeConnector: "
              + outputPipeConnector.getClass().getName()
              + " heartbeat failed, or encountered failure when transferring generic event. Failure: "
              + e.getMessage(),
          e);
    }

    lastHeartbeatEventInjectTime = System.currentTimeMillis();

    event.onTransferred();
    PipeDataRegionConnectorMetrics.getInstance().markPipeHeartbeatEvent(taskID);
  }

  @Override
  public void close() {
    if (!attributeSortedString.startsWith("schema_")) {
      PipeDataRegionConnectorMetrics.getInstance().deregister(taskID);
    } else {
      PipeSchemaRegionConnectorMetrics.getInstance().deregister(taskID);
    }

    isClosed.set(true);
    try {
      outputPipeConnector.close();
    } catch (final Exception e) {
      LOGGER.info(
          "Exception occurred when closing pipe connector subtask {}, root cause: {}",
          taskID,
          ErrorHandlingUtils.getRootCause(e).getMessage(),
          e);
    } finally {
      inputPendingQueue.forEach(
          event -> {
            if (event instanceof EnrichedEvent) {
              ((EnrichedEvent) event).clearReferenceCount(PipeEventCollector.class.getName());
            }
          });
      inputPendingQueue.clear();

      // Should be called after outputPipeConnector.close()
      super.close();
    }
  }

  /**
   * When a pipe is dropped, the connector maybe reused and will not be closed. So we just discard
   * its queued events in the output pipe connector.
   */
  public void discardEventsOfPipe(final String pipeNameToDrop) {
    if (outputPipeConnector instanceof IoTDBDataRegionAsyncConnector) {
      ((IoTDBDataRegionAsyncConnector) outputPipeConnector).discardEventsOfPipe(pipeNameToDrop);
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
    return inputPendingQueue.getTsFileInsertionEventCount();
  }

  public int getTabletInsertionEventCount() {
    return inputPendingQueue.getTabletInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return inputPendingQueue.getPipeHeartbeatEventCount();
  }

  public int getAsyncConnectorRetryEventQueueSize() {
    return outputPipeConnector instanceof IoTDBDataRegionAsyncConnector
        ? ((IoTDBDataRegionAsyncConnector) outputPipeConnector).getRetryEventQueueSize()
        : 0;
  }

  // For performance, this will not acquire lock and does not guarantee the correct
  // result. However, this shall not cause any exceptions when concurrently read & written.
  public int getEventCount(final String pipeName) {
    final AtomicInteger count = new AtomicInteger(0);
    try {
      inputPendingQueue.forEach(
          event -> {
            if (event instanceof EnrichedEvent
                && pipeName.equals(((EnrichedEvent) event).getPipeName())) {
              count.incrementAndGet();
            }
          });
    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Exception occurred when counting event of pipe {}, root cause: {}",
            pipeName,
            ErrorHandlingUtils.getRootCause(e).getMessage(),
            e);
      }
    }
    return count.get()
        + (outputPipeConnector instanceof IoTDBDataRegionAsyncConnector
            ? ((IoTDBDataRegionAsyncConnector) outputPipeConnector).getRetryEventCount(pipeName)
            : 0);
  }

  //////////////////////////// Error report ////////////////////////////

  @Override
  protected String getRootCause(final Throwable throwable) {
    return ErrorHandlingUtils.getRootCause(throwable).getMessage();
  }

  @Override
  protected void report(final EnrichedEvent event, final PipeRuntimeException exception) {
    PipeAgent.runtime().report(event, exception);
  }
}
