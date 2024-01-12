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
import org.apache.iotdb.commons.pipe.task.DecoratingLock;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.task.subtask.PipeTransferSubtask;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.connector.protocol.thrift.async.IoTDBThriftAsyncConnector;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.metric.PipeConnectorMetrics;
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

public class PipeConnectorSubtask extends PipeTransferSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtask.class);

  // For input and output
  private final BoundedBlockingPendingQueue<Event> inputPendingQueue;

  // For thread pool to execute callbacks
  protected final DecoratingLock callbackDecoratingLock = new DecoratingLock();

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String attributeSortedString;
  private final int connectorIndex;

  // Now parallel connectors run the same time, thus the heartbeat events are not sure
  // to trigger the general event transfer function, causing potentially such as
  // the random delay of the batch transmission. Therefore, here we inject cron events
  // when no event can be pulled.
  private static final PipeHeartbeatEvent CRON_HEARTBEAT_EVENT =
      new PipeHeartbeatEvent("cron", false);
  private static final long CRON_HEARTBEAT_EVENT_INJECT_INTERVAL_SECONDS =
      PipeConfig.getInstance().getPipeSubtaskExecutorCronHeartbeatEventIntervalSeconds();
  private long lastHeartbeatEventInjectTime = System.currentTimeMillis();

  public PipeConnectorSubtask(
      String taskID,
      long creationTime,
      String attributeSortedString,
      int connectorIndex,
      BoundedBlockingPendingQueue<Event> inputPendingQueue,
      PipeConnector outputPipeConnector) {
    super(taskID, creationTime, outputPipeConnector);
    this.attributeSortedString = attributeSortedString;
    this.connectorIndex = connectorIndex;
    this.inputPendingQueue = inputPendingQueue;
    PipeConnectorMetrics.getInstance().register(this);
  }

  @Override
  protected boolean executeOnce() {
    if (isClosed.get()) {
      return false;
    }

    final Event event = lastEvent != null ? lastEvent : inputPendingQueue.waitedPoll();
    // Record this event for retrying on connection failure or other exceptions
    setLastEvent(event);

    try {
      if (event == null) {
        if (System.currentTimeMillis() - lastHeartbeatEventInjectTime
            > CRON_HEARTBEAT_EVENT_INJECT_INTERVAL_SECONDS) {
          transferHeartbeatEvent(CRON_HEARTBEAT_EVENT);
        }
        return false;
      }

      if (event instanceof TabletInsertionEvent) {
        outputPipeConnector.transfer((TabletInsertionEvent) event);
        PipeConnectorMetrics.getInstance().markTabletEvent(taskID);
      } else if (event instanceof TsFileInsertionEvent) {
        outputPipeConnector.transfer((TsFileInsertionEvent) event);
        PipeConnectorMetrics.getInstance().markTsFileEvent(taskID);
      } else if (event instanceof PipeHeartbeatEvent) {
        transferHeartbeatEvent((PipeHeartbeatEvent) event);
      } else {
        outputPipeConnector.transfer(event);
      }

      releaseLastEvent(true);
    } catch (PipeConnectionException e) {
      if (!isClosed.get()) {
        throw e;
      } else {
        LOGGER.info("PipeConnectionException in pipe transfer, ignored because pipe is dropped.");
        releaseLastEvent(false);
      }
    } catch (Exception e) {
      if (!isClosed.get()) {
        throw new PipeException(
            "Error occurred during executing PipeConnector#transfer, perhaps need to check "
                + "whether the implementation of PipeConnector is correct "
                + "according to the pipe-api description.",
            e);
      } else {
        LOGGER.info("Exception in pipe transfer, ignored because pipe is dropped.");
        releaseLastEvent(false);
      }
    }

    return true;
  }

  private void transferHeartbeatEvent(PipeHeartbeatEvent event) {
    try {
      outputPipeConnector.heartbeat();
      outputPipeConnector.transfer(event);
    } catch (Exception e) {
      throw new PipeConnectionException(
          "PipeConnector: "
              + outputPipeConnector.getClass().getName()
              + " heartbeat failed, or encountered failure when transferring generic event.",
          e);
    }

    lastHeartbeatEventInjectTime = System.currentTimeMillis();

    event.onTransferred();
    PipeConnectorMetrics.getInstance().markPipeHeartbeatEvent(taskID);
  }

  @Override
  public void close() {
    PipeConnectorMetrics.getInstance().deregister(taskID);
    isClosed.set(true);
    try {
      outputPipeConnector.close();
    } catch (Exception e) {
      LOGGER.info(
          "Error occurred during closing PipeConnector, perhaps need to check whether the "
              + "implementation of PipeConnector is correct according to the pipe-api description.",
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
  public void discardEventsOfPipe(String pipeNameToDrop) {
    if (outputPipeConnector instanceof IoTDBThriftAsyncConnector) {
      ((IoTDBThriftAsyncConnector) outputPipeConnector).discardEventsOfPipe(pipeNameToDrop);
    }
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getAttributeSortedString() {
    return attributeSortedString;
  }

  public int getConnectorIndex() {
    return connectorIndex;
  }

  public Integer getTsFileInsertionEventCount() {
    return inputPendingQueue.getTsFileInsertionEventCount();
  }

  public Integer getTabletInsertionEventCount() {
    return inputPendingQueue.getTabletInsertionEventCount();
  }

  public Integer getPipeHeartbeatEventCount() {
    return inputPendingQueue.getPipeHeartbeatEventCount();
  }

  public Integer getAsyncConnectorRetryEventQueueSize() {
    return outputPipeConnector instanceof IoTDBThriftAsyncConnector
        ? ((IoTDBThriftAsyncConnector) outputPipeConnector).getRetryEventQueueSize()
        : 0;
  }

  //////////////////////////// Error report ////////////////////////////
  @Override
  protected String getRootCause(Throwable throwable) {
    return ErrorHandlingUtils.getRootCause(throwable).getMessage();
  }

  @Override
  protected void report(EnrichedEvent event, PipeRuntimeException exception) {
    PipeAgent.runtime().report(event, exception);
  }
}
