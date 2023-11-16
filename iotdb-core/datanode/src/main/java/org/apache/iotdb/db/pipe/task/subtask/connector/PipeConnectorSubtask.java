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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.db.pipe.metric.PipeConnectorMetrics;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.task.subtask.DecoratingLock;
import org.apache.iotdb.db.pipe.task.subtask.PipeSubtask;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileBatchInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.util.concurrent.ExecutorService;

public class PipeConnectorSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtask.class);

  // For input and output
  private final BoundedBlockingPendingQueue<Event> inputPendingQueue;
  private final PipeConnector outputPipeConnector;

  // For thread pool to execute callbacks
  protected final DecoratingLock callbackDecoratingLock = new DecoratingLock();
  protected ExecutorService subtaskCallbackListeningExecutor;

  // For controlling subtask submitting, making sure that a subtask is submitted to only one thread
  // at a time
  protected volatile boolean isSubmitted = false;

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
    super(taskID, creationTime);
    this.attributeSortedString = attributeSortedString;
    this.connectorIndex = connectorIndex;
    this.inputPendingQueue = inputPendingQueue;
    this.outputPipeConnector = outputPipeConnector;
    PipeConnectorMetrics.getInstance().register(this);
  }

  @Override
  public void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService subtaskCallbackListeningExecutor,
      PipeSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskCallbackListeningExecutor = subtaskCallbackListeningExecutor;
    this.subtaskScheduler = subtaskScheduler;
  }

  @Override
  public Boolean call() throws Exception {
    final boolean hasAtLeastOneEventProcessed = super.call();

    // Wait for the callable to be decorated by Futures.addCallback in the executorService
    // to make sure that the callback can be submitted again on success or failure.
    callbackDecoratingLock.waitForDecorated();

    return hasAtLeastOneEventProcessed;
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
      } else if (event instanceof TsFileBatchInsertionEvent) {
        outputPipeConnector.transfer((TsFileBatchInsertionEvent) event);
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
  public synchronized void onSuccess(Boolean hasAtLeastOneEventProcessed) {
    isSubmitted = false;

    super.onSuccess(hasAtLeastOneEventProcessed);
  }

  @Override
  public synchronized void onFailure(@NotNull Throwable throwable) {
    isSubmitted = false;

    if (isClosed.get()) {
      LOGGER.info("onFailure in pipe transfer, ignored because pipe is dropped.");
      releaseLastEvent(false);
      return;
    }

    // Retry to connect to the target system if the connection is broken
    if (throwable instanceof PipeConnectionException) {
      LOGGER.warn(
          "PipeConnectionException occurred, retrying to connect to the target system...",
          throwable);

      int retry = 0;
      while (retry < MAX_RETRY_TIMES) {
        try {
          outputPipeConnector.handshake();
          LOGGER.info("Successfully reconnected to the target system.");
          break;
        } catch (Exception e) {
          retry++;
          LOGGER.warn(
              "Failed to reconnect to the target system, retrying ... "
                  + "after [{}/{}] time(s) retries.",
              retry,
              MAX_RETRY_TIMES,
              e);
          try {
            Thread.sleep(retry * PipeConfig.getInstance().getPipeConnectorRetryIntervalMs());
          } catch (InterruptedException interruptedException) {
            LOGGER.info(
                "Interrupted while sleeping, perhaps need to check "
                    + "whether the thread is interrupted.",
                interruptedException);
            Thread.currentThread().interrupt();
          }
        }
      }

      // Stop current pipe task if failed to reconnect to the target system after MAX_RETRY_TIMES
      // times
      if (retry == MAX_RETRY_TIMES) {
        if (lastEvent instanceof EnrichedEvent) {
          LOGGER.warn(
              "Failed to reconnect to the target system after {} times, "
                  + "stopping current pipe task {}... "
                  + "Status shown when query the pipe will be 'STOPPED'. "
                  + "Please restart the task by executing 'START PIPE' manually if needed.",
              MAX_RETRY_TIMES,
              taskID,
              throwable);

          ((EnrichedEvent) lastEvent)
              .reportException(
                  new PipeRuntimeConnectorCriticalException(
                      throwable.getMessage()
                          + ", root cause: "
                          + ErrorHandlingUtils.getRootCause(throwable).getMessage()));
        } else {
          LOGGER.error(
              "Failed to reconnect to the target system after {} times, "
                  + "stopping current pipe task {} locally... "
                  + "Status shown when query the pipe will be 'RUNNING' instead of 'STOPPED', "
                  + "but the task is actually stopped. "
                  + "Please restart the task by executing 'START PIPE' manually if needed.",
              MAX_RETRY_TIMES,
              taskID,
              throwable);
        }

        // Although the pipe task will be stopped, we still don't release the last event here
        // Because we need to keep it for the next retry. If user wants to restart the task,
        // the last event will be processed again. The last event will be released when the task
        // is dropped or the process is running normally.

        // Stop current pipe task if failed to reconnect to the target system after MAX_RETRY_TIMES
        return;
      }
    } else {
      LOGGER.warn(
          "A non-PipeConnectionException occurred, exception message: {}",
          throwable.getMessage(),
          throwable);
    }

    // Handle other exceptions as usual
    super.onFailure(new PipeRuntimeConnectorCriticalException(throwable.getMessage()));
  }

  /**
   * Submit a subTask to the executor to keep it running. Note that the function will be called when
   * connector starts or the subTask finishes the last round, Thus the "isRunning" sign is added to
   * avoid concurrent problem of the two, ensuring two or more submitting threads generates only one
   * winner.
   */
  @Override
  public synchronized void submitSelf() {
    if (shouldStopSubmittingSelf.get() || isSubmitted) {
      return;
    }

    callbackDecoratingLock.markAsDecorating();
    try {
      final ListenableFuture<Boolean> nextFuture = subtaskWorkerThreadPoolExecutor.submit(this);
      Futures.addCallback(nextFuture, this, subtaskCallbackListeningExecutor);
      isSubmitted = true;
    } finally {
      callbackDecoratingLock.markAsDecorated();
    }
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
}
