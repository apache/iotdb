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

package org.apache.iotdb.db.pipe.task.subtask;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConnectorSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConnectorSubtask.class);

  private final BoundedBlockingPendingQueue<Event> inputPendingQueue;
  private final PipeConnector outputPipeConnector;

  private static final int HEARTBEAT_CHECK_INTERVAL = 1000;
  private int executeOnceInvokedTimes;

  /** @param taskID connectorAttributeSortedString */
  public PipeConnectorSubtask(
      String taskID,
      BoundedBlockingPendingQueue<Event> inputPendingQueue,
      PipeConnector outputPipeConnector) {
    super(taskID);
    this.inputPendingQueue = inputPendingQueue;
    this.outputPipeConnector = outputPipeConnector;
    executeOnceInvokedTimes = 0;
  }

  @Override
  protected synchronized boolean executeOnce() {
    try {
      if (executeOnceInvokedTimes++ % HEARTBEAT_CHECK_INTERVAL == 0) {
        outputPipeConnector.heartbeat();
      }
    } catch (Exception e) {
      throw new PipeConnectionException(
          "PipeConnector: failed to connect to the target system.", e);
    }

    final Event event = lastEvent != null ? lastEvent : inputPendingQueue.poll();
    // record this event for retrying on connection failure or other exceptions
    lastEvent = event;
    if (event == null) {
      return false;
    }

    try {
      if (event instanceof TabletInsertionEvent) {
        outputPipeConnector.transfer((TabletInsertionEvent) event);
      } else if (event instanceof TsFileInsertionEvent) {
        outputPipeConnector.transfer((TsFileInsertionEvent) event);
      } else {
        outputPipeConnector.transfer(event);
      }

      releaseLastEvent();
    } catch (PipeConnectionException e) {
      throw e;
    } catch (Exception e) {
      throw new PipeException(
          "Error occurred during executing PipeConnector#transfer, perhaps need to check "
              + "whether the implementation of PipeConnector is correct "
              + "according to the pipe-api description.",
          e);
    }

    return true;
  }

  @Override
  public void onFailure(@NotNull Throwable throwable) {
    // retry to connect to the target system if the connection is broken
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

      // stop current pipe task if failed to reconnect to the target system after MAX_RETRY_TIMES
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
              .reportException(new PipeRuntimeConnectorCriticalException(throwable.getMessage()));
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

          // FIXME: non-EnrichedEvent should be reported to the ConfigNode instead of being logged
        }

        // although the pipe task will be stopped, we still don't release the last event here
        // because we need to keep it for the next retry. if user wants to restart the task,
        // the last event will be processed again. the last event will be released when the task
        // is dropped or the process is running normally.

        // stop current pipe task if failed to reconnect to the target system after MAX_RETRY_TIMES
        return;
      }
    } else {
      LOGGER.warn(
          "A non-PipeConnectionException occurred, exception message: {}",
          throwable.getMessage(),
          throwable);
    }

    // handle other exceptions as usual
    super.onFailure(new PipeRuntimeConnectorCriticalException(throwable.getMessage()));
  }

  @Override
  // synchronized for outputPipeConnector.close() and releaseLastEvent() in super.close()
  // make sure that the lastEvent will not be updated after pipeProcessor.close() to avoid
  // resource leak because of the lastEvent is not released.
  public synchronized void close() {
    try {
      outputPipeConnector.close();

      // should be called after outputPipeConnector.close()
      super.close();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.info(
          "Error occurred during closing PipeConnector, perhaps need to check whether the "
              + "implementation of PipeConnector is correct according to the pipe-api description.",
          e);
    }
  }
}
