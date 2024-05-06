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

package org.apache.iotdb.commons.pipe.task.subtask;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.task.DecoratingLock;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public abstract class PipeAbstractConnectorSubtask extends PipeReportableSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeAbstractConnectorSubtask.class);

  // For output (transfer events to the target system in connector)
  protected PipeConnector outputPipeConnector;

  // For thread pool to execute callbacks
  protected final DecoratingLock callbackDecoratingLock = new DecoratingLock();
  protected ExecutorService subtaskCallbackListeningExecutor;

  // For controlling subtask submitting, making sure that
  // a subtask is submitted to only one thread at a time
  protected volatile boolean isSubmitted = false;

  protected PipeAbstractConnectorSubtask(
      String taskID, long creationTime, PipeConnector outputPipeConnector) {
    super(taskID, creationTime);
    this.outputPipeConnector = outputPipeConnector;
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
  public synchronized void onSuccess(Boolean hasAtLeastOneEventProcessed) {
    isSubmitted = false;

    super.onSuccess(hasAtLeastOneEventProcessed);
  }

  @Override
  public synchronized void onFailure(Throwable throwable) {
    isSubmitted = false;

    if (isClosed.get()) {
      LOGGER.info("onFailure in pipe transfer, ignored because pipe is dropped.", throwable);
      clearReferenceCountAndReleaseLastEvent();
      return;
    }

    if (throwable instanceof PipeConnectionException) {
      // Retry to connect to the target system if the connection is broken
      // We should reconstruct the client before re-submit the subtask
      if (onPipeConnectionException(throwable)) {
        // return if the pipe task should be stopped
        return;
      }
    }

    // Handle exceptions if any available clients exist
    // Notice that the PipeRuntimeConnectorCriticalException must be thrown here
    // because the upper layer relies on this to stop all the related pipe tasks
    // Other exceptions may cause the subtask to stop forever and can not be restarted
    super.onFailure(
        throwable instanceof PipeRuntimeConnectorCriticalException
            ? throwable
            : new PipeRuntimeConnectorCriticalException(throwable.getMessage()));
  }

  /**
   * @return {@code true} if the {@link PipeSubtask} should be stopped, {@code false} otherwise
   */
  private boolean onPipeConnectionException(Throwable throwable) {
    LOGGER.warn(
        "PipeConnectionException occurred, {} retries to handshake with the target system.",
        outputPipeConnector.getClass().getName(),
        throwable);

    int retry = 0;
    while (retry < MAX_RETRY_TIMES) {
      try {
        outputPipeConnector.handshake();
        LOGGER.info(
            "{} handshakes with the target system successfully.",
            outputPipeConnector.getClass().getName());
        break;
      } catch (Exception e) {
        retry++;
        LOGGER.warn(
            "{} failed to handshake with the target system for {} times, "
                + "will retry at most {} times.",
            outputPipeConnector.getClass().getName(),
            retry,
            MAX_RETRY_TIMES,
            e);
        try {
          Thread.sleep(retry * PipeConfig.getInstance().getPipeConnectorRetryIntervalMs());
        } catch (InterruptedException interruptedException) {
          LOGGER.info(
              "Interrupted while sleeping, will retry to handshake with the target system.",
              interruptedException);
          Thread.currentThread().interrupt();
        }
      }
    }

    // Stop current pipe task directly if failed to reconnect to
    // the target system after MAX_RETRY_TIMES times
    if (retry == MAX_RETRY_TIMES && lastEvent instanceof EnrichedEvent) {
      report(
          (EnrichedEvent) lastEvent,
          new PipeRuntimeConnectorCriticalException(
              throwable.getMessage() + ", root cause: " + getRootCause(throwable)));
      LOGGER.warn(
          "{} failed to handshake with the target system after {} times, "
              + "stopping current subtask {} (creation time: {}, simple class: {}). "
              + "Status shown when query the pipe will be 'STOPPED'. "
              + "Please restart the task by executing 'START PIPE' manually if needed.",
          outputPipeConnector.getClass().getName(),
          MAX_RETRY_TIMES,
          taskID,
          creationTime,
          this.getClass().getSimpleName(),
          throwable);

      // Although the pipe task will be stopped, we still don't release the last event here
      // Because we need to keep it for the next retry. If user wants to restart the task,
      // the last event will be processed again. The last event will be released when the task
      // is dropped or the process is running normally.

      // Stop current pipe task if failed to reconnect to the target system after MAX_RETRY_TIMES
      return true;
    }

    // For non enriched event, forever retry.
    // For enriched event, retry if connection is set up successfully.
    return false;
  }

  /**
   * Submit a {@link PipeSubtask} to the executor to keep it running. Note that the function will be
   * called when connector starts or the subTask finishes the last round, Thus the {@link
   * PipeAbstractConnectorSubtask#isSubmitted} sign is added to avoid concurrent problem of the two,
   * ensuring two or more submitting threads generates only one winner.
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
}
