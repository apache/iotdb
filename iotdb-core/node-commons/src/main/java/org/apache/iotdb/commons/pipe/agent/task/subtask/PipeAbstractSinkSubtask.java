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

package org.apache.iotdb.commons.pipe.agent.task.subtask;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeSinkNonReportTimeConfigurableException;
import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.utils.ErrorHandlingCommonUtils;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.tsfile.external.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class PipeAbstractSinkSubtask extends PipeReportableSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeAbstractSinkSubtask.class);

  // For output (transfer events to the target system in connector)
  protected PipeConnector outputPipeSink;

  // For thread pool to execute callbacks
  protected ExecutorService subtaskCallbackListeningExecutor;

  // For controlling subtask submitting, making sure that
  // a subtask is submitted to only one thread at a time
  protected volatile boolean isSubmitted = false;

  // For cleaning up the last event when the pipe is dropped
  @SuppressWarnings("java:S3077")
  protected volatile Event lastExceptionEvent;

  protected long sleepInterval = PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalInitMs();
  protected long lastExceptionTime = Long.MAX_VALUE;

  protected PipeAbstractSinkSubtask(
      final String taskID, final long creationTime, final PipeConnector outputPipeSink) {
    super(taskID, creationTime);
    this.outputPipeSink = outputPipeSink;
  }

  @Override
  public void bindExecutors(
      final ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      final ListeningScheduledExecutorService subtaskWorkerScheduledExecutor,
      final ExecutorService subtaskCallbackListeningExecutor,
      final PipeSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskWorkerScheduledExecutor = subtaskWorkerScheduledExecutor;
    this.subtaskCallbackListeningExecutor = subtaskCallbackListeningExecutor;
    this.subtaskScheduler = subtaskScheduler;
  }

  @Override
  public void onSuccess(final Boolean hasAtLeastOneEventProcessed) {
    preScheduleLowPriorityTask(100);

    synchronized (this) {
      isSubmitted = false;

      super.onSuccess(hasAtLeastOneEventProcessed);
    }
  }

  @Override
  public void onFailure(final Throwable throwable) {
    preScheduleLowPriorityTask(100);

    synchronized (this) {
      isSubmitted = false;

      if (tryIgnoreFailure(throwable)) {
        return;
      }
    }

    if (throwable instanceof PipeConnectionException) {
      // Retry to connect to the target system if the connection is broken. Do not hold the subtask
      // lock here because handshaking with an external sink may block for a long time, while drop
      // pipe needs the same lock to discard in-flight events.
      if (onPipeConnectionException(throwable)) {
        return;
      }

      synchronized (this) {
        if (tryIgnoreFailure(throwable)) {
          return;
        }

        if (PipeConfig.getInstance().isPipeSinkRetryLocallyForConnectionError()) {
          super.onFailure(
              new PipeRuntimeSinkNonReportTimeConfigurableException(
                  throwable.getMessage(), Long.MAX_VALUE));
          return;
        }
      }
    }

    synchronized (this) {
      if (tryIgnoreFailure(throwable)) {
        return;
      }
      handleFailure(throwable);
    }
  }

  private boolean tryIgnoreFailure(final Throwable throwable) {
    if (isClosed.get()) {
      LOGGER.info(PipeMessages.ON_FAILURE_IGNORED_CONNECTOR_DROPPED, throwable);
      clearReferenceCountAndReleaseLastEvent(null);
      return true;
    }

    // We assume that the event is cleared as the "lastEvent" in processor subtask and reaches the
    // connector subtask. Then, it may fail because of released resource and block the other pipes
    // using the same connector. We simply discard it.
    if (lastExceptionEvent instanceof EnrichedEvent
        && ((EnrichedEvent) lastExceptionEvent).isReleased()) {
      LOGGER.info(PipeMessages.ON_FAILURE_IGNORED_EVENT_RELEASED, throwable);
      submitSelf();
      return true;
    }

    // If lastExceptionEvent != lastEvent, it indicates that the lastEvent's reference has been
    // changed because the pipe of it has been dropped. In that case, we just discard the event.
    if (lastEvent != lastExceptionEvent) {
      LOGGER.info(PipeMessages.ON_FAILURE_IGNORED_EVENT_PIPE_DROPPED, throwable);
      clearReferenceCountAndReleaseLastExceptionEvent();
      submitSelf();
      return true;
    }

    return false;
  }

  private void handleFailure(final Throwable throwable) {
    // Handle exceptions if any available clients exist
    // Notice that the PipeRuntimeConnectorCriticalException must be thrown here
    // because the upper layer relies on this to stop all the related pipe tasks
    // Other exceptions may cause the subtask to stop forever and can not be restarted
    if (throwable instanceof PipeRuntimeSinkCriticalException) {
      super.onFailure(throwable);
    } else {
      // Print stack trace for better debugging
      PipeLogger.log(
          LOGGER::warn, throwable, PipeMessages.NON_CRITICAL_EXCEPTION_WILL_THROW_CRITICAL);
      super.onFailure(new PipeRuntimeSinkCriticalException(throwable.getMessage()));
    }
  }

  /**
   * @return {@code true} if the {@link PipeSubtask} should be stopped, {@code false} otherwise
   */
  private boolean onPipeConnectionException(final Throwable throwable) {
    PipeLogger.log(
        LOGGER::warn,
        throwable,
        PipeMessages.PIPE_CONNECTION_EXCEPTION_RETRYING,
        outputPipeSink.getClass().getName());

    int retry = 0;
    while (retry < MAX_RETRY_TIMES) {
      try {
        if (!handshakeOutputPipeSink()) {
          return false;
        }
        LOGGER.info(PipeMessages.HANDSHAKE_SUCCESS, outputPipeSink.getClass().getName());
        break;
      } catch (final Exception e) {
        retry++;
        LOGGER.warn(
            PipeMessages.HANDSHAKE_FAILED_RETRYING,
            outputPipeSink.getClass().getName(),
            retry,
            MAX_RETRY_TIMES,
            e);
        try {
          sleepIfNoHighPriorityTask(getHandshakeRetrySleepInterval(e, retry));
        } catch (final InterruptedException interruptedException) {
          LOGGER.info(
              PipeMessages.INTERRUPTED_WHILE_SLEEPING_RETRY_HANDSHAKE, interruptedException);
          Thread.currentThread().interrupt();
        }
      }
    }

    // Stop current pipe task directly if failed to reconnect to
    // the target system after MAX_RETRY_TIMES times
    if (retry == MAX_RETRY_TIMES
        && lastEvent instanceof EnrichedEvent
        && !PipeConfig.getInstance().isPipeSinkRetryLocallyForConnectionError()) {
      report(
          (EnrichedEvent) lastEvent,
          new PipeRuntimeSinkCriticalException(
              throwable.getMessage()
                  + PipeMessages.EXCEPTION_ROOT_CAUSE_A22E94DE
                  + getRootCause(throwable)));
      LOGGER.warn(
          PipeMessages.HANDSHAKE_FAILED_STOPPING,
          outputPipeSink.getClass().getName(),
          MAX_RETRY_TIMES,
          getDisplayTaskID(),
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

  protected boolean handshakeOutputPipeSink() throws Exception {
    outputPipeSink.handshake();
    return true;
  }

  private long getHandshakeRetrySleepInterval(final Throwable throwable, final int retry) {
    final long defaultInterval = retry * PipeConfig.getInstance().getPipeSinkRetryIntervalMs();
    return isAuthenticationFailure(throwable)
        ? Math.max(defaultInterval, AUTHENTICATION_FAILURE_RETRY_INTERVAL_MS)
        : defaultInterval;
  }

  /**
   * Submit a {@link PipeSubtask} to the executor to keep it running. Note that the function will be
   * called when connector starts or the subTask finishes the last round, Thus the {@link
   * PipeAbstractSinkSubtask#isSubmitted} sign is added to avoid concurrent problem of the two,
   * ensuring two or more submitting threads generates only one winner.
   */
  @Override
  public synchronized void submitSelf() {
    if (shouldStopSubmittingSelf.get() || isSubmitted) {
      return;
    }

    final long schedulingDelayInMs = getNextSchedulingDelayInMs();
    if (schedulingDelayInMs > 0) {
      isSubmitted = true;
      subtaskWorkerScheduledExecutor.schedule(
          // Keep the isSubmitted placeholder set before the delayed submission to avoid duplicate
          // schedules, so the delayed task should not mark it again.
          () -> submitSelfToWorker(false), schedulingDelayInMs, TimeUnit.MILLISECONDS);
      return;
    }

    submitSelfToWorker(true);
  }

  @Override
  protected boolean shouldStopSubmittingSelfInCurrentCall() {
    return peekSchedulingDelayInMs() > 0;
  }

  private synchronized void submitSelfToWorker(final boolean shouldMarkSubmitted) {
    if (shouldStopSubmittingSelf.get()) {
      isSubmitted = false;
      return;
    }

    final ListenableFuture<Boolean> nextFuture = subtaskWorkerThreadPoolExecutor.submit(this);
    registerCallbackHookAfterSubmit(nextFuture);
    if (shouldMarkSubmitted) {
      isSubmitted = true;
    }
  }

  private long getNextSchedulingDelayInMs() {
    return consumeSchedulingDelayInMs();
  }

  protected long peekSchedulingDelayInMs() {
    return 0;
  }

  protected long consumeSchedulingDelayInMs() {
    return 0;
  }

  protected void registerCallbackHookAfterSubmit(final ListenableFuture<Boolean> future) {
    Futures.addCallback(future, this, subtaskCallbackListeningExecutor);
  }

  protected synchronized void setLastExceptionEvent(final Event event) {
    lastExceptionEvent = event;
  }

  protected synchronized void clearReferenceCountAndReleaseLastExceptionEvent() {
    if (lastExceptionEvent != null) {
      if (lastExceptionEvent instanceof EnrichedEvent
          && !((EnrichedEvent) lastExceptionEvent).isReleased()) {
        ((EnrichedEvent) lastExceptionEvent).clearReferenceCount(PipeSubtask.class.getName());
      }
      lastExceptionEvent = null;
    }
  }

  public void sleep4NonReportException() {
    if (sleepInterval < PipeConfig.getInstance().getPipeSinkSubtaskSleepIntervalMaxMs()) {
      sleepInterval <<= 1;
    }
    try {
      Thread.sleep(sleepInterval);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  protected void handleException(final Event event, final Exception e) {
    if (e instanceof PipeRuntimeOutOfMemoryCriticalException
        || ExceptionUtils.getRootCause(e) instanceof PipeRuntimeOutOfMemoryCriticalException) {
      PipeLogger.log(LOGGER::info, e, PipeMessages.TEMPORARILY_OUT_OF_MEMORY);
    } else if (e instanceof PipeRuntimeSinkNonReportTimeConfigurableException) {
      if (lastExceptionTime == Long.MAX_VALUE) {
        lastExceptionTime = System.currentTimeMillis();
      }
      if (System.currentTimeMillis() - lastExceptionTime
          < ((PipeRuntimeSinkNonReportTimeConfigurableException) e).getInterval()) {
        sleep4NonReportException();
        return;
      }
      handlePipeException(event, (PipeException) e);
    } else if (e instanceof PipeException) {
      handlePipeException(event, (PipeException) e);
    } else {
      if (!isClosed.get()) {
        setLastExceptionEvent(event);
        throw new PipeException(
            String.format(
                PipeMessages.EXCEPTION_IN_PIPE_TRANSFER_FORMAT,
                getDisplayTaskID(),
                event instanceof EnrichedEvent
                    ? ((EnrichedEvent) event).coreReportMessage()
                    : event,
                ErrorHandlingCommonUtils.getRootCause(e).getMessage()),
            e);
      } else {
        LOGGER.info(
            PipeMessages.EXCEPTION_IN_PIPE_TRANSFER_IGNORED,
            e.getMessage() != null ? " Message: " + e.getMessage() : "");
        clearReferenceCountAndReleaseLastEvent(event);
      }
    }
  }

  protected void handlePipeException(final Event event, final PipeException e) {
    if (!isClosed.get()) {
      setLastExceptionEvent(event);
      throw e;
    } else {
      LOGGER.info(
          PipeMessages.PIPE_EXCEPTION_IGNORED,
          e.getClass().getSimpleName(),
          e.getMessage() != null ? " Message: " + e.getMessage() : "");
      clearReferenceCountAndReleaseLastEvent(event);
    }
  }
}
