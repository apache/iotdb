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

package org.apache.iotdb.db.pipe.agent.task.subtask.processor;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.processor.donothing.DoNothingProcessor;
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeReportableSubtask;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.pipe.resource.PipeResourceFailureType;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.commons.utils.ErrorHandlingCommonUtils;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.processor.PipeProcessorMetrics;
import org.apache.iotdb.db.pipe.processor.pipeconsensus.PipeConsensusProcessor;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class PipeProcessorSubtask extends PipeReportableSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtask.class);

  private static final ExecutorService TS_FILE_PARSER_EXECUTOR =
      IoTDBThreadPoolFactory.newCachedThreadPool(
          ThreadName.PIPE_TSFILE_PARSER_EXECUTOR_POOL.getName());

  private static final AtomicReference<PipeProcessorSubtaskWorkerManager> subtaskWorkerManager =
      new AtomicReference<>();

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String pipeName;
  private final String pipeNameWithCreationTime; // cache for better performance
  private final int regionId;

  private final EventSupplier inputEventSupplier;
  private final PipeProcessor pipeProcessor;
  private final PipeEventCollector outputEventCollector;

  private final int tsFileParserParallelism;
  private final Object tsFileParserTaskLock = new Object();
  private final Deque<TsFileParserTask> inFlightTsFileParserTasks = new ArrayDeque<>();
  private Event pendingEventAfterTsFileParserBarrier;
  private volatile PipeTsFileInsertionEvent retryingFailedTsFileParserEvent;

  // This variable is used to distinguish between old and new subtasks before and after stuck
  // restart.
  private final long subtaskCreationTime;

  public PipeProcessorSubtask(
      final String taskID,
      final String pipeName,
      final long creationTime,
      final int regionId,
      final EventSupplier inputEventSupplier,
      final PipeProcessor pipeProcessor,
      final PipeEventCollector outputEventCollector) {
    this(
        taskID,
        pipeName,
        creationTime,
        regionId,
        inputEventSupplier,
        pipeProcessor,
        outputEventCollector,
        1);
  }

  public PipeProcessorSubtask(
      final String taskID,
      final String pipeName,
      final long creationTime,
      final int regionId,
      final EventSupplier inputEventSupplier,
      final PipeProcessor pipeProcessor,
      final PipeEventCollector outputEventCollector,
      final int tsFileParserParallelism) {
    super(taskID, creationTime);
    this.pipeName = pipeName;
    this.pipeNameWithCreationTime = pipeName + "_" + creationTime;
    this.regionId = regionId;
    this.inputEventSupplier = inputEventSupplier;
    this.pipeProcessor = pipeProcessor;
    this.outputEventCollector = outputEventCollector;
    this.tsFileParserParallelism =
        pipeProcessor.getClass() == DoNothingProcessor.class
            ? Math.max(1, tsFileParserParallelism)
            : 1;
    this.subtaskCreationTime = System.currentTimeMillis();

    // Only register dataRegions
    if (StorageEngine.getInstance().getAllDataRegionIds().contains(new DataRegionId(regionId))) {
      PipeProcessorMetrics.getInstance().register(this);
    }
  }

  @Override
  public void bindExecutors(
      final ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      final ExecutorService ignored,
      final PipeSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskScheduler = subtaskScheduler;

    // double check locking for constructing PipeProcessorSubtaskWorkerManager
    if (subtaskWorkerManager.get() == null) {
      synchronized (PipeProcessorSubtaskWorkerManager.class) {
        if (subtaskWorkerManager.get() == null) {
          subtaskWorkerManager.set(
              new PipeProcessorSubtaskWorkerManager(subtaskWorkerThreadPoolExecutor));
        }
      }
    }
    subtaskWorkerManager.get().schedule(this);
  }

  @Override
  protected boolean executeOnce() throws Exception {
    if (isClosed.get()) {
      return false;
    }

    // Preserve the event currently being retried. Other parser failures are reaped after this
    // event has been resubmitted, otherwise a later failure could overwrite lastEvent.
    final TsFileParserTaskResult failedResult =
        lastEvent == null ? reapCompletedTsFileParserTasks() : null;
    if (failedResult != null) {
      if (!retainFailedTsFileParserEvent(failedResult.event)) {
        return false;
      }
      if (ExceptionUtils.getRootCause(failedResult.exception)
          instanceof PipeRuntimeOutOfMemoryCriticalException) {
        PipeLogger.log(
            LOGGER::info,
            "Temporarily out of memory in parallel TsFile parsing, will wait for memory to release. Message: %s",
            failedResult.exception.getMessage());
        return false;
      }
      retryingFailedTsFileParserEvent = failedResult.event;
      throw new PipeException(
          String.format(
              "Exception in parallel TsFile parsing, subtask: %s, event: %s, root cause: %s",
              taskID,
              failedResult.event.coreReportMessage(),
              ErrorHandlingCommonUtils.getRootCause(failedResult.exception).getMessage()),
          failedResult.exception);
    }

    final Event event = getNextEvent();

    if (Objects.isNull(event)) {
      return false;
    }

    if (shouldParseTsFileEventInPool(event)) {
      final PipeTsFileInsertionEvent tsFileInsertionEvent = (PipeTsFileInsertionEvent) event;
      if (!tsFileInsertionEvent.tryReserveTsFileParserMemory()) {
        return false;
      }

      try {
        outputEventCollector.prepareTsFileEventForParallelParsing(tsFileInsertionEvent);
        submitTsFileParserTask(tsFileInsertionEvent);
        setLastEvent(null);
        return true;
      } catch (final Exception e) {
        tsFileInsertionEvent.close();
        throw e;
      }
    }

    if (event != retryingFailedTsFileParserEvent && deferEventUntilTsFileParserBarrier(event)) {
      setLastEvent(null);
      return false;
    }

    outputEventCollector.resetFlags();
    try {
      // event can be supplied after the subtask is closed, so we need to check isClosed here
      if (!isClosed.get()) {
        if (event instanceof TabletInsertionEvent) {
          pipeProcessor.process((TabletInsertionEvent) event, outputEventCollector);
          PipeProcessorMetrics.getInstance().markTabletEvent(taskID);
        } else if (event instanceof TsFileInsertionEvent) {
          pipeProcessor.process((TsFileInsertionEvent) event, outputEventCollector);
          PipeProcessorMetrics.getInstance().markTsFileEvent(taskID);
          PipeDataNodeSinglePipeMetrics.getInstance()
              .markTsFileCollectInvocationCount(
                  pipeNameWithCreationTime, outputEventCollector.getCollectInvocationCount());
        } else if (event instanceof PipeHeartbeatEvent) {
          pipeProcessor.process(event, outputEventCollector);
          ((PipeHeartbeatEvent) event).onProcessed();
          PipeProcessorMetrics.getInstance().markPipeHeartbeatEvent(taskID);
        } else {
          pipeProcessor.process(
              event instanceof UserDefinedEnrichedEvent
                  ? ((UserDefinedEnrichedEvent) event).getUserDefinedEvent()
                  : event,
              outputEventCollector);
        }
      }

      final boolean shouldReport =
          !isClosed.get()
              // If an event does not generate any events except itself at this stage, it is divided
              // into two categories:
              // 1. If the event is collected and passed to the connector, the reference count of
              // the event may eventually be zero in the processor (the connector reduces the
              // reference count first, and then the processor reduces the reference count), at this
              // time, the progress of the event needs to be reported.
              // 2. If the event is not collected (not passed to the connector), the reference count
              // of the event must be zero in the processor stage, at this time, the progress of the
              // event needs to be reported.
              && (outputEventCollector.hasNoGeneratedEvent()
                  || event instanceof PipeTsFileInsertionEvent
                      && ((PipeTsFileInsertionEvent) event).isProgressReportManagedByTsFileParser())
              // If the event's reference count cannot be increased, it means that the event has
              // been released, and the progress of the event can not be reported.
              && !outputEventCollector.isFailedToIncreaseReferenceCount()
              // Events generated from consensusPipe's transferred data should never be reported.
              && !(pipeProcessor instanceof PipeConsensusProcessor);
      if (!shouldReport
          && event instanceof PipeTsFileInsertionEvent
          && ((PipeTsFileInsertionEvent) event).isProgressReportManagedByTsFileParser()) {
        ((PipeTsFileInsertionEvent) event).abortProgressReportManagedByTsFileParser();
        ((PipeTsFileInsertionEvent) event).skipReportOnCommit();
      }
      if (shouldReport
          && event instanceof EnrichedEvent
          && outputEventCollector.hasNoCollectInvocationAfterReset()
          && ((EnrichedEvent) event).getCommitId() <= EnrichedEvent.NO_COMMIT_ID) {
        // An event should be reported here when it is not passed to the connector stage, and it
        // does not generate any new events to be passed to the connector. In our system, before
        // reporting an event, we need to enrich a commitKey and commitId, which is done in the
        // collector stage. But for the event that not passed to the connector and not generate any
        // new events, the collector stage is not triggered, so we need to enrich the commitKey and
        // commitId here.
        PipeEventCommitManager.getInstance()
            .enrichWithCommitterKeyAndCommitId((EnrichedEvent) event, creationTime, regionId);
      }
      decreaseReferenceCountAndReleaseLastEvent(event, shouldReport);
      if (event == retryingFailedTsFileParserEvent) {
        retryingFailedTsFileParserEvent = null;
      }
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      recordResourceFailure(event, PipeResourceFailureType.MEMORY_TIMEOUT);
      PipeLogger.log(
          LOGGER::info,
          "Temporarily out of memory in pipe event processing, will wait for the memory to release. Message: %s",
          e.getMessage());
      return false;
    } catch (final Exception e) {
      if (ExceptionUtils.getRootCause(e) instanceof PipeRuntimeOutOfMemoryCriticalException) {
        recordResourceFailure(event, PipeResourceFailureType.MEMORY_TIMEOUT);
        PipeLogger.log(
            LOGGER::info,
            "Temporarily out of memory in pipe event processing, will wait for the memory to release. Message: %s",
            e.getMessage());
        return false;
      }
      if (!isClosed.get()) {
        throw new PipeException(
            String.format(
                "Exception in pipe process, subtask: %s, last event: %s, root cause: %s",
                taskID,
                lastEvent instanceof EnrichedEvent
                    ? ((EnrichedEvent) lastEvent).coreReportMessage()
                    : lastEvent,
                ErrorHandlingCommonUtils.getRootCause(e).getMessage()),
            e);
      } else {
        LOGGER.info(
            "Exception in pipe event processing, ignored because pipe is dropped.{}",
            e.getMessage() != null ? " Message: " + e.getMessage() : "");
        clearReferenceCountAndReleaseLastEvent(event);
      }
    }

    return true;
  }

  private Event getNextEvent() throws Exception {
    if (lastEvent != null) {
      return lastEvent;
    }

    synchronized (tsFileParserTaskLock) {
      if (pendingEventAfterTsFileParserBarrier != null) {
        if (!inFlightTsFileParserTasks.isEmpty()) {
          return null;
        }
        final Event event = pendingEventAfterTsFileParserBarrier;
        pendingEventAfterTsFileParserBarrier = null;
        setLastEvent(event);
        return event;
      }

      if (inFlightTsFileParserTasks.size() >= tsFileParserParallelism) {
        return null;
      }
    }

    final Event event = UserDefinedEnrichedEvent.maybeOf(inputEventSupplier.supply());
    setLastEvent(event);
    return event;
  }

  private synchronized boolean retainFailedTsFileParserEvent(final PipeTsFileInsertionEvent event) {
    if (isClosed.get()) {
      if (!event.isReleased()) {
        event.clearReferenceCount(PipeProcessorSubtask.class.getName());
      }
      return false;
    }
    lastEvent = event;
    return true;
  }

  private boolean shouldParseTsFileEventInPool(final Event event) {
    return tsFileParserParallelism > 1
        && event instanceof PipeTsFileInsertionEvent
        && event != retryingFailedTsFileParserEvent
        && outputEventCollector.shouldParseTsFileEvent((PipeTsFileInsertionEvent) event);
  }

  private boolean deferEventUntilTsFileParserBarrier(final Event event) {
    // These control events do not depend on parser completion. ProgressReportEvent is committed
    // after preceding parser tasks, while PipeHeartbeatEvent does not need ordered commits. Let
    // them pass so they do not split a run of parseable TsFiles into small parser batches.
    if (event instanceof ProgressReportEvent || event instanceof PipeHeartbeatEvent) {
      return false;
    }

    synchronized (tsFileParserTaskLock) {
      if (isClosed.get() || inFlightTsFileParserTasks.isEmpty()) {
        return false;
      }
      pendingEventAfterTsFileParserBarrier = event;
      return true;
    }
  }

  private void submitTsFileParserTask(final PipeTsFileInsertionEvent event) {
    final TsFileParserTask task =
        new TsFileParserTask(event, outputEventCollector.forkForTsFileParser());
    synchronized (tsFileParserTaskLock) {
      if (isClosed.get()) {
        task.cancel();
        return;
      }
      inFlightTsFileParserTasks.addLast(task);
    }

    try {
      task.setFuture(TS_FILE_PARSER_EXECUTOR.submit(task::execute));
    } catch (final RuntimeException e) {
      synchronized (tsFileParserTaskLock) {
        inFlightTsFileParserTasks.remove(task);
      }
      task.cancel();
      throw e;
    }
  }

  private TsFileParserTaskResult reapCompletedTsFileParserTasks()
      throws InterruptedException, ExecutionException {
    while (true) {
      final TsFileParserTask task;
      synchronized (tsFileParserTaskLock) {
        TsFileParserTask completedTask = null;
        final Iterator<TsFileParserTask> iterator = inFlightTsFileParserTasks.iterator();
        while (iterator.hasNext()) {
          final TsFileParserTask candidate = iterator.next();
          if (candidate.isDone()) {
            completedTask = candidate;
            iterator.remove();
            break;
          }
        }
        task = completedTask;
        if (task == null) {
          return null;
        }
      }

      final TsFileParserTaskResult result;
      try {
        result = task.getResult();
      } catch (final CancellationException e) {
        if (isClosed.get()) {
          return null;
        }
        throw e;
      }
      if (result.exception != null) {
        return result;
      }
    }
  }

  private void completeTsFileParserTask(
      final PipeTsFileInsertionEvent event, final PipeEventCollector eventCollector) {
    final boolean shouldReport =
        !isClosed.get()
            && (event.isProgressReportManagedByTsFileParser()
                || eventCollector.hasNoGeneratedEvent())
            && !eventCollector.isFailedToIncreaseReferenceCount();
    if (!shouldReport && event.isProgressReportManagedByTsFileParser()) {
      event.abortProgressReportManagedByTsFileParser();
      event.skipReportOnCommit();
    }
    if (shouldReport
        && eventCollector.hasNoCollectInvocationAfterReset()
        && event.getCommitId() <= EnrichedEvent.NO_COMMIT_ID) {
      PipeEventCommitManager.getInstance()
          .enrichWithCommitterKeyAndCommitId(event, creationTime, regionId);
    }

    if (!event.isReleased()) {
      event.decreaseReferenceCount(PipeProcessorSubtask.class.getName(), shouldReport);
    }
  }

  private class TsFileParserTask {

    private final PipeTsFileInsertionEvent event;
    private final PipeEventCollector eventCollector;

    private Future<TsFileParserTaskResult> future;
    private boolean isStarted;
    private boolean isFinished;
    private boolean isCancelled;
    private boolean ownsEvent = true;

    private TsFileParserTask(
        final PipeTsFileInsertionEvent event, final PipeEventCollector eventCollector) {
      this.event = event;
      this.eventCollector = eventCollector;
    }

    private TsFileParserTaskResult execute() {
      synchronized (this) {
        if (isCancelled) {
          return TsFileParserTaskResult.cancelled(event);
        }
        isStarted = true;
      }

      try {
        eventCollector.resetFlags();
        eventCollector.collect(event);
        event.close();

        synchronized (this) {
          if (isCancelled || isClosed.get()) {
            releaseOwnedEvent();
            isFinished = true;
            return TsFileParserTaskResult.cancelled(event);
          }
        }

        PipeProcessorMetrics.getInstance().markTsFileEvent(taskID);
        PipeDataNodeSinglePipeMetrics.getInstance()
            .markTsFileCollectInvocationCount(
                pipeNameWithCreationTime, eventCollector.getCollectInvocationCount());
        completeTsFileParserTask(event, eventCollector);
        synchronized (this) {
          ownsEvent = false;
          isFinished = true;
        }
        return TsFileParserTaskResult.success(event);
      } catch (final Exception e) {
        event.releaseTsFileParserMemoryIfReserved();
        synchronized (this) {
          if (isCancelled || isClosed.get()) {
            releaseOwnedEvent();
            isFinished = true;
            return TsFileParserTaskResult.cancelled(event);
          }
          isFinished = true;
        }
        return TsFileParserTaskResult.failure(event, e);
      }
    }

    private synchronized void setFuture(final Future<TsFileParserTaskResult> future) {
      this.future = future;
      if (isCancelled) {
        future.cancel(true);
      }
    }

    private synchronized boolean isDone() {
      return future != null && future.isDone();
    }

    private TsFileParserTaskResult getResult() throws InterruptedException, ExecutionException {
      final Future<TsFileParserTaskResult> currentFuture;
      synchronized (this) {
        currentFuture = future;
      }
      final TsFileParserTaskResult result = currentFuture.get();
      if (result.exception != null) {
        synchronized (this) {
          ownsEvent = false;
        }
      }
      return result;
    }

    private void cancel() {
      final Future<TsFileParserTaskResult> currentFuture;
      synchronized (this) {
        isCancelled = true;
        if ((!isStarted || isFinished) && ownsEvent) {
          releaseOwnedEvent();
        }
        currentFuture = future;
      }
      if (currentFuture != null) {
        currentFuture.cancel(true);
      }
    }

    private void releaseOwnedEvent() {
      if (!ownsEvent) {
        return;
      }
      event.close();
      if (!event.isReleased()) {
        event.clearReferenceCount(PipeProcessorSubtask.class.getName());
      }
      ownsEvent = false;
    }
  }

  private static class TsFileParserTaskResult {

    private final PipeTsFileInsertionEvent event;
    private final Exception exception;

    private TsFileParserTaskResult(
        final PipeTsFileInsertionEvent event, final Exception exception) {
      this.event = event;
      this.exception = exception;
    }

    private static TsFileParserTaskResult success(final PipeTsFileInsertionEvent event) {
      return new TsFileParserTaskResult(event, null);
    }

    private static TsFileParserTaskResult failure(
        final PipeTsFileInsertionEvent event, final Exception exception) {
      return new TsFileParserTaskResult(event, exception);
    }

    private static TsFileParserTaskResult cancelled(final PipeTsFileInsertionEvent event) {
      return new TsFileParserTaskResult(event, null);
    }
  }

  @Override
  public void submitSelf() {
    // this subtask won't be submitted to the executor directly
    // instead, it will be executed by the PipeProcessorSubtaskWorker
    // and the worker will be submitted to the executor
  }

  @Override
  public void onSuccess(final Boolean hasAtLeastOneEventProcessed) {
    if (retryingFailedTsFileParserEvent != null) {
      submitSelf();
      return;
    }
    super.onSuccess(hasAtLeastOneEventProcessed);
  }

  public boolean isStoppedByException() {
    return lastEvent instanceof EnrichedEvent && retryCount.get() > MAX_RETRY_TIMES;
  }

  @Override
  public void close() {
    // Always deregister the metrics to avoid the deletion of the data region
    PipeProcessorMetrics.getInstance().deregister(taskID);
    try {
      isClosed.set(true);
      retryingFailedTsFileParserEvent = null;
      final Event pendingEvent;
      synchronized (tsFileParserTaskLock) {
        inFlightTsFileParserTasks.forEach(TsFileParserTask::cancel);
        inFlightTsFileParserTasks.clear();
        pendingEvent = pendingEventAfterTsFileParserBarrier;
        pendingEventAfterTsFileParserBarrier = null;
      }
      if (pendingEvent instanceof EnrichedEvent && !((EnrichedEvent) pendingEvent).isReleased()) {
        ((EnrichedEvent) pendingEvent).clearReferenceCount(PipeProcessorSubtask.class.getName());
      }
      pipeProcessor.close();
      // It is important to note that even if the subtask and its corresponding processor are
      // closed, the execution thread may still deliver events downstream.
    } catch (final Exception e) {
      LOGGER.info(
          "Exception occurred when closing pipe processor subtask {}, root cause: {}",
          taskID,
          ErrorHandlingCommonUtils.getRootCause(e).getMessage(),
          e);
    } finally {
      // should be called after pipeProcessor.close()
      super.close();
    }
  }

  boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final PipeProcessorSubtask that = (PipeProcessorSubtask) obj;
    return Objects.equals(this.taskID, that.taskID)
        && Objects.equals(this.subtaskCreationTime, that.subtaskCreationTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskID, subtaskCreationTime);
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getPipeName() {
    return pipeName;
  }

  public int getRegionId() {
    return regionId;
  }

  //////////////////////////// Error report ////////////////////////////

  @Override
  protected String getRootCause(final Throwable throwable) {
    return ErrorHandlingCommonUtils.getRootCause(throwable).getMessage();
  }

  @Override
  protected void report(final EnrichedEvent event, final PipeRuntimeException exception) {
    PipeDataNodeAgent.runtime().report(event, exception);
  }

  private void recordResourceFailure(final Event event, final PipeResourceFailureType failureType) {
    if (event instanceof EnrichedEvent) {
      final EnrichedEvent enrichedEvent = (EnrichedEvent) event;
      PipeDataNodeAgent.task()
          .recordPipeResourceFailure(
              enrichedEvent.getPipeName(), enrichedEvent.getCreationTime(), failureType);
    }
  }
}
