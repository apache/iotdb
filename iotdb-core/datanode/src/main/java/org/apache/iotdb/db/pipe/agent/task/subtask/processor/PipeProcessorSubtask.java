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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.commons.pipe.agent.task.execution.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.agent.task.subtask.PipeReportableSubtask;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.resource.log.PipeLogger;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.processor.PipeProcessorMetrics;
import org.apache.iotdb.db.pipe.processor.pipeconsensus.PipeConsensusProcessor;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.tsfile.external.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class PipeProcessorSubtask extends PipeReportableSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtask.class);

  private static final AtomicReference<PipeProcessorSubtaskWorkerManager> subtaskWorkerManager =
      new AtomicReference<>();

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String pipeName;
  private final String pipeNameWithCreationTime; // cache for better performance
  private final int regionId;

  private final EventSupplier inputEventSupplier;
  private final PipeProcessor pipeProcessor;
  private final PipeEventCollector outputEventCollector;

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
    super(taskID, creationTime);
    this.pipeName = pipeName;
    this.pipeNameWithCreationTime = pipeName + "_" + creationTime;
    this.regionId = regionId;
    this.inputEventSupplier = inputEventSupplier;
    this.pipeProcessor = pipeProcessor;
    this.outputEventCollector = outputEventCollector;
    this.subtaskCreationTime = System.currentTimeMillis();

    // Only register dataRegions
    if (StorageEngine.getInstance().getAllDataRegionIds().contains(new DataRegionId(regionId))
        || PipeRuntimeMeta.isSourceExternal(regionId)) {
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

    final Event event =
        lastEvent != null
            ? lastEvent
            : UserDefinedEnrichedEvent.maybeOf(inputEventSupplier.supply());
    // Record the last event for retry when exception occurs
    setLastEvent(event);

    if (Objects.isNull(event)) {
      return false;
    }

    outputEventCollector.resetFlags();
    try {
      if (event instanceof EnrichedEvent) {
        ((EnrichedEvent) event).throwIfNoPrivilege();
      }
      // event can be supplied after the subtask is closed, so we need to check isClosed here
      if (!isClosed.get()) {
        if (event instanceof TabletInsertionEvent) {
          if (event instanceof PipeInsertNodeTabletInsertionEvent
              && ((PipeInsertNodeTabletInsertionEvent) event).shouldParse4Privilege()) {
            final AtomicReference<Exception> ex = new AtomicReference<>();
            ((PipeInsertNodeTabletInsertionEvent) event)
                .toRawTabletInsertionEvents()
                .forEach(
                    rawTabletInsertionEvent -> {
                      try {
                        pipeProcessor.process(rawTabletInsertionEvent, outputEventCollector);
                      } catch (Exception e) {
                        ex.set(e);
                      }
                    });
            if (ex.get() != null) {
              throw ex.get();
            }
          } else {
            pipeProcessor.process((TabletInsertionEvent) event, outputEventCollector);
          }
          PipeProcessorMetrics.getInstance().markTabletEvent(taskID);
        } else if (event instanceof TsFileInsertionEvent) {
          // We have to parse the privilege first, to avoid passing no-privilege data to processor
          if (event instanceof PipeTsFileInsertionEvent
              && ((PipeTsFileInsertionEvent) event).shouldParse4Privilege()) {
            try (final PipeTsFileInsertionEvent tsFileInsertionEvent =
                (PipeTsFileInsertionEvent) event) {
              final AtomicReference<Exception> ex = new AtomicReference<>();
              tsFileInsertionEvent.consumeTabletInsertionEventsWithRetry(
                  event1 -> {
                    try {
                      pipeProcessor.process(event1, outputEventCollector);
                    } catch (Exception e) {
                      ex.set(e);
                    }
                  },
                  "PipeProcessorSubtask::executeOnce");
              if (ex.get() != null) {
                throw ex.get();
              }
            }
          } else {
            pipeProcessor.process((TsFileInsertionEvent) event, outputEventCollector);
          }
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
              && outputEventCollector.hasNoGeneratedEvent()
              // If the event's reference count cannot be increased, it means that the event has
              // been released, and the progress of the event can not be reported.
              && !outputEventCollector.isFailedToIncreaseReferenceCount()
              // Events generated from consensusPipe's transferred data should never be reported.
              && !(pipeProcessor instanceof PipeConsensusProcessor);
      if (shouldReport
          && event instanceof EnrichedEvent
          && outputEventCollector.hasNoCollectInvocationAfterReset()) {
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
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      PipeLogger.log(
          LOGGER::info,
          e,
          "Temporarily out of memory in pipe event processing, will wait for the memory to release.");
      return false;
    } catch (final Exception e) {
      if (ExceptionUtils.getRootCause(e) instanceof PipeRuntimeOutOfMemoryCriticalException) {
        PipeLogger.log(
            LOGGER::info,
            e,
            "Temporarily out of memory in pipe event processing, will wait for the memory to release.");
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
                ErrorHandlingUtils.getRootCause(e).getMessage()),
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

  @Override
  public void submitSelf() {
    // this subtask won't be submitted to the executor directly
    // instead, it will be executed by the PipeProcessorSubtaskWorker
    // and the worker will be submitted to the executor
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
      pipeProcessor.close();
      // It is important to note that even if the subtask and its corresponding processor are
      // closed, the execution thread may still deliver events downstream.
    } catch (final Exception e) {
      LOGGER.info(
          "Exception occurred when closing pipe processor subtask {}, root cause: {}",
          taskID,
          ErrorHandlingUtils.getRootCause(e).getMessage(),
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
    return ErrorHandlingUtils.getRootCause(throwable).getMessage();
  }

  @Override
  protected void report(final EnrichedEvent event, final PipeRuntimeException exception) {
    PipeDataNodeAgent.runtime().report(event, exception);
  }
}
