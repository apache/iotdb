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

package org.apache.iotdb.db.pipe.task.subtask.processor;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.task.EventSupplier;
import org.apache.iotdb.commons.pipe.task.subtask.PipeReportableSubtask;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.metric.PipeProcessorMetrics;
import org.apache.iotdb.db.pipe.task.connection.PipeEventCollector;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class PipeProcessorSubtask extends PipeReportableSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtask.class);

  private static final AtomicReference<PipeProcessorSubtaskWorkerManager> subtaskWorkerManager =
      new AtomicReference<>();

  private final EventSupplier inputEventSupplier;
  private final PipeProcessor pipeProcessor;
  private final PipeEventCollector outputEventCollector;

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String pipeName;
  private final int dataRegionId;

  // This variable is used to distinguish between old and new subtasks before and after stuck
  // restart.
  private final long subtaskCreationTime;

  public PipeProcessorSubtask(
      final String taskID,
      final long creationTime,
      final String pipeName,
      final int dataRegionId,
      final EventSupplier inputEventSupplier,
      final PipeProcessor pipeProcessor,
      final PipeEventCollector outputEventCollector) {
    super(taskID, creationTime);
    this.subtaskCreationTime = System.currentTimeMillis();
    this.pipeName = pipeName;
    this.dataRegionId = dataRegionId;
    this.inputEventSupplier = inputEventSupplier;
    this.pipeProcessor = pipeProcessor;
    this.outputEventCollector = outputEventCollector;
    PipeProcessorMetrics.getInstance().register(this);
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

    if (
    // Though there is no event to process, there may still be some buffered events
    // in the outputEventCollector. Return true if there are still buffered events,
    // false otherwise.
    event == null
        // If there are still buffered events, process them first, the newly supplied
        // event will be processed in the next round.
        || !outputEventCollector.isBufferQueueEmpty()) {
      return outputEventCollector.tryCollectBufferedEvents();
    }

    outputEventCollector.resetCollectInvocationCount();
    try {
      // event can be supplied after the subtask is closed, so we need to check isClosed here
      if (!isClosed.get()) {
        if (event instanceof TabletInsertionEvent) {
          pipeProcessor.process((TabletInsertionEvent) event, outputEventCollector);
          PipeProcessorMetrics.getInstance().markTabletEvent(taskID);
        } else if (event instanceof TsFileInsertionEvent) {
          pipeProcessor.process((TsFileInsertionEvent) event, outputEventCollector);
          PipeProcessorMetrics.getInstance().markTsFileEvent(taskID);
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
      decreaseReferenceCountAndReleaseLastEvent(
          !isClosed.get() && outputEventCollector.hasNoCollectInvocationAfterReset());
    } catch (final PipeRuntimeOutOfMemoryCriticalException e) {
      LOGGER.info(
          "Temporarily out of memory in pipe event processing, will wait for the memory to release.",
          e);
      return false;
    } catch (final Exception e) {
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
        LOGGER.info("Exception in pipe event processing, ignored because pipe is dropped.", e);
        clearReferenceCountAndReleaseLastEvent();
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
    PipeProcessorMetrics.getInstance().deregister(taskID);
    try {
      isClosed.set(true);

      // pipeProcessor closes first, then no more events will be added into outputEventCollector.
      // only after that, outputEventCollector can be closed.
      pipeProcessor.close();
    } catch (final Exception e) {
      LOGGER.info(
          "Exception occurred when closing pipe processor subtask {}, root cause: {}",
          taskID,
          ErrorHandlingUtils.getRootCause(e).getMessage(),
          e);
    } finally {
      outputEventCollector.close();

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

  public int getDataRegionId() {
    return dataRegionId;
  }

  public int getTabletInsertionEventCount() {
    return outputEventCollector.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return outputEventCollector.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return outputEventCollector.getPipeHeartbeatEventCount();
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
