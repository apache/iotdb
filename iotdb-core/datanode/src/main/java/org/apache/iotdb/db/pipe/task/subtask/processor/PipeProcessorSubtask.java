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

import org.apache.iotdb.commons.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.task.EventSupplier;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.UserDefinedEnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.metric.PipeProcessorMetrics;
import org.apache.iotdb.db.pipe.task.connection.EnrichedDeque;
import org.apache.iotdb.db.pipe.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.task.subtask.PipeDataNodeSubtask;
import org.apache.iotdb.db.utils.ErrorHandlingUtils;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class PipeProcessorSubtask extends PipeDataNodeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeProcessorSubtask.class);

  private static final AtomicReference<PipeProcessorSubtaskWorkerManager> subtaskWorkerManager =
      new AtomicReference<>();

  private final EventSupplier inputEventSupplier;
  private final PipeProcessor pipeProcessor;
  private final PipeEventCollector outputEventCollector;

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  private final String pipeName;
  private final int dataRegionId;

  // A queue for events parsed by pattern or time
  private final EnrichedDeque<Event> parsedEventQueue;

  public PipeProcessorSubtask(
      String taskID,
      long creationTime,
      String pipeName,
      int dataRegionId,
      EventSupplier inputEventSupplier,
      PipeProcessor pipeProcessor,
      PipeEventCollector outputEventCollector) {
    super(taskID, creationTime);
    this.pipeName = pipeName;
    this.dataRegionId = dataRegionId;
    this.inputEventSupplier = inputEventSupplier;
    this.pipeProcessor = pipeProcessor;
    this.outputEventCollector = outputEventCollector;
    this.parsedEventQueue = new EnrichedDeque<>(new LinkedList<>());
    PipeProcessorMetrics.getInstance().register(this);
  }

  @Override
  public void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService ignored,
      PipeSubtaskScheduler subtaskScheduler) {
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

    final Event event;
    if (lastEvent != null) {
      event = lastEvent;
    } else if (!parsedEventQueue.isEmpty()) {
      event = parsedEventQueue.poll();
    } else {
      Event originalEvent = UserDefinedEnrichedEvent.maybeOf(inputEventSupplier.supply());
      parseEventByPatternAndTime(originalEvent);
      event = parsedEventQueue.poll(); // If null is polled, will skip this round of process.
    }
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

      releaseLastEvent(true);
    } catch (Exception e) {
      if (!isClosed.get()) {
        throw new PipeException(
            String.format(
                "Exception in pipe process, subtask: %s, last event: %s, root cause: %s",
                taskID, lastEvent, ErrorHandlingUtils.getRootCause(e).getMessage()),
            e);
      } else {
        LOGGER.info("Exception in pipe event processing, ignored because pipe is dropped.", e);
        releaseLastEvent(false);
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

  @Override
  public void close() {
    PipeProcessorMetrics.getInstance().deregister(taskID);
    try {
      isClosed.set(true);

      // clear the events in queue first
      parsedEventQueue.forEach(
          event -> {
            if (event instanceof EnrichedEvent) {
              ((EnrichedEvent) event).clearReferenceCount(PipeProcessorSubtask.class.getName());
            }
          });
      parsedEventQueue.clear();

      // pipeProcessor closes before outputEventCollector, then no more events will be
      // added into outputEventCollector. only after that, outputEventCollector can be closed.
      pipeProcessor.close();
    } catch (Exception e) {
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
  public boolean equals(Object that) {
    return that instanceof PipeProcessorSubtask
        && this.taskID.equals(((PipeProcessorSubtask) that).taskID);
  }

  @Override
  public int hashCode() {
    return taskID.hashCode();
  }

  //////////////////////////// Parsing events ////////////////////////////

  /** Parse an event by pattern and time, then add parsed events into {@code parsedEventQueue} */
  private void parseEventByPatternAndTime(Event sourceEvent) throws Exception {
    if (sourceEvent instanceof PipeTsFileInsertionEvent) {
      parseEventByPatternAndTime((PipeTsFileInsertionEvent) sourceEvent);
    } else if (sourceEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      parseEventByPatternAndTime((PipeInsertNodeTabletInsertionEvent) sourceEvent);
    } else if (sourceEvent instanceof PipeRawTabletInsertionEvent) {
      parseEventByPatternAndTime((PipeRawTabletInsertionEvent) sourceEvent);
    } else {
      parsedEventQueue.offer(sourceEvent);
    }
  }

  private void parseEventByPatternAndTime(PipeTsFileInsertionEvent sourceEvent) throws Exception {
    if (!sourceEvent.waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          sourceEvent.getTsFile());
      return;
    }

    if (sourceEvent.shouldParsePatternOrTime()) {
      try {
        sourceEvent.toTabletInsertionEvents().forEach(parsedEventQueue::offer);
      } finally {
        sourceEvent.close();
      }
    } else {
      // If no need to parse, just put it into the queue.
      parsedEventQueue.offer(sourceEvent);
    }
  }

  private void parseEventByPatternAndTime(PipeInsertNodeTabletInsertionEvent sourceEvent) {
    if (sourceEvent.shouldParsePatternOrTime()) {
      parsedEventQueue.offer(sourceEvent.parseEventWithPatternOrTime());
    } else {
      parsedEventQueue.offer(sourceEvent);
    }
  }

  private void parseEventByPatternAndTime(PipeRawTabletInsertionEvent sourceEvent) {
    if (sourceEvent.shouldParsePatternOrTime()) {
      parsedEventQueue.offer(sourceEvent.parseEventWithPatternOrTime());
    } else if (!sourceEvent.hasNoNeedParsingAndIsEmpty()) {
      // Ignore the event if hasNoNeedParsingAndIsEmpty() is true
      parsedEventQueue.offer(sourceEvent);
    }
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
}
