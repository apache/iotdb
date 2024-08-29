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

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.pipe.api.event.Event;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PipeSubtask
    implements FutureCallback<Boolean>, Callable<Boolean>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtask.class);

  // Used for identifying the subtask
  protected final String taskID;

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  protected long creationTime;

  // For thread pool to execute subtasks
  protected ListeningExecutorService subtaskWorkerThreadPoolExecutor;

  // For controlling the subtask execution
  protected final AtomicBoolean shouldStopSubmittingSelf = new AtomicBoolean(true);
  protected final AtomicBoolean isClosed = new AtomicBoolean(false);
  protected PipeSubtaskScheduler subtaskScheduler;

  // For fail-over
  public static final int MAX_RETRY_TIMES = 5;
  protected final AtomicInteger retryCount = new AtomicInteger(0);
  protected Event lastEvent;

  protected PipeSubtask(final String taskID, final long creationTime) {
    super();
    this.taskID = taskID;
    this.creationTime = creationTime;
  }

  public abstract void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService subtaskCallbackListeningExecutor,
      PipeSubtaskScheduler subtaskScheduler);

  @Override
  public Boolean call() throws Exception {
    boolean hasAtLeastOneEventProcessed = false;

    try {
      // If the scheduler allows to schedule, then try to consume an event
      while (subtaskScheduler.schedule()) {
        // If the event is consumed successfully, then continue to consume the next event
        // otherwise, stop consuming
        if (!executeOnce()) {
          break;
        }
        hasAtLeastOneEventProcessed = true;
      }
    } finally {
      // Reset the scheduler to make sure that the scheduler can schedule again
      subtaskScheduler.reset();
    }

    return hasAtLeastOneEventProcessed;
  }

  /** Should be synchronized with {@link PipeSubtask#decreaseReferenceCountAndReleaseLastEvent} */
  protected synchronized void setLastEvent(final Event event) {
    lastEvent = event;
  }

  /**
   * Try to consume an {@link Event} by the pipe plugin.
   *
   * @return {@code true} if the {@link Event} is consumed successfully, {@code false} if no more
   *     {@link Event} can be consumed
   * @throws Exception if any error occurs when consuming the {@link Event}
   */
  @SuppressWarnings("squid:S112") // Allow to throw Exception
  protected abstract boolean executeOnce() throws Exception;

  @Override
  public synchronized void onSuccess(final Boolean hasAtLeastOneEventProcessed) {
    final int totalRetryCount = retryCount.getAndSet(0);

    submitSelf();

    if (totalRetryCount != 0) {
      LOGGER.warn(
          "Successfully executed subtask {}({}) after {} retries.",
          taskID,
          this.getClass().getSimpleName(),
          totalRetryCount);
    }
  }

  /**
   * Submit the {@link PipeSubtask}. Be sure to add parallel check since a {@link PipeSubtask} is
   * currently not designed to run in parallel.
   */
  public abstract void submitSelf();

  public void allowSubmittingSelf() {
    retryCount.set(0);
    shouldStopSubmittingSelf.set(false);
  }

  /**
   * Set the {@link PipeSubtask#shouldStopSubmittingSelf} state from {@code false} to {@code true},
   * in order to stop submitting the {@link PipeSubtask}.
   *
   * @return {@code true} if the {@link PipeSubtask#shouldStopSubmittingSelf} state is changed from
   *     {@code false} to {@code true}, {@code false} otherwise
   */
  public boolean disallowSubmittingSelf() {
    return !shouldStopSubmittingSelf.getAndSet(true);
  }

  public boolean isSubmittingSelf() {
    return !shouldStopSubmittingSelf.get();
  }

  @Override
  public void close() {
    clearReferenceCountAndReleaseLastEvent(null);
  }

  protected synchronized void decreaseReferenceCountAndReleaseLastEvent(
      final Event actualLastEvent, final boolean shouldReport) {
    // lastEvent may be set to null due to PipeConnectorSubtask#discardEventsOfPipe
    if (lastEvent != null) {
      if (lastEvent instanceof EnrichedEvent && !((EnrichedEvent) lastEvent).isReleased()) {
        ((EnrichedEvent) lastEvent)
            .decreaseReferenceCount(PipeSubtask.class.getName(), shouldReport);
      }
      lastEvent = null;
      return;
    }

    // If lastEvent is set to null due to PipeConnectorSubtask#discardEventsOfPipe (connector close)
    // and finally exception occurs, we need to release the actual last event from the connector
    // given by the parameter
    if (actualLastEvent instanceof EnrichedEvent
        && !((EnrichedEvent) actualLastEvent).isReleased()) {
      ((EnrichedEvent) actualLastEvent)
          .decreaseReferenceCount(PipeSubtask.class.getName(), shouldReport);
    }
  }

  protected synchronized void clearReferenceCountAndReleaseLastEvent(final Event actualLastEvent) {
    // lastEvent may be set to null due to PipeConnectorSubtask#discardEventsOfPipe
    if (lastEvent != null) {
      if (lastEvent instanceof EnrichedEvent && !((EnrichedEvent) lastEvent).isReleased()) {
        ((EnrichedEvent) lastEvent).clearReferenceCount(PipeSubtask.class.getName());
      }
      lastEvent = null;
      return;
    }

    // If lastEvent is set to null due to PipeConnectorSubtask#discardEventsOfPipe (connector close)
    // and finally exception occurs, we need to release the actual last event from the connector
    // given by the parameter
    if (actualLastEvent instanceof EnrichedEvent
        && !((EnrichedEvent) actualLastEvent).isReleased()) {
      ((EnrichedEvent) actualLastEvent).clearReferenceCount(PipeSubtask.class.getName());
    }
  }

  public String getTaskID() {
    return taskID;
  }

  public long getCreationTime() {
    return creationTime;
  }
}
