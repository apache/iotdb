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

  protected PipeSubtask(String taskID, long creationTime) {
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

  /** Should be synchronized with {@link PipeSubtask#releaseLastEvent} */
  protected synchronized void setLastEvent(Event event) {
    lastEvent = event;
  }

  /**
   * Try to consume an event by the pipe plugin.
   *
   * @return true if the event is consumed successfully, false if no more event can be consumed
   * @throws Exception if any error occurs when consuming the event
   */
  @SuppressWarnings("squid:S112") // Allow to throw Exception
  protected abstract boolean executeOnce() throws Exception;

  @Override
  public void onSuccess(Boolean hasAtLeastOneEventProcessed) {
    retryCount.set(0);
    submitSelf();
  }

  /**
   * Submit the subTask. Be sure to add parallel check since a subtask is currently not designed to
   * run in parallel.
   */
  public abstract void submitSelf();

  public void allowSubmittingSelf() {
    retryCount.set(0);
    shouldStopSubmittingSelf.set(false);
  }

  /**
   * Set the shouldStopSubmittingSelf state from false to true, in order to stop submitting the
   * subtask.
   *
   * @return true if the shouldStopSubmittingSelf state is changed from false to true, false
   *     otherwise
   */
  public boolean disallowSubmittingSelf() {
    return !shouldStopSubmittingSelf.getAndSet(true);
  }

  public boolean isSubmittingSelf() {
    return !shouldStopSubmittingSelf.get();
  }

  // synchronized for close() and releaseLastEvent(). make sure that the lastEvent
  // will not be updated after pipeProcessor.close() to avoid resource leak
  // because of the lastEvent is not released.
  @Override
  public void close() {
    releaseLastEvent(false);
  }

  protected abstract void releaseLastEvent(boolean shouldReport);

  public String getTaskID() {
    return taskID;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public int getRetryCount() {
    return retryCount.get();
  }
}
