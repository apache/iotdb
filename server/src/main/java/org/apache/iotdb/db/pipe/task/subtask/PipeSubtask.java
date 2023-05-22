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

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.event.Event;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PipeSubtask implements FutureCallback<Void>, Callable<Void>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtask.class);

  protected final String taskID;

  private ListeningExecutorService subtaskWorkerThreadPoolExecutor;
  private ExecutorService subtaskCallbackListeningExecutor;
  private final DecoratingLock callbackDecoratingLock = new DecoratingLock();
  private final AtomicBoolean shouldStopSubmittingSelf = new AtomicBoolean(true);

  protected static final int MAX_RETRY_TIMES = 5;
  private final AtomicInteger retryCount = new AtomicInteger(0);
  protected Throwable lastFailedCause;

  protected Event lastEvent;

  public PipeSubtask(String taskID) {
    super();
    this.taskID = taskID;
  }

  public void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService subtaskCallbackListeningExecutor) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskCallbackListeningExecutor = subtaskCallbackListeningExecutor;
  }

  @Override
  public Void call() throws Exception {
    executeOnce();

    // wait for the callable to be decorated by Futures.addCallback in the executorService
    // to make sure that the callback can be submitted again on success or failure.
    callbackDecoratingLock.waitForDecorated();

    return null;
  }

  /**
   * try to consume an event by the pipe plugin.
   *
   * @return true if the event is consumed successfully, false if no more event can be consumed
   * @throws Exception if any error occurs when consuming the event
   */
  protected abstract boolean executeOnce() throws Exception;

  @Override
  public void onSuccess(Void result) {
    retryCount.set(0);
    submitSelf();
  }

  @Override
  public void onFailure(@NotNull Throwable throwable) {
    if (retryCount.get() < MAX_RETRY_TIMES) {
      retryCount.incrementAndGet();
      submitSelf();
    } else {
      LOGGER.warn(
          "Subtask {} failed, has been retried for {} times, last failed because of {}",
          taskID,
          retryCount,
          throwable);
      lastFailedCause = throwable;

      PipeAgent.runtime().report(this);

      // although the pipe task will be stopped, we still don't release the last event here
      // because we need to keep it for the next retry. if user wants to restart the task,
      // the last event will be processed again. the last event will be released when the task
      // is dropped or the process is running normally.
    }
  }

  public void submitSelf() {
    if (shouldStopSubmittingSelf.get()) {
      return;
    }

    callbackDecoratingLock.markAsDecorating();
    try {
      final ListenableFuture<Void> nextFuture = subtaskWorkerThreadPoolExecutor.submit(this);
      Futures.addCallback(nextFuture, this, subtaskCallbackListeningExecutor);
    } finally {
      callbackDecoratingLock.markAsDecorated();
    }
  }

  public void allowSubmittingSelf() {
    retryCount.set(0);
    shouldStopSubmittingSelf.set(false);
  }

  public void disallowSubmittingSelf() {
    shouldStopSubmittingSelf.set(true);
  }

  public boolean isSubmittingSelf() {
    return !shouldStopSubmittingSelf.get();
  }

  @Override
  public synchronized void close() {
    releaseLastEvent();
  }

  protected void releaseLastEvent() {
    if (lastEvent != null) {
      if (lastEvent instanceof EnrichedEvent) {
        ((EnrichedEvent) lastEvent).decreaseReferenceCount(PipeSubtask.class.getName());
      }
      lastEvent = null;
    }
  }

  public String getTaskID() {
    return taskID;
  }

  public Throwable getLastFailedCause() {
    return lastFailedCause;
  }
}
