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

package org.apache.iotdb.db.pipe.task.callable;

import org.apache.iotdb.db.pipe.agent.runtime.PipeRuntimeAgent;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PipeSubtask implements FutureCallback<Void>, Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSubtask.class);

  private final DecoratingLock decoratingLock = new DecoratingLock();

  private final String taskID;

  private ListeningExecutorService executorService;
  private Throwable lastFailedCause;

  private static final int MAX_RETRY_TIMES = 5;
  private final AtomicInteger retryCount = new AtomicInteger(0);

  private volatile boolean shouldStopSubmittingSelf = true;

  public PipeSubtask(String taskID) {
    super();
    this.taskID = taskID;
  }

  public void bindExecutorService(ListeningExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public Void call() throws Exception {
    executeForAWhile();

    // wait for the callable to be decorated by Futures.addCallback in the executorService
    // to make sure that the callback can be submitted again on success or failure.
    decoratingLock.waitForDecorated();

    return null;
  }

  protected abstract void executeForAWhile() throws Exception;

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
      PipeRuntimeAgent.setupAndGetInstance().report(this);
    }
  }

  /**
   * this may cause RejectedExecutionException when the executorService is shutdown. we just ignore
   * it.
   */
  public void submitSelf() {
    if (shouldStopSubmittingSelf) {
      // we change the flag to false to make sure that the subtask can be submitted again
      shouldStopSubmittingSelf = false;
      return;
    }

    decoratingLock.markAsDecorating();
    try {
      final ListenableFuture<Void> nextFuture = executorService.submit(this);
      Futures.addCallback(nextFuture, this, executorService);
    } finally {
      decoratingLock.markAsDecorated();
    }
  }

  public void allowSubmittingSelf() {
    shouldStopSubmittingSelf = false;
  }

  public void disallowSubmittingSelf() {
    shouldStopSubmittingSelf = true;
  }

  public boolean isSubmittingSelf() {
    return !shouldStopSubmittingSelf;
  }

  public String getTaskID() {
    return taskID;
  }

  public Throwable getLastFailedCause() {
    return lastFailedCause;
  }
}
