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

import org.apache.iotdb.commons.utils.TestOnly;
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
  private final String taskID;
  private ListeningExecutorService executorService;
  private final int MAX_RETRY_TIMES = 5;
  private final AtomicInteger retryCount = new AtomicInteger(0);

  public PipeSubtask(String taskID) {
    super();
    this.taskID = taskID;
  }

  public String getTaskID() {
    return taskID;
  }

  public PipeSubtask setListeningExecutorService(ListeningExecutorService executorService) {
    this.executorService = executorService;
    return this;
  }

  public void stop() {
    Thread.currentThread().interrupt();
  }

  @Override
  public abstract Void call() throws Exception;

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
      LOGGER.warn("Subtask {} failed, retry {} times", taskID, retryCount);
      PipeRuntimeAgent.setupAndGetInstance().report(this);
      stop();
    }
  }

  void submitSelf() {
    ListenableFuture<Void> nextFuture = executorService.submit(this);
    Futures.addCallback(nextFuture, this, executorService);
  }

  public String getSubtaskID() {
    return taskID;
  }

  @TestOnly
  public int getRetryCount() {
    return retryCount.get();
  }
}
