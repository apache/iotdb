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

import org.apache.iotdb.commons.pipe.execution.scheduler.PipeConfigSubtaskScheduler;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.schema.IoTDBSchemaConnector;
import org.apache.iotdb.commons.pipe.task.DecoratingLock;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeConfigSubtask
    implements FutureCallback<Boolean>, Callable<Boolean>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigSubtask.class);

  // Used for identifying the subtask
  private final String taskID;

  // For thread pool to execute subtasks
  private ListeningExecutorService subtaskWorkerThreadPoolExecutor;

  // For controlling the subtask execution
  private final AtomicBoolean shouldStopSubmittingSelf = new AtomicBoolean(true);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private PipeConfigSubtaskScheduler subtaskScheduler;

  // For fail-over
  public static final int MAX_RETRY_TIMES = 5;
  private final AtomicInteger retryCount = new AtomicInteger(0);
  private Event lastEvent;

  private final PipeConnector outputPipeConnector;

  // For thread pool to execute callbacks
  private final DecoratingLock callbackDecoratingLock = new DecoratingLock();
  private ExecutorService subtaskCallbackListeningExecutor;

  // For controlling subtask submitting, making sure that a subtask is submitted to only one thread
  // at a time
  private volatile boolean isSubmitted = false;

  public PipeConfigSubtask(
      String taskID,
      Map<String, String> extractorAttributes,
      Map<String, String> connectorAttributes)
      throws Exception {
    this.taskID = taskID;
    this.outputPipeConnector = new IoTDBSchemaConnector();
    // This connector takes the responsibility of both extractor and connector,
    // so we need to merge the attributes of extractor and connector
    Map<String, String> allAttributes = new HashMap<>();
    allAttributes.putAll(extractorAttributes);
    allAttributes.putAll(connectorAttributes);
    this.outputPipeConnector.validate(
        new PipeParameterValidator(new PipeParameters(allAttributes)));
  }

  public void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService subtaskCallbackListeningExecutor,
      PipeConfigSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskCallbackListeningExecutor = subtaskCallbackListeningExecutor;
    this.subtaskScheduler = subtaskScheduler;
  }

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

  /** Should be synchronized with {@link PipeConfigSubtask#releaseLastEvent} */
  private synchronized void setLastEvent(Event event) {
    lastEvent = event;
  }

  /**
   * Try to consume an event by the pipe plugin.
   *
   * @return true if the event is consumed successfully, false if no more event can be consumed
   * @throws Exception if any error occurs when consuming the event
   */
  @SuppressWarnings("squid:S112") // Allow to throw Exception
  private boolean executeOnce() throws Exception {
    if (isClosed.get()) {
      return false;
    }

    final Event event = lastEvent != null ? lastEvent : null; // TODO: get an event from upstream
    // Record the last event for retry when exception occurs
    setLastEvent(event);
    // TODO: process the event if necessary

    try {
      if (event == null) {
        return false;
      }

      if (event instanceof TabletInsertionEvent) {
        outputPipeConnector.transfer((TabletInsertionEvent) event);
      } else if (event instanceof TsFileInsertionEvent) {
        outputPipeConnector.transfer((TsFileInsertionEvent) event);
      } else {
        outputPipeConnector.transfer(event);
      }

      releaseLastEvent(true);
    } catch (PipeConnectionException e) {
      if (!isClosed.get()) {
        throw e;
      } else {
        LOGGER.info("PipeConnectionException in pipe transfer, ignored because pipe is dropped.");
        releaseLastEvent(false);
      }
    } catch (Exception e) {
      if (!isClosed.get()) {
        throw new PipeException(
            "Error occurred during executing PipeConnector#transfer, perhaps need to check "
                + "whether the implementation of PipeConnector is correct "
                + "according to the pipe-api description.",
            e);
      } else {
        LOGGER.info("Exception in pipe transfer, ignored because pipe is dropped.");
        releaseLastEvent(false);
      }
    }

    return true;
  }

  @Override
  public void onSuccess(Boolean hasAtLeastOneEventProcessed) {
    retryCount.set(0);
    submitSelf();
  }

  @Override
  public void onFailure(Throwable throwable) {
    // TODO: handle failures
    retryCount.set(0);
    submitSelf();
  }

  /**
   * Submit the subTask. Be sure to add parallel check since a subtask is currently not designed to
   * run in parallel.
   */
  public void submitSelf() {
    if (shouldStopSubmittingSelf.get() || isSubmitted) {
      return;
    }

    callbackDecoratingLock.markAsDecorating();
    try {
      final ListenableFuture<Boolean> nextFuture = subtaskWorkerThreadPoolExecutor.submit(this);
      Futures.addCallback(nextFuture, this, subtaskCallbackListeningExecutor);
      isSubmitted = true;
    } finally {
      callbackDecoratingLock.markAsDecorated();
    }
  }

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
    isClosed.set(true);
    try {
      outputPipeConnector.close();
    } catch (Exception e) {
      LOGGER.info(
          "Error occurred during closing PipeConnector, perhaps need to check whether the "
              + "implementation of PipeConnector is correct according to the pipe-api description.",
          e);
    } finally {
      // Should be after outputPipeConnector.close()
      releaseLastEvent(false);
    }
  }

  private synchronized void releaseLastEvent(boolean shouldReport) {
    if (lastEvent != null) {
      // TODO: should decrease reference count here
      lastEvent = null;
    }
  }

  public String getTaskID() {
    return taskID;
  }

  public int getRetryCount() {
    return retryCount.get();
  }
}
