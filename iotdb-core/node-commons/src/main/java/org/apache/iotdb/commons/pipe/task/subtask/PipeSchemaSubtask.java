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
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.schema.IoTDBSchemaConnector;
import org.apache.iotdb.commons.pipe.plugin.builtin.extractor.schema.IoTDBSchemaExtractor;
import org.apache.iotdb.commons.pipe.task.DecoratingLock;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class PipeSchemaSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeSchemaSubtask.class);

  private final IoTDBSchemaExtractor inputPipeExtractor;
  private final IoTDBSchemaConnector outputPipeConnector;

  // For thread pool to execute callbacks
  private final DecoratingLock callbackDecoratingLock = new DecoratingLock();
  private ExecutorService subtaskCallbackListeningExecutor;

  // For controlling subtask submitting, making sure that a subtask is submitted to only one thread
  // at a time
  private volatile boolean isSubmitted = false;

  public PipeSchemaSubtask(
      String taskID,
      long creationTime,
      Map<String, String> extractorAttributes,
      Map<String, String> connectorAttributes)
      throws Exception {
    super(taskID, creationTime);

    this.inputPipeExtractor = new IoTDBSchemaExtractor();
    PipeParameters extractorParameters = new PipeParameters(extractorAttributes);
    this.inputPipeExtractor.validate(new PipeParameterValidator(extractorParameters));
    // do nothing in customize() now
    this.inputPipeExtractor.customize(extractorParameters, () -> null);

    this.outputPipeConnector = new IoTDBSchemaConnector();
    PipeParameters connectorParameters = new PipeParameters(connectorAttributes);
    this.outputPipeConnector.validate(new PipeParameterValidator(connectorParameters));
    // do nothing in customize() now
    this.outputPipeConnector.customize(connectorParameters, () -> null);
  }

  public void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService subtaskCallbackListeningExecutor,
      PipeSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskCallbackListeningExecutor = subtaskCallbackListeningExecutor;
    this.subtaskScheduler = subtaskScheduler;
  }

  /**
   * Try to consume an event by the pipe plugin.
   *
   * @return true if the event is consumed successfully, false if no more event can be consumed
   * @throws Exception if any error occurs when consuming the event
   */
  @SuppressWarnings("squid:S112") // Allow to throw Exception
  protected boolean executeOnce() throws Exception {
    if (isClosed.get()) {
      return false;
    }

    final Event event = lastEvent != null ? lastEvent : inputPipeExtractor.supply();
    // Record the last event for retry when exception occurs
    setLastEvent(event);

    try {
      if (event == null) {
        return false;
      }

      outputPipeConnector.transfer(event);

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
        throw new PipeException("Error occurred during executing PipeConnector#transfer.", e);
      } else {
        LOGGER.info("Exception in pipe transfer, ignored because pipe is dropped.");
        releaseLastEvent(false);
      }
    }

    return true;
  }

  @Override
  public Boolean call() throws Exception {
    final boolean hasAtLeastOneEventProcessed = super.call();

    // Wait for the callable to be decorated by Futures.addCallback in the executorService
    // to make sure that the callback can be submitted again on success or failure.
    callbackDecoratingLock.waitForDecorated();

    return hasAtLeastOneEventProcessed;
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

  // synchronized for close() and releaseLastEvent(). make sure that the lastEvent
  // will not be updated after pipeProcessor.close() to avoid resource leak
  // because of the lastEvent is not released.
  @Override
  public void close() {
    isClosed.set(true);
    try {
      outputPipeConnector.close();
    } catch (Exception e) {
      LOGGER.info("Error occurred during closing PipeConnector.", e);
    } finally {
      // Should be after outputPipeConnector.close()
      super.close();
    }
  }

  protected synchronized void releaseLastEvent(boolean shouldReport) {
    if (lastEvent != null) {
      // TODO: should decrease reference count here
      lastEvent = null;
    }
  }
}
