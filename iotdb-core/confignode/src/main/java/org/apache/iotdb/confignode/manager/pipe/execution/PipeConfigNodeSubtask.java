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

package org.apache.iotdb.confignode.manager.pipe.execution;

import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.task.DecoratingLock;
import org.apache.iotdb.commons.pipe.task.subtask.PipeSubtask;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
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

import static org.apache.iotdb.db.protocol.client.ConfigNodeInfo.CONFIG_REGION_ID;

public class PipeConfigNodeSubtask extends PipeSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigNodeSubtask.class);

  // Pipe plugins for this subtask
  private final PipeExtractor extractor;
  // TODO: currently unused
  @SuppressWarnings("unused")
  private final PipeProcessor processor;

  private final PipeConnector connector;

  // For thread pool to execute callbacks
  private final DecoratingLock callbackDecoratingLock = new DecoratingLock();
  private ExecutorService subtaskCallbackListeningExecutor;

  // For controlling subtask submitting, making sure that
  // a subtask is submitted to only one thread at a time
  private volatile boolean isSubmitted = false;

  public PipeConfigNodeSubtask(
      String pipeName,
      long creationTime,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes)
      throws Exception {
    super(pipeName, creationTime);

    extractor = initExtractor(extractorAttributes);
    processor = initProcessor(processorAttributes);
    connector = initConnector(connectorAttributes);

    // TODO: connect extractor, processor and connector
  }

  private PipeExtractor initExtractor(Map<String, String> extractorAttributes) throws Exception {
    final PipeParameters extractorParameters = new PipeParameters(extractorAttributes);

    // 1. Construct extractor
    final PipeExtractor extractor =
        PipeConfigNodeAgent.plugin().reflectExtractor(extractorParameters);

    // 2. Validate extractor parameters
    extractor.validate(new PipeParameterValidator(extractorParameters));

    // 3. Customize extractor
    final PipeTaskRuntimeConfiguration runtimeConfiguration =
        new PipeTaskRuntimeConfiguration(
            // TODO: check CONFIG_REGION_ID.getId()
            new PipeTaskRuntimeEnvironment(taskID, creationTime, CONFIG_REGION_ID.getId()));
    extractor.customize(extractorParameters, runtimeConfiguration);

    return extractor;
  }

  private PipeProcessor initProcessor(Map<String, String> processorAttributes) throws Exception {
    final PipeParameters processorParameters = new PipeParameters(processorAttributes);

    // 1. Construct processor
    final PipeProcessor processor =
        PipeConfigNodeAgent.plugin().reflectProcessor(processorParameters);

    // 2. Validate processor parameters
    processor.validate(new PipeParameterValidator(processorParameters));

    // 3. Customize processor
    final PipeTaskRuntimeConfiguration runtimeConfiguration =
        new PipeTaskRuntimeConfiguration(
            // TODO: check CONFIG_REGION_ID.getId()
            new PipeTaskRuntimeEnvironment(taskID, creationTime, CONFIG_REGION_ID.getId()));
    processor.customize(processorParameters, runtimeConfiguration);

    return processor;
  }

  private PipeConnector initConnector(Map<String, String> connectorAttributes) throws Exception {
    final PipeParameters connectorParameters = new PipeParameters(connectorAttributes);

    // 1. Construct connector
    final PipeConnector connector =
        PipeConfigNodeAgent.plugin().reflectConnector(connectorParameters);

    // 2. Validate connector parameters
    connector.validate(new PipeParameterValidator(connectorParameters));

    // 3. Customize connector
    final PipeTaskRuntimeConfiguration runtimeConfiguration =
        new PipeTaskRuntimeConfiguration(
            // TODO: check CONFIG_REGION_ID.getId()
            new PipeTaskRuntimeEnvironment(taskID, creationTime, CONFIG_REGION_ID.getId()));
    connector.customize(connectorParameters, runtimeConfiguration);

    // 4. Handshake
    connector.handshake();

    return connector;
  }

  @Override
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
  @Override
  protected boolean executeOnce() throws Exception {
    if (isClosed.get()) {
      return false;
    }

    final Event event = lastEvent != null ? lastEvent : extractor.supply();
    // Record the last event for retry when exception occurs
    setLastEvent(event);

    try {
      if (event == null) {
        return false;
      }

      connector.transfer(event);

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
  public synchronized void onSuccess(Boolean hasAtLeastOneEventProcessed) {
    isSubmitted = false;

    super.onSuccess(hasAtLeastOneEventProcessed);
  }

  @Override
  public synchronized void onFailure(Throwable throwable) {
    isSubmitted = false;

    if (isClosed.get()) {
      LOGGER.info(
          "onFailure in pipe config node subtask, ignored because pipe is dropped.", throwable);
      releaseLastEvent(false);
      return;
    }

    if (retryCount.get() == 0) {
      LOGGER.warn(
          "Failed to execute subtask {}({}), because of {}. Will retry forever until success.",
          taskID,
          this.getClass().getSimpleName(),
          throwable.getMessage(),
          throwable);
    }

    retryCount.incrementAndGet();
    LOGGER.warn(
        "Retry executing subtask {}({}), retry count {}",
        taskID,
        this.getClass().getSimpleName(),
        retryCount.get());
    try {
      Thread.sleep(1000L);
    } catch (InterruptedException e) {
      LOGGER.warn(
          "Interrupted when retrying to execute subtask {}({})",
          taskID,
          this.getClass().getSimpleName());
      Thread.currentThread().interrupt();
    }

    submitSelf();
  }

  /**
   * Submit the subTask. Be sure to add parallel check since a subtask is currently not designed to
   * run in parallel.
   */
  @Override
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
      extractor.close();
    } catch (Exception e) {
      LOGGER.info("Error occurred during closing PipeExtractor.", e);
    }

    try {
      processor.close();
    } catch (Exception e) {
      LOGGER.info("Error occurred during closing PipeProcessor.", e);
    }

    try {
      connector.close();
    } catch (Exception e) {
      LOGGER.info("Error occurred during closing PipeConnector.", e);
    } finally {
      // Should be after connector.close()
      super.close();
    }
  }

  @Override
  protected synchronized void releaseLastEvent(boolean shouldReport) {
    if (lastEvent != null) {
      // TODO: should decrease reference count here
      lastEvent = null;
    }
  }
}
