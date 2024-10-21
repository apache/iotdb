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

package org.apache.iotdb.confignode.manager.pipe.agent.runtime;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.runtime.PipePeriodicalJobExecutor;
import org.apache.iotdb.commons.pipe.agent.runtime.PipePeriodicalPhantomReferenceCleaner;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.extractor.ConfigRegionListeningQueue;
import org.apache.iotdb.confignode.manager.pipe.resource.PipeConfigNodeCopiedFileDirStartupCleaner;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class PipeConfigNodeRuntimeAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigNodeRuntimeAgent.class);

  private final PipeConfigRegionListener regionListener = new PipeConfigRegionListener();

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final PipePeriodicalJobExecutor pipePeriodicalJobExecutor =
      new PipePeriodicalJobExecutor();

  private final PipePeriodicalPhantomReferenceCleaner pipePeriodicalPhantomReferenceCleaner =
      new PipePeriodicalPhantomReferenceCleaner();

  @Override
  public synchronized void start() {
    PipeConfig.getInstance().printAllConfigs();

    // PipeTasks will not be started here and will be started by "HandleLeaderChange"
    // procedure when the consensus layer notify leader ready

    // Clean sender (connector) hardlink snapshot dir
    PipeConfigNodeCopiedFileDirStartupCleaner.clean();

    // Clean receiver file dir
    PipeConfigNodeAgent.receiver().cleanPipeReceiverDir();

    // Start periodical job executor
    pipePeriodicalJobExecutor.start();

    if (PipeConfig.getInstance().getPipeEventReferenceTrackingEnabled()) {
      pipePeriodicalPhantomReferenceCleaner.start();
    }

    isShutdown.set(false);
    LOGGER.info("PipeRuntimeConfigNodeAgent started");
  }

  @Override
  public synchronized void stop() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);

    // Stop periodical job executor
    pipePeriodicalJobExecutor.stop();

    PipeConfigNodeAgent.task().dropAllPipeTasks();

    LOGGER.info("PipeRuntimeConfigNodeAgent stopped");
  }

  public boolean isShutdown() {
    return isShutdown.get();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PIPE_RUNTIME_CONFIG_NODE_AGENT;
  }

  //////////////////////////// Region Listener ////////////////////////////

  public ConfigRegionListeningQueue listener() {
    return regionListener.listener();
  }

  public void increaseListenerReference(final PipeParameters parameters)
      throws IllegalPathException {
    regionListener.increaseReference(parameters);
  }

  public void decreaseListenerReference(final PipeParameters parameters)
      throws IllegalPathException {
    regionListener.decreaseReference(parameters);
  }

  /** Notify the region listener that the leader is ready to allow pipe operations. */
  public void notifyLeaderReady() {
    regionListener.notifyLeaderReady();
  }

  /** Notify the region listener that the leader is unavailable to stop pipe operations. */
  public void notifyLeaderUnavailable() {
    regionListener.notifyLeaderUnavailable();
  }

  /**
   * Check if the leader is ready to allow pipe operations.
   *
   * @return true if the leader is ready to allow pipe operations
   */
  public boolean isLeaderReady() {
    return regionListener.isLeaderReady();
  }

  //////////////////////////// Runtime Exception Handlers ////////////////////////////

  public void report(final EnrichedEvent event, final PipeRuntimeException pipeRuntimeException) {
    if (event.getPipeTaskMeta() != null) {
      report(event.getPipeTaskMeta(), pipeRuntimeException);
    } else {
      LOGGER.warn("Attempt to report pipe exception to a null PipeTaskMeta.", pipeRuntimeException);
    }
  }

  private void report(
      final PipeTaskMeta pipeTaskMeta, final PipeRuntimeException pipeRuntimeException) {
    LOGGER.warn(
        "Report PipeRuntimeException to local PipeTaskMeta({}), exception message: {}",
        pipeTaskMeta,
        pipeRuntimeException.getMessage(),
        pipeRuntimeException);

    pipeTaskMeta.trackExceptionMessage(pipeRuntimeException);

    // Stop all pipes locally if critical exception occurs
    if (pipeRuntimeException instanceof PipeRuntimeCriticalException) {
      PipeConfigNodeAgent.task().stopAllPipesWithCriticalException();
    }
  }

  /////////////////////////// Periodical Job Executor ///////////////////////////

  public void registerPeriodicalJob(String id, Runnable periodicalJob, long intervalInSeconds) {
    pipePeriodicalJobExecutor.register(id, periodicalJob, intervalInSeconds);
  }

  public void registerPhantomReferenceCleanJob(
      String id, Runnable periodicalJob, long intervalInSeconds) {
    pipePeriodicalPhantomReferenceCleaner.register(id, periodicalJob, intervalInSeconds);
  }
}
