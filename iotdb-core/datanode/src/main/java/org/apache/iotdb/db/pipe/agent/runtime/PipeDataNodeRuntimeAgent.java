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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.agent.runtime.PipePeriodicalJobExecutor;
import org.apache.iotdb.commons.pipe.agent.runtime.PipePeriodicalPhantomReferenceCleaner;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningQueue;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.service.ResourcesInformationHolder;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeDataNodeRuntimeAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeRuntimeAgent.class);
  private static final int DATA_NODE_ID = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final PipeSchemaRegionListenerManager regionListenerManager =
      new PipeSchemaRegionListenerManager();

  private final SimpleProgressIndexAssigner simpleProgressIndexAssigner =
      new SimpleProgressIndexAssigner();

  private final PipePeriodicalJobExecutor pipePeriodicalJobExecutor =
      new PipePeriodicalJobExecutor();

  private final PipePeriodicalPhantomReferenceCleaner pipePeriodicalPhantomReferenceCleaner =
      new PipePeriodicalPhantomReferenceCleaner();

  //////////////////////////// System Service Interface ////////////////////////////

  public synchronized void preparePipeResources(
      ResourcesInformationHolder resourcesInformationHolder) throws StartupException {
    // Clean sender (connector) hardlink file dir and snapshot dir
    PipeDataNodeHardlinkOrCopiedFileDirStartupCleaner.clean();

    // Clean receiver file dir
    PipeDataNodeAgent.receiver().cleanPipeReceiverDirs();

    PipeAgentLauncher.launchPipePluginAgent(resourcesInformationHolder);
    simpleProgressIndexAssigner.start();
  }

  @Override
  public synchronized void start() throws StartupException {
    PipeConfig.getInstance().printAllConfigs();
    PipeAgentLauncher.launchPipeTaskAgent();

    registerPeriodicalJob(
        "PipeTaskAgent#restartAllStuckPipes",
        PipeDataNodeAgent.task()::restartAllStuckPipes,
        PipeConfig.getInstance().getPipeStuckRestartIntervalSeconds());
    pipePeriodicalJobExecutor.start();

    if (PipeConfig.getInstance().getPipeEventReferenceTrackingEnabled()) {
      pipePeriodicalPhantomReferenceCleaner.start();
    }

    isShutdown.set(false);
  }

  @Override
  public synchronized void stop() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);

    pipePeriodicalJobExecutor.stop();
    PipeDataNodeAgent.task().dropAllPipeTasks();
  }

  public boolean isShutdown() {
    return isShutdown.get();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PIPE_RUNTIME_DATA_NODE_AGENT;
  }

  ////////////////////// Region Listener //////////////////////

  public Set<SchemaRegionId> listeningSchemaRegionIds() {
    return regionListenerManager.regionIds();
  }

  public SchemaRegionListeningQueue schemaListener(SchemaRegionId schemaRegionId) {
    return regionListenerManager.listener(schemaRegionId);
  }

  public int increaseAndGetSchemaListenerReferenceCount(SchemaRegionId schemaRegionId) {
    return regionListenerManager.increaseAndGetReferenceCount(schemaRegionId);
  }

  public int decreaseAndGetSchemaListenerReferenceCount(SchemaRegionId schemaRegionId) {
    return regionListenerManager.decreaseAndGetReferenceCount(schemaRegionId);
  }

  public void notifySchemaLeaderReady(SchemaRegionId schemaRegionId) {
    regionListenerManager.notifyLeaderReady(schemaRegionId);
  }

  public void notifySchemaLeaderUnavailable(SchemaRegionId schemaRegionId) {
    regionListenerManager.notifyLeaderUnavailable(schemaRegionId);
  }

  public boolean isSchemaLeaderReady(SchemaRegionId schemaRegionId) {
    return regionListenerManager.isLeaderReady(schemaRegionId);
  }

  ////////////////////// SimpleConsensus ProgressIndex Assigner //////////////////////

  public void assignSimpleProgressIndexIfNeeded(InsertNode insertNode) {
    simpleProgressIndexAssigner.assignIfNeeded(insertNode);
  }

  ////////////////////// PipeConsensus ProgressIndex Assigner //////////////////////

  public ProgressIndex assignProgressIndexForPipeConsensus() {
    return new RecoverProgressIndex(
        DATA_NODE_ID, simpleProgressIndexAssigner.getSimpleProgressIndex());
  }

  ////////////////////// Load ProgressIndex Assigner //////////////////////

  public void assignProgressIndexForTsFileLoad(TsFileResource tsFileResource) {
    // override the progress index of the tsfile resource, not to update the progress index
    tsFileResource.setProgressIndex(getNextProgressIndexForTsFileLoad());
  }

  public RecoverProgressIndex getNextProgressIndexForTsFileLoad() {
    return new RecoverProgressIndex(
        DATA_NODE_ID, simpleProgressIndexAssigner.getSimpleProgressIndex());
  }

  ////////////////////// Recover ProgressIndex Assigner //////////////////////

  public void assignProgressIndexForTsFileRecovery(TsFileResource tsFileResource) {
    tsFileResource.updateProgressIndex(
        new RecoverProgressIndex(
            DATA_NODE_ID, simpleProgressIndexAssigner.getSimpleProgressIndex()));
  }

  ////////////////////// Provided for Subscription Agent //////////////////////

  public int getRebootTimes() {
    return simpleProgressIndexAssigner.getRebootTimes();
  }

  //////////////////////////// Runtime Exception Handlers ////////////////////////////

  public void report(EnrichedEvent event, PipeRuntimeException pipeRuntimeException) {
    if (event.getPipeTaskMeta() != null) {
      report(event.getPipeTaskMeta(), pipeRuntimeException);
    } else {
      LOGGER.warn("Attempt to report pipe exception to a null PipeTaskMeta.", pipeRuntimeException);
    }
  }

  public void report(PipeTaskMeta pipeTaskMeta, PipeRuntimeException pipeRuntimeException) {
    LOGGER.warn(
        "Report PipeRuntimeException to local PipeTaskMeta({}), exception message: {}",
        pipeTaskMeta,
        pipeRuntimeException.getMessage(),
        pipeRuntimeException);

    pipeTaskMeta.trackExceptionMessage(pipeRuntimeException);

    // Quick stop all pipes locally if critical exception occurs,
    // no need to wait for the next heartbeat cycle.
    if (pipeRuntimeException instanceof PipeRuntimeCriticalException) {
      PipeDataNodeAgent.task().stopAllPipesWithCriticalException();
    }
  }

  /////////////////////////// Periodical Job Executor ///////////////////////////

  public void registerPeriodicalJob(String id, Runnable periodicalJob, long intervalInSeconds) {
    pipePeriodicalJobExecutor.register(id, periodicalJob, intervalInSeconds);
  }

  @TestOnly
  public void startPeriodicalJobExecutor() {
    pipePeriodicalJobExecutor.start();
  }

  @TestOnly
  public void stopPeriodicalJobExecutor() {
    pipePeriodicalJobExecutor.stop();
  }

  @TestOnly
  public void clearPeriodicalJobExecutor() {
    pipePeriodicalJobExecutor.clear();
  }

  public void registerPhantomReferenceCleanJob(
      String id, Runnable periodicalJob, long intervalInSeconds) {
    pipePeriodicalPhantomReferenceCleaner.register(id, periodicalJob, intervalInSeconds);
  }
}
