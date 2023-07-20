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

import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.resource.file.PipeHardlinkFileDirStartupCleaner;
import org.apache.iotdb.db.service.ResourcesInformationHolder;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class PipeRuntimeAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRuntimeAgent.class);
  private static final int DATA_NODE_ID = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

  private static final AtomicBoolean isShutdown = new AtomicBoolean(false);

  private final SimpleConsensusProgressIndexAssigner simpleConsensusProgressIndexAssigner =
      new SimpleConsensusProgressIndexAssigner();

  //////////////////////////// System Service Interface ////////////////////////////

  public synchronized void preparePipeResources(
      ResourcesInformationHolder resourcesInformationHolder) throws StartupException {
    PipeAgent.receiver().cleanPipeReceiverDir();
    PipeHardlinkFileDirStartupCleaner.clean();
    PipeAgentLauncher.launchPipePluginAgent(resourcesInformationHolder);
    simpleConsensusProgressIndexAssigner.start();
  }

  @Override
  public synchronized void start() throws StartupException {
    PipeConfig.getInstance().printAllConfigs();
    PipeAgentLauncher.launchPipeTaskAgent();

    isShutdown.set(false);
  }

  @Override
  public synchronized void stop() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);

    PipeAgent.task().dropAllPipeTasks();
  }

  public boolean isShutdown() {
    return isShutdown.get();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PIPE_RUNTIME_AGENT;
  }

  ////////////////////// SimpleConsensus ProgressIndex Assigner //////////////////////

  public void assignSimpleProgressIndexIfNeeded(TsFileResource tsFileResource) {
    simpleConsensusProgressIndexAssigner.assignIfNeeded(tsFileResource);
  }

  ////////////////////// Recover ProgressIndex Assigner //////////////////////

  public void assignRecoverProgressIndexForTsFileRecovery(TsFileResource tsFileResource) {
    tsFileResource.updateProgressIndex(
        new RecoverProgressIndex(
            DATA_NODE_ID,
            simpleConsensusProgressIndexAssigner.getSimpleProgressIndexForTsFileRecovery()));
  }

  //////////////////////////// Runtime Exception Handlers ////////////////////////////

  public void report(PipeTaskMeta pipeTaskMeta, PipeRuntimeException pipeRuntimeException) {
    LOGGER.warn(
        "Report PipeRuntimeException to local PipeTaskMeta({}), exception message: {}",
        pipeTaskMeta,
        pipeRuntimeException.getMessage(),
        pipeRuntimeException);
    pipeTaskMeta.trackExceptionMessage(pipeRuntimeException);
  }
}
