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

package org.apache.iotdb.confignode.manager.pipe.transfer.agent.runtime;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.PipeConfigNodeAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class PipeRuntimeConfigNodeAgent implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeRuntimeConfigNodeAgent.class);

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  @Override
  public synchronized void start() {
    PipeConfig.getInstance().printAllConfigs();
    // PipeTasks will not be started here and will be started by "HandleLeaderChange"
    // procedure when the consensus layer notify leader ready
    // TODO: clean sender (connector) hardlink snapshot dir
    PipeConfigNodeAgent.receiver().cleanPipeReceiverDir();
    isShutdown.set(false);
  }

  @Override
  public synchronized void stop() {
    if (isShutdown.get()) {
      return;
    }
    isShutdown.set(true);

    PipeConfigNodeAgent.task().dropAllPipeTasks();
  }

  public boolean isShutdown() {
    return isShutdown.get();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PIPE_RUNTIME_CONFIG_NODE_AGENT;
  }

  //////////////////////////// Runtime Exception Handlers ////////////////////////////

  public void report(PipeTaskMeta pipeTaskMeta, PipeRuntimeException pipeRuntimeException) {
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
}
