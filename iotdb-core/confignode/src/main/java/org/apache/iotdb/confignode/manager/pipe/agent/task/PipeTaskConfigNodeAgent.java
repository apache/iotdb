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

package org.apache.iotdb.confignode.manager.pipe.agent.task;

import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.manager.pipe.execution.PipeConfigNodeSubtask;
import org.apache.iotdb.confignode.manager.pipe.execution.PipeConfigNodeSubtaskExecutor;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * State transition diagram of a pipe task:
 *
 * <p><code>
 * |----------------|                     |---------| --> start pipe --> |---------|                   |---------|
 * | initial status | --> create pipe --> | STOPPED |                    | RUNNING | --> drop pipe --> | DROPPED |
 * |----------------|                     |---------| <-- stop  pipe <-- |---------|                   |---------|
 *                                             |                                                            |
 *                                             | ----------------------> drop pipe -----------------------> |
 * </code>
 *
 * <p>Other transitions are not allowed, will be ignored when received in the pipe task agent.
 */
public class PipeTaskConfigNodeAgent extends PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskConfigNodeAgent.class);

  public PipeTaskConfigNodeAgent() {
    super();
  }

  /** Should only be called by PipeTaskInfo. */
  public PipeMetaKeeper getPipeMetaKeeper() {
    return pipeMetaKeeper;
  }

  public boolean createPipe(PipeMeta newPipeMeta) {
    acquireWriteLock();
    try {
      final String pipeName = newPipeMeta.getStaticMeta().getPipeName();
      final long creationTime = newPipeMeta.getStaticMeta().getCreationTime();
      final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
      if (existedPipeMeta != null) {
        if (!checkBeforeCreatePipe(existedPipeMeta, pipeName, creationTime)) {
          return false;
        }

        // Drop the pipe if
        // 1. The pipe with the same name but with different creation time has been created before
        // 2. The pipe with the same name and the same creation time has been dropped before, but
        // the
        //  pipe task meta has not been cleaned up
        dropPipe(pipeName, existedPipeMeta.getStaticMeta().getCreationTime());
      }
      PipeConfigNodeSubtaskExecutor.getInstance()
          .register(
              new PipeConfigNodeSubtask(
                  newPipeMeta.getStaticMeta().getPipeName(),
                  newPipeMeta.getStaticMeta().getCreationTime(),
                  newPipeMeta.getStaticMeta().getExtractorParameters().getAttribute(),
                  newPipeMeta.getStaticMeta().getConnectorParameters().getAttribute()));

      // No matter the pipe status is RUNNING or STOPPED, we always set the status of pipe meta
      // to STOPPED when it is created. The STOPPED status should always be the initial status
      // of a pipe, which makes the status transition logic simpler.
      final AtomicReference<PipeStatus> pipeStatus = newPipeMeta.getRuntimeMeta().getStatus();
      final boolean needToStartPipe = pipeStatus.get() == PipeStatus.RUNNING;
      pipeStatus.set(PipeStatus.STOPPED);

      pipeMetaKeeper.addPipeMeta(pipeName, newPipeMeta);

      // If the pipe status from config node is RUNNING, we will start the pipe later.
      return needToStartPipe;
    } catch (Exception e) {
      throw new PipeException(
          String.format(
              "failed to create subtask for schema pipe %s, because: %s",
              newPipeMeta.getStaticMeta().getPipeName(), e.getMessage()),
          e);
    } finally {
      releaseWriteLock();
    }
  }

  public void dropPipe(String pipeName, long creationTime) {
    acquireWriteLock();
    try {
      final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
      if (!checkBeforeDropPipe(existedPipeMeta, pipeName, creationTime)) {
        return;
      }

      existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

      PipeConfigNodeSubtaskExecutor.getInstance().deregister(pipeName);

      // Remove pipe meta from pipe meta keeper
      pipeMetaKeeper.removePipeMeta(pipeName);
    } finally {
      releaseWriteLock();
    }
  }

  public void dropPipe(String pipeName) {
    acquireWriteLock();
    try {
      final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
      if (!checkBeforeDropPipe(existedPipeMeta, pipeName)) {
        return;
      }

      existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.DROPPED);

      PipeConfigNodeSubtaskExecutor.getInstance().deregister(pipeName);

      // Remove pipe meta from pipe meta keeper
      pipeMetaKeeper.removePipeMeta(pipeName);
    } finally {
      releaseWriteLock();
    }
  }

  public void startPipe(String pipeName, long creationTime) {
    acquireWriteLock();
    try {
      final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

      if (!checkBeforeStartPipe(existedPipeMeta, pipeName, creationTime)) {
        return;
      }

      PipeConfigNodeSubtaskExecutor.getInstance().start(pipeName);

      // Set pipe meta status to RUNNING
      existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.RUNNING);
      // Clear exception messages if started successfully
      existedPipeMeta
          .getRuntimeMeta()
          .getConsensusGroupId2TaskMetaMap()
          .values()
          .forEach(PipeTaskMeta::clearExceptionMessages);
    } finally {
      releaseWriteLock();
    }
  }

  public void stopPipe(String pipeName, long creationTime) {
    acquireWriteLock();
    try {
      final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);

      if (!checkBeforeStopPipe(existedPipeMeta, pipeName, creationTime)) {
        return;
      }

      PipeConfigNodeSubtaskExecutor.getInstance().stop(pipeName);

      // Set pipe meta status to STOPPED
      existedPipeMeta.getRuntimeMeta().getStatus().set(PipeStatus.STOPPED);
    } finally {
      releaseWriteLock();
    }
  }

  public void handleSinglePipeMetaChange(PipeMeta newPipeMeta) {
    // TODO
  }

  public void handleLeaderChanged() {
    // TODO
  }

  public void handleLeaderReady() {
    // TODO
  }
}
