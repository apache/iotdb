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

package org.apache.iotdb.db.pipe.agent.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeTaskAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskAgent.class);

  private final PipeMetaKeeper pipeMetaKeeper;

  public PipeTaskAgent() {
    pipeMetaKeeper = new PipeMetaKeeper();
  }

  ////////////////////////// Pipe Task Management //////////////////////////

  public void createPipe(PipeMeta pipeMeta) {
    final String pipeName = pipeMeta.getStaticMeta().getPipeName();
    final long creationTime = pipeMeta.getStaticMeta().getCreationTime();

    // check if the pipe has already been created before
    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    if (existedPipeMeta != null) {
      if (existedPipeMeta.getStaticMeta().getCreationTime() == creationTime) {
        switch (existedPipeMeta.getRuntimeMeta().getStatus().get()) {
          case STOPPED:
          case RUNNING:
            LOGGER.info(
                "Pipe {} (creation time = {}) has already been created. Current status = {}. Skip creating.",
                pipeName,
                creationTime,
                existedPipeMeta.getRuntimeMeta().getStatus().get().name());
            return;
          case DROPPED:
            LOGGER.info(
                "Pipe {} (creation time = {}) has already been dropped. Current status = {}. Recreating.",
                pipeName,
                creationTime,
                existedPipeMeta.getRuntimeMeta().getStatus().get().name());
            // break to drop the pipe meta and recreate it
            break;
          default:
            throw new IllegalStateException(
                "Unexpected status: " + existedPipeMeta.getRuntimeMeta().getStatus().get().name());
        }
      }

      // drop the pipe if
      // 1. the pipe with the same name but with different creation time has been created before
      // 2. the pipe with the same name and the same creation time has been dropped before, but the
      //  pipe task meta has not been cleaned up
      dropPipe(pipeName, existedPipeMeta.getStaticMeta().getCreationTime());
    }

    // build pipe task by consensus group
    pipeMeta
        .getRuntimeMeta()
        .getConsensusGroupIdToTaskMetaMap()
        .forEach(
            ((consensusGroupId, pipeTaskMeta) -> {
              createPipeTaskByConsensusGroup(
                  pipeName, creationTime, consensusGroupId, pipeTaskMeta);
            }));
    // add pipe meta to pipe meta keeper
    pipeMetaKeeper.addPipeMeta(pipeMeta.getStaticMeta().getPipeName(), pipeMeta);
  }

  public void createPipeTaskByConsensusGroup(
      String pipeName,
      long creationTime,
      TConsensusGroupId consensusGroupId,
      PipeTaskMeta pipeTaskMeta) {}

  public void dropPipe(String pipeName, long creationTime) {}

  public void dropPipeTaskByConsensusGroup(
      String pipeName, long creationTime, TConsensusGroupId consensusGroupId) {}

  public void startPipe(String pipeName, long creationTime) {}

  public void startPipeTaskByConsensusGroup(
      String pipeName, long creationTime, TConsensusGroupId consensusGroupId) {}

  public void stopPipe(String pipeName, long creationTime) {}

  public void stopPipeTaskByConsensusGroup(
      String pipeName, long creationTime, TConsensusGroupId consensusGroupId) {}
}
