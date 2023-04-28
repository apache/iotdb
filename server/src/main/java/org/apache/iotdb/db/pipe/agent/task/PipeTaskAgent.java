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

    final PipeMeta existedPipeMeta = pipeMetaKeeper.getPipeMeta(pipeName);
    if (existedPipeMeta != null) {
      if (existedPipeMeta.getStaticMeta().getCreationTime() == creationTime) {
        switch (existedPipeMeta.getRuntimeMeta().getStatus().get()) {
          case STOPPED:
          case RUNNING:
          case DROPPED:
          default:
        }
        return;
      }

      dropPipe(pipeName, existedPipeMeta.getStaticMeta().getCreationTime());
    }
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
