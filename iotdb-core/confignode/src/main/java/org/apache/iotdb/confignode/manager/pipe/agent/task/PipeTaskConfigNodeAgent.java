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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeMetaKeeper;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State transition diagram of a pipe task:
 *
 * <p><code>
 * |----------------|                     |---------| --> stop  pipe --> |---------|                   |---------|
 * | initial status | --> create pipe --> | RUNNING |                    | STOPPED | --> drop pipe --> | DROPPED |
 * |----------------|                     |---------| <-- start pipe <-- |---------|                   |---------|
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

  ///////////////////////// Manage by consensusGroupId /////////////////////////

  @Override
  public void createPipeTask(
      PipeMetaKeeper pipeMetaKeeper,
      TConsensusGroupId consensusGroupId,
      PipeStaticMeta pipeStaticMeta,
      PipeTaskMeta pipeTaskMeta) {
    //    if (pipeTaskManager.getPipeTask(pipeStaticMeta, consensusGroupId) != null) {
    //      LOGGER.warn(
    //          "Pipe task {} has already been created, skip creating it again",
    //          pipeStaticMeta.getPipeName());
    //      return;
    //    }
    //
    //    if (pipeTaskMeta.getLeaderDataNodeId()
    //        == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()) {
    //      final PipeConfigNodeTask configNodeTask =
    //          new PipeConfigNodeTask(
    //              pipeStaticMeta.getPipeName(),
    //              consensusGroupId,
    //              new PipeConfigNodeTaskStage(
    //                  pipeStaticMeta.getPipeName(),
    //                  pipeStaticMeta.getCreationTime(),
    //                  pipeStaticMeta.getExtractorParameters().getAttribute(),
    //                  pipeStaticMeta.getConnectorParameters().getAttribute(),
    //                  consensusGroupId));
    //      configNodeTask.create();
    //      pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, configNodeTask);
    //    }
    //
    //    pipeMetaKeeper
    //        .getPipeMeta(pipeStaticMeta.getPipeName())
    //        .getRuntimeMeta()
    //        .getConsensusGroupId2TaskMetaMap()
    //        .put(consensusGroupId, pipeTaskMeta);
  }

  @Override
  public void dropPipeTask(
      PipeMetaKeeper pipeMetaKeeper,
      TConsensusGroupId regionGroupId,
      PipeStaticMeta pipeStaticMeta) {
    //    if (pipeTaskManager.getPipeTask(pipeStaticMeta, regionGroupId) == null) {
    //      LOGGER.warn(
    //          "Pipe task {} has already been dropped, skip dropping it again",
    //          pipeStaticMeta.getPipeName());
    //      return;
    //    }
    //
    //    pipeMetaKeeper
    //        .getPipeMeta(pipeStaticMeta.getPipeName())
    //        .getRuntimeMeta()
    //        .getConsensusGroupId2TaskMetaMap()
    //        .remove(regionGroupId);
    //    final PipeDataNodeTask pipeTask = pipeTaskManager.removePipeTask(pipeStaticMeta,
    // regionGroupId);
    //    if (pipeTask != null) {
    //      pipeTask.drop();
    //    }
  }

  @Override
  public void startPipeTask(TConsensusGroupId regionGroupId, PipeStaticMeta pipeStaticMeta) {
    //    final PipeDataNodeTask pipeTask = pipeTaskManager.getPipeTask(pipeStaticMeta,
    // regionGroupId);
    //    if (pipeTask != null) {
    //      pipeTask.start();
    //    }
  }

  public void stopPipeTask(TConsensusGroupId regionGroupId, PipeStaticMeta pipeStaticMeta) {
    //    final PipeDataNodeTask pipeTask = pipeTaskManager.getPipeTask(pipeStaticMeta,
    // regionGroupId);
    //    if (pipeTask != null) {
    //      pipeTask.stop();
    //    }
  }

  public void handleSinglePipeMetaChanges(PipeMetaKeeper pipeMetaKeeper, PipeMeta newPipeMeta) {
    // TODO
  }

  public void handleLeaderChanged(PipeMetaKeeper pipeMetaKeeper) {
    //    pipeMetaKeeper
    //        .getPipeMetaList()
    //        .forEach(
    //            pipeMeta ->
    //                pipeTaskManager
    //                    .getPipeTasks(pipeMeta.getStaticMeta())
    //                    .values()
    //                    .forEach(PipeDataNodeTask::stop));
  }

  public void handleLeaderReady(PipeMetaKeeper pipeMetaKeeper) {
    //    pipeMetaKeeper
    //        .getPipeMetaList()
    //        .forEach(
    //            pipeMeta -> {
    //              if (pipeMeta.getRuntimeMeta().getStatus().get() == PipeStatus.RUNNING) {
    //                pipeTaskManager
    //                    .getPipeTasks(pipeMeta.getStaticMeta())
    //                    .values()
    //                    .forEach(PipeDataNodeTask::start);
    //              }
    //            });
  }
}
