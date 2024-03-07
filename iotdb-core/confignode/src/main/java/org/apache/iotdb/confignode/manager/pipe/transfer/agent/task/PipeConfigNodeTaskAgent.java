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

package org.apache.iotdb.confignode.manager.pipe.transfer.agent.task;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.transfer.extractor.ConfigRegionListeningFilter;
import org.apache.iotdb.confignode.manager.pipe.transfer.task.PipeConfigNodeTask;
import org.apache.iotdb.confignode.manager.pipe.transfer.task.PipeConfigNodeTaskBuilder;
import org.apache.iotdb.confignode.manager.pipe.transfer.task.PipeConfigNodeTaskStage;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaRespExceptionMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PipeConfigNodeTaskAgent extends PipeTaskAgent {

  @Override
  protected boolean isShutdown() {
    return PipeConfigNodeAgent.runtime().isShutdown();
  }

  @Override
  protected Map<Integer, PipeTask> buildPipeTasks(PipeMeta pipeMetaFromConfigNode) {
    return new PipeConfigNodeTaskBuilder(pipeMetaFromConfigNode).build();
  }

  @Override
  protected void createPipeTask(
      int consensusGroupId, PipeStaticMeta pipeStaticMeta, PipeTaskMeta pipeTaskMeta)
      throws IllegalPathException {
    // Advance the extractor parameters parsing logic to avoid creating un-relevant pipeTasks
    if (consensusGroupId == Integer.MIN_VALUE
        && pipeTaskMeta.getLeaderNodeId()
            == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()
        && !ConfigRegionListeningFilter.parseListeningPlanTypeSet(
                pipeStaticMeta.getExtractorParameters())
            .isEmpty()) {
      final PipeConfigNodeTask pipeTask =
          new PipeConfigNodeTask(
              new PipeConfigNodeTaskStage(
                  pipeStaticMeta.getPipeName(),
                  pipeStaticMeta.getCreationTime(),
                  pipeStaticMeta.getExtractorParameters().getAttribute(),
                  pipeStaticMeta.getProcessorParameters().getAttribute(),
                  pipeStaticMeta.getConnectorParameters().getAttribute(),
                  pipeTaskMeta));
      pipeTask.create();
      pipeTaskManager.addPipeTask(pipeStaticMeta, consensusGroupId, pipeTask);
    }

    pipeMetaKeeper
        .getPipeMeta(pipeStaticMeta.getPipeName())
        .getRuntimeMeta()
        .getConsensusGroupId2TaskMetaMap()
        .put(consensusGroupId, pipeTaskMeta);
  }

  public void stopAllPipesWithCriticalException() {
    super.stopAllPipesWithCriticalException(
        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId());
  }

  public void dropPipeOnConfigTaskAgent(String pipeName) {
    // Operate tasks only after leader gets ready
    if (!PipeConfigNodeAgent.runtime().isLeaderReady()) {
      return;
    }
    TPushPipeMetaRespExceptionMessage message = PipeConfigNodeAgent.task().handleDropPipe(pipeName);
    if (message != null) {
      pipeMetaKeeper
          .getPipeMeta(message.getPipeName())
          .getRuntimeMeta()
          .getNodeId2PipeRuntimeExceptionMap()
          .put(
              ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
              new PipeRuntimeCriticalException(message.getMessage(), message.getTimeStamp()));
    }
  }

  public void handleSinglePipeMetaChangeOnConfigTaskAgent(PipeMeta pipeMeta) {
    // Operate tasks only after leader gets ready
    if (!PipeConfigNodeAgent.runtime().isLeaderReady()) {
      return;
    }
    // The new agent meta has separated status to enable control by diff
    // Yet the taskMetaMap is reused to let configNode pipe report directly to the
    // original meta. No lock is needed since the configNode taskMeta is only
    // altered by one pipe.
    PipeMeta agentMeta =
        new PipeMeta(
            pipeMeta.getStaticMeta(),
            new PipeRuntimeMeta(pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap()));
    agentMeta.getRuntimeMeta().getStatus().set(pipeMeta.getRuntimeMeta().getStatus().get());

    TPushPipeMetaRespExceptionMessage message =
        PipeConfigNodeAgent.task().handleSinglePipeMetaChanges(agentMeta);
    if (message != null) {
      pipeMetaKeeper
          .getPipeMeta(message.getPipeName())
          .getRuntimeMeta()
          .getNodeId2PipeRuntimeExceptionMap()
          .put(
              ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
              new PipeRuntimeCriticalException(message.getMessage(), message.getTimeStamp()));
    }
  }

  public void handlePipeMetaChangesOnConfigTaskAgent() {
    // Operate tasks only after leader get ready
    if (!PipeConfigNodeAgent.runtime().isLeaderReady()) {
      return;
    }
    List<PipeMeta> pipeMetas = new ArrayList<>();
    for (PipeMeta pipeMeta : pipeMetaKeeper.getPipeMetaList()) {
      // The new agent meta has separated status to enable control by diff
      // Yet the taskMetaMap is reused to let configNode pipe report directly to the
      // original meta. No lock is needed since the configNode taskMeta is only
      // altered by one pipe.
      PipeMeta agentMeta =
          new PipeMeta(
              pipeMeta.getStaticMeta(),
              new PipeRuntimeMeta(pipeMeta.getRuntimeMeta().getConsensusGroupId2TaskMetaMap()));
      agentMeta.getRuntimeMeta().getStatus().set(pipeMeta.getRuntimeMeta().getStatus().get());
      pipeMetas.add(agentMeta);
    }
    PipeConfigNodeAgent.task()
        .handlePipeMetaChanges(pipeMetas)
        .forEach(
            message ->
                pipeMetaKeeper
                    .getPipeMeta(message.getPipeName())
                    .getRuntimeMeta()
                    .getNodeId2PipeRuntimeExceptionMap()
                    .put(
                        ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId(),
                        new PipeRuntimeCriticalException(
                            message.getMessage(), message.getTimeStamp())));
  }
}
