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
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.transfer.extractor.PipeConfigPlanListeningFilter;
import org.apache.iotdb.confignode.manager.pipe.transfer.task.PipeConfigNodeTask;
import org.apache.iotdb.confignode.manager.pipe.transfer.task.PipeConfigNodeTaskBuilder;
import org.apache.iotdb.confignode.manager.pipe.transfer.task.PipeConfigNodeTaskStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PipeTaskConfigNodeAgent extends PipeTaskAgent {
  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskConfigNodeAgent.class);

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
        && !PipeConfigPlanListeningFilter.parseListeningPlanTypeSet(
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
}
