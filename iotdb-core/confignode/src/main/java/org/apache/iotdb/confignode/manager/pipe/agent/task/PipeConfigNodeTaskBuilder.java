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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.pipe.extractor.ConfigRegionListeningFilter;

import java.util.HashMap;
import java.util.Map;

public class PipeConfigNodeTaskBuilder {

  private final PipeMeta pipeMeta;

  public PipeConfigNodeTaskBuilder(PipeMeta pipeMeta) {
    this.pipeMeta = pipeMeta;
  }

  public Map<Integer, PipeTask> build() throws IllegalPathException {
    final PipeStaticMeta pipeStaticMeta = pipeMeta.getStaticMeta();
    final PipeRuntimeMeta pipeRuntimeMeta = pipeMeta.getRuntimeMeta();

    final Map<Integer, PipeTask> consensusGroupIdToPipeTaskMap = new HashMap<>();
    for (Map.Entry<Integer, PipeTaskMeta> consensusGroupIdToPipeTaskMeta :
        pipeRuntimeMeta.getConsensusGroupId2TaskMetaMap().entrySet()) {
      final int consensusGroupId = consensusGroupIdToPipeTaskMeta.getKey();

      if (consensusGroupId == Integer.MIN_VALUE
          && consensusGroupIdToPipeTaskMeta.getValue().getLeaderNodeId()
              == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()
          && !ConfigRegionListeningFilter.parseListeningPlanTypeSet(
                  pipeStaticMeta.getExtractorParameters())
              .isEmpty()) {
        consensusGroupIdToPipeTaskMap.put(
            consensusGroupId,
            new PipeConfigNodeTask(
                new PipeConfigNodeTaskStage(
                    pipeStaticMeta.getPipeName(),
                    pipeStaticMeta.getCreationTime(),
                    pipeStaticMeta.getExtractorParameters().getAttribute(),
                    pipeStaticMeta.getProcessorParameters().getAttribute(),
                    pipeStaticMeta.getConnectorParameters().getAttribute(),
                    consensusGroupIdToPipeTaskMeta.getValue())));
      }
    }
    return consensusGroupIdToPipeTaskMap;
  }
}
