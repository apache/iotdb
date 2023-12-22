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

package org.apache.iotdb.confignode.manager.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import java.util.HashMap;
import java.util.Map;

public class PipeConfigNodeTaskBuilder {

  private final PipeMeta pipeMeta;

  public PipeConfigNodeTaskBuilder(PipeMeta pipeMeta) {
    this.pipeMeta = pipeMeta;
  }

  public Map<TConsensusGroupId, PipeTask> build() {
    final PipeStaticMeta pipeStaticMeta = pipeMeta.getStaticMeta();
    final PipeRuntimeMeta pipeRuntimeMeta = pipeMeta.getRuntimeMeta();

    final Map<TConsensusGroupId, PipeTask> consensusGroupIdToPipeTaskMap = new HashMap<>();
    for (Map.Entry<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToPipeTaskMeta :
        pipeRuntimeMeta.getConsensusGroupId2TaskMetaMap().entrySet()) {
      final TConsensusGroupId consensusGroupId = consensusGroupIdToPipeTaskMeta.getKey();

      switch (consensusGroupId.getType()) {
        case ConfigRegion:
          // TODO: getLeaderDataNodeId -> getLeaderNodeId
          if (consensusGroupIdToPipeTaskMeta.getValue().getLeaderDataNodeId()
              == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()) {
            consensusGroupIdToPipeTaskMap.put(
                consensusGroupId,
                new PipeConfigNodeTask(
                    new PipeConfigNodeTaskStage(
                        pipeStaticMeta.getPipeName(),
                        pipeStaticMeta.getCreationTime(),
                        pipeStaticMeta.getExtractorParameters().getAttribute(),
                        pipeStaticMeta.getProcessorParameters().getAttribute(),
                        pipeStaticMeta.getConnectorParameters().getAttribute())));
          }
          break;
        default:
          break;
      }
    }
    return consensusGroupIdToPipeTaskMap;
  }
}
