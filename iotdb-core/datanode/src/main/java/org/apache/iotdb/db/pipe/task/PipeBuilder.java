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

package org.apache.iotdb.db.pipe.task;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.util.HashMap;
import java.util.Map;

public class PipeBuilder {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final PipeMeta pipeMeta;

  public PipeBuilder(PipeMeta pipeMeta) {
    this.pipeMeta = pipeMeta;
  }

  public Map<TConsensusGroupId, PipeTask> build() {
    final PipeStaticMeta pipeStaticMeta = pipeMeta.getStaticMeta();

    final Map<TConsensusGroupId, PipeTask> consensusGroupIdToPipeTaskMap = new HashMap<>();

    final PipeRuntimeMeta pipeRuntimeMeta = pipeMeta.getRuntimeMeta();
    for (Map.Entry<TConsensusGroupId, PipeTaskMeta> consensusGroupIdToPipeTaskMeta :
        pipeRuntimeMeta.getConsensusGroupId2TaskMetaMap().entrySet()) {
      if (consensusGroupIdToPipeTaskMeta.getValue().getLeaderDataNodeId()
          == CONFIG.getDataNodeId()) {
        consensusGroupIdToPipeTaskMap.put(
            consensusGroupIdToPipeTaskMeta.getKey(),
            new PipeTaskBuilder(
                    pipeStaticMeta,
                    consensusGroupIdToPipeTaskMeta.getKey(),
                    consensusGroupIdToPipeTaskMeta.getValue())
                .build());
      }
    }

    return consensusGroupIdToPipeTaskMap;
  }
}
