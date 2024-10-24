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

package org.apache.iotdb.db.pipe.agent.task.builder;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.task.PipeTask;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.extractor.schemaregion.SchemaRegionListeningFilter;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipeDataNodeBuilder {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private final PipeMeta pipeMeta;

  public PipeDataNodeBuilder(PipeMeta pipeMeta) {
    this.pipeMeta = pipeMeta;
  }

  public Map<Integer, PipeTask> build() throws IllegalPathException {
    final PipeStaticMeta pipeStaticMeta = pipeMeta.getStaticMeta();
    final PipeRuntimeMeta pipeRuntimeMeta = pipeMeta.getRuntimeMeta();

    final List<DataRegionId> dataRegionIds = StorageEngine.getInstance().getAllDataRegionIds();
    final List<SchemaRegionId> schemaRegionIds = SchemaEngine.getInstance().getAllSchemaRegionIds();

    final Map<Integer, PipeTask> consensusGroupIdToPipeTaskMap = new HashMap<>();
    for (Map.Entry<Integer, PipeTaskMeta> consensusGroupIdToPipeTaskMeta :
        pipeRuntimeMeta.getConsensusGroupId2TaskMetaMap().entrySet()) {
      final int consensusGroupId = consensusGroupIdToPipeTaskMeta.getKey();
      final PipeTaskMeta pipeTaskMeta = consensusGroupIdToPipeTaskMeta.getValue();

      if (pipeTaskMeta.getLeaderNodeId() == CONFIG.getDataNodeId()) {
        final PipeParameters extractorParameters = pipeStaticMeta.getExtractorParameters();
        final DataRegionId dataRegionId = new DataRegionId(consensusGroupId);
        final boolean needConstructDataRegionTask =
            dataRegionIds.contains(dataRegionId)
                && DataRegionListeningFilter.shouldDataRegionBeListened(
                    extractorParameters, dataRegionId);
        final boolean needConstructSchemaRegionTask =
            schemaRegionIds.contains(new SchemaRegionId(consensusGroupId))
                && !SchemaRegionListeningFilter.parseListeningPlanTypeSet(extractorParameters)
                    .isEmpty();

        // Advance the extractor parameters parsing logic to avoid creating un-relevant pipeTasks
        if (needConstructDataRegionTask || needConstructSchemaRegionTask) {
          consensusGroupIdToPipeTaskMap.put(
              consensusGroupId,
              new PipeDataNodeTaskBuilder(pipeStaticMeta, consensusGroupId, pipeTaskMeta).build());
        }
      }
    }
    return consensusGroupIdToPipeTaskMap;
  }
}
