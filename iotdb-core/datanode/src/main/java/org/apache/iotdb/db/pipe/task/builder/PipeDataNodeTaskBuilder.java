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

package org.apache.iotdb.db.pipe.task.builder;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.execution.executor.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.executor.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskConnectorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskExtractorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskProcessorStage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.HashMap;
import java.util.Map;

public abstract class PipeDataNodeTaskBuilder {

  private final PipeStaticMeta pipeStaticMeta;
  private final TConsensusGroupId regionId;
  private final PipeTaskMeta pipeTaskMeta;

  protected final PipeProcessorSubtaskExecutor processorExecutor;
  protected final PipeConnectorSubtaskExecutor connectorExecutor;

  protected final Map<String, String> systemParameters;

  protected PipeDataNodeTaskBuilder(
      PipeStaticMeta pipeStaticMeta,
      TConsensusGroupId regionId,
      PipeTaskMeta pipeTaskMeta,
      PipeProcessorSubtaskExecutor processorExecutor,
      PipeConnectorSubtaskExecutor connectorExecutor) {
    this.pipeStaticMeta = pipeStaticMeta;
    this.regionId = regionId;
    this.pipeTaskMeta = pipeTaskMeta;
    this.processorExecutor = processorExecutor;
    this.connectorExecutor = connectorExecutor;
    systemParameters = generateSystemParameters();
  }

  public PipeDataNodeTask build() {
    // Event flow: extractor -> processor -> connector

    // We first build the extractor and connector, then build the processor.
    final PipeTaskExtractorStage extractorStage =
        new PipeTaskExtractorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            blendUserAndSystemParameters(pipeStaticMeta.getExtractorParameters()),
            regionId,
            pipeTaskMeta);

    final PipeTaskConnectorStage connectorStage =
        new PipeTaskConnectorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            blendUserAndSystemParameters(pipeStaticMeta.getConnectorParameters()),
            regionId,
            connectorExecutor);

    // The processor connects the extractor and connector.
    final PipeTaskProcessorStage processorStage =
        new PipeTaskProcessorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            blendUserAndSystemParameters(pipeStaticMeta.getProcessorParameters()),
            regionId,
            extractorStage.getEventSupplier(),
            connectorStage.getPipeConnectorPendingQueue(),
            processorExecutor);

    return new PipeDataNodeTask(
        pipeStaticMeta.getPipeName(), regionId, extractorStage, processorStage, connectorStage);
  }

  private Map<String, String> generateSystemParameters() {
    final Map<String, String> systemParameters = new HashMap<>();
    if (!(pipeTaskMeta.getProgressIndex() instanceof MinimumProgressIndex)) {
      systemParameters.put(SystemConstant.RESTART_KEY, Boolean.TRUE.toString());
    }
    return systemParameters;
  }

  private PipeParameters blendUserAndSystemParameters(PipeParameters userParameters) {
    // Deep copy the user parameters to avoid modification of the original parameters.
    // If the original parameters are modified, progress index report will be affected.
    final Map<String, String> blendedParameters = new HashMap<>(userParameters.getAttribute());
    blendedParameters.putAll(systemParameters);
    return new PipeParameters(blendedParameters);
  }
}
