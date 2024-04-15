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

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeType;
import org.apache.iotdb.db.pipe.execution.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskConnectorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskExtractorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskProcessorStage;
import org.apache.iotdb.db.subscription.task.stage.SubscriptionTaskConnectorStage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.HashMap;
import java.util.Map;

public class PipeDataNodeTaskBuilder {

  private final PipeStaticMeta pipeStaticMeta;
  private final int regionId;
  private final PipeTaskMeta pipeTaskMeta;

  private static final PipeProcessorSubtaskExecutor PROCESSOR_EXECUTOR;
  private static final Map<PipeType, PipeConnectorSubtaskExecutor> CONNECTOR_EXECUTOR_MAP;

  static {
    PROCESSOR_EXECUTOR = PipeSubtaskExecutorManager.getInstance().getProcessorExecutor();
    CONNECTOR_EXECUTOR_MAP = new HashMap<>();
    CONNECTOR_EXECUTOR_MAP.put(
        PipeType.USER, PipeSubtaskExecutorManager.getInstance().getConnectorExecutor());
    CONNECTOR_EXECUTOR_MAP.put(
        PipeType.SUBSCRIPTION, PipeSubtaskExecutorManager.getInstance().getSubscriptionExecutor());
  }

  protected final Map<String, String> systemParameters = new HashMap<>();

  public PipeDataNodeTaskBuilder(
      PipeStaticMeta pipeStaticMeta, int regionId, PipeTaskMeta pipeTaskMeta) {
    this.pipeStaticMeta = pipeStaticMeta;
    this.regionId = regionId;
    this.pipeTaskMeta = pipeTaskMeta;
    generateSystemParameters();
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

    final PipeTaskConnectorStage connectorStage;
    PipeType pipeType = pipeStaticMeta.getPipeType();
    if (PipeType.SUBSCRIPTION.equals(pipeType)) {
      connectorStage =
          new SubscriptionTaskConnectorStage(
              pipeStaticMeta.getPipeName(),
              pipeStaticMeta.getCreationTime(),
              blendUserAndSystemParameters(pipeStaticMeta.getConnectorParameters()),
              regionId,
              CONNECTOR_EXECUTOR_MAP.get(pipeType));
    } else { // user pipe
      connectorStage =
          new PipeTaskConnectorStage(
              pipeStaticMeta.getPipeName(),
              pipeStaticMeta.getCreationTime(),
              blendUserAndSystemParameters(pipeStaticMeta.getConnectorParameters()),
              regionId,
              CONNECTOR_EXECUTOR_MAP.get(pipeType));
    }

    // The processor connects the extractor and connector.
    final PipeTaskProcessorStage processorStage =
        new PipeTaskProcessorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            blendUserAndSystemParameters(pipeStaticMeta.getProcessorParameters()),
            regionId,
            extractorStage.getEventSupplier(),
            connectorStage.getPipeConnectorPendingQueue(),
            PROCESSOR_EXECUTOR,
            pipeTaskMeta);

    return new PipeDataNodeTask(
        pipeStaticMeta.getPipeName(), regionId, extractorStage, processorStage, connectorStage);
  }

  private void generateSystemParameters() {
    if (!(pipeTaskMeta.getProgressIndex() instanceof MinimumProgressIndex)) {
      systemParameters.put(SystemConstant.RESTART_KEY, Boolean.TRUE.toString());
    }
  }

  private PipeParameters blendUserAndSystemParameters(PipeParameters userParameters) {
    // Deep copy the user parameters to avoid modification of the original parameters.
    // If the original parameters are modified, progress index report will be affected.
    final Map<String, String> blendedParameters = new HashMap<>(userParameters.getAttribute());
    blendedParameters.putAll(systemParameters);
    return new PipeParameters(blendedParameters);
  }
}
