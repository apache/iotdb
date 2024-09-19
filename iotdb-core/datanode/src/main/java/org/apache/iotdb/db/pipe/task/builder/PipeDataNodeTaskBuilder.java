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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.task.meta.PipeType;
import org.apache.iotdb.db.pipe.execution.PipeConnectorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.extractor.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.pipe.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskConnectorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskExtractorStage;
import org.apache.iotdb.db.pipe.task.stage.PipeTaskProcessorStage;
import org.apache.iotdb.db.subscription.task.stage.SubscriptionTaskConnectorStage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_FORMAT_HYBRID_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_FORMAT_TABLET_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_FORMAT_KEY;

public class PipeDataNodeTaskBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeTaskBuilder.class);

  private final PipeStaticMeta pipeStaticMeta;
  private final int regionId;
  private final PipeTaskMeta pipeTaskMeta;

  private static final PipeProcessorSubtaskExecutor PROCESSOR_EXECUTOR;
  private static final Map<PipeType, PipeConnectorSubtaskExecutor> CONNECTOR_EXECUTOR_MAP;

  static {
    PROCESSOR_EXECUTOR = PipeSubtaskExecutorManager.getInstance().getProcessorExecutor();
    CONNECTOR_EXECUTOR_MAP = new EnumMap<>(PipeType.class);
    CONNECTOR_EXECUTOR_MAP.put(
        PipeType.USER, PipeSubtaskExecutorManager.getInstance().getConnectorExecutor());
    CONNECTOR_EXECUTOR_MAP.put(
        PipeType.SUBSCRIPTION, PipeSubtaskExecutorManager.getInstance().getSubscriptionExecutor());
    CONNECTOR_EXECUTOR_MAP.put(
        PipeType.CONSENSUS, PipeSubtaskExecutorManager.getInstance().getConsensusExecutor());
  }

  protected final Map<String, String> systemParameters = new HashMap<>();

  public PipeDataNodeTaskBuilder(
      final PipeStaticMeta pipeStaticMeta, final int regionId, final PipeTaskMeta pipeTaskMeta) {
    this.pipeStaticMeta = pipeStaticMeta;
    this.regionId = regionId;
    this.pipeTaskMeta = pipeTaskMeta;
    generateSystemParameters();
  }

  public PipeDataNodeTask build() {
    // Event flow: extractor -> processor -> connector

    // Analyzes the PipeParameters to identify potential conflicts.
    final PipeParameters extractorParameters =
        blendUserAndSystemParameters(pipeStaticMeta.getExtractorParameters());
    final PipeParameters connectorParameters =
        blendUserAndSystemParameters(pipeStaticMeta.getConnectorParameters());
    checkConflict(extractorParameters, connectorParameters);

    // We first build the extractor and connector, then build the processor.
    final PipeTaskExtractorStage extractorStage =
        new PipeTaskExtractorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            extractorParameters,
            regionId,
            pipeTaskMeta);

    final PipeTaskConnectorStage connectorStage;
    final PipeType pipeType = pipeStaticMeta.getPipeType();

    if (PipeType.SUBSCRIPTION.equals(pipeType)) {
      connectorStage =
          new SubscriptionTaskConnectorStage(
              pipeStaticMeta.getPipeName(),
              pipeStaticMeta.getCreationTime(),
              connectorParameters,
              regionId,
              CONNECTOR_EXECUTOR_MAP.get(pipeType));
    } else { // user pipe or consensus pipe
      connectorStage =
          new PipeTaskConnectorStage(
              pipeStaticMeta.getPipeName(),
              pipeStaticMeta.getCreationTime(),
              connectorParameters,
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
            pipeTaskMeta,
            pipeStaticMeta
                .getConnectorParameters()
                .getStringOrDefault(
                    Arrays.asList(CONNECTOR_FORMAT_KEY, SINK_FORMAT_KEY),
                    CONNECTOR_FORMAT_HYBRID_VALUE)
                .equals(CONNECTOR_FORMAT_TABLET_VALUE));

    return new PipeDataNodeTask(
        pipeStaticMeta.getPipeName(), regionId, extractorStage, processorStage, connectorStage);
  }

  private void generateSystemParameters() {
    if (!(pipeTaskMeta.getProgressIndex() instanceof MinimumProgressIndex)) {
      systemParameters.put(SystemConstant.RESTART_KEY, Boolean.TRUE.toString());
    }
  }

  private PipeParameters blendUserAndSystemParameters(final PipeParameters userParameters) {
    // Deep copy the user parameters to avoid modification of the original parameters.
    // If the original parameters are modified, progress index report will be affected.
    final Map<String, String> blendedParameters = new HashMap<>(userParameters.getAttribute());
    blendedParameters.putAll(systemParameters);
    return new PipeParameters(blendedParameters);
  }

  private void checkConflict(
      final PipeParameters extractorParameters, final PipeParameters connectorParameters) {

    try {
      final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
          DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(extractorParameters);
      if (!insertionDeletionListeningOptionPair.right) {
        return;
      }
    } catch (IllegalPathException e) {
      LOGGER.warn(
          "PipeDataNodeTaskBuilder failed to parse 'inclusion' and 'exclusion' parameters: {}",
          e.getMessage(),
          e);
      return;
    }

    final Boolean isRealtime =
        connectorParameters.getBooleanByKeys(
            PipeConnectorConstant.CONNECTOR_REALTIME_FIRST_KEY,
            PipeConnectorConstant.SINK_REALTIME_FIRST_KEY);
    if (isRealtime == null) {
      connectorParameters.addAttribute(PipeConnectorConstant.CONNECTOR_REALTIME_FIRST_KEY, "false");
      LOGGER.info(
          "PipeDataNodeTaskBuilder: When 'inclusion' contains 'data.delete', 'realtime-first' is defaulted to 'false' to prevent sync issues after deletion.");
      return;
    }

    if (isRealtime) {
      LOGGER.warn(
          "PipeDataNodeTaskBuilder: When 'inclusion' includes 'data.delete', 'realtime-first' set to 'true' may result in data synchronization issues after deletion.");
    }
  }
}
