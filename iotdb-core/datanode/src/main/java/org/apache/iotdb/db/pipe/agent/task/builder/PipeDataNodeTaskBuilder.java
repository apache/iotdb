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

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeType;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.db.pipe.agent.task.PipeDataNodeTask;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.agent.task.stage.PipeTaskProcessorStage;
import org.apache.iotdb.db.pipe.agent.task.stage.PipeTaskSinkStage;
import org.apache.iotdb.db.pipe.agent.task.stage.PipeTaskSourceStage;
import org.apache.iotdb.db.pipe.source.dataregion.DataRegionListeningFilter;
import org.apache.iotdb.db.subscription.task.stage.SubscriptionTaskSinkStage;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_HYBRID_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_TABLET_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY;

public class PipeDataNodeTaskBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataNodeTaskBuilder.class);

  private final PipeStaticMeta pipeStaticMeta;
  private final int regionId;
  private final PipeTaskMeta pipeTaskMeta;

  private static final PipeProcessorSubtaskExecutor PROCESSOR_EXECUTOR =
      PipeSubtaskExecutorManager.getInstance().getProcessorExecutor();

  protected final Map<String, String> systemParameters = new HashMap<>();

  public PipeDataNodeTaskBuilder(
      final PipeStaticMeta pipeStaticMeta, final int regionId, final PipeTaskMeta pipeTaskMeta) {
    this.pipeStaticMeta = pipeStaticMeta;
    this.regionId = regionId;
    this.pipeTaskMeta = pipeTaskMeta;
    generateSystemParameters();
  }

  public PipeDataNodeTask build() {
    // Event flow: source -> processor -> sink

    // Analyzes the PipeParameters to identify potential conflicts.
    final PipeParameters sourceParameters =
        blendUserAndSystemParameters(pipeStaticMeta.getSourceParameters());
    final PipeParameters sinkParameters =
        blendUserAndSystemParameters(pipeStaticMeta.getSinkParameters());
    checkConflict(sourceParameters, sinkParameters);
    injectParameters(sourceParameters, sinkParameters);

    // We first build the source and sink, then build the processor.
    final PipeTaskSourceStage sourceStage =
        new PipeTaskSourceStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            sourceParameters,
            regionId,
            pipeTaskMeta);

    final PipeTaskSinkStage sinkStage;
    final PipeType pipeType = pipeStaticMeta.getPipeType();

    if (PipeType.SUBSCRIPTION.equals(pipeType)) {
      sinkStage =
          new SubscriptionTaskSinkStage(
              pipeStaticMeta.getPipeName(),
              pipeStaticMeta.getCreationTime(),
              sinkParameters,
              regionId,
              PipeSubtaskExecutorManager.getInstance().getSubscriptionExecutor());
    } else { // user pipe or consensus pipe
      sinkStage =
          new PipeTaskSinkStage(
              pipeStaticMeta.getPipeName(),
              pipeStaticMeta.getCreationTime(),
              sinkParameters,
              regionId,
              pipeType.equals(PipeType.USER)
                  ? PipeSubtaskExecutorManager.getInstance().getConnectorExecutorSupplier()
                  : PipeSubtaskExecutorManager.getInstance()::getConsensusExecutor);
    }

    // The processor connects the source and sink.
    final PipeTaskProcessorStage processorStage =
        new PipeTaskProcessorStage(
            pipeStaticMeta.getPipeName(),
            pipeStaticMeta.getCreationTime(),
            blendUserAndSystemParameters(pipeStaticMeta.getProcessorParameters()),
            regionId,
            sourceStage.getEventSupplier(),
            sinkStage.getPipeConnectorPendingQueue(),
            PROCESSOR_EXECUTOR,
            pipeTaskMeta,
            pipeStaticMeta
                .getSinkParameters()
                .getStringOrDefault(
                    Arrays.asList(CONNECTOR_FORMAT_KEY, SINK_FORMAT_KEY),
                    CONNECTOR_FORMAT_HYBRID_VALUE)
                .equals(CONNECTOR_FORMAT_TABLET_VALUE),
            PipeType.SUBSCRIPTION.equals(pipeType));

    return new PipeDataNodeTask(
        pipeStaticMeta.getPipeName(), regionId, sourceStage, processorStage, sinkStage);
  }

  private void generateSystemParameters() {
    if (!(pipeTaskMeta.getProgressIndex() instanceof MinimumProgressIndex)
        || pipeTaskMeta.isNewlyAdded()) {
      systemParameters.put(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.TRUE.toString());
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
      final PipeParameters sourceParameters, final PipeParameters sinkParameters) {
    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair;
    final boolean shouldTerminatePipeOnAllHistoricalEventsConsumed;

    try {
      insertionDeletionListeningOptionPair =
          DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(sourceParameters);
      shouldTerminatePipeOnAllHistoricalEventsConsumed =
          PipeTaskAgent.isSnapshotMode(sourceParameters);
    } catch (final IllegalPathException e) {
      LOGGER.warn(
          "PipeDataNodeTaskBuilder failed to parse 'inclusion' and 'exclusion' parameters: {}",
          e.getMessage(),
          e);
      return;
    }

    if (insertionDeletionListeningOptionPair.right
        || shouldTerminatePipeOnAllHistoricalEventsConsumed) {
      final Boolean isRealtime =
          sinkParameters.getBooleanByKeys(
              PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY,
              PipeSinkConstant.SINK_REALTIME_FIRST_KEY);
      if (isRealtime == null) {
        sinkParameters.addAttribute(PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY, "false");
        if (insertionDeletionListeningOptionPair.right) {
          LOGGER.info(
              "PipeDataNodeTaskBuilder: When 'inclusion' contains 'data.delete', 'realtime-first' is defaulted to 'false' to prevent sync issues after deletion.");
        } else {
          LOGGER.info(
              "PipeDataNodeTaskBuilder: When source uses snapshot model, 'realtime-first' is defaulted to 'false' to prevent premature halt before transfer completion.");
        }
      } else if (isRealtime) {
        if (insertionDeletionListeningOptionPair.right) {
          LOGGER.warn(
              "PipeDataNodeTaskBuilder: When 'inclusion' includes 'data.delete', 'realtime-first' set to 'true' may result in data synchronization issues after deletion.");
        } else {
          LOGGER.warn(
              "PipeDataNodeTaskBuilder: When source uses snapshot model, 'realtime-first' set to 'true' may cause prevent premature halt before transfer completion.");
        }
      }
    }

    final boolean isRealtimeEnabled =
        sourceParameters.getBooleanOrDefault(
            Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
            EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE);

    if (isRealtimeEnabled && !shouldTerminatePipeOnAllHistoricalEventsConsumed) {
      final Boolean enableSendTsFileLimit =
          sinkParameters.getBooleanByKeys(
              PipeSinkConstant.SINK_ENABLE_SEND_TSFILE_LIMIT,
              PipeSinkConstant.CONNECTOR_ENABLE_SEND_TSFILE_LIMIT);

      if (enableSendTsFileLimit == null) {
        sinkParameters.addAttribute(PipeSinkConstant.SINK_ENABLE_SEND_TSFILE_LIMIT, "true");
        LOGGER.info(
            "PipeDataNodeTaskBuilder: When the realtime sync is enabled, we enable rate limiter in sending tsfile by default to reserve disk and network IO for realtime sending.");
      } else if (!enableSendTsFileLimit) {
        LOGGER.warn(
            "PipeDataNodeTaskBuilder: When the realtime sync is enabled, not enabling the rate limiter in sending tsfile may introduce delay for realtime sending.");
      }
    }
  }

  private void injectParameters(
      final PipeParameters sourceParameters, final PipeParameters sinkParameters) {
    final boolean isSourceExternal =
        !BuiltinPipePlugin.BUILTIN_SOURCES.contains(
            sourceParameters
                .getStringOrDefault(
                    Arrays.asList(PipeSourceConstant.EXTRACTOR_KEY, PipeSourceConstant.SOURCE_KEY),
                    BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
                .toLowerCase());

    final String sinkPluginName =
        sinkParameters
            .getStringOrDefault(
                Arrays.asList(PipeSinkConstant.CONNECTOR_KEY, PipeSinkConstant.SINK_KEY),
                BuiltinPipePlugin.IOTDB_THRIFT_SINK.getPipePluginName())
            .toLowerCase();
    final boolean isWriteBackSink =
        BuiltinPipePlugin.WRITE_BACK_CONNECTOR.getPipePluginName().equals(sinkPluginName)
            || BuiltinPipePlugin.WRITE_BACK_SINK.getPipePluginName().equals(sinkPluginName);

    if (isSourceExternal && isWriteBackSink) {
      sinkParameters.addAttribute(
          PipeSinkConstant.CONNECTOR_USE_EVENT_USER_NAME_KEY, Boolean.TRUE.toString());
    }
  }
}
