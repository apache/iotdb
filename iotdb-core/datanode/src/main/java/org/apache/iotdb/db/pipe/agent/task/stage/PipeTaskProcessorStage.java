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

package org.apache.iotdb.db.pipe.agent.task.stage;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.stage.PipeTaskStage;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class PipeTaskProcessorStage extends PipeTaskStage {

  private final PipeProcessorSubtaskExecutor executor;
  private final ReentrantLock multiProcessorLock = new ReentrantLock();
  private final Set<PipeProcessorSubtask> pipeProcessorSubtasks = new HashSet<>();

  /**
   * @param pipeName pipe name
   * @param creationTime pipe creation time
   * @param pipeProcessorParameters used to create {@link PipeProcessor}
   * @param regionId {@link DataRegion} id
   * @param pipeExtractorInputEventSupplier used to input {@link Event}s from {@link PipeExtractor}
   * @param pipeConnectorOutputPendingQueue used to output {@link Event}s to {@link PipeConnector}
   * @throws PipeException if failed to {@link PipeProcessor#validate(PipeParameterValidator)} or
   *     {@link PipeProcessor#customize(PipeParameters, PipeProcessorRuntimeConfiguration)}}
   */
  public PipeTaskProcessorStage(
      final String pipeName,
      final long creationTime,
      final PipeParameters pipeProcessorParameters,
      final int regionId,
      final EventSupplier pipeExtractorInputEventSupplier,
      final UnboundedBlockingPendingQueue<Event> pipeConnectorOutputPendingQueue,
      final PipeProcessorSubtaskExecutor executor,
      final PipeTaskMeta pipeTaskMeta,
      final boolean forceTabletFormat,
      final boolean skipParsing) {
    final PipeProcessorRuntimeConfiguration runtimeConfiguration =
        new PipeTaskRuntimeConfiguration(
            new PipeTaskProcessorRuntimeEnvironment(
                pipeName, creationTime, regionId, pipeTaskMeta));
    final PipeProcessor pipeProcessor =
        StorageEngine.getInstance().getAllDataRegionIds().contains(new DataRegionId(regionId))
            ? PipeDataNodeAgent.plugin()
                .dataRegion()
                .getConfiguredProcessor(
                    pipeProcessorParameters.getStringOrDefault(
                        PipeProcessorConstant.PROCESSOR_KEY,
                        BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName()),
                    pipeProcessorParameters,
                    runtimeConfiguration)
            : PipeDataNodeAgent.plugin()
                .schemaRegion()
                .getConfiguredProcessor(
                    pipeProcessorParameters.getStringOrDefault(
                        PipeProcessorConstant.PROCESSOR_KEY,
                        BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName()),
                    pipeProcessorParameters,
                    runtimeConfiguration);

    // Should add creation time in taskID, because subtasks are stored in the hashmap
    // PipeProcessorSubtaskWorker.subtasks, and deleted subtasks will be removed by
    // a timed thread. If a pipe is deleted and created again before its subtask is
    // removed, the new subtask will have the same pipeName and regionId as the
    // old one, so we need creationTime to make their hash code different in the map.
    final String taskId = pipeName + "_" + regionId + "_" + creationTime;
    final boolean isUsedForConsensusPipe = pipeName.contains(PipeStaticMeta.CONSENSUS_PIPE_PREFIX);

    // We allow users to specify subtask num that is larger than thread num
    // to enable the control of cpu time amongst different pipes
    // i.e. 5 threads, pipeA ->  7 tasks, pipeB -> 3 tasks, then the pipeA
    // runs on approximately 3.5 cpus, pipeB 1.5 cpus
    final int taskNum =
        pipeProcessorParameters.getIntOrDefault(
            PipeProcessorConstant.PROCESSOR_IOTDB_PARALLEL_TASKS_KEY,
            PipeProcessorConstant.PROCESSOR_IOTDB_PARALLEL_TASKS_DEFAULT_VALUE);

    for (int i = 0; i < taskNum; ++i) {
      final PipeEventCollector pipeConnectorOutputEventCollector =
          new PipeEventCollector(
              pipeConnectorOutputPendingQueue,
              creationTime,
              regionId,
              forceTabletFormat,
              skipParsing,
              isUsedForConsensusPipe);
      this.pipeProcessorSubtasks.add(
          new PipeProcessorSubtask(
              taskId,
              pipeName,
              creationTime,
              regionId,
              getExtractorSupplier(pipeExtractorInputEventSupplier),
              pipeProcessor,
              pipeConnectorOutputEventCollector));
    }

    this.executor = executor;
  }

  private EventSupplier getExtractorSupplier(final EventSupplier extractorSupplier) {
    return () -> {
      if (pipeProcessorSubtasks.size() > 1) {
        multiProcessorLock.lock();
      }
      try {
        return extractorSupplier.supply();
      } finally {
        if (pipeProcessorSubtasks.size() > 1) {
          multiProcessorLock.unlock();
        }
      }
    };
  }

  @Override
  public void createSubtask() throws PipeException {
    pipeProcessorSubtasks.forEach(executor::register);
  }

  @Override
  public void startSubtask() throws PipeException {
    pipeProcessorSubtasks.forEach(subtask -> executor.start(subtask.getTaskID()));
  }

  @Override
  public void stopSubtask() throws PipeException {
    pipeProcessorSubtasks.forEach(subtask -> executor.stop(subtask.getTaskID()));
  }

  @Override
  public void dropSubtask() throws PipeException {
    pipeProcessorSubtasks.forEach(subtask -> executor.deregister(subtask.getTaskID()));
  }
}
