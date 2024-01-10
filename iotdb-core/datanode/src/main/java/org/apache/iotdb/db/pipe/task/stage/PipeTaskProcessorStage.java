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

package org.apache.iotdb.db.pipe.task.stage;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskProcessorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.task.EventSupplier;
import org.apache.iotdb.commons.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.task.stage.PipeTaskStage;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.execution.executor.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.task.subtask.processor.PipeProcessorSubtask;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskProcessorStage extends PipeTaskStage {

  private final PipeProcessorSubtaskExecutor executor;

  private final PipeProcessorSubtask pipeProcessorSubtask;

  /**
   * @param pipeName pipe name
   * @param creationTime pipe creation time
   * @param pipeProcessorParameters used to create pipe processor
   * @param dataRegionId data region id
   * @param pipeExtractorInputEventSupplier used to input events from pipe extractor
   * @param pipeConnectorOutputPendingQueue used to output events to pipe connector
   * @throws PipeException if failed to validate or customize
   */
  public PipeTaskProcessorStage(
      String pipeName,
      long creationTime,
      PipeParameters pipeProcessorParameters,
      TConsensusGroupId dataRegionId,
      EventSupplier pipeExtractorInputEventSupplier,
      BoundedBlockingPendingQueue<Event> pipeConnectorOutputPendingQueue,
      PipeProcessorSubtaskExecutor executor) {
    final PipeProcessor pipeProcessor =
        PipeAgent.plugin().dataRegion().reflectProcessor(pipeProcessorParameters);

    // Validate and customize should be called before createSubtask. this allows extractor exposing
    // exceptions in advance.
    try {
      // 1. validate processor parameters
      pipeProcessor.validate(new PipeParameterValidator(pipeProcessorParameters));

      // 2. customize processor
      final PipeProcessorRuntimeConfiguration runtimeConfiguration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskProcessorRuntimeEnvironment(
                  pipeName, creationTime, dataRegionId.getId()));
      pipeProcessor.customize(pipeProcessorParameters, runtimeConfiguration);
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }

    // Should add creation time in taskID, because subtasks are stored in the hashmap
    // PipeProcessorSubtaskWorker.subtasks, and deleted subtasks will be removed by
    // a timed thread. If a pipe is deleted and created again before its subtask is
    // removed, the new subtask will have the same pipeName and dataRegionId as the
    // old one, so we need creationTime to make their hash code different in the map.
    final String taskId = pipeName + "_" + dataRegionId.getId() + "_" + creationTime;
    final PipeEventCollector pipeConnectorOutputEventCollector =
        new PipeEventCollector(pipeConnectorOutputPendingQueue, dataRegionId.getId());
    this.pipeProcessorSubtask =
        new PipeProcessorSubtask(
            taskId,
            creationTime,
            pipeName,
            dataRegionId.getId(),
            pipeExtractorInputEventSupplier,
            pipeProcessor,
            pipeConnectorOutputEventCollector);

    this.executor = executor;
  }

  @Override
  public void createSubtask() throws PipeException {
    executor.register(pipeProcessorSubtask);
  }

  @Override
  public void startSubtask() throws PipeException {
    executor.start(pipeProcessorSubtask.getTaskID());
  }

  @Override
  public void stopSubtask() throws PipeException {
    executor.stop(pipeProcessorSubtask.getTaskID());
  }

  @Override
  public void dropSubtask() throws PipeException {
    executor.deregister(pipeProcessorSubtask.getTaskID());
  }
}
