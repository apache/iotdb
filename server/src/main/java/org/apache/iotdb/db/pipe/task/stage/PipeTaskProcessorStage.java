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
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.db.pipe.core.event.view.collector.PipeEventCollector;
import org.apache.iotdb.db.pipe.core.processor.PipeDoNothingProcessor;
import org.apache.iotdb.db.pipe.execution.executor.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.task.queue.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.queue.EventSupplier;
import org.apache.iotdb.db.pipe.task.subtask.PipeProcessorSubtask;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.processor.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskProcessorStage extends PipeTaskStage {

  private final PipeProcessorSubtaskExecutor executor =
      PipeSubtaskExecutorManager.getInstance().getProcessorSubtaskExecutor();

  private final PipeRuntimeEnvironment pipeRuntimeEnvironment;
  private final PipeParameters pipeProcessorParameters;
  private final PipeProcessor pipeProcessor;
  private final PipeProcessorSubtask pipeProcessorSubtask;

  /**
   * @param pipeName pipe name
   * @param creationTime pipe creation time
   * @param pipeProcessorParameters used to create pipe processor
   * @param dataRegionId data region id
   * @param pipeCollectorInputEventSupplier used to input events from pipe collector
   * @param pipeConnectorOutputPendingQueue used to output events to pipe connector
   */
  public PipeTaskProcessorStage(
      String pipeName,
      long creationTime,
      PipeParameters pipeProcessorParameters,
      TConsensusGroupId dataRegionId,
      EventSupplier pipeCollectorInputEventSupplier,
      BoundedBlockingPendingQueue<Event> pipeConnectorOutputPendingQueue) {
    this.pipeRuntimeEnvironment =
        new PipeTaskRuntimeEnvironment(pipeName, creationTime, dataRegionId.getId());
    this.pipeProcessorParameters = pipeProcessorParameters;

    final String taskId = pipeName + "_" + dataRegionId;
    pipeProcessor =
        pipeProcessorParameters
                .getStringOrDefault(
                    PipeProcessorConstant.PROCESSOR_KEY,
                    BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName())
                .equals(BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName())
            ? new PipeDoNothingProcessor()
            : PipeAgent.plugin().reflectProcessor(pipeProcessorParameters);
    final PipeEventCollector pipeConnectorOutputEventCollector =
        new PipeEventCollector(pipeConnectorOutputPendingQueue);

    this.pipeProcessorSubtask =
        new PipeProcessorSubtask(
            taskId,
            pipeCollectorInputEventSupplier,
            pipeProcessor,
            pipeConnectorOutputEventCollector);
  }

  @Override
  public void createSubtask() throws PipeException {
    try {
      // 1. validate processor parameters
      pipeProcessor.validate(new PipeParameterValidator(pipeProcessorParameters));

      // 2. customize processor
      final PipeProcessorRuntimeConfiguration runtimeConfiguration =
          new PipeTaskRuntimeConfiguration(pipeRuntimeEnvironment);
      pipeProcessor.customize(pipeProcessorParameters, runtimeConfiguration);
      // TODO: use runtimeConfiguration to configure processor
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }

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
