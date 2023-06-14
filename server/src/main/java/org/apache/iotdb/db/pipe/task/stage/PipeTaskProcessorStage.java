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
import org.apache.iotdb.commons.pipe.task.meta.PipeStaticMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.db.pipe.execution.executor.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.processor.PipeDoNothingProcessor;
import org.apache.iotdb.db.pipe.task.connection.BoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.connection.EventSupplier;
import org.apache.iotdb.db.pipe.task.connection.PipeEventCollector;
import org.apache.iotdb.db.pipe.task.subtask.PipeProcessorSubtask;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskProcessorStage extends PipeTaskStage {

  private final PipeProcessorSubtaskExecutor executor =
      PipeSubtaskExecutorManager.getInstance().getProcessorSubtaskExecutor();

  private final PipeProcessorSubtask pipeProcessorSubtask;

  /**
   * @param pipeStaticMeta static meta
   * @param regionId data region id
   * @param pipeCollectorInputEventSupplier used to input events from pipe collector
   * @param pipeConnectorOutputPendingQueue used to output events to pipe connector
   */
  public PipeTaskProcessorStage(
      PipeStaticMeta pipeStaticMeta,
      TConsensusGroupId regionId,
      EventSupplier pipeCollectorInputEventSupplier,
      BoundedBlockingPendingQueue<Event> pipeConnectorOutputPendingQueue) {
    final PipeParameters pipeProcessorParameters = pipeStaticMeta.getProcessorParameters();
    final PipeProcessor pipeProcessor =
        pipeProcessorParameters
                .getStringOrDefault(
                    PipeProcessorConstant.PROCESSOR_KEY,
                    BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName())
                .equals(BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName())
            ? new PipeDoNothingProcessor()
            : PipeAgent.plugin().reflectProcessor(pipeProcessorParameters);

    // validate and customize should be called before createSubtask. this allows collector exposing
    // exceptions in advance.
    try {
      // 1. validate processor parameters
      pipeProcessor.validate(new PipeParameterValidator(pipeProcessorParameters));

      // 2. customize processor
      final PipeProcessorRuntimeConfiguration runtimeConfiguration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskRuntimeEnvironment(pipeStaticMeta, regionId));
      pipeProcessor.customize(pipeProcessorParameters, runtimeConfiguration);
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }

    final String taskId = pipeStaticMeta.getPipeName() + "_" + regionId;
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
