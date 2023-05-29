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
import org.apache.iotdb.commons.pipe.task.meta.PipeStatus;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.core.event.view.collector.PipeEventCollector;
import org.apache.iotdb.db.pipe.execution.executor.PipeProcessorSubtaskExecutor;
import org.apache.iotdb.db.pipe.execution.executor.PipeSubtaskExecutorManager;
import org.apache.iotdb.db.pipe.task.queue.EventSupplier;
import org.apache.iotdb.db.pipe.task.queue.ListenableBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.queue.ListenableBoundedBlockingPendingQueue;
import org.apache.iotdb.db.pipe.task.subtask.PipeProcessorSubtask;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.processor.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import javax.annotation.Nullable;

public class PipeTaskProcessorStage extends PipeTaskStage {

  protected final PipeProcessorSubtaskExecutor executor =
      PipeSubtaskExecutorManager.getInstance().getProcessorSubtaskExecutor();

  protected final PipeParameters pipeProcessorParameters;
  protected final PipeProcessor pipeProcessor;
  protected final PipeProcessorSubtask pipeProcessorSubtask;

  protected final ListenableBlockingPendingQueue<Event> pipeCollectorInputPendingQueue;
  protected final ListenableBlockingPendingQueue<Event> pipeConnectorOutputPendingQueue;

  /**
   * @param pipeName pipe name
   * @param dataRegionId data region id
   * @param taskMeta pipe task meta
   * @param pipeCollectorInputEventSupplier used to input events from pipe collector
   * @param pipeCollectorInputPendingQueue used to listen whether pipe collector event queue is from
   *     empty to not empty or from not empty to empty, null means no need to listen
   * @param pipeProcessorParameters used to create pipe processor
   * @param pipeConnectorOutputPendingQueue used to output events to pipe connector
   */
  public PipeTaskProcessorStage(
      String pipeName,
      TConsensusGroupId dataRegionId,
      PipeTaskMeta taskMeta,
      EventSupplier pipeCollectorInputEventSupplier,
      @Nullable ListenableBlockingPendingQueue<Event> pipeCollectorInputPendingQueue,
      PipeParameters pipeProcessorParameters,
      ListenableBoundedBlockingPendingQueue<Event> pipeConnectorOutputPendingQueue) {
    this.pipeProcessorParameters = pipeProcessorParameters;

    final String taskId = pipeName + "_" + dataRegionId;
    pipeProcessor = PipeAgent.plugin().reflectProcessor(pipeProcessorParameters);
    final PipeEventCollector pipeConnectorOutputEventCollector =
        new PipeEventCollector(pipeConnectorOutputPendingQueue);

    this.pipeProcessorSubtask =
        new PipeProcessorSubtask(
            taskId,
            taskMeta,
            pipeCollectorInputEventSupplier,
            pipeProcessor,
            pipeConnectorOutputEventCollector);

    final PipeTaskStage pipeTaskStage = this;
    this.pipeCollectorInputPendingQueue =
        pipeCollectorInputPendingQueue != null
            ? pipeCollectorInputPendingQueue
                .registerEmptyToNotEmptyListener(
                    taskId,
                    () -> {
                      // status can be changed by other threads calling pipeTaskStage's methods
                      synchronized (pipeTaskStage) {
                        if (status == PipeStatus.RUNNING) {
                          executor.start(pipeProcessorSubtask.getTaskID());
                        }
                      }
                    })
                .registerNotEmptyToEmptyListener(
                    taskId, () -> executor.stop(pipeProcessorSubtask.getTaskID()))
            : null;
    this.pipeConnectorOutputPendingQueue =
        pipeConnectorOutputPendingQueue
            .registerNotFullToFullListener(
                taskId, () -> executor.stop(pipeProcessorSubtask.getTaskID()))
            .registerFullToNotFullListener(
                taskId,
                () -> {
                  // status can be changed by other threads calling pipeTaskStage's methods
                  synchronized (pipeTaskStage) {
                    // only start when the pipe is running
                    if (status == PipeStatus.RUNNING) {
                      pipeConnectorOutputEventCollector.tryCollectBufferedEvents();
                      executor.start(pipeProcessorSubtask.getTaskID());
                    }
                  }
                });
  }

  @Override
  public void createSubtask() throws PipeException {
    try {
      // 1. validate processor parameters
      pipeProcessor.validate(new PipeParameterValidator(pipeProcessorParameters));

      // 2. customize processor
      final PipeProcessorRuntimeConfiguration runtimeConfiguration =
          new PipeProcessorRuntimeConfiguration();
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
    final String taskId = pipeProcessorSubtask.getTaskID();

    if (pipeCollectorInputPendingQueue != null) {
      pipeCollectorInputPendingQueue.removeEmptyToNotEmptyListener(taskId);
      pipeCollectorInputPendingQueue.removeNotEmptyToEmptyListener(taskId);
    }

    pipeConnectorOutputPendingQueue.removeNotFullToFullListener(taskId);
    pipeConnectorOutputPendingQueue.removeFullToNotFullListener(taskId);

    executor.deregister(taskId);
  }
}
