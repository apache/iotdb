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

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.core.collector.IoTDBDataRegionCollector;
import org.apache.iotdb.db.pipe.task.queue.EventSupplier;
import org.apache.iotdb.db.pipe.task.queue.ListenableUnblockingPendingQueue;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.PipeParameters;
import org.apache.iotdb.pipe.api.customizer.collector.PipeCollectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import java.util.HashMap;

public class PipeTaskCollectorStage extends PipeTaskStage {

  private final PipeParameters collectorParameters;

  /**
   * TODO: have a better way to control busy/idle status of PipeTaskCollectorStage.
   *
   * <p>Currently, this field is for IoTDBDataRegionCollector only. IoTDBDataRegionCollector uses
   * collectorPendingQueue as an internal data structure to store realtime events.
   *
   * <p>PendingQueue can detect whether the queue is empty or not, and it can notify the
   * PipeTaskProcessorStage to stop processing data when the queue is empty to avoid unnecessary
   * processing, and it also can notify the PipeTaskProcessorStage to start processing data when the
   * queue is not empty.
   */
  private ListenableUnblockingPendingQueue<Event> collectorPendingQueue;

  private final PipeCollector pipeCollector;

  public PipeTaskCollectorStage(String dataRegionId, PipeParameters collectorParameters) {
    // TODO: avoid if-else, use reflection to create collector all the time
    if (collectorParameters
        .getStringOrDefault(
            PipeCollectorConstant.COLLECTOR_KEY,
            BuiltinPipePlugin.DEFAULT_COLLECTOR.getPipePluginName())
        .equals(BuiltinPipePlugin.DEFAULT_COLLECTOR.getPipePluginName())) {
      // we want to pass data region id to collector, so we need to create a new collector
      // parameters and put data region id into it. we can't put data region id into collector
      // parameters directly, because the given collector parameters may be used by other pipe task.
      this.collectorParameters =
          new PipeParameters(new HashMap<>(collectorParameters.getAttribute()));
      // set data region id to collector parameters, so that collector can get data region id inside
      // collector
      this.collectorParameters
          .getAttribute()
          .put(PipeCollectorConstant.DATA_REGION_KEY, dataRegionId);

      collectorPendingQueue = new ListenableUnblockingPendingQueue<>();
      this.pipeCollector = new IoTDBDataRegionCollector(collectorPendingQueue);
    } else {
      this.collectorParameters = collectorParameters;

      this.pipeCollector = PipeAgent.plugin().reflectCollector(collectorParameters);
    }
  }

  @Override
  public void createSubtask() throws PipeException {
    try {
      // 1. validate collector parameters
      pipeCollector.validate(new PipeParameterValidator(collectorParameters));

      // 2. customize collector
      final PipeCollectorRuntimeConfiguration runtimeConfiguration =
          new PipeCollectorRuntimeConfiguration();
      pipeCollector.customize(collectorParameters, runtimeConfiguration);
      // TODO: use runtimeConfiguration to configure collector
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  @Override
  public void startSubtask() throws PipeException {
    try {
      pipeCollector.start();
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  @Override
  public void stopSubtask() throws PipeException {
    // collector continuously collects data, so do nothing in stop
  }

  @Override
  public void dropSubtask() throws PipeException {
    try {
      pipeCollector.close();
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  public EventSupplier getEventSupplier() {
    return pipeCollector::supply;
  }

  public ListenableUnblockingPendingQueue<Event> getCollectorPendingQueue() {
    return collectorPendingQueue;
  }
}
