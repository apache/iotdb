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
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.collector.IoTDBDataRegionCollector;
import org.apache.iotdb.db.pipe.config.constant.PipeCollectorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskCollectorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.db.pipe.task.connection.EventSupplier;
import org.apache.iotdb.pipe.api.PipeCollector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskCollectorStage extends PipeTaskStage {

  private final PipeCollector pipeCollector;

  public PipeTaskCollectorStage(
      PipeStaticMeta pipeStaticMeta, TConsensusGroupId regionId, PipeTaskMeta pipeTaskMeta) {
    final PipeParameters collectorParameters = pipeStaticMeta.getCollectorParameters();

    // TODO: avoid if-else, use reflection to create collector all the time
    this.pipeCollector =
        collectorParameters
                .getStringOrDefault(
                    PipeCollectorConstant.COLLECTOR_KEY,
                    BuiltinPipePlugin.IOTDB_COLLECTOR.getPipePluginName())
                .equals(BuiltinPipePlugin.IOTDB_COLLECTOR.getPipePluginName())
            ? new IoTDBDataRegionCollector()
            : PipeAgent.plugin().reflectCollector(collectorParameters);

    // validate and customize should be called before createSubtask. this allows collector exposing
    // exceptions in advance.
    try {
      // 1. validate collector parameters
      pipeCollector.validate(new PipeParameterValidator(collectorParameters));

      // 2. customize collector
      final PipeTaskRuntimeConfiguration runtimeConfiguration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskCollectorRuntimeEnvironment(pipeStaticMeta, regionId, pipeTaskMeta));
      pipeCollector.customize(collectorParameters, runtimeConfiguration);
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  @Override
  public void createSubtask() throws PipeException {
    // do nothing
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
}
