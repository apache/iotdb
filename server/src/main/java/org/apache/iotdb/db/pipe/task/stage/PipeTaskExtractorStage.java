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
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.db.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.db.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.extractor.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.task.connection.EventSupplier;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class PipeTaskExtractorStage extends PipeTaskStage {

  private final PipeExtractor pipeExtractor;

  public PipeTaskExtractorStage(
      String pipeName,
      long creationTime,
      PipeParameters extractorParameters,
      TConsensusGroupId dataRegionId,
      PipeTaskMeta pipeTaskMeta) {
    pipeExtractor =
        extractorParameters
                .getStringOrDefault(
                    PipeExtractorConstant.EXTRACTOR_KEY,
                    BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
                .equals(BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName())
            ? new IoTDBDataRegionExtractor()
            : PipeAgent.plugin().reflectExtractor(extractorParameters);

    // validate and customize should be called before createSubtask. this allows extractor exposing
    // exceptions in advance.
    try {
      // 1. validate extractor parameters
      pipeExtractor.validate(new PipeParameterValidator(extractorParameters));

      // 2. customize extractor
      final PipeTaskRuntimeConfiguration runtimeConfiguration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskExtractorRuntimeEnvironment(
                  pipeName, creationTime, dataRegionId.getId(), pipeTaskMeta));
      pipeExtractor.customize(extractorParameters, runtimeConfiguration);
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
      pipeExtractor.start();
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  @Override
  public void stopSubtask() throws PipeException {
    // extractor continuously collects data, so do nothing in stop
  }

  @Override
  public void dropSubtask() throws PipeException {
    try {
      pipeExtractor.close();
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  public EventSupplier getEventSupplier() {
    return pipeExtractor::supply;
  }
}
