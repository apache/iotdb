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
import org.apache.iotdb.commons.pipe.agent.task.connection.EventSupplier;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.agent.task.stage.PipeTaskStage;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeTaskExtractorStage extends PipeTaskStage {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskExtractorStage.class);

  private final PipeExtractor pipeExtractor;

  public PipeTaskExtractorStage(
      String pipeName,
      long creationTime,
      PipeParameters extractorParameters,
      int regionId,
      PipeTaskMeta pipeTaskMeta) {
    pipeExtractor =
        StorageEngine.getInstance().getAllDataRegionIds().contains(new DataRegionId(regionId))
            ? PipeDataNodeAgent.plugin().dataRegion().reflectExtractor(extractorParameters)
            : PipeDataNodeAgent.plugin().schemaRegion().reflectExtractor(extractorParameters);

    // Validate and customize should be called before createSubtask. this allows extractor exposing
    // exceptions in advance.
    try {
      // 1. Validate extractor parameters
      pipeExtractor.validate(new PipeParameterValidator(extractorParameters));

      // 2. Customize extractor
      final PipeTaskRuntimeConfiguration runtimeConfiguration =
          new PipeTaskRuntimeConfiguration(
              new PipeTaskExtractorRuntimeEnvironment(
                  pipeName, creationTime, regionId, pipeTaskMeta));
      pipeExtractor.customize(extractorParameters, runtimeConfiguration);
    } catch (Exception e) {
      try {
        pipeExtractor.close();
      } catch (Exception closeException) {
        LOGGER.warn(
            "Failed to close extractor after failed to initialize extractor. "
                + "Ignore this exception.",
            closeException);
      }
      throw new PipeException(e.getMessage(), e);
    }
  }

  @Override
  public void createSubtask() throws PipeException {
    // Do nothing
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
    // Extractor continuously extracts data, so do nothing in stop
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
