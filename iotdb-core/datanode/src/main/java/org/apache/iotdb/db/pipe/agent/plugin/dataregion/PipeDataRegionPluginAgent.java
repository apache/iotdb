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

package org.apache.iotdb.db.pipe.agent.plugin.dataregion;

import org.apache.iotdb.commons.pipe.agent.plugin.PipePluginAgent;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeProcessorConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSinkConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSourceConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.DataNodePipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import java.util.Map;

public class PipeDataRegionPluginAgent extends PipePluginAgent {

  public PipeDataRegionPluginAgent(DataNodePipePluginMetaKeeper pipePluginMetaKeeper) {
    super(pipePluginMetaKeeper);
  }

  @Override
  protected PipeSourceConstructor createPipeExtractorConstructor(
      PipePluginMetaKeeper pipePluginMetaKeeper) {
    return new PipeDataRegionSourceConstructor((DataNodePipePluginMetaKeeper) pipePluginMetaKeeper);
  }

  @Override
  protected PipeProcessorConstructor createPipeProcessorConstructor(
      PipePluginMetaKeeper pipePluginMetaKeeper) {
    return new PipeDataRegionProcessorConstructor(
        (DataNodePipePluginMetaKeeper) pipePluginMetaKeeper);
  }

  @Override
  protected PipeSinkConstructor createPipeConnectorConstructor(
      PipePluginMetaKeeper pipePluginMetaKeeper) {
    return new PipeDataRegionSinkConstructor((DataNodePipePluginMetaKeeper) pipePluginMetaKeeper);
  }

  @Override
  public void validate(
      String pipeName,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes)
      throws Exception {
    PipeExtractor temporaryExtractor = validateExtractor(extractorAttributes);
    PipeProcessor temporaryProcessor = validateProcessor(processorAttributes);
    PipeConnector temporaryConnector = validateConnector(pipeName, connectorAttributes);

    // validate visibility
    // TODO: validate visibility for schema region and config region
    Visibility pipeVisibility =
        VisibilityUtils.calculateFromExtractorParameters(new PipeParameters(extractorAttributes));
    Visibility extractorVisibility =
        VisibilityUtils.calculateFromPluginClass(temporaryExtractor.getClass());
    Visibility processorVisibility =
        VisibilityUtils.calculateFromPluginClass(temporaryProcessor.getClass());
    Visibility connectorVisibility =
        VisibilityUtils.calculateFromPluginClass(temporaryConnector.getClass());
    if (!VisibilityUtils.isCompatible(
        pipeVisibility, extractorVisibility, processorVisibility, connectorVisibility)) {
      throw new PipeParameterNotValidException(
          String.format(
              "The visibility of the pipe (%s, %s) is not compatible with the visibility of the extractor (%s, %s, %s), processor (%s, %s, %s), and connector (%s, %s, %s).",
              pipeName,
              pipeVisibility,
              extractorAttributes,
              temporaryExtractor.getClass().getName(),
              extractorVisibility,
              processorAttributes,
              temporaryProcessor.getClass().getName(),
              processorVisibility,
              connectorAttributes,
              temporaryConnector.getClass().getName(),
              connectorVisibility));
    }
  }
}
