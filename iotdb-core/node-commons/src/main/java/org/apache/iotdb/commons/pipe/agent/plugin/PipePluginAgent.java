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

package org.apache.iotdb.commons.pipe.agent.plugin;

import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskTemporaryRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMetaKeeper;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class PipePluginAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginAgent.class);

  private final PipePluginConstructor pipeExtractorConstructor;
  private final PipePluginConstructor pipeProcessorConstructor;
  private final PipePluginConstructor pipeConnectorConstructor;

  protected PipePluginAgent(PipePluginMetaKeeper pipePluginMetaKeeper) {
    pipeExtractorConstructor = createPipeExtractorConstructor(pipePluginMetaKeeper);
    pipeProcessorConstructor = createPipeProcessorConstructor(pipePluginMetaKeeper);
    pipeConnectorConstructor = createPipeConnectorConstructor(pipePluginMetaKeeper);
  }

  protected abstract PipePluginConstructor createPipeExtractorConstructor(
      PipePluginMetaKeeper pipePluginMetaKeeper);

  protected abstract PipePluginConstructor createPipeProcessorConstructor(
      PipePluginMetaKeeper pipePluginMetaKeeper);

  protected abstract PipePluginConstructor createPipeConnectorConstructor(
      PipePluginMetaKeeper pipePluginMetaKeeper);

  public final PipeExtractor reflectExtractor(PipeParameters extractorParameters) {
    return (PipeExtractor) pipeExtractorConstructor.reflectPlugin(extractorParameters);
  }

  public final PipeProcessor reflectProcessor(PipeParameters processorParameters) {
    return (PipeProcessor) pipeProcessorConstructor.reflectPlugin(processorParameters);
  }

  public final PipeConnector reflectConnector(PipeParameters connectorParameters) {
    return (PipeConnector) pipeConnectorConstructor.reflectPlugin(connectorParameters);
  }

  public void validate(
      String pipeName,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes)
      throws Exception {
    final PipeParameters extractorParameters = new PipeParameters(extractorAttributes);
    final PipeExtractor temporaryExtractor = reflectExtractor(extractorParameters);
    try {
      temporaryExtractor.validate(new PipeParameterValidator(extractorParameters));
    } finally {
      try {
        temporaryExtractor.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close temporary extractor: {}", e.getMessage(), e);
      }
    }

    final PipeParameters processorParameters = new PipeParameters(processorAttributes);
    final PipeProcessor temporaryProcessor = reflectProcessor(processorParameters);
    try {
      temporaryProcessor.validate(new PipeParameterValidator(processorParameters));
    } finally {
      try {
        temporaryProcessor.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close temporary processor: {}", e.getMessage(), e);
      }
    }

    final PipeParameters connectorParameters = new PipeParameters(connectorAttributes);
    final PipeConnector temporaryConnector = reflectConnector(connectorParameters);
    try {
      temporaryConnector.validate(new PipeParameterValidator(connectorParameters));
      temporaryConnector.customize(
          connectorParameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskTemporaryRuntimeEnvironment(pipeName)));
      temporaryConnector.handshake();
    } finally {
      try {
        temporaryConnector.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close temporary connector: {}", e.getMessage(), e);
      }
    }
  }
}
