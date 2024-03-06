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

import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskTemporaryRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMetaKeeper;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipePlugin;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class PipePluginAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginAgent.class);

  protected final PipePluginMetaKeeper pipePluginMetaKeeper;
  private final PipePluginConstructor pipeExtractorConstructor;
  private final PipePluginConstructor pipeProcessorConstructor;
  private final PipePluginConstructor pipeConnectorConstructor;

  protected PipePluginAgent(PipePluginMetaKeeper pipePluginMetaKeeper) {
    this.pipePluginMetaKeeper = pipePluginMetaKeeper;
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
    validateExtractor(extractorAttributes);
    validateProcessor(processorAttributes);
    validateConnector(pipeName, connectorAttributes);
  }

  public void validateExtractor(Map<String, String> extractorAttributes) throws Exception {
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
  }

  public void validateProcessor(Map<String, String> processorAttributes) throws Exception {
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
  }

  public void validateConnector(String pipeName, Map<String, String> connectorAttributes)
      throws Exception {
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

  /**
   * Get the registered subClasses names of the given parent {@link PipePlugin}. This method is
   * usually used to dynamically pick one or more {@link PipePlugin} as the "plugin" of the parent
   * class.
   *
   * @param parentClass the parent {@link PipePlugin} to be checked
   * @return All the pluginNames of the plugin's subClass
   * @throws PipeException if any exception occurs
   */
  public final List<String> getSubPluginNamesWithSpecifiedParent(
      Class<? extends PipePlugin> parentClass) throws PipeException {
    return Arrays.stream(pipePluginMetaKeeper.getAllPipePluginMeta())
        .filter(
            pipePluginMeta -> {
              try {
                return ((PipePlugin)
                            pipePluginMetaKeeper
                                .getPluginClass(pipePluginMeta.getPluginName())
                                .getDeclaredConstructor()
                                .newInstance())
                        .getClass()
                        .getSuperclass()
                    == parentClass;
              } catch (InstantiationException
                  | InvocationTargetException
                  | NoSuchMethodException
                  | IllegalAccessException e) {
                String errorMessage =
                    String.format(
                        "Failed to reflect PipePlugin %s(%s) instance, because %s",
                        pipePluginMeta.getPluginName(), pipePluginMeta.getClassName(), e);
                LOGGER.warn(errorMessage, e);
                throw new PipeException(errorMessage);
              }
            })
        .map(PipePluginMeta::getPluginName)
        .collect(Collectors.toList());
  }

  /**
   * Use the pipeProcessorName, {@link PipeParameters} and {@link PipeProcessorRuntimeConfiguration}
   * to construct a fully prepared {@link PipeProcessor}. Note that the {@link PipeParameters} with
   * the processor's name will be used to validate and customize the {@link PipeProcessor}
   * regardless of its original processor name k-v pair, yet the original {@link PipeParameters} is
   * left unchanged.
   *
   * @param pipeProcessorName the processor's pluginName
   * @param processorParameters the parameters
   * @param runtimeConfigurations the runtimeConfigurations
   * @return the customized {@link PipeProcessor}
   */
  public final PipeProcessor getConfiguredProcessor(
      String pipeProcessorName,
      PipeParameters processorParameters,
      PipeProcessorRuntimeConfiguration runtimeConfigurations) {
    // TODO: Replace schema processor & config processor's construction
    HashMap<String, String> processorKeyMap = new HashMap<>();
    processorKeyMap.put(PipeProcessorConstant.PROCESSOR_KEY, pipeProcessorName);
    PipeParameters replacedParameters =
        processorParameters.addOrReplaceEquivalentAttributesWithClone(
            new PipeParameters(processorKeyMap));
    PipeProcessor processor = reflectProcessor(replacedParameters);
    // Validate and customize should be called to expose exceptions in advance
    // and configure the processor.
    try {
      processor.validate(new PipeParameterValidator(replacedParameters));
      processor.customize(replacedParameters, runtimeConfigurations);
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
    return processor;
  }
}
