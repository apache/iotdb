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

import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeProcessorConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSinkConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSourceConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskTemporaryRuntimeEnvironment;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class PipePluginAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginAgent.class);

  protected final PipePluginMetaKeeper pipePluginMetaKeeper;
  private final PipeSourceConstructor pipeExtractorConstructor;
  private final PipeProcessorConstructor pipeProcessorConstructor;
  private final PipeSinkConstructor pipeSinkConstructor;

  protected PipePluginAgent(final PipePluginMetaKeeper pipePluginMetaKeeper) {
    this.pipePluginMetaKeeper = pipePluginMetaKeeper;
    pipeExtractorConstructor = createPipeSourceConstructor(pipePluginMetaKeeper);
    pipeProcessorConstructor = createPipeProcessorConstructor(pipePluginMetaKeeper);
    pipeSinkConstructor = createPipeSinkConstructor(pipePluginMetaKeeper);
  }

  protected abstract PipeSourceConstructor createPipeSourceConstructor(
      final PipePluginMetaKeeper pipePluginMetaKeeper);

  protected abstract PipeProcessorConstructor createPipeProcessorConstructor(
      final PipePluginMetaKeeper pipePluginMetaKeeper);

  protected abstract PipeSinkConstructor createPipeSinkConstructor(
      final PipePluginMetaKeeper pipePluginMetaKeeper);

  public final PipeExtractor reflectSource(final PipeParameters sourceParameters) {
    return pipeExtractorConstructor.reflectPlugin(sourceParameters);
  }

  public final PipeProcessor reflectProcessor(final PipeParameters processorParameters) {
    return pipeProcessorConstructor.reflectPlugin(processorParameters);
  }

  public final PipeConnector reflectSink(final PipeParameters sinkParameters) {
    return pipeSinkConstructor.reflectPlugin(sinkParameters);
  }

  public void validate(
      final String pipeName,
      final Map<String, String> sourceAttributes,
      final Map<String, String> processorAttributes,
      final Map<String, String> sinkAttributes)
      throws Exception {
    validateSource(sourceAttributes);
    validateProcessor(processorAttributes);
    validateSink(pipeName, sinkAttributes);
  }

  protected PipeExtractor validateSource(final Map<String, String> sourceAttributes)
      throws Exception {
    final PipeParameters sourceParameters = new PipeParameters(sourceAttributes);
    final PipeExtractor temporaryExtractor = reflectSource(sourceParameters);
    try {
      temporaryExtractor.validate(new PipeParameterValidator(sourceParameters));
    } finally {
      try {
        temporaryExtractor.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close temporary source: {}", e.getMessage(), e);
      }
    }
    return temporaryExtractor;
  }

  protected PipeProcessor validateProcessor(Map<String, String> processorAttributes)
      throws Exception {
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
    return temporaryProcessor;
  }

  protected PipeConnector validateSink(String pipeName, Map<String, String> sinkAttributes)
      throws Exception {
    final PipeParameters sinkParameters = new PipeParameters(sinkAttributes);
    final PipeConnector temporarySink = reflectSink(sinkParameters);
    try {
      temporarySink.validate(new PipeParameterValidator(sinkParameters));
      temporarySink.customize(
          sinkParameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskTemporaryRuntimeEnvironment(pipeName)));
      temporarySink.handshake();
    } finally {
      try {
        temporarySink.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close temporary connector: {}", e.getMessage(), e);
      }
    }
    return temporarySink;
  }

  /**
   * Get the registered subClasses names of the given parent {@link PipeProcessor}. This method is
   * usually used to dynamically pick one or more {@link PipeProcessor} as the "plugin" of the
   * parent class.
   *
   * @param parentClass the parent {@link PipeProcessor} to be checked
   * @return All the pluginNames of the plugin's subClass
   * @throws PipeException if any exception occurs
   */
  public final List<String> getSubProcessorNamesWithSpecifiedParent(
      Class<? extends PipeProcessor> parentClass) throws PipeException {
    return StreamSupport.stream(pipePluginMetaKeeper.getAllPipePluginMeta().spliterator(), false)
        .map(pipePluginMeta -> pipePluginMeta.getPluginName().toLowerCase())
        .filter(
            pluginName -> {
              try (PipeProcessor processor =
                  (PipeProcessor) pipeProcessorConstructor.reflectPluginByKey(pluginName)) {
                return processor.getClass().getSuperclass() == parentClass;
              } catch (Exception e) {
                return false;
              }
            })
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
    final HashMap<String, String> processorKeyMap = new HashMap<>();
    if (Objects.nonNull(pipeProcessorName)) {
      processorKeyMap.put(PipeProcessorConstant.PROCESSOR_KEY, pipeProcessorName);
    }
    final PipeParameters replacedParameters =
        processorParameters.addOrReplaceEquivalentAttributesWithClone(
            new PipeParameters(processorKeyMap));
    final PipeProcessor processor = reflectProcessor(replacedParameters);
    // Validate and customize should be called to expose exceptions in advance
    // and configure the processor.
    try {
      processor.validate(new PipeParameterValidator(replacedParameters));
      processor.customize(replacedParameters, runtimeConfigurations);
      return processor;
    } catch (Exception e) {
      try {
        processor.close();
      } catch (Exception closeException) {
        LOGGER.warn(
            "Failed to close processor after failed to initialize processor. "
                + "Ignore this exception.",
            closeException);
      }
      throw new PipeException(e.getMessage(), e);
    }
  }
}
