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

import org.apache.iotdb.commons.i18n.PipeMessages;
import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeProcessorConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSinkConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.constructor.PipeSourceConstructor;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMetaKeeper;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class PipePluginAgent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginAgent.class);

  protected final PipePluginMetaKeeper pipePluginMetaKeeper;
  private final PipeSourceConstructor pipeExtractorConstructor;
  private final PipeProcessorConstructor pipeProcessorConstructor;
  private final PipeSinkConstructor pipeSinkConstructor;

  private static final Set<String> PARAMETER_VALUE_PRESERVATION_KEYS =
      Set.of(
          "path",
          "path.inclusion",
          "path.exclusion",
          "pattern",
          "pattern.inclusion",
          "pattern.exclusion",
          "database-name",
          "table-name",
          "database",
          "table",
          "user",
          "topic",
          "consumer-group",
          "username",
          "password",
          "scp.password",
          "cli-hostname",
          "ip",
          "host",
          "node-urls",
          "ssl.trust-store-path",
          "ssl.trust-store-pwd",
          "ssl.key-store-path",
          "ssl.key-store-pwd",
          "output.database",
          "output.measurements",
          "output.series",
          "output-series",
          "new-db-name",
          "opcda.clsid",
          "opcda.progid",
          "consensus.pipe-name");

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

  public static PipeParameters normalizeSourceParameters(final PipeParameters parameters) {
    return normalizeSourceParameters(parameters, null);
  }

  public static PipeParameters normalizeSourceParametersForUpdate(
      final PipeParameters parameters, final PipeParameters existingParameters) {
    return normalizeSourceParametersForUpdate(parameters, existingParameters, false);
  }

  public static PipeParameters normalizeSourceParametersForUpdate(
      final PipeParameters parameters,
      final PipeParameters existingParameters,
      final boolean replaceAll) {
    return normalizeSourceParameters(parameters, replaceAll ? null : existingParameters);
  }

  private static PipeParameters normalizeSourceParameters(
      final PipeParameters parameters, final PipeParameters existingParameters) {
    return normalizePluginParameters(
        parameters,
        existingParameters,
        Arrays.asList(PipeSourceConstant.EXTRACTOR_KEY, PipeSourceConstant.SOURCE_KEY),
        null,
        BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName(),
        PipeExtractor.class);
  }

  public static PipeParameters normalizeProcessorParameters(final PipeParameters parameters) {
    return normalizeProcessorParameters(parameters, null, null);
  }

  public static PipeParameters normalizeProcessorParametersForUpdate(
      final PipeParameters parameters, final PipeParameters existingParameters) {
    return normalizeProcessorParameters(parameters, existingParameters, null);
  }

  private static PipeParameters normalizeProcessorParameters(
      final PipeParameters parameters,
      final PipeParameters existingParameters,
      final String selectedPluginName) {
    return normalizePluginParameters(
        parameters,
        existingParameters,
        Collections.singletonList(PipeProcessorConstant.PROCESSOR_KEY),
        selectedPluginName,
        BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName(),
        PipeProcessor.class);
  }

  public static PipeParameters normalizeSinkParameters(final PipeParameters parameters) {
    return normalizeSinkParameters(parameters, null);
  }

  public static PipeParameters normalizeSinkParametersForUpdate(
      final PipeParameters parameters, final PipeParameters existingParameters) {
    return normalizeSinkParameters(parameters, existingParameters);
  }

  private static PipeParameters normalizeSinkParameters(
      final PipeParameters parameters, final PipeParameters existingParameters) {
    final PipeParameters defaultParameters =
        existingParameters == null ? parameters : existingParameters;
    return normalizePluginParameters(
        parameters,
        existingParameters,
        Arrays.asList(PipeSinkConstant.CONNECTOR_KEY, PipeSinkConstant.SINK_KEY),
        null,
        PipeSinkConstant.getConnectorOrSinkNameWithDefault(defaultParameters),
        PipeConnector.class);
  }

  private static PipeParameters normalizePluginParameters(
      final PipeParameters parameters,
      final PipeParameters existingParameters,
      final List<String> pluginNameKeys,
      final String selectedPluginName,
      final String defaultPluginName,
      final Class<?> pluginClass) {
    String pluginName = selectedPluginName;
    if (pluginName == null) {
      pluginName = getPluginName(parameters, pluginNameKeys);
    }
    if (pluginName == null && existingParameters != null) {
      pluginName = getPluginName(existingParameters, pluginNameKeys);
    }
    if (pluginName == null) {
      pluginName = defaultPluginName;
    }
    final boolean builtinPlugin = isBuiltinPlugin(pluginName, pluginClass);
    final Map<String, String> normalizedAttributes = new HashMap<>();
    parameters
        .getAttribute()
        .forEach(
            (key, value) -> {
              if (key == null) {
                normalizedAttributes.put(null, value);
                return;
              }

              final String trimmedKey = key.trim();
              final String normalizedKey = trimmedKey.toLowerCase(Locale.ROOT);
              if (isPluginNameKey(trimmedKey, pluginNameKeys)) {
                normalizedAttributes.put(
                    normalizedKey,
                    value == null || !builtinPlugin
                        ? value
                        : value.trim().toLowerCase(Locale.ROOT));
              } else if (builtinPlugin) {
                normalizedAttributes.put(
                    normalizedKey,
                    value == null || shouldPreserveParameterValue(normalizedKey)
                        ? value
                        : value.trim().toLowerCase(Locale.ROOT));
              } else {
                normalizedAttributes.put(key, value);
              }
            });
    return new PipeParameters(normalizedAttributes);
  }

  private static String getPluginName(
      final PipeParameters parameters, final List<String> pluginNameKeys) {
    if (parameters == null) {
      return null;
    }
    for (final String pluginNameKey : pluginNameKeys) {
      for (final Map.Entry<String, String> entry : parameters.getAttribute().entrySet()) {
        if (entry.getKey() != null
            && pluginNameKey.equalsIgnoreCase(entry.getKey().trim())
            && entry.getValue() != null) {
          return entry.getValue();
        }
      }
    }
    return null;
  }

  private static boolean isPluginNameKey(final String key, final List<String> pluginNameKeys) {
    for (final String pluginNameKey : pluginNameKeys) {
      if (pluginNameKey.equalsIgnoreCase(key)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isBuiltinPlugin(final String pluginName, final Class<?> pluginClass) {
    if (pluginName == null) {
      return false;
    }
    final String normalizedPluginName = pluginName.trim().toLowerCase(Locale.ROOT);
    return Arrays.stream(BuiltinPipePlugin.values())
        .anyMatch(
            builtinPipePlugin ->
                pluginClass.isAssignableFrom(builtinPipePlugin.getPipePluginClass())
                    && normalizedPluginName.equals(builtinPipePlugin.getPipePluginName()));
  }

  private static boolean shouldPreserveParameterValue(final String normalizedKey) {
    return PARAMETER_VALUE_PRESERVATION_KEYS.contains(
        PipeParameters.KeyReducer.reduce(normalizedKey).toLowerCase(Locale.ROOT));
  }

  public void validate(
      final String pipeName,
      final Map<String, String> sourceAttributes,
      final Map<String, String> processorAttributes,
      final Map<String, String> sinkAttributes)
      throws Exception {
    validateSource(pipeName, sourceAttributes);
    validateProcessor(processorAttributes);
    validateSink(pipeName, sinkAttributes);
  }

  protected PipeExtractor validateSource(
      final String pipeName, final Map<String, String> sourceAttributes) throws Exception {
    final PipeParameters sourceParameters =
        normalizeSourceParameters(new PipeParameters(sourceAttributes));
    final PipeExtractor temporarySource = reflectSource(sourceParameters);
    try {
      temporarySource.validate(new PipeParameterValidator(sourceParameters));
      temporarySource.customize(
          sourceParameters,
          new PipeTaskRuntimeConfiguration(new PipeTaskTemporaryRuntimeEnvironment(pipeName)));
    } finally {
      try {
        temporarySource.close();
      } catch (final Exception e) {
        LOGGER.warn(PipeMessages.FAILED_TO_CLOSE_TEMPORARY_SOURCE, e.getMessage(), e);
      }
    }
    return temporarySource;
  }

  protected PipeProcessor validateProcessor(final Map<String, String> processorAttributes)
      throws Exception {
    final PipeParameters processorParameters =
        normalizeProcessorParameters(new PipeParameters(processorAttributes));
    final PipeProcessor temporaryProcessor = reflectProcessor(processorParameters);
    try {
      temporaryProcessor.validate(new PipeParameterValidator(processorParameters));
    } finally {
      try {
        temporaryProcessor.close();
      } catch (final Exception e) {
        LOGGER.warn(PipeMessages.FAILED_TO_CLOSE_TEMPORARY_PROCESSOR, e.getMessage(), e);
      }
    }
    return temporaryProcessor;
  }

  protected PipeConnector validateSink(
      final String pipeName, final Map<String, String> sinkAttributes) throws Exception {
    final PipeParameters sinkParameters =
        normalizeSinkParameters(new PipeParameters(sinkAttributes));
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
      } catch (final Exception e) {
        LOGGER.warn(PipeMessages.FAILED_TO_CLOSE_TEMPORARY_CONNECTOR, e.getMessage(), e);
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
      final Class<? extends PipeProcessor> parentClass) throws PipeException {
    return StreamSupport.stream(pipePluginMetaKeeper.getAllPipePluginMeta().spliterator(), false)
        .map(pipePluginMeta -> pipePluginMeta.getPluginName().toLowerCase())
        .filter(
            pluginName -> {
              try (PipeProcessor processor =
                  (PipeProcessor) pipeProcessorConstructor.reflectPluginByKey(pluginName)) {
                return processor.getClass().getSuperclass() == parentClass;
              } catch (final Exception e) {
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
    final PipeParameters normalizedParameters =
        normalizeProcessorParameters(processorParameters, null, pipeProcessorName);
    final HashMap<String, String> processorKeyMap = new HashMap<>();
    if (Objects.nonNull(pipeProcessorName)) {
      processorKeyMap.put(PipeProcessorConstant.PROCESSOR_KEY, pipeProcessorName);
    }
    final PipeParameters mergedParameters =
        normalizedParameters.addOrReplaceEquivalentAttributesWithClone(
            new PipeParameters(processorKeyMap));
    final PipeParameters replacedParameters = normalizeProcessorParameters(mergedParameters);
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
        LOGGER.warn(PipeMessages.FAILED_TO_CLOSE_PROCESSOR_AFTER_INIT, closeException);
      }
      throw new PipeException(e.getMessage(), e);
    }
  }
}
