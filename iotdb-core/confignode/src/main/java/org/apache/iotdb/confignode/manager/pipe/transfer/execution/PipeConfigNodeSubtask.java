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

package org.apache.iotdb.confignode.manager.pipe.transfer.execution;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeException;
import org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.task.subtask.PipeAbstractConnectorSubtask;
import org.apache.iotdb.confignode.manager.pipe.transfer.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.transfer.extractor.IoTDBConfigRegionExtractor;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.iotdb.db.protocol.client.ConfigNodeInfo.CONFIG_REGION_ID;

public class PipeConfigNodeSubtask extends PipeAbstractConnectorSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigNodeSubtask.class);

  private final PipeTaskMeta pipeTaskMeta;

  // Pipe plugins for this subtask
  private PipeExtractor extractor;
  // TODO: currently unused
  @SuppressWarnings("unused")
  private PipeProcessor processor;

  public PipeConfigNodeSubtask(
      String pipeName,
      long creationTime,
      Map<String, String> extractorAttributes,
      Map<String, String> processorAttributes,
      Map<String, String> connectorAttributes,
      PipeTaskMeta pipeTaskMeta)
      throws Exception {
    // We initialize outputPipeConnector by initConnector()
    super(pipeName, creationTime, null);
    this.pipeTaskMeta = pipeTaskMeta;

    initExtractor(extractorAttributes);
    initProcessor(processorAttributes);
    initConnector(connectorAttributes);
  }

  private void initExtractor(Map<String, String> extractorAttributes) throws Exception {
    final PipeParameters extractorParameters = new PipeParameters(extractorAttributes);

    // 1. Construct extractor
    extractor = PipeConfigNodeAgent.plugin().reflectExtractor(extractorParameters);

    // 2. Validate extractor parameters
    extractor.validate(new PipeParameterValidator(extractorParameters));

    // 3. Customize extractor
    final PipeTaskRuntimeConfiguration runtimeConfiguration =
        new PipeTaskRuntimeConfiguration(
            new PipeTaskExtractorRuntimeEnvironment(
                taskID, creationTime, CONFIG_REGION_ID.getId(), pipeTaskMeta));
    extractor.customize(extractorParameters, runtimeConfiguration);
  }

  private void initProcessor(Map<String, String> processorAttributes) {
    final PipeParameters processorParameters = new PipeParameters(processorAttributes);

    final PipeTaskRuntimeConfiguration runtimeConfiguration =
        new PipeTaskRuntimeConfiguration(
            new PipeTaskRuntimeEnvironment(taskID, creationTime, CONFIG_REGION_ID.getId()));

    processor =
        PipeConfigNodeAgent.plugin()
            .getConfiguredProcessor(
                processorParameters.getStringOrDefault(
                    PipeProcessorConstant.PROCESSOR_KEY,
                    BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName()),
                processorParameters,
                runtimeConfiguration);
  }

  private void initConnector(Map<String, String> connectorAttributes) throws Exception {
    final PipeParameters connectorParameters = new PipeParameters(connectorAttributes);

    // 1. Construct connector
    outputPipeConnector = PipeConfigNodeAgent.plugin().reflectConnector(connectorParameters);

    // 2. Validate connector parameters
    outputPipeConnector.validate(new PipeParameterValidator(connectorParameters));

    // 3. Customize connector
    final PipeTaskRuntimeConfiguration runtimeConfiguration =
        new PipeTaskRuntimeConfiguration(
            new PipeTaskRuntimeEnvironment(taskID, creationTime, CONFIG_REGION_ID.getId()));
    outputPipeConnector.customize(connectorParameters, runtimeConfiguration);

    // 4. Handshake
    outputPipeConnector.handshake();
  }

  public void start() {
    try {
      extractor.start();
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  /**
   * Try to consume an {@link Event} by the {@link IoTDBConfigRegionExtractor}.
   *
   * @return {@code true} if the {@link Event} is consumed successfully, {@code false} if no more
   *     {@link Event} can be consumed
   * @throws Exception if any error occurs when consuming the {@link Event}
   */
  @Override
  protected boolean executeOnce() throws Exception {
    if (isClosed.get()) {
      return false;
    }

    final Event event = lastEvent != null ? lastEvent : extractor.supply();
    // Record the last event for retry when exception occurs
    setLastEvent(event);

    try {
      if (event == null) {
        return false;
      }

      outputPipeConnector.transfer(event);

      releaseLastEvent(true);
    } catch (PipeException e) {
      if (!isClosed.get()) {
        throw e;
      } else {
        LOGGER.info(
            "{} in pipe transfer, ignored because pipe is dropped.",
            e.getClass().getSimpleName(),
            e);
        releaseLastEvent(false);
      }
    } catch (Exception e) {
      if (!isClosed.get()) {
        throw new PipeException(
            String.format(
                "Exception in pipe transfer, subtask: %s, last event: %s", taskID, lastEvent),
            e);
      } else {
        LOGGER.info("Exception in pipe transfer, ignored because pipe is dropped.", e);
        releaseLastEvent(false);
      }
    }

    return true;
  }

  // synchronized for close() and releaseLastEvent(). make sure that the lastEvent
  // will not be updated after pipeProcessor.close() to avoid resource leak
  // because of the lastEvent is not released.
  @Override
  public void close() {
    isClosed.set(true);

    try {
      extractor.close();
    } catch (Exception e) {
      LOGGER.info("Error occurred during closing PipeExtractor.", e);
    }

    try {
      processor.close();
    } catch (Exception e) {
      LOGGER.info("Error occurred during closing PipeProcessor.", e);
    }

    try {
      outputPipeConnector.close();
    } catch (Exception e) {
      LOGGER.info("Error occurred during closing PipeConnector.", e);
    } finally {
      // Should be after connector.close()
      super.close();
    }
  }

  //////////////////////////// Error report ////////////////////////////

  @Override
  protected String getRootCause(Throwable throwable) {
    return throwable != null ? throwable.getMessage() : null;
  }

  @Override
  protected void report(EnrichedEvent event, PipeRuntimeException exception) {
    PipeConfigNodeAgent.runtime().report(event, exception);
  }
}
