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

package org.apache.iotdb.db.pipe.extractor.mqtt;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskExtractorRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.db.protocol.mqtt.BrokerAuthenticator;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.interception.InterceptHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTTExtractor is an external Extractor that uses the MQTT protocol to receive data. It starts an
 * MQTT broker and listens for incoming messages, which are then processed and passed to the pending
 * queue.
 */
@TreeModel
@TableModel
public class MQTTExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MQTTExtractor.class);

  protected String pipeName;
  protected long creationTime;
  protected PipeTaskMeta pipeTaskMeta;
  protected final UnboundedBlockingPendingQueue<EnrichedEvent> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

  protected IConfig config;
  protected List<InterceptHandler> handlers;
  protected IAuthenticator authenticator;
  private final Server server = new Server();

  protected final AtomicBoolean isClosed = new AtomicBoolean(false);

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    if (!validator
        .getParameters()
        .getBooleanOrDefault(
            Arrays.asList(
                PipeExtractorConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_KEY,
                PipeExtractorConstant.EXTERNAL_SOURCE_SINGLE_INSTANCE_PER_NODE_KEY),
            PipeExtractorConstant.EXTERNAL_EXTRACTOR_SINGLE_INSTANCE_PER_NODE_DEFAULT_VALUE)) {
      throw new PipeParameterNotValidException("single mode should be true in MQTT extractor");
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskExtractorRuntimeEnvironment environment =
        (PipeTaskExtractorRuntimeEnvironment) configuration.getRuntimeEnvironment();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    pipeTaskMeta = environment.getPipeTaskMeta();
    config = createBrokerConfig(parameters);
    handlers = new ArrayList<>(1);
    handlers.add(new MQTTPublishHandler(parameters, environment, pendingQueue));
    authenticator = new BrokerAuthenticator();
  }

  @Override
  public void start() throws Exception {
    try {
      server.startServer(config, handlers, null, authenticator, null);
    } catch (IOException e) {
      throw new RuntimeException("Exception while starting server", e);
    }

    LOGGER.info(
        "Start MQTT Extractor successfully,listening on ip {} port {}",
        config.getProperty(BrokerConstants.HOST_PROPERTY_NAME),
        config.getProperty(BrokerConstants.PORT_PROPERTY_NAME));

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOGGER.info("Stopping IoTDB MQTT Extractor...");
                  shutdown();
                  LOGGER.info("MQTT Extractor stopped.");
                }));
  }

  @Override
  public Event supply() throws Exception {
    if (isClosed.get()) {
      return null;
    }
    EnrichedEvent event = pendingQueue.directPoll();
    return event;
  }

  @Override
  public void close() throws Exception {
    if (!isClosed.get()) {
      shutdown();
      isClosed.set(true);
    }
  }

  private IConfig createBrokerConfig(final PipeParameters pipeParameters) {
    final Properties properties = new Properties();
    properties.setProperty(
        BrokerConstants.HOST_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeExtractorConstant.MQTT_BROKER_HOST_KEY,
            PipeExtractorConstant.MQTT_BROKER_HOST_DEFAULT_VALUE));
    properties.setProperty(
        BrokerConstants.PORT_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeExtractorConstant.MQTT_BROKER_PORT_KEY,
            PipeExtractorConstant.MQTT_BROKER_PORT_DEFAULT_VALUE));
    properties.setProperty(
        BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE,
        pipeParameters.getStringOrDefault(
            PipeExtractorConstant.MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_KEY,
            PipeExtractorConstant.MQTT_BROKER_INTERCEPTOR_THREAD_POOL_SIZE_DEFAULT_VALUE));
    properties.setProperty(
        BrokerConstants.DATA_PATH_PROPERTY_NAME,
        pipeParameters.getStringOrDefault(
            PipeExtractorConstant.MQTT_DATA_PATH_PROPERTY_NAME_KEY,
            PipeExtractorConstant.MQTT_DATA_PATH_PROPERTY_NAME_DEFAULT_VALUE));
    properties.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
    properties.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
    properties.setProperty(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "true");
    properties.setProperty(
        BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
        String.valueOf(
            pipeParameters.getIntOrDefault(
                PipeExtractorConstant.MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_KEY,
                Integer.parseInt(
                    PipeExtractorConstant.MQTT_NETTY_MAX_BYTES_PROPERTY_NAME_DEFAULT_VALUE))));
    return new MemoryConfig(properties);
  }

  public void shutdown() {
    server.stopServer();
  }
}
